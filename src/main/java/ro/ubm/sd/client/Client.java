package ro.ubm.sd.client;

import ro.ubm.sd.common.ChecksumUtil;
import ro.ubm.sd.common.ChunkPolicy;
import ro.ubm.sd.common.Protocol;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Interfata in terminal pentru client (mod interactiv sau comanda directa in linia de comanda).
 * Exemplu interactiv:
 *   java -cp target/classes ro.ubm.sd.client.Client --coordinator localhost:7001
 * Exemple directe (Etapa 2):
 *   java -cp target/classes ro.ubm.sd.client.Client put localhost:7002 test.bin
 *   java -cp target/classes ro.ubm.sd.client.Client get localhost:7002 test.bin ./out
 */
public class Client {

    private static final int CONNECT_TIMEOUT_MS = 2000;
    private static final int DATA_TIMEOUT_MS = 60_000;
    private static final int MAX_PARALLEL_CHUNKS = 4;
    private static final String DEFAULT_COORDINATOR = "localhost:7001";

    private final String coordinatorAddress;
    private final String coordinatorHost;
    private final int coordinatorPort;

    public Client(String coordinatorAddress) {
        this.coordinatorAddress = coordinatorAddress;
        String[] parts = parseHostPort(coordinatorAddress);
        this.coordinatorHost = parts[0];
        this.coordinatorPort = Integer.parseInt(parts[1]);
    }

    public void start() {
        System.out.printf("[client] coordonator configurat: %s; comenzi: put, get, ls, info, rm, nodes, freeze <port>, unfreeze <port>, help, exit%n",
                coordinatorAddress);

        if (System.console() == null && !hasStandardInput()) {
            return;
        }

        try (Scanner scanner = new Scanner(System.in)) {
            while (true) {
                System.out.print("> ");
                if (!scanner.hasNextLine()) {
                    break;
                }
                String line = scanner.nextLine().trim();
                if (line.isEmpty()) {
                    continue;
                }

                List<String> tokens = tokenizeLine(line);
                if (tokens.isEmpty()) {
                    continue;
                }
                boolean shouldContinue = executeCommand(tokens.toArray(new String[0]));
                if (!shouldContinue) {
                    return;
                }
            }
        }
    }

    /**
     * Executa o comanda. Returneaza false doar la exit/quit.
     */
    public boolean executeCommand(String[] parts) {
        String command = parts[0].toLowerCase();
        switch (command) {
            case "put":
                handlePutCommand(parts);
                return true;
            case "get":
                handleGetCommand(parts);
                return true;
            case "ls":
                listFiles();
                return true;
            case "info":
            case "stat":
                if (parts.length < 2) {
                    System.out.println("Utilizare: info <nume_sau_id>");
                } else {
                    showFileInfo(parts[1]);
                }
                return true;
            case "rm":
                if (parts.length < 2) {
                    System.out.println("Utilizare: rm <nume_sau_id>");
                } else {
                    deleteFile(parts[1]);
                }
                return true;
            case "nodes":
                queryNodesStatus();
                return true;
            case "freeze":
                if (parts.length < 2) {
                    System.out.println("Utilizare: freeze <port_nod> (ex: freeze 7002)");
                } else {
                    sendTestCommandToNode(parts[1], "FREEZE");
                }
                return true;
            case "unfreeze":
            case "resume":
                if (parts.length < 2) {
                    System.out.println("Utilizare: unfreeze <port_nod> (ex: unfreeze 7002)");
                } else {
                    sendTestCommandToNode(parts[1], "UNFREEZE");
                }
                return true;
            case "help":
                printHelp();
                return true;
            case "exit":
            case "quit":
                System.out.println("[client] Inchidere client.");
                return false;
            default:
                System.out.println("Comanda necunoscuta. Comenzi disponibile: put, get, ls, info, rm, nodes, freeze <port>, unfreeze <port>, help, exit");
                return true;
        }
    }

    private void handlePutCommand(String[] parts) {
        if (parts.length == 2) {
            uploadCoordinated(parts[1]);
        } else if (parts.length == 3) {
            uploadDirect(parts[1], parts[2]);
        } else {
            System.out.println("Utilizare: put <fisier_local>  SAU  put <nod|host:port> <fisier_local>");
        }
    }

    private void handleGetCommand(String[] parts) {
        if (parts.length == 2) {
            downloadCoordinated(parts[1], ".");
        } else if (parts.length == 3) {
            if (isDirectNodeTarget(parts[1], parts[2])) {
                downloadDirect(parts[1], parts[2], ".");
            } else {
                downloadCoordinated(parts[1], parts[2]);
            }
        } else if (parts.length == 4) {
            downloadDirect(parts[1], parts[2], parts[3]);
        } else {
            System.out.println("Utilizare: get <nume_sau_id> [dir_dest]  SAU  get <nod|host:port> <nume_fisier> [dir_dest]");
        }
    }

    private void uploadCoordinated(String localFileArg) {
        Path localPath = Paths.get(localFileArg);
        String fileName = validateLocalUploadFile(localPath);
        if (fileName == null) {
            return;
        }

        try {
            long size = Files.size(localPath);
            long plannedChunkSize = ChunkPolicy.chunkSizeFor(size);
            int plannedChunkCount = ChunkPolicy.chunkCountFor(size);
            ChecksumUtil.FileHashSummary hashSummary =
                    ChecksumUtil.sha256WithChunks(localPath, size, plannedChunkSize, plannedChunkCount);
            String fullSha = hashSummary.getFullChecksum();

            String fileId;
            long chunkSize;
            int chunkCount;
            List<UploadChunkPlan> plan = new ArrayList<>();

            try (Socket coordSock = connectSocket(coordinatorHost, coordinatorPort, DATA_TIMEOUT_MS)) {
                OutputStream coordOut = coordSock.getOutputStream();
                InputStream coordIn = coordSock.getInputStream();
                Protocol.writeHeaderLine(coordOut,
                        String.format("ALLOCATE %s %d %s", fileName, size, fullSha));

                String allocResp = Protocol.readHeaderLine(coordIn);
                if (allocResp == null) {
                    System.out.println("[client] Eroare: coordonatorul a inchis conexiunea la ALLOCATE.");
                    return;
                }
                if (!allocResp.startsWith("ALLOC_PLAN ")) {
                    System.out.println("[client] Alocare refuzata: " + allocResp);
                    return;
                }

                // ALLOC_PLAN <fileId> <chunkSize> <chunkCount>
                String[] ap = allocResp.split("\\s+");
                if (ap.length < 4) {
                    System.out.println("[client] Raspuns ALLOC_PLAN invalid: " + allocResp);
                    return;
                }
                fileId = ap[1];
                chunkSize = Long.parseLong(ap[2]);
                chunkCount = Integer.parseInt(ap[3]);
                if (chunkCount < 1) {
                    System.out.println("[client] Numar invalid de bucati la ALLOC_PLAN: " + chunkCount);
                    return;
                }

                long expectedOffset = 0L;
                for (int i = 0; i < chunkCount; i++) {
                    String chunkLine = Protocol.readHeaderLine(coordIn);
                    if (chunkLine == null) {
                        System.out.println("[client] Eroare: plan de alocare incomplet de la coordonator.");
                        return;
                    }
                    String[] cp = chunkLine.trim().split("\\s+");
                    if (cp.length < 6 || !"CHUNK".equalsIgnoreCase(cp[0])) {
                        System.out.println("[client] Linie CHUNK invalida in ALLOC_PLAN: " + chunkLine);
                        return;
                    }
                    int idx = Integer.parseInt(cp[1]);
                    String chunkId = cp[2];
                    long offset = Long.parseLong(cp[3]);
                    long chunkLen = Long.parseLong(cp[4]);
                    NodeTarget target = parseNodeEndpoint(cp[5]);
                    if (idx != i || offset != expectedOffset || chunkLen < 0) {
                        System.out.println("[client] Plan de bucati nealiniat de la coordonator: " + chunkLine);
                        return;
                    }
                    expectedOffset += chunkLen;
                    plan.add(new UploadChunkPlan(idx, chunkId, offset, chunkLen, target));
                }
                if (expectedOffset != size) {
                    System.out.printf("[client] Eroare: suma dimensiunilor bucatilor (%d) difera de dimensiunea fisierului (%d).%n",
                            expectedOffset, size);
                    return;
                }
            }

            if (chunkSize != plannedChunkSize || chunkCount != plannedChunkCount) {
                hashSummary = ChecksumUtil.sha256WithChunks(localPath, size, chunkSize, chunkCount);
                fullSha = hashSummary.getFullChecksum();
            }
            List<String> chunkHashes = hashSummary.getChunkChecksums();

            // Incarcam bucatile in paralel cu pool limitat; daca oricare esueaza, anulam tot si curatam.
            int workerCount = Math.min(chunkCount, MAX_PARALLEL_CHUNKS);
            ExecutorService pool = Executors.newFixedThreadPool(workerCount);
            AtomicBoolean failed = new AtomicBoolean(false);
            AtomicReference<String> errorMsg = new AtomicReference<>();
            List<UploadedChunk> uploaded = Collections.synchronizedList(new ArrayList<>());

            try {
                List<Future<?>> futures = new ArrayList<>(chunkCount);
                for (int i = 0; i < plan.size(); i++) {
                    final UploadChunkPlan item = plan.get(i);
                    final String chunkSha = chunkHashes.get(i);
                    futures.add(pool.submit(() -> {
                        if (failed.get()) {
                            return;
                        }
                        try {
                            boolean ok = uploadChunkRange(item.target, localPath, item.chunkId,
                                    item.offset, item.size, chunkSha);
                            if (ok) {
                                uploaded.add(new UploadedChunk(item.chunkId, item.target));
                            } else {
                                failed.set(true);
                                errorMsg.compareAndSet(null,
                                        "Transferul bucatii " + item.index + " catre " + item.target.id + " a esuat.");
                            }
                        } catch (Exception e) {
                            failed.set(true);
                            errorMsg.compareAndSet(null,
                                    "Eroare la transferul bucatii " + item.index + " catre "
                                            + item.target.id + ": " + e.getMessage());
                        }
                    }));
                }

                for (Future<?> f : futures) {
                    try {
                        f.get();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        failed.set(true);
                        errorMsg.compareAndSet(null, "Incarcarea a fost intrerupta.");
                    } catch (ExecutionException e) {
                        failed.set(true);
                        Throwable cause = e.getCause() != null ? e.getCause() : e;
                        errorMsg.compareAndSet(null, cause.getMessage());
                    }
                }
            } finally {
                pool.shutdownNow();
            }

            if (failed.get()) {
                cleanupUploadedChunks(uploaded);
                System.out.println("[client] Incarcare anulata (fara COMMIT): "
                        + (errorMsg.get() != null ? errorMsg.get() : "eroare necunoscuta"));
                return;
            }

            String commitResp;
            try (Socket coordSock = connectSocket(coordinatorHost, coordinatorPort, DATA_TIMEOUT_MS)) {
                OutputStream coordOut = coordSock.getOutputStream();
                Protocol.writeHeaderLine(coordOut, String.format("COMMIT %s %s %d %s %d %d",
                        fileId, fileName, size, fullSha, chunkSize, chunkCount));
                for (int i = 0; i < plan.size(); i++) {
                    UploadChunkPlan item = plan.get(i);
                    Protocol.writeHeaderLine(coordOut, String.format("CHUNK %d %s %d %d %s %s",
                            item.index, item.chunkId, item.offset, item.size, chunkHashes.get(i), item.target.id));
                }
                commitResp = Protocol.readHeaderLine(coordSock.getInputStream());
            }

            if (!"OK".equals(commitResp)) {
                cleanupUploadedChunks(uploaded);
                System.out.println("[client] Eroare la COMMIT pe coordonator: " + commitResp);
                return;
            }

            Set<String> usedNodes = new LinkedHashSet<>();
            for (UploadChunkPlan item : plan) {
                usedNodes.add(item.target.id);
            }
            System.out.printf("[client] Incarcat cu succes: %s (id=%s, dimensiune=%d octeti, bucati=%d x %s, sha256=%s, noduri=%s)%n",
                    fileName, fileId, size, chunkCount, ChunkPolicy.formatHumanSize(chunkSize), fullSha, usedNodes);
        } catch (IOException e) {
            System.out.printf("[client] Eroare la incarcarea fisierului '%s': %s%n", localFileArg, e.getMessage());
        }
    }

    private boolean uploadChunkRange(NodeTarget target, Path localPath, String chunkId,
                                     long offset, long size, String chunkSha) throws IOException {
        try (Socket nodeSock = connectSocket(target.host, target.port, DATA_TIMEOUT_MS)) {
            OutputStream out = nodeSock.getOutputStream();
            InputStream in = nodeSock.getInputStream();

            Protocol.writeHeaderLine(out, String.format("PUT %s %d %s", chunkId, size, chunkSha));
            Protocol.sendFileRange(localPath, offset, size, out);

            String resp = Protocol.readHeaderLine(in);
            if (resp == null) {
                System.out.printf("[client] Nodul %s:%d a inchis conexiunea fara raspuns pentru %s.%n",
                        target.host, target.port, chunkId);
                return false;
            }
            if (!resp.startsWith("STORED ")) {
                System.out.printf("[client] Nodul %s a respins bucata %s: %s%n", target.id, chunkId, resp);
                return false;
            }
            String serverSha = resp.substring("STORED ".length()).trim();
            if (!chunkSha.equalsIgnoreCase(serverSha)) {
                System.out.printf("[client] Checksum diferit pentru bucata %s (%s vs %s).%n",
                        chunkId, serverSha, chunkSha);
                return false;
            }
            return true;
        }
    }

    private void cleanupUploadedChunks(List<UploadedChunk> uploaded) {
        for (UploadedChunk chunk : uploaded) {
            try (Socket s = connectSocket(chunk.target.host, chunk.target.port, CONNECT_TIMEOUT_MS)) {
                Protocol.writeHeaderLine(s.getOutputStream(), "REMOVE_REPLICA " + chunk.chunkId);
                Protocol.readHeaderLine(s.getInputStream());
            } catch (IOException ignored) {
            }
        }
    }

    private void uploadDirect(String nodeArg, String localFileArg) {
        Path localPath = Paths.get(localFileArg);
        String fileName = validateLocalUploadFile(localPath);
        if (fileName == null) {
            return;
        }

        NodeTarget target = resolveTarget(nodeArg);
        if (target == null) {
            return;
        }

        try {
            long size = Files.size(localPath);
            String sha256 = ChecksumUtil.sha256(localPath);
            uploadToNode(target.host, target.port, localPath, fileName, size, sha256);
        } catch (IOException e) {
            System.out.printf("[client] Eroare la incarcarea directa a '%s': %s%n", localFileArg, e.getMessage());
        }
    }

    private boolean uploadToNode(String host, int port, Path localPath, String fileName, long size, String sha256)
            throws IOException {
        try (Socket nodeSock = connectSocket(host, port, DATA_TIMEOUT_MS)) {
            OutputStream out = nodeSock.getOutputStream();
            InputStream in = nodeSock.getInputStream();

            Protocol.writeHeaderLine(out, String.format("PUT %s %d %s", fileName, size, sha256));
            Protocol.sendFileBytes(localPath, out);

            String resp = Protocol.readHeaderLine(in);
            if (resp == null) {
                System.out.printf("[client] Nodul %s:%d a inchis conexiunea fara raspuns.%n", host, port);
                return false;
            }
            System.out.println(resp);
            if (!resp.startsWith("STORED ")) {
                return false;
            }
            String serverSha = resp.substring("STORED ".length()).trim();
            if (!sha256.equalsIgnoreCase(serverSha)) {
                System.out.printf("[client] Avertisment: checksum diferit raportat de nod (%s vs %s).%n",
                        serverSha, sha256);
                return false;
            }
            return true;
        }
    }

    private void downloadCoordinated(String nameOrId, String outDirArg) {
        try {
            String fileName;
            long expectedSize;
            String metaSha;
            int chunkCount;
            List<DownloadChunkPlan> chunks = new ArrayList<>();

            try (Socket coordSock = connectSocket(coordinatorHost, coordinatorPort, DATA_TIMEOUT_MS)) {
                OutputStream coordOut = coordSock.getOutputStream();
                InputStream coordIn = coordSock.getInputStream();
                Protocol.writeHeaderLine(coordOut, "LOCATE " + nameOrId);

                String locateResp = Protocol.readHeaderLine(coordIn);
                if (locateResp == null) {
                    System.out.println("[client] Eroare: coordonatorul a inchis conexiunea la LOCATE.");
                    return;
                }
                if ("NOT_FOUND".equals(locateResp) || locateResp.startsWith("ERROR ")) {
                    System.out.println(locateResp);
                    return;
                }
                if (!locateResp.startsWith("LOCATIONS ")) {
                    System.out.println("[client] Raspuns LOCATE invalid: " + locateResp);
                    return;
                }

                // LOCATIONS <uuid> <name> <size> <fullSha256> <chunkSize> <chunkCount>
                String[] lp = locateResp.split("\\s+");
                if (lp.length < 7) {
                    System.out.println("[client] Raspuns LOCATE incomplet: " + locateResp);
                    return;
                }
                fileName = Protocol.sanitizeFileName(lp[2]);
                expectedSize = Long.parseLong(lp[3]);
                metaSha = lp[4].trim().toLowerCase();
                chunkCount = Integer.parseInt(lp[6]);

                for (int i = 0; i < chunkCount; i++) {
                    String chunkLine = Protocol.readHeaderLine(coordIn);
                    if (chunkLine == null) {
                        System.out.println("[client] Eroare: raspuns LOCATIONS incomplet.");
                        return;
                    }
                    String[] cp = chunkLine.trim().split("\\s+");
                    if (cp.length < 7 || !"CHUNK".equalsIgnoreCase(cp[0])) {
                        System.out.println("[client] Linie CHUNK invalida in LOCATIONS: " + chunkLine);
                        return;
                    }
                    int idx = Integer.parseInt(cp[1]);
                    String chunkId = cp[2];
                    long offset = Long.parseLong(cp[3]);
                    long chunkLen = Long.parseLong(cp[4]);
                    String chunkSha = cp[5].trim().toLowerCase();
                    String[] epRaw = cp[6].split(",");
                    List<NodeTarget> targets = new ArrayList<>();
                    for (String ep : epRaw) {
                        if (!ep.trim().isEmpty()) {
                            targets.add(parseNodeEndpoint(ep.trim()));
                        }
                    }
                    chunks.add(new DownloadChunkPlan(idx, chunkId, offset, chunkLen, chunkSha, targets));
                }
            }

            Path outDir = Paths.get(outDirArg);
            Files.createDirectories(outDir);
            Path finalTarget = outDir.resolve(fileName);
            Path tmpTarget = outDir.resolve(fileName + ".part");

            int workerCount = Math.min(Math.max(1, chunkCount), MAX_PARALLEL_CHUNKS);
            ExecutorService pool = Executors.newFixedThreadPool(workerCount);
            AtomicBoolean failed = new AtomicBoolean(false);
            AtomicReference<String> errorMsg = new AtomicReference<>();

            try (FileChannel channel = FileChannel.open(tmpTarget,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                List<Future<?>> futures = new ArrayList<>(chunks.size());
                for (DownloadChunkPlan chunk : chunks) {
                    futures.add(pool.submit(() -> {
                        if (failed.get()) {
                            return;
                        }
                        if (!downloadChunkWithFallback(chunk, channel)) {
                            failed.set(true);
                            errorMsg.compareAndSet(null,
                                    "Nu s-a putut descarca valid bucata " + chunk.index + " (" + chunk.chunkId + ").");
                        }
                    }));
                }

                for (Future<?> f : futures) {
                    try {
                        f.get();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        failed.set(true);
                        errorMsg.compareAndSet(null, "Descarcarea a fost intrerupta.");
                    } catch (ExecutionException e) {
                        failed.set(true);
                        Throwable cause = e.getCause() != null ? e.getCause() : e;
                        errorMsg.compareAndSet(null, cause.getMessage());
                    }
                }
            } finally {
                pool.shutdownNow();
            }

            if (failed.get()) {
                Files.deleteIfExists(tmpTarget);
                System.out.println("[client] Descarcare esuata: "
                        + (errorMsg.get() != null ? errorMsg.get() : "eroare necunoscuta"));
                return;
            }

            long actualSize = Files.size(tmpTarget);
            if (actualSize != expectedSize) {
                Files.deleteIfExists(tmpTarget);
                System.out.printf("[client] Eroare: dimensiune invalida pentru %s (%d vs %d octeti).%n",
                        fileName, actualSize, expectedSize);
                return;
            }

            String actualFullSha = ChecksumUtil.sha256(tmpTarget);
            if (!actualFullSha.equalsIgnoreCase(metaSha)) {
                Files.deleteIfExists(tmpTarget);
                System.out.printf("[client] Eroare: checksum global invalid pentru %s (primit=%s, asteptat=%s).%n",
                        fileName, actualFullSha, metaSha);
                return;
            }

            Protocol.moveAtomically(tmpTarget, finalTarget);
            System.out.printf("descarcat %s, checksum OK (%s, %d bucati)%n", finalTarget, actualFullSha, chunkCount);
        } catch (IllegalArgumentException e) {
            System.out.println("[client] " + e.getMessage());
        } catch (IOException e) {
            System.out.printf("[client] Eroare la interogarea coordonatorului (%s): %s%n",
                    coordinatorAddress, e.getMessage());
        }
    }

    private boolean downloadChunkWithFallback(DownloadChunkPlan chunk, FileChannel channel) {
        for (NodeTarget target : chunk.targets) {
            try (Socket nodeSock = connectSocket(target.host, target.port, DATA_TIMEOUT_MS)) {
                OutputStream out = nodeSock.getOutputStream();
                InputStream in = nodeSock.getInputStream();

                Protocol.writeHeaderLine(out, "GET " + chunk.chunkId);
                String header = Protocol.readHeaderLine(in);
                if (header == null || "NOT_FOUND".equals(header) || header.startsWith("ERROR ")
                        || !header.startsWith("FILE ")) {
                    continue;
                }

                String[] fp = header.split("\\s+");
                if (fp.length < 3) {
                    continue;
                }
                long serverSize = Long.parseLong(fp[1]);
                String serverSha = fp[2].trim().toLowerCase();
                if (serverSize != chunk.size || !serverSha.equalsIgnoreCase(chunk.checksum)) {
                    System.out.printf("[client] Antet invalid de pe %s pentru bucata %d.%n",
                            target.id, chunk.index);
                    continue;
                }

                String actualSha = Protocol.receiveBytesToChannel(in, chunk.size, channel, chunk.offset);
                if (!actualSha.equalsIgnoreCase(chunk.checksum)) {
                    System.out.printf("[client] Checksum invalid pentru bucata %d de pe %s (%s vs %s).%n",
                            chunk.index, target.id, actualSha, chunk.checksum);
                    continue;
                }
                return true;
            } catch (IOException e) {
                System.out.printf("[client] Nodul %s (%s:%d) indisponibil pentru bucata %d: %s%n",
                        target.id, target.host, target.port, chunk.index, e.getMessage());
            }
        }
        return false;
    }

    private void downloadDirect(String nodeArg, String remoteFileName, String outDirArg) {
        NodeTarget target = resolveTarget(nodeArg);
        if (target == null) {
            return;
        }
        String sanitized;
        try {
            sanitized = Protocol.sanitizeFileName(remoteFileName);
        } catch (IllegalArgumentException e) {
            System.out.println("[client] " + e.getMessage());
            return;
        }
        try {
            downloadFromNode(target.host, target.port, sanitized, Paths.get(outDirArg), null);
        } catch (IOException e) {
            System.out.printf("[client] Eroare la descarcarea de pe %s:%d: %s%n",
                    target.host, target.port, e.getMessage());
        }
    }

    private boolean downloadFromNode(String host, int port, String fileName, Path outDir, String expectedMetaSha)
            throws IOException {
        try (Socket nodeSock = connectSocket(host, port, DATA_TIMEOUT_MS)) {
            OutputStream out = nodeSock.getOutputStream();
            InputStream in = nodeSock.getInputStream();

            Protocol.writeHeaderLine(out, "GET " + fileName);
            String header = Protocol.readHeaderLine(in);
            if (header == null) {
                System.out.printf("[client] Nodul %s:%d a inchis conexiunea fara raspuns.%n", host, port);
                return false;
            }
            if ("NOT_FOUND".equals(header) || header.startsWith("ERROR ")) {
                System.out.println(header);
                return false;
            }
            if (!header.startsWith("FILE ")) {
                System.out.println("[client] Raspuns GET invalid: " + header);
                return false;
            }

            // FILE <size> <sha256>
            String[] fp = header.split("\\s+");
            if (fp.length < 3) {
                System.out.println("[client] Antet FILE invalid: " + header);
                return false;
            }
            long size = Long.parseLong(fp[1]);
            String serverSha = fp[2].trim().toLowerCase();

            Files.createDirectories(outDir);
            Path finalTarget = outDir.resolve(fileName);
            Path tmpTarget = outDir.resolve(fileName + ".part");
            try {
                String actualSha = Protocol.receiveBytes(in, size, tmpTarget);
                if (!actualSha.equalsIgnoreCase(serverSha)
                        || (expectedMetaSha != null && !actualSha.equalsIgnoreCase(expectedMetaSha))) {
                    Files.deleteIfExists(tmpTarget);
                    System.out.printf("[client] Eroare: checksum invalid pentru %s (primit=%s, asteptat=%s).%n",
                            fileName, actualSha, expectedMetaSha != null ? expectedMetaSha : serverSha);
                    return false;
                }
                Protocol.moveAtomically(tmpTarget, finalTarget);
                System.out.printf("descarcat %s, checksum OK (%s)%n", finalTarget, actualSha);
                return true;
            } catch (IOException e) {
                Files.deleteIfExists(tmpTarget);
                throw e;
            }
        }
    }

    private void listFiles() {
        try (Socket socket = connectSocket(coordinatorHost, coordinatorPort, CONNECT_TIMEOUT_MS)) {
            Protocol.writeHeaderLine(socket.getOutputStream(), "LIST");
            InputStream in = socket.getInputStream();
            String header = Protocol.readHeaderLine(in);
            if (header == null || !header.startsWith("FILE_LIST ")) {
                System.out.println("[client] Raspuns invalid la LIST: " + header);
                return;
            }

            int count = Integer.parseInt(header.split("\\s+")[1]);
            if (count == 0) {
                System.out.println("(niciun fisier stocat)");
                return;
            }

            System.out.println("==========================================================================================================================");
            System.out.printf(" %-36s | %-20s | %-12s | %-14s | %-14s | %s%n",
                    "UUID", "NUME", "DIMENSIUNE", "CHUNKS", "SHA-256", "NODURI");
            System.out.println("--------------------------------------------------------------------------------------------------------------------------");
            for (int i = 0; i < count; i++) {
                String line = Protocol.readHeaderLine(in);
                if (line == null) {
                    break;
                }
                String[] fp = line.trim().split("\\s+");
                if (fp.length >= 7) {
                    String shaShort = fp[3].length() > 12 ? fp[3].substring(0, 12) + ".." : fp[3];
                    long chunkSz = Long.parseLong(fp[5]);
                    String chunkSummary = fp[4] + " (" + ChunkPolicy.formatHumanSize(chunkSz) + ")";
                    System.out.printf(" %-36s | %-20s | %10s B | %-14s | %-14s | %s%n",
                            fp[0], fp[1], fp[2], chunkSummary, shaShort, fp[6]);
                } else if (fp.length >= 5) {
                    String shaShort = fp[3].length() > 12 ? fp[3].substring(0, 12) + ".." : fp[3];
                    System.out.printf(" %-36s | %-20s | %10s B | %-14s | %-14s | %s%n",
                            fp[0], fp[1], fp[2], "1 (-)", shaShort, fp[4]);
                }
            }
            System.out.println("==========================================================================================================================");
        } catch (IOException e) {
            System.out.printf("[client] Eroare la listarea fisierelor (%s): %s%n",
                    coordinatorAddress, e.getMessage());
        }
    }

    private void showFileInfo(String nameOrId) {
        try (Socket socket = connectSocket(coordinatorHost, coordinatorPort, CONNECT_TIMEOUT_MS)) {
            Protocol.writeHeaderLine(socket.getOutputStream(), "LOCATE " + nameOrId);
            InputStream in = socket.getInputStream();
            String header = Protocol.readHeaderLine(in);
            if (header == null) {
                System.out.println("[client] Coordonatorul a inchis conexiunea la info.");
                return;
            }
            if ("NOT_FOUND".equals(header) || header.startsWith("ERROR ")) {
                System.out.println(header);
                return;
            }
            if (!header.startsWith("LOCATIONS ")) {
                System.out.println("[client] Raspuns invalid la info: " + header);
                return;
            }

            // LOCATIONS <uuid> <name> <size> <fullSha256> <chunkSize> <chunkCount>
            String[] lp = header.split("\\s+");
            if (lp.length < 7) {
                System.out.println("[client] Raspuns LOCATE incomplet: " + header);
                return;
            }
            long fileSize = Long.parseLong(lp[3]);
            long chunkSize = Long.parseLong(lp[5]);
            int chunkCount = Integer.parseInt(lp[6]);

            System.out.println("==============================================================================================================");
            System.out.printf(" Fisier: %s (UUID: %s)%n", lp[2], lp[1]);
            System.out.printf(" Dimensiune: %d octeti (%s) | Bucati: %d (chunk max: %s) | SHA-256: %s%n",
                    fileSize, ChunkPolicy.formatHumanSize(fileSize), chunkCount,
                    ChunkPolicy.formatHumanSize(chunkSize), lp[4]);
            System.out.println("--------------------------------------------------------------------------------------------------------------");
            System.out.printf(" %-3s | %-44s | %-12s | %-12s | %-14s | %s%n",
                    "#", "CHUNK ID", "OFFSET", "DIMENSIUNE", "SHA-256", "NODURI");
            System.out.println("--------------------------------------------------------------------------------------------------------------");
            for (int i = 0; i < chunkCount; i++) {
                String line = Protocol.readHeaderLine(in);
                if (line == null) {
                    break;
                }
                String[] cp = line.trim().split("\\s+");
                if (cp.length < 7) {
                    continue;
                }
                String shaShort = cp[5].length() > 12 ? cp[5].substring(0, 12) + ".." : cp[5];
                System.out.printf(" %-3s | %-44s | %12s | %10s B | %-14s | %s%n",
                        cp[1], cp[2], cp[3], cp[4], shaShort, cp[6]);
            }
            System.out.println("==============================================================================================================");
        } catch (IOException e) {
            System.out.printf("[client] Eroare la interogarea info '%s' (%s): %s%n",
                    nameOrId, coordinatorAddress, e.getMessage());
        }
    }

    private void deleteFile(String nameOrId) {
        try (Socket socket = connectSocket(coordinatorHost, coordinatorPort, DATA_TIMEOUT_MS)) {
            Protocol.writeHeaderLine(socket.getOutputStream(), "DELETE " + nameOrId);
            String resp = Protocol.readHeaderLine(socket.getInputStream());
            System.out.println(resp != null ? resp : "[client] Niciun raspuns de la coordonator.");
        } catch (IOException e) {
            System.out.printf("[client] Eroare la stergerea '%s' (%s): %s%n",
                    nameOrId, coordinatorAddress, e.getMessage());
        }
    }

    private void queryNodesStatus() {
        try (Socket socket = connectSocket(coordinatorHost, coordinatorPort, CONNECT_TIMEOUT_MS)) {
            Protocol.writeHeaderLine(socket.getOutputStream(), "NODES_STATUS");
            InputStream in = socket.getInputStream();

            String header = Protocol.readHeaderLine(in);
            if (header == null || !header.startsWith("CLUSTER_NODES")) {
                System.out.println("[client] Raspuns invalid de la coordonator: " + header
                        + " (verifica ca nodul n1 este pornit cu ultima versiune compilata: .\\scripts\\start-local.ps1)");
                return;
            }

            String[] hParts = header.split("\\s+");
            int count = Integer.parseInt(hParts[1]);
            String coordId = hParts.length >= 3 ? hParts[2] : "necunoscut";
            int intervalSec = hParts.length >= 4 ? Integer.parseInt(hParts[3]) : 5;
            int maxIntervals = hParts.length >= 5 ? Integer.parseInt(hParts[4]) : 5;

            System.out.println("========================================================================================");
            System.out.printf(" STARE CLUSTER (Coordonator: %s | Interval HB: %ds | Prag CAZUT: %d intervale)%n",
                    coordId, intervalSec, maxIntervals);
            System.out.println("----------------------------------------------------------------------------------------");
            System.out.printf(" %-5s | %-16s | %-9s | %-16s | %-14s | %-8s | %s%n",
                    "ID", "ADRESA", "STARE", "INTERVALE RATATE", "ULTIMUL HB", "PID", "ROL");
            System.out.println("----------------------------------------------------------------------------------------");

            long now = System.currentTimeMillis();
            for (int i = 0; i < count; i++) {
                String line = Protocol.readHeaderLine(in);
                if (line == null) {
                    break;
                }
                String[] np = line.trim().split("\\s+");
                if (np.length < 6) {
                    continue;
                }
                String id = np[0];
                String address = np[1];
                String state = np[2];
                long lastHb = Long.parseLong(np[3]);
                int missed = Integer.parseInt(np[4]);
                boolean isCoord = Boolean.parseBoolean(np[5]);
                long pid = np.length >= 7 ? Long.parseLong(np[6]) : -1L;

                long elapsedSec = Math.max(0L, (now - lastHb) / 1000L);
                String role = isCoord ? "[COORDONATOR]" : "stocare";
                System.out.printf(" %-5s | %-16s | %-9s | %d / %d intervale  | acum %3d sec   | %-8s | %s%n",
                        id, address, state, missed, maxIntervals, elapsedSec, (pid > 0 ? String.valueOf(pid) : "-"), role);
            }
            System.out.println("========================================================================================");
        } catch (IOException e) {
            System.out.printf("[client] Eroare la conectarea cu coordonatorul (%s): %s%n",
                    coordinatorAddress, e.getMessage());
        }
    }

    private void sendTestCommandToNode(String targetArg, String command) {
        String host = coordinatorHost;
        int port;
        try {
            if (targetArg.contains(":")) {
                String[] hp = parseHostPort(targetArg);
                host = hp[0];
                port = Integer.parseInt(hp[1]);
            } else {
                port = Integer.parseInt(targetArg);
            }
        } catch (IllegalArgumentException e) {
            System.out.println("Port invalid: " + targetArg + " (exemplu corect: freeze 7002)");
            return;
        }

        try (Socket socket = connectSocket(host, port, CONNECT_TIMEOUT_MS)) {
            Protocol.writeHeaderLine(socket.getOutputStream(), command);
            String reply = Protocol.readHeaderLine(socket.getInputStream());
            System.out.printf("[client] Raspuns de la %s:%d -> %s%n", host, port, reply);
        } catch (IOException e) {
            System.out.printf("[client] Nu s-a putut contacta nodul %s:%d: %s%n", host, port, e.getMessage());
        }
    }

    private String validateLocalUploadFile(Path localPath) {
        if (!Files.exists(localPath) || !Files.isRegularFile(localPath)) {
            System.out.printf("[client] Fisierul local nu exista: %s%n", localPath);
            return null;
        }
        try {
            long size = Files.size(localPath);
            if (size > Protocol.MAX_FILE_SIZE) {
                System.out.printf("[client] Fisierul depaseste limita maxima de %d octeti.%n", Protocol.MAX_FILE_SIZE);
                return null;
            }
            Path namePath = localPath.getFileName();
            if (namePath == null) {
                System.out.println("[client] Nume de fisier invalid.");
                return null;
            }
            return Protocol.sanitizeFileName(namePath.toString());
        } catch (IOException e) {
            System.out.printf("[client] Nu s-a putut citi dimensiunea fisierului '%s': %s%n", localPath, e.getMessage());
            return null;
        } catch (IllegalArgumentException e) {
            System.out.println("[client] " + e.getMessage());
            return null;
        }
    }

    private boolean isDirectNodeTarget(String arg1, String arg2) {
        if (arg1.contains(":")) {
            return true;
        }
        if (arg2.startsWith(".") || arg2.contains("/") || arg2.contains("\\") || Files.isDirectory(Paths.get(arg2))) {
            return false;
        }
        Map<String, String> knownNodes = fetchClusterNodeAddresses();
        return knownNodes.containsKey(arg1);
    }

    private NodeTarget resolveTarget(String nodeArg) {
        if (nodeArg.contains(":")) {
            try {
                String[] hp = parseHostPort(nodeArg);
                return new NodeTarget(nodeArg, hp[0], Integer.parseInt(hp[1]));
            } catch (IllegalArgumentException e) {
                System.out.println("[client] Adresa nod invalida: " + nodeArg);
                return null;
            }
        }
        Map<String, String> knownNodes = fetchClusterNodeAddresses();
        String addr = knownNodes.get(nodeArg);
        if (addr == null) {
            System.out.printf("[client] Nod necunoscut '%s' (foloseste ex: localhost:7002 sau un ID din 'nodes').%n", nodeArg);
            return null;
        }
        String[] hp = parseHostPort(addr);
        return new NodeTarget(nodeArg, hp[0], Integer.parseInt(hp[1]));
    }

    private Map<String, String> fetchClusterNodeAddresses() {
        Map<String, String> map = new HashMap<>();
        try (Socket socket = connectSocket(coordinatorHost, coordinatorPort, CONNECT_TIMEOUT_MS)) {
            Protocol.writeHeaderLine(socket.getOutputStream(), "NODES_STATUS");
            InputStream in = socket.getInputStream();
            String header = Protocol.readHeaderLine(in);
            if (header == null || !header.startsWith("CLUSTER_NODES ")) {
                return map;
            }
            int count = Integer.parseInt(header.split("\\s+")[1]);
            for (int i = 0; i < count; i++) {
                String line = Protocol.readHeaderLine(in);
                if (line == null) {
                    break;
                }
                String[] np = line.trim().split("\\s+");
                if (np.length >= 2) {
                    map.put(np[0], np[1]);
                }
            }
        } catch (Exception ignored) {
        }
        return map;
    }

    private static Socket connectSocket(String host, int port, int soTimeoutMs) throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
        socket.setSoTimeout(soTimeoutMs);
        return socket;
    }

    private static NodeTarget parseNodeEndpoint(String endpoint) {
        int atIdx = endpoint.indexOf('@');
        if (atIdx > 0) {
            String id = endpoint.substring(0, atIdx);
            String[] hp = parseHostPort(endpoint.substring(atIdx + 1));
            return new NodeTarget(id, hp[0], Integer.parseInt(hp[1]));
        }
        String[] hp = parseHostPort(endpoint);
        return new NodeTarget(endpoint, hp[0], Integer.parseInt(hp[1]));
    }

    private static String[] parseHostPort(String address) {
        int colonIdx = address.lastIndexOf(':');
        if (colonIdx <= 0 || colonIdx == address.length() - 1) {
            throw new IllegalArgumentException("Format invalid host:port: " + address);
        }
        String host = address.substring(0, colonIdx);
        int port = Integer.parseInt(address.substring(colonIdx + 1));
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("Port invalid: " + port);
        }
        return new String[]{host, String.valueOf(port)};
    }

    private static List<String> tokenizeLine(String line) {
        List<String> tokens = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        char quoteChar = 0;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == quoteChar) {
                    inQuotes = false;
                } else {
                    current.append(c);
                }
            } else if (c == '"' || c == '\'') {
                inQuotes = true;
                quoteChar = c;
            } else if (Character.isWhitespace(c)) {
                if (current.length() > 0) {
                    tokens.add(current.toString());
                    current.setLength(0);
                }
            } else {
                current.append(c);
            }
        }
        if (current.length() > 0) {
            tokens.add(current.toString());
        }
        return tokens;
    }

    private void printHelp() {
        System.out.println("Comenzi disponibile:");
        System.out.println("  put <fisier_local>                           - incarcare distribuita pe bucati (ALLOCATE -> PUT paralel -> COMMIT)");
        System.out.println("  put <nod|host:port> <fisier_local>           - incarcare directa pe un nod (ex: put localhost:7002 test.bin)");
        System.out.println("  get <nume_sau_id> [dir_dest]                 - descarcare paralela (LOCATE -> GET paralel + SHA-256)");
        System.out.println("  get <nod|host:port> <nume> [dir_dest]        - descarcare directa de pe un nod");
        System.out.println("  ls                                           - listeaza fisierele inregistrate pe coordonator");
        System.out.println("  info <nume_sau_id>                           - afiseaza distributia pe bucati si noduri pentru un fisier");
        System.out.println("  rm <nume_sau_id>                             - sterge fisierul din metadate si toate bucatile de pe noduri");
        System.out.println("  nodes                                        - afiseaza starea nodurilor din cluster");
        System.out.println("  freeze <port> / unfreeze <port>              - opreste/reia heartbeat-ul unui nod pentru test");
        System.out.println("  exit                                         - inchide clientul");
    }

    private boolean hasStandardInput() {
        try {
            return System.in != null;
        } catch (Exception e) {
            return false;
        }
    }

    public static void main(String[] args) {
        String coordinator = null;
        List<String> cmdArgs = new ArrayList<>();

        for (int i = 0; i < args.length; i++) {
            if ("--coordinator".equals(args[i])) {
                if (i + 1 >= args.length) {
                    printUsageAndExit();
                    return;
                }
                coordinator = args[++i];
            } else {
                cmdArgs.add(args[i]);
            }
        }

        if (cmdArgs.isEmpty()) {
            if (coordinator == null || coordinator.trim().isEmpty() || !coordinator.contains(":")) {
                printUsageAndExit();
                return;
            }
            new Client(coordinator).start();
            return;
        }

        String effectiveCoordinator = (coordinator != null && !coordinator.trim().isEmpty())
                ? coordinator
                : DEFAULT_COORDINATOR;
        Client client = new Client(effectiveCoordinator);
        client.executeCommand(cmdArgs.toArray(new String[0]));
    }

    private static void printUsageAndExit() {
        System.err.println("Utilizare interactiva: java -cp target/classes ro.ubm.sd.client.Client --coordinator <host:port>");
        System.err.println("Utilizare directa:     java -cp target/classes ro.ubm.sd.client.Client [--coordinator <host:port>] <comanda> [args]");
        System.exit(1);
    }

    private static final class UploadChunkPlan {
        final int index;
        final String chunkId;
        final long offset;
        final long size;
        final NodeTarget target;

        UploadChunkPlan(int index, String chunkId, long offset, long size, NodeTarget target) {
            this.index = index;
            this.chunkId = chunkId;
            this.offset = offset;
            this.size = size;
            this.target = target;
        }
    }

    private static final class UploadedChunk {
        final String chunkId;
        final NodeTarget target;

        UploadedChunk(String chunkId, NodeTarget target) {
            this.chunkId = chunkId;
            this.target = target;
        }
    }

    private static final class DownloadChunkPlan {
        final int index;
        final String chunkId;
        final long offset;
        final long size;
        final String checksum;
        final List<NodeTarget> targets;

        DownloadChunkPlan(int index, String chunkId, long offset, long size,
                          String checksum, List<NodeTarget> targets) {
            this.index = index;
            this.chunkId = chunkId;
            this.offset = offset;
            this.size = size;
            this.checksum = checksum;
            this.targets = targets;
        }
    }

    private static final class NodeTarget {
        final String id;
        final String host;
        final int port;

        NodeTarget(String id, String host, int port) {
            this.id = id;
            this.host = host;
            this.port = port;
        }
    }
}
