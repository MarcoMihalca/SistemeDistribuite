package ro.ubm.sd.node;

import ro.ubm.sd.common.ChecksumUtil;
import ro.ubm.sd.common.ChunkMetadata;
import ro.ubm.sd.common.ChunkPolicy;
import ro.ubm.sd.common.FileMetadata;
import ro.ubm.sd.common.NodeInfo;
import ro.ubm.sd.common.NodeState;
import ro.ubm.sd.common.Protocol;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Nod de stocare din sistemul distribuit (poate avea si rolul de coordonator).
 * Porneste ca proces separat si isi primeste configuratia din linia de comanda:
 * --id <id> --port <port> --dir <director> [--coordinator] [--host <host>]
 */
public class StorageNode {

    public static final int HEARTBEAT_INTERVAL_SECONDS = 5;
    public static final int SUSPECT_INTERVAL_THRESHOLD = 3;
    public static final int CAZUT_INTERVAL_THRESHOLD = 5;

    private static final String MULTICAST_GROUP = "230.0.0.1";
    private static final int MULTICAST_PORT = 6999;
    private static final int[] DEFAULT_LOCAL_PORTS = {7001, 7002, 7003, 7004, 7005};

    private final String id;
    private final String host;
    private final int port;
    private final String storageDirText;
    private final Path storageDir;
    private volatile boolean coordinator;
    private final long pid;
    private final String multicastGroup;
    private final int multicastPort;
    private final boolean probeDefaultPorts;

    private volatile boolean running = true;
    private volatile boolean heartbeatPaused = false;
    private ServerSocket serverSocket;
    private MulticastSocket multicastSocket;
    private final ExecutorService tcpPool;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "heartbeat-scheduler");
        t.setDaemon(true);
        return t;
    });

    // Tabelele mentinute in memorie de coordonator (Pasul 3)
    private final Map<UUID, FileMetadata> fileMetadataTable = new ConcurrentHashMap<>();
    private final Map<String, NodeInfo> clusterNodes = new ConcurrentHashMap<>();
    private final AtomicInteger allocationCounter = new AtomicInteger(0);
    private final Object metadataLock = new Object();

    public StorageNode(String id, String host, int port, String storageDirText, boolean coordinator) {
        this(id, host, port, storageDirText, coordinator, MULTICAST_GROUP, MULTICAST_PORT, true);
    }

    public StorageNode(String id, String host, int port, String storageDirText, boolean coordinator,
                       String multicastGroup, int multicastPort, boolean probeDefaultPorts) {
        this.id = id;
        this.host = host;
        this.port = port;
        this.storageDirText = storageDirText;
        this.storageDir = Paths.get(storageDirText);
        this.coordinator = coordinator;
        this.pid = resolveCurrentPid();
        this.multicastGroup = multicastGroup;
        this.multicastPort = multicastPort;
        this.probeDefaultPorts = probeDefaultPorts;
        this.tcpPool = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "tcp-handler-" + id);
            t.setDaemon(true);
            return t;
        });
    }

    private static long resolveCurrentPid() {
        try {
            String jvmName = ManagementFactory.getRuntimeMXBean().getName();
            int atIndex = jvmName.indexOf('@');
            if (atIndex > 0) {
                return Long.parseLong(jvmName.substring(0, atIndex));
            }
        } catch (Exception ignored) {
        }
        return -1L;
    }

    public void start() throws IOException {
        // Creeaza directorul de stocare local daca lipseste
        Files.createDirectories(storageDir);

        // Inregistreaza nodul curent in tabela locala
        NodeInfo selfInfo = new NodeInfo(id, host, port, NodeState.ACTIV, System.currentTimeMillis(), coordinator, pid);
        clusterNodes.put(id, selfInfo);

        // Porneste serverul TCP pentru mesaje de control, heartbeat si comenzi client
        this.serverSocket = new ServerSocket(port);
        Thread tcpAcceptThread = new Thread(this::acceptTcpLoop, "tcp-server-" + id);
        tcpAcceptThread.setDaemon(true);
        tcpAcceptThread.start();

        // Porneste receptorul UDP multicast (daca reteaua locala permite)
        startMulticastReceiver();

        String roleDescription = coordinator ? "coordonator + nod de stocare" : "nod de stocare";
        System.out.printf("[%s] pornit (PID=%d): port %d, director %s, rol: %s (interval heartbeat: %ds, prag CAZUT: %d intervale)%n",
                id, pid, port, storageDirText, roleDescription, HEARTBEAT_INTERVAL_SECONDS, CAZUT_INTERVAL_THRESHOLD);

        // Trimite imediat primul semnal de viata la pornire si apoi la fiecare 5 secunde
        scheduler.scheduleAtFixedRate(this::emitHeartbeatCycle, 0, HEARTBEAT_INTERVAL_SECONDS, TimeUnit.SECONDS);

        // La fiecare interval de 5 secunde, evalueaza contorul de intervale ratate pentru toate nodurile
        scheduler.scheduleAtFixedRate(this::evaluateNodesIntervalCycle,
                HEARTBEAT_INTERVAL_SECONDS, HEARTBEAT_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    private void acceptTcpLoop() {
        while (running && !serverSocket.isClosed()) {
            try {
                final Socket clientSocket = serverSocket.accept();
                tcpPool.submit(() -> handleTcpConnection(clientSocket));
            } catch (IOException e) {
                if (running) {
                    System.err.printf("[%s] Eroare la acceptarea conexiunii TCP: %s%n", id, e.getMessage());
                }
            }
        }
    }

    private void handleTcpConnection(Socket socket) {
        try (Socket s = socket) {
            s.setSoTimeout(60_000);
            InputStream in = s.getInputStream();
            OutputStream out = s.getOutputStream();

            String line = Protocol.readHeaderLine(in);
            if (line == null || line.trim().isEmpty()) {
                return;
            }

            String[] parts = line.trim().split("\\s+");
            String command = parts[0].toUpperCase();

            switch (command) {
                case "HEARTBEAT":
                    handleHeartbeatMessage(parts);
                    Protocol.writeHeaderLine(out, "OK");
                    break;
                case "NODES_STATUS":
                    handleNodesStatus(out);
                    break;
                case "FREEZE":
                    heartbeatPaused = true;
                    System.out.printf("[%s] [TEST] Emiterea de HEARTBEAT a fost OPRITA (freeze). Procesul (PID=%d) asteapta detectia...%n", id, pid);
                    Protocol.writeHeaderLine(out, "FROZEN " + id);
                    break;
                case "UNFREEZE":
                    heartbeatPaused = false;
                    System.out.printf("[%s] [TEST] Emiterea de HEARTBEAT a fost RELUATA.%n", id);
                    emitHeartbeatCycle();
                    Protocol.writeHeaderLine(out, "UNFROZEN " + id);
                    break;
                case "FORCE_KILL":
                    System.err.printf("[%s] [KILL] Primit semnal FORCE_KILL de la coordonator (5 intervale ratate). Oprire proces (PID=%d)...%n", id, pid);
                    Protocol.writeHeaderLine(out, "KILLED " + id);
                    System.exit(137);
                    break;
                case "PUT":
                    handlePut(parts, in, out);
                    break;
                case "GET":
                    handleGet(parts, out);
                    break;
                case "ALLOCATE":
                    handleAllocate(parts, out);
                    break;
                case "COMMIT":
                    handleCommit(parts, in, out);
                    break;
                case "LOCATE":
                    handleLocate(parts, out);
                    break;
                case "LIST":
                    handleList(out);
                    break;
                case "DELETE":
                    handleDelete(parts, out);
                    break;
                case "REMOVE_REPLICA":
                    handleRemoveReplica(parts, out);
                    break;
                default:
                    Protocol.writeHeaderLine(out, "ERROR UNKNOWN_COMMAND");
                    break;
            }
        } catch (Throwable t) {
            if (running) {
                System.err.printf("[%s] Eroare la procesarea comenzii TCP: %s%n", id, t.toString());
            }
        }
    }

    private void handleNodesStatus(OutputStream out) throws IOException {
        List<NodeInfo> sorted = clusterNodes.values().stream()
                .sorted(Comparator.comparing(NodeInfo::getId))
                .collect(Collectors.toList());
        String coordId = sorted.stream()
                .filter(NodeInfo::isCoordinator)
                .map(NodeInfo::getId)
                .findFirst()
                .orElse("necunoscut");

        Protocol.writeHeaderLine(out, String.format("CLUSTER_NODES %d %s %d %d",
                sorted.size(), coordId, HEARTBEAT_INTERVAL_SECONDS, CAZUT_INTERVAL_THRESHOLD));
        for (NodeInfo n : sorted) {
            Protocol.writeHeaderLine(out, String.format("%s %s:%d %s %d %d %b %d",
                    n.getId(),
                    n.getHost(),
                    n.getPort(),
                    n.getState().name(),
                    n.getLastHeartbeat(),
                    n.getMissedIntervals(),
                    n.isCoordinator(),
                    n.getPid()));
        }
    }

    private void handlePut(String[] parts, InputStream in, OutputStream out) throws IOException {
        if (parts.length != 4) {
            Protocol.writeHeaderLine(out, "ERROR INVALID_PUT_HEADER");
            return;
        }
        String fileName;
        try {
            fileName = Protocol.sanitizeFileName(parts[1]);
        } catch (IllegalArgumentException e) {
            Protocol.writeHeaderLine(out, "ERROR INVALID_FILENAME");
            return;
        }

        long size;
        try {
            size = Long.parseLong(parts[2]);
        } catch (NumberFormatException e) {
            Protocol.writeHeaderLine(out, "ERROR INVALID_SIZE");
            return;
        }
        if (size < 0 || size > Protocol.MAX_FILE_SIZE) {
            Protocol.writeHeaderLine(out, size < 0 ? "ERROR INVALID_SIZE" : "ERROR FILE_TOO_LARGE");
            return;
        }

        String expectedSha = parts[3].trim().toLowerCase();
        Path tmp = storageDir.resolve(fileName + ".part");
        try {
            String actualSha = Protocol.receiveBytes(in, size, tmp);
            if (!actualSha.equalsIgnoreCase(expectedSha)) {
                Files.deleteIfExists(tmp);
                Protocol.writeHeaderLine(out, "ERROR CHECKSUM_MISMATCH");
                return;
            }
            Protocol.moveAtomically(tmp, storageDir.resolve(fileName));
            System.out.printf("[%s] [PUT] Stocat %s (%d octeti, sha256=%s)%n", id, fileName, size, actualSha);
            Protocol.writeHeaderLine(out, "STORED " + actualSha);
        } catch (IOException e) {
            Files.deleteIfExists(tmp);
            throw e;
        }
    }

    private void handleGet(String[] parts, OutputStream out) throws IOException {
        if (parts.length != 2) {
            Protocol.writeHeaderLine(out, "ERROR INVALID_GET_HEADER");
            return;
        }
        String fileName;
        try {
            fileName = Protocol.sanitizeFileName(parts[1]);
        } catch (IllegalArgumentException e) {
            Protocol.writeHeaderLine(out, "ERROR INVALID_FILENAME");
            return;
        }

        Path target = storageDir.resolve(fileName);
        if (!Files.isRegularFile(target)) {
            Protocol.writeHeaderLine(out, "NOT_FOUND");
            return;
        }

        long size = Files.size(target);
        String sha256 = ChecksumUtil.sha256(target);
        Protocol.writeHeaderLine(out, "FILE " + size + " " + sha256);
        Protocol.sendFileBytes(target, out);
        System.out.printf("[%s] [GET] Trimis %s (%d octeti)%n", id, fileName, size);
    }

    private void handleAllocate(String[] parts, OutputStream out) throws IOException {
        if (!coordinator) {
            Protocol.writeHeaderLine(out, "ERROR NOT_COORDINATOR");
            return;
        }
        if (parts.length != 4) {
            Protocol.writeHeaderLine(out, "ERROR INVALID_ALLOCATE_HEADER");
            return;
        }
        String fileName;
        try {
            fileName = Protocol.sanitizeFileName(parts[1]);
        } catch (IllegalArgumentException e) {
            Protocol.writeHeaderLine(out, "ERROR INVALID_FILENAME");
            return;
        }

        long size;
        try {
            size = Long.parseLong(parts[2]);
        } catch (NumberFormatException e) {
            Protocol.writeHeaderLine(out, "ERROR INVALID_SIZE");
            return;
        }
        if (size < 0 || size > Protocol.MAX_FILE_SIZE) {
            Protocol.writeHeaderLine(out, size < 0 ? "ERROR INVALID_SIZE" : "ERROR FILE_TOO_LARGE");
            return;
        }

        boolean alreadyExists = fileMetadataTable.values().stream()
                .anyMatch(m -> m.getName().equals(fileName));
        if (alreadyExists) {
            Protocol.writeHeaderLine(out, "ERROR FILE_ALREADY_EXISTS");
            return;
        }

        List<NodeInfo> activeNodes = clusterNodes.values().stream()
                .filter(n -> n.getState() == NodeState.ACTIV)
                .sorted(Comparator.comparing(NodeInfo::getId))
                .collect(Collectors.toList());
        if (activeNodes.isEmpty()) {
            Protocol.writeHeaderLine(out, "ERROR NO_ACTIVE_NODES");
            return;
        }

        long chunkSize = ChunkPolicy.chunkSizeFor(size);
        int chunkCount = ChunkPolicy.chunkCountFor(size);
        UUID fileId = UUID.randomUUID();

        // Least-Bytes-Used Striping: calculam incarcarea curenta a fiecarui nod activ
        Map<String, Long> loadBytes = new HashMap<>();
        for (NodeInfo n : activeNodes) {
            loadBytes.put(n.getId(), 0L);
        }
        for (FileMetadata existing : fileMetadataTable.values()) {
            for (ChunkMetadata cm : existing.getChunks()) {
                for (String replicaNodeId : cm.getReplicaNodes()) {
                    Long current = loadBytes.get(replicaNodeId);
                    if (current != null) {
                        loadBytes.put(replicaNodeId, current + cm.getSize());
                    }
                }
            }
        }

        int tieStart = Math.floorMod(allocationCounter.getAndIncrement(), activeNodes.size());
        Protocol.writeHeaderLine(out, String.format("ALLOC_PLAN %s %d %d", fileId, chunkSize, chunkCount));
        for (int i = 0; i < chunkCount; i++) {
            long offset = (long) i * chunkSize;
            long chunkLen = ChunkPolicy.chunkLengthAt(size, chunkSize, i);
            String chunkId = ChunkPolicy.chunkFileName(fileId, i);
            NodeInfo chosen = pickLeastLoadedNode(activeNodes, loadBytes, (tieStart + i) % activeNodes.size());
            loadBytes.put(chosen.getId(), loadBytes.get(chosen.getId()) + chunkLen);
            Protocol.writeHeaderLine(out, String.format("CHUNK %d %s %d %d %s@%s:%d",
                    i, chunkId, offset, chunkLen, chosen.getId(), chosen.getHost(), chosen.getPort()));
        }
    }

    private NodeInfo pickLeastLoadedNode(List<NodeInfo> activeNodes, Map<String, Long> loadBytes, int startIndex) {
        NodeInfo best = activeNodes.get(startIndex);
        long bestLoad = loadBytes.get(best.getId());
        for (int step = 1; step < activeNodes.size(); step++) {
            NodeInfo candidate = activeNodes.get((startIndex + step) % activeNodes.size());
            long candidateLoad = loadBytes.get(candidate.getId());
            if (candidateLoad < bestLoad) {
                best = candidate;
                bestLoad = candidateLoad;
            }
        }
        return best;
    }

    private void handleCommit(String[] parts, InputStream in, OutputStream out) throws IOException {
        if (!coordinator) {
            Protocol.writeHeaderLine(out, "ERROR NOT_COORDINATOR");
            return;
        }
        if (parts.length != 7) {
            Protocol.writeHeaderLine(out, "ERROR INVALID_COMMIT_HEADER");
            return;
        }
        UUID fileId;
        String fileName;
        long size;
        long chunkSize;
        int chunkCount;
        try {
            fileId = UUID.fromString(parts[1]);
            fileName = Protocol.sanitizeFileName(parts[2]);
            size = Long.parseLong(parts[3]);
            chunkSize = Long.parseLong(parts[5]);
            chunkCount = Integer.parseInt(parts[6]);
        } catch (IllegalArgumentException e) {
            Protocol.writeHeaderLine(out, "ERROR INVALID_COMMIT_DATA");
            return;
        }
        String fullChecksum = parts[4].trim().toLowerCase();
        if (size < 0 || size > Protocol.MAX_FILE_SIZE || chunkSize < 0
                || fullChecksum.isEmpty() || chunkCount < 1 || chunkCount > 4096) {
            Protocol.writeHeaderLine(out, "ERROR INVALID_COMMIT_DATA");
            return;
        }

        List<ChunkMetadata> chunks = new ArrayList<>(chunkCount);
        long expectedOffset = 0L;
        for (int expectedIdx = 0; expectedIdx < chunkCount; expectedIdx++) {
            String chunkLine = Protocol.readHeaderLine(in);
            if (chunkLine == null) {
                Protocol.writeHeaderLine(out, "ERROR INCOMPLETE_COMMIT");
                return;
            }
            String[] cp = chunkLine.trim().split("\\s+");
            if (cp.length != 7 || !"CHUNK".equalsIgnoreCase(cp[0])) {
                Protocol.writeHeaderLine(out, "ERROR INVALID_COMMIT_CHUNK");
                return;
            }
            int idx;
            String chunkId;
            long offset;
            long chunkLen;
            try {
                idx = Integer.parseInt(cp[1]);
                chunkId = Protocol.sanitizeFileName(cp[2]);
                offset = Long.parseLong(cp[3]);
                chunkLen = Long.parseLong(cp[4]);
            } catch (IllegalArgumentException e) {
                Protocol.writeHeaderLine(out, "ERROR INVALID_COMMIT_CHUNK");
                return;
            }
            String chunkSha = cp[5].trim().toLowerCase();
            List<String> nodeIds = Arrays.stream(cp[6].split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toList());

            if (idx != expectedIdx || offset != expectedOffset || chunkLen < 0
                    || chunkSha.isEmpty() || nodeIds.isEmpty()) {
                Protocol.writeHeaderLine(out, "ERROR INVALID_COMMIT_CHUNK");
                return;
            }
            expectedOffset += chunkLen;
            chunks.add(new ChunkMetadata(idx, chunkId, offset, chunkLen, chunkSha, nodeIds));
        }

        if (expectedOffset != size) {
            Protocol.writeHeaderLine(out, "ERROR CHUNK_SIZE_MISMATCH");
            return;
        }

        FileMetadata meta = new FileMetadata(fileId, fileName, size, fullChecksum, chunkSize, chunks);
        synchronized (metadataLock) {
            boolean duplicateName = fileMetadataTable.values().stream()
                    .anyMatch(m -> m.getName().equals(fileName) && !m.getId().equals(fileId));
            if (duplicateName) {
                Protocol.writeHeaderLine(out, "ERROR FILE_ALREADY_EXISTS");
                return;
            }
            fileMetadataTable.put(fileId, meta);
        }
        System.out.printf("[%s] [COMMIT] Inregistrat %s (id=%s, size=%d, chunks=%d, noduri=%s)%n",
                id, fileName, fileId, size, chunks.size(), meta.getReplicaNodes());
        Protocol.writeHeaderLine(out, "OK");
    }

    private void handleLocate(String[] parts, OutputStream out) throws IOException {
        if (!coordinator) {
            Protocol.writeHeaderLine(out, "ERROR NOT_COORDINATOR");
            return;
        }
        if (parts.length != 2) {
            Protocol.writeHeaderLine(out, "ERROR INVALID_LOCATE_HEADER");
            return;
        }
        FileMetadata meta = findMetadata(parts[1]);
        if (meta == null) {
            Protocol.writeHeaderLine(out, "NOT_FOUND");
            return;
        }

        List<String> chunkLines = new ArrayList<>(meta.getChunkCount());
        for (ChunkMetadata chunk : meta.getChunks()) {
            String endpoints = chunk.getReplicaNodes().stream()
                    .map(clusterNodes::get)
                    .filter(n -> n != null && n.getState() != NodeState.CAZUT)
                    .map(n -> String.format("%s@%s:%d", n.getId(), n.getHost(), n.getPort()))
                    .collect(Collectors.joining(","));
            if (endpoints.isEmpty()) {
                Protocol.writeHeaderLine(out, "ERROR CHUNK_OFFLINE " + chunk.getIndex());
                return;
            }
            chunkLines.add(String.format("CHUNK %d %s %d %d %s %s",
                    chunk.getIndex(), chunk.getChunkId(), chunk.getOffset(),
                    chunk.getSize(), chunk.getChecksum(), endpoints));
        }

        Protocol.writeHeaderLine(out, String.format("LOCATIONS %s %s %d %s %d %d",
                meta.getId(), meta.getName(), meta.getSize(), meta.getChecksum(),
                meta.getChunkSize(), meta.getChunkCount()));
        for (String line : chunkLines) {
            Protocol.writeHeaderLine(out, line);
        }
    }

    private void handleList(OutputStream out) throws IOException {
        if (!coordinator) {
            Protocol.writeHeaderLine(out, "ERROR NOT_COORDINATOR");
            return;
        }
        List<FileMetadata> files = fileMetadataTable.values().stream()
                .sorted(Comparator.comparing(FileMetadata::getName))
                .collect(Collectors.toList());

        Protocol.writeHeaderLine(out, "FILE_LIST " + files.size());
        for (FileMetadata m : files) {
            Protocol.writeHeaderLine(out, String.format("%s %s %d %s %d %d %s",
                    m.getId(), m.getName(), m.getSize(), m.getChecksum(),
                    m.getChunkCount(), m.getChunkSize(), String.join(",", m.getReplicaNodes())));
        }
    }

    private void handleDelete(String[] parts, OutputStream out) throws IOException {
        if (!coordinator) {
            Protocol.writeHeaderLine(out, "ERROR NOT_COORDINATOR");
            return;
        }
        if (parts.length != 2) {
            Protocol.writeHeaderLine(out, "ERROR INVALID_DELETE_HEADER");
            return;
        }
        FileMetadata meta;
        synchronized (metadataLock) {
            meta = findMetadata(parts[1]);
            if (meta == null) {
                Protocol.writeHeaderLine(out, "NOT_FOUND");
                return;
            }
            fileMetadataTable.remove(meta.getId());
        }

        for (ChunkMetadata chunk : meta.getChunks()) {
            for (String nodeId : chunk.getReplicaNodes()) {
                if (nodeId.equals(this.id)) {
                    Files.deleteIfExists(storageDir.resolve(chunk.getChunkId()));
                    continue;
                }
                NodeInfo target = clusterNodes.get(nodeId);
                if (target != null && target.getState() != NodeState.CAZUT) {
                    try (Socket s = new Socket()) {
                        s.connect(new InetSocketAddress(target.getHost(), target.getPort()), 1000);
                        s.setSoTimeout(2000);
                        Protocol.writeHeaderLine(s.getOutputStream(), "REMOVE_REPLICA " + chunk.getChunkId());
                        Protocol.readHeaderLine(s.getInputStream());
                    } catch (IOException ignored) {
                    }
                }
            }
        }
        System.out.printf("[%s] [DELETE] Sters %s (id=%s, chunks=%d)%n",
                id, meta.getName(), meta.getId(), meta.getChunkCount());
        Protocol.writeHeaderLine(out, String.format("DELETED %s %s %d",
                meta.getId(), meta.getName(), meta.getChunkCount()));
    }

    private void handleRemoveReplica(String[] parts, OutputStream out) throws IOException {
        if (parts.length != 2) {
            Protocol.writeHeaderLine(out, "ERROR INVALID_REMOVE_HEADER");
            return;
        }
        String fileName;
        try {
            fileName = Protocol.sanitizeFileName(parts[1]);
        } catch (IllegalArgumentException e) {
            Protocol.writeHeaderLine(out, "ERROR INVALID_FILENAME");
            return;
        }
        boolean deleted = Files.deleteIfExists(storageDir.resolve(fileName));
        if (deleted) {
            System.out.printf("[%s] [REMOVE_REPLICA] Sters %s de pe disc%n", id, fileName);
        }
        Protocol.writeHeaderLine(out, (deleted ? "REMOVED " : "NOT_FOUND ") + fileName);
    }

    private FileMetadata findMetadata(String query) {
        try {
            UUID uuid = UUID.fromString(query);
            FileMetadata byId = fileMetadataTable.get(uuid);
            if (byId != null) {
                return byId;
            }
        } catch (IllegalArgumentException ignored) {
        }
        for (FileMetadata meta : fileMetadataTable.values()) {
            if (meta.getName().equals(query)) {
                return meta;
            }
        }
        return null;
    }

    @SuppressWarnings("deprecation")
    private void startMulticastReceiver() {
        try {
            multicastSocket = new MulticastSocket(multicastPort);
            multicastSocket.setReuseAddress(true);
            InetAddress group = InetAddress.getByName(multicastGroup);
            multicastSocket.joinGroup(group);

            Thread multicastThread = new Thread(() -> {
                byte[] buf = new byte[512];
                while (running && !multicastSocket.isClosed()) {
                    try {
                        DatagramPacket packet = new DatagramPacket(buf, buf.length);
                        multicastSocket.receive(packet);
                        String msg = new String(packet.getData(), packet.getOffset(), packet.getLength(), StandardCharsets.UTF_8).trim();
                        String[] parts = msg.split("\\s+");
                        if (parts.length >= 5 && "HEARTBEAT".equalsIgnoreCase(parts[0])) {
                            handleHeartbeatMessage(parts);
                        }
                    } catch (IOException e) {
                        if (running) {
                            break;
                        }
                    }
                }
            }, "udp-multicast-" + id);
            multicastThread.setDaemon(true);
            multicastThread.start();
        } catch (IOException ignored) {
            // Daca UDP multicast este restrictionat de OS/firewall, comunicatia TCP locala preia automat
        }
    }

    /**
     * Emite semnalul de HEARTBEAT la fiecare interval de 5 secunde.
     */
    private void emitHeartbeatCycle() {
        try {
            NodeInfo selfInfo = clusterNodes.get(id);
            if (selfInfo != null) {
                selfInfo.recordHeartbeat(host, port, coordinator, pid);
            }

            if (heartbeatPaused || !running) {
                return;
            }

            String msg = String.format("HEARTBEAT %s %s %d %b %d", id, host, port, coordinator, pid);

            // 1. Emite prin UDP Multicast pentru descoperire in retea
            if (multicastSocket != null && !multicastSocket.isClosed()) {
                try {
                    byte[] data = msg.getBytes(StandardCharsets.UTF_8);
                    InetAddress group = InetAddress.getByName(multicastGroup);
                    DatagramPacket packet = new DatagramPacket(data, data.length, group, multicastPort);
                    multicastSocket.send(packet);
                } catch (IOException ignored) {
                }
            }

            // 2. Emite prin TCP catre nodurile cunoscute si porturile locale standard (garanteaza functionarea pe Windows localhost)
            Set<String> targets = new HashSet<>();
            if (probeDefaultPorts) {
                for (int localPort : DEFAULT_LOCAL_PORTS) {
                    if (localPort != this.port) {
                        targets.add(this.host + ":" + localPort);
                    }
                }
            }
            for (NodeInfo info : clusterNodes.values()) {
                if (!info.getId().equals(this.id)) {
                    targets.add(info.getHost() + ":" + info.getPort());
                }
            }

            for (String target : targets) {
                String[] hp = target.split(":");
                String targetHost = hp[0];
                int targetPort = Integer.parseInt(hp[1]);
                try (Socket s = new Socket()) {
                    s.connect(new InetSocketAddress(targetHost, targetPort), 400);
                    PrintWriter out = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true);
                    out.println(msg);
                    out.flush();
                } catch (IOException ignored) {
                    // Nodul tinta nu este pornit sau nu raspunde in acest interval
                }
            }
        } catch (Throwable t) {
            System.err.printf("[%s] Eroare in emitHeartbeatCycle: %s%n", id, t.toString());
        }
    }

    private void handleHeartbeatMessage(String[] parts) {
        if (parts.length < 5) {
            return;
        }
        String senderId = parts[1];
        if (senderId.equals(this.id)) {
            return;
        }
        String senderHost = parts[2];
        int senderPort;
        try {
            senderPort = Integer.parseInt(parts[3]);
        } catch (NumberFormatException e) {
            return;
        }
        boolean senderCoord = Boolean.parseBoolean(parts[4]);
        long senderPid = -1L;
        if (parts.length >= 6) {
            try {
                senderPid = Long.parseLong(parts[5]);
            } catch (NumberFormatException ignored) {
            }
        }

        final long finalPid = senderPid;
        clusterNodes.compute(senderId, (key, existing) -> {
            if (existing == null) {
                NodeInfo created = new NodeInfo(senderId, senderHost, senderPort,
                        NodeState.ACTIV, System.currentTimeMillis(), senderCoord, finalPid);
                System.out.printf("[%s] [DESCOPERIRE] Nod detectat: %s (%s:%d, PID=%d, coordonator=%b) -> ACTIV%n",
                        id, senderId, senderHost, senderPort, finalPid, senderCoord);
                return created;
            }

            NodeState oldState = existing.getState();
            int oldMissed = existing.getMissedIntervals();
            existing.recordHeartbeat(senderHost, senderPort, senderCoord, finalPid);

            if (oldState != NodeState.ACTIV || oldMissed > 0) {
                System.out.printf("[%s] [HEARTBEAT] Nodul %s a trimis heartbeat -> ACTIV (revenit din %s, intervale ratate resetate de la %d la 0)%n",
                        id, senderId, oldState, oldMissed);
            }
            return existing;
        });
    }

    /**
     * Evalueaza la fiecare interval de 5 secunde cate batai a ratat fiecare nod:
     * - 3 intervale ratate -> SUSPECT
     * - 5 intervale ratate -> CAZUT + kill proces
     */
    private void evaluateNodesIntervalCycle() {
        try {
            for (NodeInfo node : clusterNodes.values()) {
                if (node.getId().equals(this.id)) {
                    node.recordHeartbeat(host, port, coordinator, pid);
                    continue;
                }

                if (node.getState() == NodeState.CAZUT) {
                    continue;
                }

                int missed = node.advanceInterval();
                if (missed >= CAZUT_INTERVAL_THRESHOLD) {
                    node.setState(NodeState.CAZUT);
                    System.out.printf("[%s] [HEARTBEAT] Nodul %s a ratat %d/%d intervale (cate %ds) -> CAZUT! (Pa! Se aplica -kill pe nod/proces PID=%d)%n",
                            id, node.getId(), missed, CAZUT_INTERVAL_THRESHOLD, HEARTBEAT_INTERVAL_SECONDS, node.getPid());
                    if (coordinator) {
                        killDeadNodeProcess(node);
                    }
                } else if (missed >= SUSPECT_INTERVAL_THRESHOLD) {
                    if (node.getState() != NodeState.SUSPECT) {
                        node.setState(NodeState.SUSPECT);
                    }
                    System.out.printf("[%s] [HEARTBEAT] Nodul %s a ratat %d/%d intervale -> SUSPECT%n",
                            id, node.getId(), missed, CAZUT_INTERVAL_THRESHOLD);
                } else if (missed > 0) {
                    System.out.printf("[%s] [HEARTBEAT] Nodul %s nu a raspuns in acest interval (%d/%d intervale ratate)%n",
                            id, node.getId(), missed, CAZUT_INTERVAL_THRESHOLD);
                }
            }
        } catch (Throwable t) {
            System.err.printf("[%s] Eroare in evaluateNodesIntervalCycle: %s%n", id, t.toString());
        }
    }

    /**
     * Opreste fortat (kill) procesul nodului declarat CAZUT dupa 5 intervale ratate.
     */
    private void killDeadNodeProcess(NodeInfo deadNode) {
        // 1. Trimite semnal FORCE_KILL pe socket daca procesul mai asculta pe port
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(deadNode.getHost(), deadNode.getPort()), 300);
            PrintWriter out = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true);
            out.println("FORCE_KILL");
            out.flush();
        } catch (IOException ignored) {
            // Procesul este deja oprit sau nu raspunde
        }

        // 2. Daca procesul ruleaza pe aceeasi masina si ii cunoastem PID-ul, aplicam kill la nivel de OS
        long targetPid = deadNode.getPid();
        if (targetPid > 0 && targetPid != this.pid) {
            try {
                String os = System.getProperty("os.name", "").toLowerCase();
                Process proc;
                if (os.contains("win")) {
                    proc = new ProcessBuilder("taskkill", "/F", "/PID", String.valueOf(targetPid)).start();
                } else {
                    proc = new ProcessBuilder("kill", "-9", String.valueOf(targetPid)).start();
                }
                proc.waitFor(2, TimeUnit.SECONDS);
                System.out.printf("[%s] [KILL] Comanda de kill pentru procesul nodului %s (PID=%d) a fost executata.%n",
                        id, deadNode.getId(), targetPid);
            } catch (Exception ignored) {
            }
        }
    }

    public void printClusterStatus() {
        System.out.println("--------------------------------------------------------------------------------");
        System.out.printf("Noduri cunoscute de [%s] (interval=%ds, SUSPECT=%d intervale, CAZUT=%d intervale):%n",
                id, HEARTBEAT_INTERVAL_SECONDS, SUSPECT_INTERVAL_THRESHOLD, CAZUT_INTERVAL_THRESHOLD);
        clusterNodes.values().stream()
                .sorted(Comparator.comparing(NodeInfo::getId))
                .forEach(n -> {
                    long elapsedSec = Math.max(0L, (System.currentTimeMillis() - n.getLastHeartbeat()) / 1000L);
                    String role = n.isCoordinator() ? " [COORDONATOR]" : "";
                    System.out.printf("  - %-4s | %-15s | stare: %-7s | ratate: %d/%d | ultimul HB: acum %2ds | PID: %d%s%n",
                            n.getId(), n.getAddress(), n.getState(), n.getMissedIntervals(),
                            CAZUT_INTERVAL_THRESHOLD, elapsedSec, n.getPid(), role);
                });
        System.out.println("--------------------------------------------------------------------------------");
    }

    public void stop() {
        running = false;
        scheduler.shutdownNow();
        tcpPool.shutdownNow();
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException ignored) {
        }
        if (multicastSocket != null) {
            multicastSocket.close();
        }
    }

    public String getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public Path getStorageDir() {
        return storageDir;
    }

    public boolean isCoordinator() {
        return coordinator;
    }

    public Map<UUID, FileMetadata> getFileMetadataTable() {
        return fileMetadataTable;
    }

    public Map<String, NodeInfo> getClusterNodes() {
        return clusterNodes;
    }

    public static void main(String[] args) {
        String id = null;
        String host = "localhost";
        int port = -1;
        String dir = null;
        boolean coordinator = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--id":
                    if (i + 1 < args.length) {
                        id = args[++i];
                    }
                    break;
                case "--host":
                    if (i + 1 < args.length) {
                        host = args[++i];
                    }
                    break;
                case "--port":
                    if (i + 1 < args.length) {
                        try {
                            port = Integer.parseInt(args[++i]);
                        } catch (NumberFormatException e) {
                            System.err.println("Eroare: port invalid: " + args[i]);
                            printUsageAndExit();
                        }
                    }
                    break;
                case "--dir":
                    if (i + 1 < args.length) {
                        dir = args[++i];
                    }
                    break;
                case "--coordinator":
                    coordinator = true;
                    break;
                default:
                    System.err.println("Argument necunoscut: " + args[i]);
                    printUsageAndExit();
                    break;
            }
        }

        if (id == null || id.trim().isEmpty() || port <= 0 || port > 65535 || dir == null || dir.trim().isEmpty()) {
            printUsageAndExit();
            return;
        }

        StorageNode node = new StorageNode(id, host, port, dir, coordinator);
        try {
            node.start();
        } catch (IOException e) {
            System.err.printf("[%s] Eroare la pornirea nodului pe portul %d / directorul %s: %s%n",
                    id, port, dir, e.getMessage());
            System.exit(1);
        }

        // Consola locala pe nod pentru inspectie rapida sau simulare blocare (freeze)
        try (Scanner scanner = new Scanner(System.in)) {
            while (scanner.hasNextLine()) {
                String cmd = scanner.nextLine().trim().toLowerCase();
                switch (cmd) {
                    case "nodes":
                    case "status":
                        node.printClusterStatus();
                        break;
                    case "freeze":
                    case "pause":
                        node.heartbeatPaused = true;
                        System.out.printf("[%s] Heartbeat oprit manual (freeze). Dupa %d intervale (%ds) va fi declarat CAZUT si oprit fortat.%n",
                                node.getId(), CAZUT_INTERVAL_THRESHOLD, HEARTBEAT_INTERVAL_SECONDS * CAZUT_INTERVAL_THRESHOLD);
                        break;
                    case "resume":
                    case "unfreeze":
                        node.heartbeatPaused = false;
                        node.emitHeartbeatCycle();
                        System.out.printf("[%s] Heartbeat reluat manual.%n", node.getId());
                        break;
                    case "exit":
                    case "quit":
                        System.out.printf("[%s] Oprire nod...%n", node.getId());
                        node.stop();
                        return;
                    case "":
                        break;
                    default:
                        System.out.println("Comenzi nod disponibile: nodes, freeze, resume, exit");
                        break;
                }
            }
        } catch (Exception ignored) {
        }

        // Mentine procesul activ daca ruleaza in fundal fara stdin
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void printUsageAndExit() {
        System.err.println("Utilizare: java -cp target/classes ro.ubm.sd.node.StorageNode "
                + "--id <id> --port <port> --dir <director> [--coordinator] [--host <host>]");
        System.exit(1);
    }
}
