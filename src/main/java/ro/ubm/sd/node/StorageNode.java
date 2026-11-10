package ro.ubm.sd.node;

import ro.ubm.sd.common.FileMetadata;
import ro.ubm.sd.common.NodeInfo;
import ro.ubm.sd.common.NodeState;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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

    private volatile boolean running = true;
    private volatile boolean heartbeatPaused = false;
    private ServerSocket serverSocket;
    private MulticastSocket multicastSocket;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "heartbeat-scheduler");
        t.setDaemon(true);
        return t;
    });

    // Tabelele mentinute in memorie de coordonator (Pasul 3)
    private final Map<UUID, FileMetadata> fileMetadataTable = new ConcurrentHashMap<>();
    private final Map<String, NodeInfo> clusterNodes = new ConcurrentHashMap<>();

    public StorageNode(String id, String host, int port, String storageDirText, boolean coordinator) {
        this.id = id;
        this.host = host;
        this.port = port;
        this.storageDirText = storageDirText;
        this.storageDir = Paths.get(storageDirText);
        this.coordinator = coordinator;
        this.pid = resolveCurrentPid();
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
                Thread handler = new Thread(() -> handleTcpConnection(clientSocket), "tcp-handler-" + id);
                handler.setDaemon(true);
                handler.start();
            } catch (IOException e) {
                if (running) {
                    System.err.printf("[%s] Eroare la acceptarea conexiunii TCP: %s%n", id, e.getMessage());
                }
            }
        }
    }

    private void handleTcpConnection(Socket socket) {
        try (Socket s = socket;
             BufferedReader reader = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
             PrintWriter writer = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true)) {

            String line = reader.readLine();
            if (line == null || line.trim().isEmpty()) {
                return;
            }

            String[] parts = line.trim().split("\\s+");
            String command = parts[0].toUpperCase();

            switch (command) {
                case "HEARTBEAT": {
                    handleHeartbeatMessage(parts);
                    writer.println("OK");
                    writer.flush();
                    break;
                }
                case "NODES_STATUS": {
                    List<NodeInfo> sorted = clusterNodes.values().stream()
                            .sorted(Comparator.comparing(NodeInfo::getId))
                            .collect(Collectors.toList());
                    String coordId = sorted.stream()
                            .filter(NodeInfo::isCoordinator)
                            .map(NodeInfo::getId)
                            .findFirst()
                            .orElse("necunoscut");

                    writer.println(String.format("CLUSTER_NODES %d %s %d %d",
                            sorted.size(), coordId, HEARTBEAT_INTERVAL_SECONDS, CAZUT_INTERVAL_THRESHOLD));
                    for (NodeInfo n : sorted) {
                        writer.println(String.format("%s %s:%d %s %d %d %b %d",
                                n.getId(),
                                n.getHost(),
                                n.getPort(),
                                n.getState().name(),
                                n.getLastHeartbeat(),
                                n.getMissedIntervals(),
                                n.isCoordinator(),
                                n.getPid()));
                    }
                    writer.flush();
                    break;
                }
                case "FREEZE": {
                    heartbeatPaused = true;
                    System.out.printf("[%s] [TEST] Emiterea de HEARTBEAT a fost OPRITA (freeze). Procesul (PID=%d) asteapta detectia...%n", id, pid);
                    writer.println("FROZEN " + id);
                    writer.flush();
                    break;
                }
                case "UNFREEZE": {
                    heartbeatPaused = false;
                    System.out.printf("[%s] [TEST] Emiterea de HEARTBEAT a fost RELUATA.%n", id);
                    emitHeartbeatCycle();
                    writer.println("UNFROZEN " + id);
                    writer.flush();
                    break;
                }
                case "FORCE_KILL": {
                    System.err.printf("[%s] [KILL] Primit semnal FORCE_KILL de la coordonator (5 intervale ratate). Oprire proces (PID=%d)...%n", id, pid);
                    writer.println("KILLED " + id);
                    writer.flush();
                    System.exit(137);
                    break;
                }
                default: {
                    writer.println("ERROR UNKNOWN_COMMAND");
                    writer.flush();
                    break;
                }
            }
        } catch (Throwable t) {
            if (running) {
                System.err.printf("[%s] Eroare la procesarea comenzii TCP: %s%n", id, t.toString());
            }
        }
    }

    @SuppressWarnings("deprecation")
    private void startMulticastReceiver() {
        try {
            multicastSocket = new MulticastSocket(MULTICAST_PORT);
            multicastSocket.setReuseAddress(true);
            InetAddress group = InetAddress.getByName(MULTICAST_GROUP);
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
                    InetAddress group = InetAddress.getByName(MULTICAST_GROUP);
                    DatagramPacket packet = new DatagramPacket(data, data.length, group, MULTICAST_PORT);
                    multicastSocket.send(packet);
                } catch (IOException ignored) {
                }
            }

            // 2. Emite prin TCP catre nodurile cunoscute si porturile locale standard (garanteaza functionarea pe Windows localhost)
            Set<String> targets = new HashSet<>();
            for (int localPort : DEFAULT_LOCAL_PORTS) {
                if (localPort != this.port) {
                    targets.add(this.host + ":" + localPort);
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
