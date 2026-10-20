package ro.ubm.sd.node;

import ro.ubm.sd.common.FileMetadata;
import ro.ubm.sd.common.NodeInfo;
import ro.ubm.sd.common.NodeState;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Nod de stocare din sistemul distribuit (poate avea si rolul de coordonator).
 * Porneste ca proces separat si isi primeste configuratia din linia de comanda:
 * --id <id> --port <port> --dir <director> [--coordinator] [--host <host>]
 */
public class StorageNode {

    private final String id;
    private final String host;
    private final int port;
    private final String storageDirText;
    private final Path storageDir;
    private volatile boolean coordinator;

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
    }

    public void start() throws IOException {
        // Creeaza directorul de stocare local daca lipseste
        Files.createDirectories(storageDir);

        // Inregistreaza nodul curent in tabela locala
        NodeInfo selfInfo = new NodeInfo(id, host, port, NodeState.ACTIV, System.currentTimeMillis(), coordinator);
        clusterNodes.put(id, selfInfo);

        String roleDescription = coordinator ? "coordonator + nod de stocare" : "nod de stocare";
        System.out.printf("[%s] pornit: port %d, director %s, rol: %s%n",
                id, port, storageDirText, roleDescription);
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
                case "--id" -> {
                    if (i + 1 < args.length) {
                        id = args[++i];
                    }
                }
                case "--host" -> {
                    if (i + 1 < args.length) {
                        host = args[++i];
                    }
                }
                case "--port" -> {
                    if (i + 1 < args.length) {
                        try {
                            port = Integer.parseInt(args[++i]);
                        } catch (NumberFormatException e) {
                            System.err.println("Eroare: port invalid: " + args[i]);
                            printUsageAndExit();
                        }
                    }
                }
                case "--dir" -> {
                    if (i + 1 < args.length) {
                        dir = args[++i];
                    }
                }
                case "--coordinator" -> coordinator = true;
                default -> {
                    System.err.println("Argument necunoscut: " + args[i]);
                    printUsageAndExit();
                }
            }
        }

        if (id == null || id.isBlank() || port <= 0 || port > 65535 || dir == null || dir.isBlank()) {
            printUsageAndExit();
            return;
        }

        StorageNode node = new StorageNode(id, host, port, dir, coordinator);
        try {
            node.start();
        } catch (IOException e) {
            System.err.printf("[%s] Eroare la crearea directorului %s: %s%n", id, dir, e.getMessage());
            System.exit(1);
        }
    }

    private static void printUsageAndExit() {
        System.err.println("Utilizare: java -cp target/classes ro.ubm.sd.node.StorageNode "
                + "--id <id> --port <port> --dir <director> [--coordinator] [--host <host>]");
        System.exit(1);
    }
}
