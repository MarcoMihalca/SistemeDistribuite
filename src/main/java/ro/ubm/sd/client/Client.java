package ro.ubm.sd.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

/**
 * Interfata in terminal pentru client.
 * Porneste ca proces separat:
 * java -cp target/classes ro.ubm.sd.client.Client --coordinator localhost:7001
 */
public class Client {

    private final String coordinatorAddress;
    private final String coordinatorHost;
    private final int coordinatorPort;

    public Client(String coordinatorAddress) {
        this.coordinatorAddress = coordinatorAddress;
        String[] parts = coordinatorAddress.split(":");
        this.coordinatorHost = parts[0];
        this.coordinatorPort = Integer.parseInt(parts[1]);
    }

    public void start() {
        System.out.printf("[client] coordonator configurat: %s; comenzi: put, get, ls, rm, nodes, freeze <port>, exit%n",
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

                String[] parts = line.split("\\s+");
                String command = parts[0].toLowerCase();

                switch (command) {
                    case "put":
                        if (parts.length < 2) {
                            System.out.println("Utilizare: put <fisier_local>");
                        } else {
                            System.out.printf("[client] Comanda 'put %s' va fi conectata prin TCP in Etapa 2 (coordonator %s).%n",
                                    parts[1], coordinatorAddress);
                        }
                        break;
                    case "get":
                        if (parts.length < 2) {
                            System.out.println("Utilizare: get <nume_sau_id> [director_destinatie]");
                        } else {
                            String outDir = parts.length >= 3 ? parts[2] : ".";
                            System.out.printf("[client] Comanda 'get %s %s' va fi conectata prin TCP in Etapa 2 (coordonator %s).%n",
                                    parts[1], outDir, coordinatorAddress);
                        }
                        break;
                    case "ls":
                        System.out.printf("[client] Comanda 'ls' va interoga coordonatorul (%s) in Etapa 2.%n",
                                coordinatorAddress);
                        break;
                    case "rm":
                        if (parts.length < 2) {
                            System.out.println("Utilizare: rm <nume_sau_id>");
                        } else {
                            System.out.printf("[client] Comanda 'rm %s' va fi trimisa coordonatorului (%s) in Etapa 2.%n",
                                    parts[1], coordinatorAddress);
                        }
                        break;
                    case "nodes":
                        queryNodesStatus();
                        break;
                    case "freeze":
                        if (parts.length < 2) {
                            System.out.println("Utilizare: freeze <port_nod> (ex: freeze 7002 - opreste heartbeat-ul nodului pt test)");
                        } else {
                            sendTestCommandToNode(parts[1], "FREEZE");
                        }
                        break;
                    case "unfreeze":
                    case "resume":
                        if (parts.length < 2) {
                            System.out.println("Utilizare: unfreeze <port_nod> (ex: unfreeze 7002)");
                        } else {
                            sendTestCommandToNode(parts[1], "UNFREEZE");
                        }
                        break;
                    case "exit":
                    case "quit":
                        System.out.println("[client] Inchidere client.");
                        return;
                    default:
                        System.out.println("Comanda necunoscuta. Comenzi disponibile: put, get, ls, rm, nodes, freeze <port>, unfreeze <port>, exit");
                        break;
                }
            }
        }
    }

    private void queryNodesStatus() {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(coordinatorHost, coordinatorPort), 2000);
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

            writer.println("NODES_STATUS");
            writer.flush();

            String header = reader.readLine();
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
                String line = reader.readLine();
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
                String[] hp = targetArg.split(":");
                host = hp[0];
                port = Integer.parseInt(hp[1]);
            } else {
                port = Integer.parseInt(targetArg);
            }
        } catch (NumberFormatException e) {
            System.out.println("Port invalid: " + targetArg + " (exemplu corect: freeze 7002)");
            return;
        }

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 2000);
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            writer.println(command);
            writer.flush();
            String reply = reader.readLine();
            System.out.printf("[client] Raspuns de la %s:%d -> %s%n", host, port, reply);
        } catch (IOException e) {
            System.out.printf("[client] Nu s-a putut contacta nodul %s:%d: %s%n", host, port, e.getMessage());
        }
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

        for (int i = 0; i < args.length; i++) {
            if ("--coordinator".equals(args[i]) && i + 1 < args.length) {
                coordinator = args[++i];
            } else {
                System.err.println("Argument necunoscut sau incomplet: " + args[i]);
                printUsageAndExit();
            }
        }

        if (coordinator == null || coordinator.trim().isEmpty() || !coordinator.contains(":")) {
            printUsageAndExit();
            return;
        }

        Client client = new Client(coordinator);
        client.start();
    }

    private static void printUsageAndExit() {
        System.err.println("Utilizare: java -cp target/classes ro.ubm.sd.client.Client --coordinator <host:port>");
        System.exit(1);
    }
}
