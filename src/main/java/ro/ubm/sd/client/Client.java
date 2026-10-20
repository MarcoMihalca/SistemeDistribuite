package ro.ubm.sd.client;

import java.util.Scanner;

/**
 * Interfata in terminal pentru client.
 * Porneste ca proces separat:
 * java -cp target/classes ro.ubm.sd.client.Client --coordinator localhost:7001
 */
public class Client {

    private final String coordinatorAddress;

    public Client(String coordinatorAddress) {
        this.coordinatorAddress = coordinatorAddress;
    }

    public void start() {
        System.out.printf("[client] coordonator configurat: %s; comenzi: put, get, ls, rm, nodes, exit%n",
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
                    case "put" -> {
                        if (parts.length < 2) {
                            System.out.println("Utilizare: put <fisier_local>");
                        } else {
                            System.out.printf("[client] Comanda 'put %s' va fi conectata prin TCP in Etapa 2 (coordonator %s).%n",
                                    parts[1], coordinatorAddress);
                        }
                    }
                    case "get" -> {
                        if (parts.length < 2) {
                            System.out.println("Utilizare: get <nume_sau_id> [director_destinatie]");
                        } else {
                            String outDir = parts.length >= 3 ? parts[2] : ".";
                            System.out.printf("[client] Comanda 'get %s %s' va fi conectata prin TCP in Etapa 2 (coordonator %s).%n",
                                    parts[1], outDir, coordinatorAddress);
                        }
                    }
                    case "ls" -> System.out.printf("[client] Comanda 'ls' va interoga coordonatorul (%s) in Etapa 2.%n",
                            coordinatorAddress);
                    case "rm" -> {
                        if (parts.length < 2) {
                            System.out.println("Utilizare: rm <nume_sau_id>");
                        } else {
                            System.out.printf("[client] Comanda 'rm %s' va fi trimisa coordonatorului (%s) in Etapa 2.%n",
                                    parts[1], coordinatorAddress);
                        }
                    }
                    case "nodes" -> System.out.printf("[client] Comanda 'nodes' va afisa starea nodurilor de la coordonator (%s) in Etapa 2.%n",
                            coordinatorAddress);
                    case "exit", "quit" -> {
                        System.out.println("[client] Inchidere client.");
                        return;
                    }
                    default -> System.out.println("Comanda necunoscuta. Comenzi disponibile: put, get, ls, rm, nodes, exit");
                }
            }
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

        if (coordinator == null || coordinator.isBlank() || !coordinator.contains(":")) {
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
