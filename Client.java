import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Client {
    private final String masterHost;
    private final int masterPort;
    private int p2pPort;

    private final Map<String, String> localResources = new ConcurrentHashMap<String, String>();
    private final Object downloadLock = new Object();
    private final ExecutorService p2pPool = Executors.newCachedThreadPool();

    private String peerName;
    private Socket masterSocket;
    private PrintWriter outMaster;
    private BufferedReader inMaster;
    private ServerSocket p2pServerSocket;
    private volatile boolean running = true;

    public Client(String masterHost, int masterPort) {
        this(masterHost, masterPort, 0);
    }

    public Client(String masterHost, int masterPort, int p2pPort) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.p2pPort = p2pPort;
    }

    public void start() {
        try {
            masterSocket = new Socket(masterHost, masterPort);
            outMaster = new PrintWriter(masterSocket.getOutputStream(), true);
            inMaster = new BufferedReader(new InputStreamReader(masterSocket.getInputStream()));

            String welcome = inMaster.readLine();
            if (welcome == null || !welcome.startsWith("WELCOME ")) {
                System.err.println("Risposta non valida dall'aggregatore.");
                return;
            }

            peerName = welcome.split(" ", 2)[1];
            System.out.println("Connesso come: " + peerName);

            p2pServerSocket = new ServerSocket(p2pPort);
            p2pPort = p2pServerSocket.getLocalPort();
            System.out.println("Server P2P attivo sulla porta: " + p2pPort);

            Thread p2pServerThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    runP2PServer();
                }
            }, "p2p-server-" + peerName);
            p2pServerThread.setDaemon(true);
            p2pServerThread.start();

            registerToMaster();
            handleCLI();
        } catch (IOException e) {
            System.err.println("Impossibile connettersi all'aggregatore: " + e.getMessage());
        } finally {
            cleanup();
        }
    }

    private void registerToMaster() throws IOException {
        String resources;
        if (localResources.isEmpty()) {
            resources = "EMPTY";
        } else {
            resources = joinStrings(localResources.keySet(), ",");
        }

        outMaster.println("REGISTER " + p2pPort + " " + resources);
        String ack = inMaster.readLine();

        if (!"REGISTER_ACK".equals(ack)) {
            throw new IOException("registrazione fallita");
        }
    }

    private void runP2PServer() {
        while (running) {
            try {
                Socket p2pClient = p2pServerSocket.accept();
                p2pPool.submit(new PeerToPeer(p2pClient, localResources, downloadLock));
            } catch (IOException e) {
                if (running) {
                    System.err.println("Errore server P2P: " + e.getMessage());
                }
            }
        }
    }

    private void handleCLI() {
        try (Scanner scanner = new Scanner(System.in)) {
            printHelp();

            while (running) {
                System.out.print("> ");

                if (!scanner.hasNextLine()) {
                    break;
                }

                String input = scanner.nextLine().trim();
                if (input.isEmpty()) {
                    continue;
                }

                String[] parts = input.split(" ", 3);
                String command = parts[0].toLowerCase(Locale.ROOT);

                try {
                    if ("help".equals(command)) {
                        printHelp();
                    } else if ("listdata".equals(command) && parts.length >= 2) {
                        if ("local".equalsIgnoreCase(parts[1])) {
                            executeLocalList();
                        } else if ("remote".equalsIgnoreCase(parts[1])) {
                            executeRemoteList();
                        } else {
                            System.out.println("Uso: listdata local | listdata remote");
                        }
                    } else if ("listpeers".equals(command)) {
                        executeListPeers();
                    } else if ("owners".equals(command) && parts.length >= 2) {
                        executeOwners(parts[1]);
                    } else if ("add".equals(command) && parts.length >= 3) {
                        executeAdd(parts[1], parts[2]);
                    } else if ("download".equals(command) && parts.length >= 2) {
                        executeRobustDownload(parts[1]);
                    } else if ("quit".equals(command)) {
                        outMaster.println("QUIT");
                        running = false;
                    } else {
                        System.out.println("Comando non valido. Digita help per vedere i comandi disponibili.");
                    }
                } catch (IOException e) {
                    System.err.println("Errore di comunicazione con l'aggregatore: " + e.getMessage());
                    running = false;
                }
            }
        }
    }

    private void printHelp() {
        System.out.println("Comandi disponibili:");
        System.out.println("- listdata local");
        System.out.println("- listdata remote");
        System.out.println("- listpeers");
        System.out.println("- owners <nome risorsa>");
        System.out.println("- add <nome risorsa> <contenuto>");
        System.out.println("- download <nome risorsa>");
        System.out.println("- quit");
    }

    private void executeLocalList() {
        System.out.println("Risorse:");
        if (localResources.isEmpty()) {
            System.out.println("(Nessuna risorsa locale)");
            return;
        }

        List<String> names = new ArrayList<String>(localResources.keySet());
        Collections.sort(names);
        for (String resource : names) {
            System.out.println("- " + resource);
        }
    }

    private void executeRemoteList() throws IOException {
        outMaster.println("GET_REMOTE_LIST");
        String response = inMaster.readLine();

        if (response == null || !response.startsWith("REMOTE_LIST ")) {
            System.out.println("Risposta non valida dall'aggregatore.");
            return;
        }

        String data = response.substring("REMOTE_LIST ".length());
        System.out.println("Risorse:");

        if ("EMPTY".equals(data)) {
            System.out.println("(Nessuna risorsa remota disponibile sulla rete)");
            return;
        }

        String[] items = data.split(";");
        for (String item : items) {
            if (item.trim().isEmpty()) {
                continue;
            }

            String[] kv = item.split(":", 2);
            if (kv.length == 2) {
                System.out.println("- " + kv[0] + ": " + kv[1]);
            }
        }
    }

    private void executeListPeers() throws IOException {
        outMaster.println("GET_PEERS");
        String response = inMaster.readLine();

        if (response == null || !response.startsWith("PEERS ")) {
            System.out.println("Risposta non valida dall'aggregatore.");
            return;
        }

        String data = response.substring("PEERS ".length());
        System.out.println("Peer attivi:");

        if ("EMPTY".equals(data)) {
            System.out.println("(Nessun altro peer attivo)");
            return;
        }

        String[] peers = data.split(",");
        for (String peer : peers) {
            System.out.println("- " + peer);
        }
    }

    private void executeOwners(String resName) throws IOException {
        outMaster.println("GET_OWNERS " + resName);
        String response = inMaster.readLine();

        if (response == null || !response.startsWith("OWNERS ")) {
            System.out.println("Risposta non valida dall'aggregatore.");
            return;
        }

        String data = response.substring("OWNERS ".length());

        if ("EMPTY".equals(data)) {
            System.out.println("Nessun peer attivo possiede la risorsa " + resName + ".");
            return;
        }

        System.out.println("La risorsa " + resName + " e' posseduta da:");
        String[] peers = data.split(",");
        for (String peer : peers) {
            System.out.println("- " + peer);
        }
    }

    private void executeAdd(String resName, String content) throws IOException {
        localResources.put(resName, content);

        outMaster.println("ADD_RESOURCE " + resName);
        String ack = inMaster.readLine();

        if ("ADD_ACK".equals(ack)) {
            System.out.println("Risorsa " + resName + " aggiunta localmente e notificata all'aggregatore.");
        } else {
            System.out.println("Risorsa aggiunta localmente, ma notifica all'aggregatore non confermata.");
        }
    }

    private void executeRobustDownload(String resName) {
        if (localResources.containsKey(resName)) {
            System.out.println("Risorsa gia' presente localmente.");
            return;
        }

        while (running) {
            String token = null;
            String targetPeer = null;

            try {
                outMaster.println("REQ_DOWNLOAD " + resName);
                String response = inMaster.readLine();

                if (response == null) {
                    System.out.println("Connessione con l'aggregatore chiusa.");
                    return;
                }

                if ("NOT_FOUND".equals(response)) {
                    System.out.println("La rilevazione non e' disponibile sulla rete.");
                    return;
                }

                if (!response.startsWith("DOWNLOAD_SOURCE ")) {
                    System.out.println("Risposta non valida dall'aggregatore: " + response);
                    return;
                }

                String[] parts = response.split(" ", 5);
                if (parts.length < 5) {
                    System.out.println("Risposta incompleta dall'aggregatore.");
                    return;
                }

                token = parts[1];
                targetPeer = parts[2];
                String targetHost = parts[3];
                int targetPort = Integer.parseInt(parts[4]);

                try (Socket p2pSocket = new Socket(targetHost, targetPort);
                     PrintWriter outP2P = new PrintWriter(p2pSocket.getOutputStream(), true);
                     BufferedReader inP2P = new BufferedReader(new InputStreamReader(p2pSocket.getInputStream()))) {

                    outP2P.println("REQUEST " + resName);
                    String p2pResponse = inP2P.readLine();

                    if (p2pResponse != null && p2pResponse.startsWith("CONTENT ")) {
                        String fileContent = p2pResponse.substring("CONTENT ".length());
                        localResources.put(resName, fileContent);

                        outMaster.println("LOG_SUCCESS " + resName + " " + targetPeer + " " + token);
                        inMaster.readLine();

                        System.out.println("Download completato con successo da " + targetPeer + ".");
                        return;
                    }

                    System.out.println("Il peer " + targetPeer + " non possiede piu' la risorsa. Provo un altro nodo...");
                    outMaster.println("DOWNLOAD_FAILED " + resName + " " + targetPeer + " " + token + " RESOURCE_NOT_FOUND");
                    inMaster.readLine();
                } catch (IOException p2pException) {
                    System.out.println("Connessione fallita con " + targetPeer + ". Provo un altro nodo...");
                    outMaster.println("DOWNLOAD_FAILED " + resName + " " + targetPeer + " " + token + " CONNECTION_FAILED");
                    inMaster.readLine();
                }
            } catch (IOException e) {
                System.err.println("Errore di rete durante il download: " + e.getMessage());
                return;
            } catch (NumberFormatException e) {
                System.err.println("Porta P2P non valida ricevuta dall'aggregatore.");
                if (token != null && targetPeer != null) {
                    try {
                        outMaster.println("DOWNLOAD_FAILED " + resName + " " + targetPeer + " " + token + " INVALID_PORT");
                        inMaster.readLine();
                    } catch (IOException ignored) {
                    }
                }
                return;
            }
        }
    }

    private void cleanup() {
        running = false;

        try {
            if (p2pServerSocket != null && !p2pServerSocket.isClosed()) {
                p2pServerSocket.close();
            }
            if (masterSocket != null && !masterSocket.isClosed()) {
                masterSocket.close();
            }
        } catch (IOException ignored) {
        }

        p2pPool.shutdownNow();
    }

    private static String joinStrings(Collection<String> values, String separator) {
        StringBuilder sb = new StringBuilder();
        for (String value : values) {
            if (sb.length() > 0) {
                sb.append(separator);
            }
            sb.append(value);
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        if (args.length < 2 || args.length > 3) {
            System.out.println("Uso: java Client <IP_Aggregatore> <Porta_Aggregatore> [Porta_P2P_Locale]");
            return;
        }

        try {
            String host = args[0];
            int masterPort = Integer.parseInt(args[1]);
            int p2pPort = args.length == 3 ? Integer.parseInt(args[2]) : 0;

            new Client(host, masterPort, p2pPort).start();
        } catch (NumberFormatException e) {
            System.err.println("Le porte devono essere numeri interi.");
        }
    }
}
