import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class Master {
    private final Map<String, Set<String>> resourcesTable = new HashMap<String, Set<String>>();
    private final ConcurrentMap<String, Integer> peerPorts = new ConcurrentHashMap<String, Integer>();
    private final ConcurrentMap<String, String> peerHosts = new ConcurrentHashMap<String, String>();
    private final List<String> downloadLog = Collections.synchronizedList(new ArrayList<String>());

    private final Object tokenLock = new Object();
    private final Set<String> busyPeers = new HashSet<String>();
    private final Map<String, String> tokenToPeer = new HashMap<String, String>();

    private final AtomicLong peerIdCounter = new AtomicLong(0);
    private final int port;
    private final ExecutorService pool = Executors.newCachedThreadPool();

    private ServerSocket serverSocket;
    private volatile boolean running = true;

    public Master(int port) {
        this.port = port;
    }

    public void startServer() {
        Thread consoleThread = new Thread(new Runnable() {
            @Override
            public void run() {
                handleConsole();
            }
        }, "master-console");
        consoleThread.setDaemon(true);
        consoleThread.start();

        try {
            serverSocket = new ServerSocket(port);
            System.out.println("[Aggregatore] In ascolto sulla porta " + port);
            System.out.println("Comandi aggregatore: listdata, log, quit");

            while (running) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    String peerName = "peer" + peerIdCounter.getAndIncrement();

                    MasterHandler handler = new MasterHandler(
                            clientSocket,
                            peerName,
                            resourcesTable,
                            peerPorts,
                            peerHosts,
                            downloadLog,
                            tokenLock,
                            busyPeers,
                            tokenToPeer
                    );

                    pool.submit(handler);
                } catch (IOException e) {
                    if (running) {
                        System.err.println("Errore accept: " + e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Errore ServerSocket: " + e.getMessage());
        } finally {
            stopServer();
        }
    }

    private void handleConsole() {
        try (Scanner scanner = new Scanner(System.in)) {
            while (running) {
                if (!scanner.hasNextLine()) {
                    break;
                }

                String command = scanner.nextLine().trim().toLowerCase(Locale.ROOT);

                switch (command) {
                    case "listdata":
                        printAvailableData();
                        break;
                    case "log":
                        printLog();
                        break;
                    case "quit":
                        System.out.println("Arresto dell'aggregatore...");
                        stopServer();
                        break;
                    case "":
                        break;
                    default:
                        System.out.println("Comando non riconosciuto. Comandi validi: listdata, log, quit");
                        break;
                }
            }
        }
    }

    private void printAvailableData() {
        synchronized (resourcesTable) {
            System.out.println("Risorse:");
            boolean found = false;

            List<String> resourceNames = new ArrayList<String>(resourcesTable.keySet());
            Collections.sort(resourceNames);

            for (String res : resourceNames) {
                List<String> activeOwners = activeOwnersOf(res);
                if (!activeOwners.isEmpty()) {
                    found = true;
                    System.out.println("- " + res + ": " + joinStrings(activeOwners, ", "));
                }
            }

            if (!found) {
                System.out.println("(Nessuna rilevazione disponibile sulla rete)");
            }
        }
    }

    private List<String> activeOwnersOf(String resName) {
        Set<String> owners = resourcesTable.get(resName);
        List<String> activeOwners = new ArrayList<String>();

        if (owners != null) {
            for (String peer : owners) {
                if (peerPorts.containsKey(peer)) {
                    activeOwners.add(peer);
                }
            }
        }

        Collections.sort(activeOwners);
        return activeOwners;
    }

    private void printLog() {
        System.out.println("Risorse scaricate:");
        synchronized (downloadLog) {
            if (downloadLog.isEmpty()) {
                System.out.println("(Nessuna richiesta di download registrata)");
                return;
            }
            for (String row : downloadLog) {
                System.out.println(row);
            }
        }
    }

    public void stopServer() {
        running = false;

        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException ignored) {
        }

        pool.shutdownNow();

        synchronized (tokenLock) {
            tokenLock.notifyAll();
        }
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
        if (args.length != 1) {
            System.out.println("Uso: java Master <porta>");
            return;
        }

        try {
            int port = Integer.parseInt(args[0]);
            new Master(port).startServer();
        } catch (NumberFormatException e) {
            System.err.println("La porta deve essere un numero intero.");
        }
    }
}
