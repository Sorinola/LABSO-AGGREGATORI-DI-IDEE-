import java.io.*;
import java.net.Socket;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

public class MasterHandler implements Runnable {
    private final Socket socket;
    private final String peerName;

    private final Map<String, Set<String>> resourcesTable;
    private final ConcurrentMap<String, Integer> peerPorts;
    private final ConcurrentMap<String, String> peerHosts;
    private final List<String> downloadLog;

    private final Object tokenLock;
    private final Set<String> busyPeers;
    private final Map<String, String> tokenToPeer;
    private final Set<String> tokensHeldByThisPeer = new HashSet<String>();

    public MasterHandler(
            Socket socket,
            String peerName,
            Map<String, Set<String>> resourcesTable,
            ConcurrentMap<String, Integer> peerPorts,
            ConcurrentMap<String, String> peerHosts,
            List<String> downloadLog,
            Object tokenLock,
            Set<String> busyPeers,
            Map<String, String> tokenToPeer
    ) {
        this.socket = socket;
        this.peerName = peerName;
        this.resourcesTable = resourcesTable;
        this.peerPorts = peerPorts;
        this.peerHosts = peerHosts;
        this.downloadLog = downloadLog;
        this.tokenLock = tokenLock;
        this.busyPeers = busyPeers;
        this.tokenToPeer = tokenToPeer;
    }

    @Override
    public void run() {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            out.println("WELCOME " + peerName);

            String line;
            while ((line = in.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }

                String[] parts = line.split(" ", 5);
                String command = parts[0];

                switch (command) {
                    case "REGISTER":
                        handleRegister(parts, out);
                        break;
                    case "ADD_RESOURCE":
                        handleAddResource(parts, out);
                        break;
                    case "GET_REMOTE_LIST":
                        handleRemoteList(out);
                        break;
                    case "GET_PEERS":
                        handleGetPeers(out);
                        break;
                    case "GET_OWNERS":
                        handleGetOwners(parts, out);
                        break;
                    case "REQ_DOWNLOAD":
                        handleDownloadRequest(parts, out);
                        break;
                    case "DOWNLOAD_FAILED":
                        handleDownloadFailed(parts, out);
                        break;
                    case "LOG_SUCCESS":
                        handleLogSuccess(parts, out);
                        break;
                    case "QUIT":
                        return;
                    default:
                        out.println("ERROR comando non riconosciuto");
                        break;
                }
            }
        } catch (IOException e) {
            System.err.println("[Aggregatore] Connessione interrotta con " + peerName + ": " + e.getMessage());
        } finally {
            cleanup();
        }
    }

    private void handleRegister(String[] parts, PrintWriter out) {
        if (parts.length < 2) {
            out.println("ERROR REGISTER non valido");
            return;
        }

        try {
            int p2pPort = Integer.parseInt(parts[1]);
            String host = socket.getInetAddress().getHostAddress();

            peerPorts.put(peerName, p2pPort);
            peerHosts.put(peerName, host);

            if (parts.length >= 3 && !"EMPTY".equals(parts[2])) {
                String[] resources = parts[2].split(",");
                synchronized (resourcesTable) {
                    for (String resource : resources) {
                        String cleaned = resource.trim();
                        if (!cleaned.isEmpty()) {
                            Set<String> owners = resourcesTable.get(cleaned);
                            if (owners == null) {
                                owners = new HashSet<String>();
                                resourcesTable.put(cleaned, owners);
                            }
                            owners.add(peerName);
                        }
                    }
                }
            }

            out.println("REGISTER_ACK");
        } catch (NumberFormatException e) {
            out.println("ERROR porta P2P non valida");
        }
    }

    private void handleAddResource(String[] parts, PrintWriter out) {
        if (parts.length < 2) {
            out.println("ERROR ADD_RESOURCE non valido");
            return;
        }

        String resName = parts[1];
        synchronized (resourcesTable) {
            Set<String> owners = resourcesTable.get(resName);
            if (owners == null) {
                owners = new HashSet<String>();
                resourcesTable.put(resName, owners);
            }
            owners.add(peerName);
        }

        out.println("ADD_ACK");
    }

    private void handleRemoteList(PrintWriter out) {
        StringBuilder sb = new StringBuilder();

        synchronized (resourcesTable) {
            List<String> resourceNames = new ArrayList<String>(resourcesTable.keySet());
            Collections.sort(resourceNames);

            for (String res : resourceNames) {
                List<String> activeOwners = getActiveOwners(res, true);
                if (!activeOwners.isEmpty()) {
                    sb.append(res)
                            .append(":")
                            .append(joinStrings(activeOwners, ","))
                            .append(";");
                }
            }
        }

        out.println("REMOTE_LIST " + (sb.length() == 0 ? "EMPTY" : sb.toString()));
    }

    private void handleGetPeers(PrintWriter out) {
        List<String> peers = new ArrayList<String>();

        for (String peer : peerPorts.keySet()) {
            if (!peer.equals(peerName)) {
                peers.add(peer);
            }
        }

        Collections.sort(peers);
        out.println("PEERS " + (peers.isEmpty() ? "EMPTY" : joinStrings(peers, ",")));
    }

    private void handleGetOwners(String[] parts, PrintWriter out) {
        if (parts.length < 2) {
            out.println("ERROR GET_OWNERS non valido");
            return;
        }

        String resName = parts[1];
        List<String> activeOwners;

        synchronized (resourcesTable) {
            activeOwners = getActiveOwners(resName, true);
        }

        out.println("OWNERS " + (activeOwners.isEmpty() ? "EMPTY" : joinStrings(activeOwners, ",")));
    }

    private void handleDownloadRequest(String[] parts, PrintWriter out) {
        if (parts.length < 2) {
            out.println("ERROR REQ_DOWNLOAD non valido");
            return;
        }

        String resName = parts[1];

        while (true) {
            List<String> candidates;

            synchronized (resourcesTable) {
                candidates = getActiveOwners(resName, true);
            }

            if (candidates.isEmpty()) {
                out.println("NOT_FOUND");
                return;
            }

            synchronized (tokenLock) {
                for (String candidate : candidates) {
                    if (!busyPeers.contains(candidate)) {
                        String token = UUID.randomUUID().toString();
                        busyPeers.add(candidate);
                        tokenToPeer.put(token, candidate);
                        tokensHeldByThisPeer.add(token);

                        String host = peerHosts.get(candidate);
                        Integer port = peerPorts.get(candidate);

                        if (host == null || port == null) {
                            releaseToken(token);
                            break;
                        }

                        out.println("DOWNLOAD_SOURCE " + token + " " + candidate + " " + host + " " + port);
                        return;
                    }
                }

                try {
                    tokenLock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    out.println("NOT_FOUND");
                    return;
                }
            }
        }
    }

    private void handleDownloadFailed(String[] parts, PrintWriter out) {
        if (parts.length < 4) {
            out.println("ERROR DOWNLOAD_FAILED non valido");
            return;
        }

        String resName = parts[1];
        String failedPeer = parts[2];
        String token = parts[3];
        String reason = parts.length >= 5 ? parts[4] : "FAILED";

        synchronized (resourcesTable) {
            Set<String> owners = resourcesTable.get(resName);
            if (owners != null) {
                owners.remove(failedPeer);
            }
        }

        addLog(resName, failedPeer, peerName, false, reason);
        releaseToken(token);
        out.println("FAILED_ACK");
    }

    private void handleLogSuccess(String[] parts, PrintWriter out) {
        if (parts.length < 4) {
            out.println("ERROR LOG_SUCCESS non valido");
            return;
        }

        String resName = parts[1];
        String fromPeer = parts[2];
        String token = parts[3];

        synchronized (resourcesTable) {
            Set<String> owners = resourcesTable.get(resName);
            if (owners == null) {
                owners = new HashSet<String>();
                resourcesTable.put(resName, owners);
            }
            owners.add(peerName);
        }

        addLog(resName, fromPeer, peerName, true, "OK");
        releaseToken(token);
        out.println("LOG_ACK");
    }

    private List<String> getActiveOwners(String resName, boolean excludeCurrentPeer) {
        Set<String> owners = resourcesTable.get(resName);
        List<String> activeOwners = new ArrayList<String>();

        if (owners != null) {
            for (String owner : owners) {
                if (excludeCurrentPeer && owner.equals(peerName)) {
                    continue;
                }
                if (peerPorts.containsKey(owner)) {
                    activeOwners.add(owner);
                }
            }
        }

        Collections.sort(activeOwners);
        return activeOwners;
    }

    private void addLog(String resName, String fromPeer, String toPeer, boolean success, String reason) {
        String timestamp = LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm"));
        String status = success ? "OK" : "FALLITO(" + reason + ")";
        downloadLog.add("- " + timestamp + " " + resName + " da: " + fromPeer + " a: " + toPeer + " " + status);
    }

    private void releaseToken(String token) {
        synchronized (tokenLock) {
            String targetPeer = tokenToPeer.remove(token);
            if (targetPeer != null) {
                busyPeers.remove(targetPeer);
            }
            tokensHeldByThisPeer.remove(token);
            tokenLock.notifyAll();
        }
    }

    private void cleanup() {
        peerPorts.remove(peerName);
        peerHosts.remove(peerName);

        synchronized (tokenLock) {
            for (String token : new HashSet<String>(tokensHeldByThisPeer)) {
                String targetPeer = tokenToPeer.remove(token);
                if (targetPeer != null) {
                    busyPeers.remove(targetPeer);
                }
            }
            tokensHeldByThisPeer.clear();
            tokenLock.notifyAll();
        }

        // Le risorse del peer restano nella tabella dell'aggregatore,
        // ma non sono scaricabili perché il peer non è più attivo.
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
}
