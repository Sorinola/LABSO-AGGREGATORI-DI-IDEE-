import java.io.*;
import java.net.Socket;
import java.util.Map;

public class PeerToPeer implements Runnable {
    private final Socket clientSocket;
    private final Map<String, String> localResources;
    private final Object downloadLock;

    public PeerToPeer(Socket clientSocket, Map<String, String> localResources, Object downloadLock) {
        this.clientSocket = clientSocket;
        this.localResources = localResources;
        this.downloadLock = downloadLock;
    }

    @Override
    public void run() {
        synchronized (downloadLock) {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                 PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

                String request = in.readLine();

                if (request == null || !request.startsWith("REQUEST ")) {
                    out.println("ERROR richiesta non valida");
                    return;
                }

                String resName = request.substring("REQUEST ".length()).trim();
                String content = localResources.get(resName);

                if (content != null) {
                    out.println("CONTENT " + content);
                    System.out.println("[P2P] Servita risorsa: " + resName);
                } else {
                    out.println("ERRORE_NON_TROVATO");
                    System.err.println("[P2P] Risorsa richiesta non trovata: " + resName);
                }
            } catch (IOException e) {
                System.err.println("[P2P] Errore di connessione: " + e.getMessage());
            } finally {
                try {
                    if (!clientSocket.isClosed()) {
                        clientSocket.close();
                    }
                } catch (IOException ignored) {
                }
            }
        }
    }
}
