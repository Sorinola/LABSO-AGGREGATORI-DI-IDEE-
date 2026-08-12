// Main.java
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    private static final int MASTER_PORT = 9000;
    private static final String MASTER_HOST = "127.0.0.1";
    private static final int PEER1_PORT = 9001;
    private static final int PEER2_PORT = 9002;

    public static void main(String[] args) {
        System.out.println("=== Avvio della simulazione dell'infrastruttura di rete ===");

        // 1. Avvia il server dell'aggregatore (Master)
        ExecutorService masterExecutor = Executors.newSingleThreadExecutor();
        masterExecutor.submit(() -> {
            new Master(MASTER_PORT).startServer();
        });
        masterExecutor.shutdown();

        // Pausa di sincronizzazione per attendere la piena attivazione del ServerSocket
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 2. Avvia il primo Nodo Sensore (Client 1)
        ExecutorService client1Executor = Executors.newSingleThreadExecutor();
        client1Executor.submit(() -> {
            System.out.println("[Main] Avvio dell'istanza Client 1 sulla porta P2P: " + PEER1_PORT);
            new Client(MASTER_HOST, MASTER_PORT, PEER1_PORT).start();
        });
        client1Executor.shutdown();

        // 3. Avvia il secondo Nodo Sensore (Client 2)
        ExecutorService client2Executor = Executors.newSingleThreadExecutor();
        client2Executor.submit(() -> {
            System.out.println("[Main] Avvio dell'istanza Client 2 sulla porta P2P: " + PEER2_PORT);
            new Client(MASTER_HOST, MASTER_PORT, PEER2_PORT).start();
        });
        client2Executor.shutdown();
    }
}