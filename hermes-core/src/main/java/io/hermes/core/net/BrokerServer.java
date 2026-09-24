package io.hermes.core.net;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The raw-socket server every broker runs for peer traffic. One virtual thread
 * accepts connections; each connection gets its own virtual thread that reads
 * frames, dispatches them to the {@link WireHandler}, and writes the response.
 */
public final class BrokerServer implements Closeable {

    /** Default ceiling on concurrent connections — sheds load beyond this. */
    public static final int DEFAULT_MAX_CONNECTIONS = 4_096;
    /** Idle read timeout; a peer that sends nothing for this long is dropped. */
    private static final int READ_TIMEOUT_MS = 60_000;

    private final int port;
    private final WireHandler handler;
    private final int maxConnections;
    private final Semaphore connectionPermits;
    private final AtomicInteger activeConnections = new AtomicInteger();
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();

    private volatile ServerSocket serverSocket;
    private volatile boolean running;

    public BrokerServer(int port, WireHandler handler) {
        this(port, handler, DEFAULT_MAX_CONNECTIONS);
    }

    public BrokerServer(int port, WireHandler handler, int maxConnections) {
        this.port = port;
        this.handler = handler;
        this.maxConnections = maxConnections;
        this.connectionPermits = new Semaphore(maxConnections);
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        running = true;
        workers.submit(this::acceptLoop);
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                // Shed load rather than spawn unbounded work: if we are at the
                // connection ceiling, close the new socket immediately.
                if (!connectionPermits.tryAcquire()) {
                    System.err.println("[hermes] connection limit " + maxConnections
                            + " reached on port " + port + "; rejecting connection");
                    closeQuietly(socket);
                    continue;
                }
                activeConnections.incrementAndGet();
                workers.submit(() -> serveConnection(socket));
            } catch (SocketException closed) {
                return;
            } catch (IOException e) {
                if (running) {
                    System.err.println("[hermes] accept failed on port " + port + ": " + e.getMessage());
                }
            }
        }
    }

    private void serveConnection(Socket socket) {
        try (socket;
             DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(READ_TIMEOUT_MS);
            while (running) {
                WireMessage request = MessageCodec.decode(in);
                WireMessage response;
                try {
                    response = handler.handle(request);
                } catch (Exception e) {
                    response = WireMessage.error(e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
                }
                MessageCodec.encode(response, out);
            }
        } catch (EOFException | SocketException disconnected) {
            // peer closed the connection; normal shutdown path
        } catch (IOException e) {
            if (running) {
                System.err.println("[hermes] connection error on port " + port + ": " + e.getMessage());
            }
        } finally {
            activeConnections.decrementAndGet();
            connectionPermits.release();
        }
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // best-effort
        }
    }

    /** Current number of connections being served — used by tests and metrics. */
    public int activeConnections() {
        return activeConnections.get();
    }

    public int port() {
        return port;
    }

    @Override
    public void close() throws IOException {
        running = false;
        if (serverSocket != null) {
            serverSocket.close();
        }
        workers.shutdownNow();
    }
}
