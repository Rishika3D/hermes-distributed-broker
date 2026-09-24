package io.hermes.core.net;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reproduces M4: the server must bound concurrent connections and shed load
 * beyond the cap, rather than spawning unbounded work per connection.
 */
@Timeout(30)
class ConnectionCapTest {

    @Test
    void serverCapsConcurrentConnectionsAndShedsExtras() throws Exception {
        int port = freePort();
        // Echo-style handler; connections stay open (client holds them) so each
        // holds a permit until it disconnects.
        BrokerServer server = new BrokerServer(port,
                request -> WireMessage.of(FrameType.HEARTBEAT_ACK), 2);
        server.start();

        List<Socket> held = new ArrayList<>();
        try {
            // Two clients connect and each completes one request → 2 permits held.
            for (int i = 0; i < 2; i++) {
                held.add(openAndPing(port));
            }
            waitForActive(server, 2);
            assertEquals(2, server.activeConnections(), "server should be at its cap of 2");

            // A third connection must be shed: the server closes it, so our read hits EOF.
            Socket third = new Socket();
            third.connect(new InetSocketAddress("localhost", port), 1000);
            int firstByte = third.getInputStream().read(); // server closed → -1
            assertEquals(-1, firstByte, "connection beyond the cap must be closed by the server");
            third.close();

            assertTrue(server.activeConnections() <= 2,
                    "active connections must never exceed the cap");
            System.out.println("[M4] cap=2 held=" + server.activeConnections()
                    + ", 3rd connection shed (EOF). Backpressure enforced.");

            // Free one; a new connection can now be served.
            held.remove(0).close();
            waitForActive(server, 1);
            held.add(openAndPing(port));
            waitForActive(server, 2);
            System.out.println("[M4] after releasing one permit, a new connection was accepted");
        } finally {
            for (Socket s : held) {
                s.close();
            }
            server.close();
        }
    }

    private static Socket openAndPing(int port) throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress("localhost", port), 1000);
        socket.setTcpNoDelay(true);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(socket.getInputStream());
        MessageCodec.encode(WireMessage.of(FrameType.HEARTBEAT), out);
        WireMessage ack = MessageCodec.decode(in); // proves the connection is served
        assertEquals(FrameType.HEARTBEAT_ACK, ack.type());
        return socket;
    }

    private static void waitForActive(BrokerServer server, int expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline && server.activeConnections() != expected) {
            Thread.sleep(20);
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
