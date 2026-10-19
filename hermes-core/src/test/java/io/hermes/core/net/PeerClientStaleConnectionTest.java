package io.hermes.core.net;

import io.hermes.core.cluster.BrokerNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A server closes connections that sit idle (BrokerServer: 60 s), and a restarted peer drops
 * all of them. The client pool must not hand such a dead connection to the next request, or
 * every first request after an idle period fails (observed in a Docker deployment: writes
 * returned 502/503 until the stale sockets had each failed once).
 */
@Timeout(30)
class PeerClientStaleConnectionTest {

    /** Echo-style peer: answers every frame with HEARTBEAT_ACK after an optional delay. */
    private static final class StubPeer implements AutoCloseable {
        final ServerSocket server;
        final List<Socket> accepted = new CopyOnWriteArrayList<>();
        final AtomicInteger connectionsAccepted = new AtomicInteger();
        volatile long replyDelayMs;

        StubPeer() throws IOException {
            server = new ServerSocket(0);
            Thread acceptor = new Thread(() -> {
                try {
                    while (true) {
                        Socket s = server.accept();
                        accepted.add(s);
                        connectionsAccepted.incrementAndGet();
                        Thread t = new Thread(() -> serve(s));
                        t.setDaemon(true);
                        t.start();
                    }
                } catch (IOException closed) {
                    // shutting down
                }
            });
            acceptor.setDaemon(true);
            acceptor.start();
        }

        private void serve(Socket s) {
            try (DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
                 DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()))) {
                while (true) {
                    MessageCodec.decode(in);
                    if (replyDelayMs > 0) {
                        Thread.sleep(replyDelayMs);
                    }
                    MessageCodec.encode(WireMessage.of(FrameType.HEARTBEAT_ACK), out);
                }
            } catch (IOException | InterruptedException ended) {
                // connection closed by us or by the client
            }
        }

        /** What an idle timeout or a peer restart does: every open connection is dropped. */
        void dropAllConnections() throws IOException {
            for (Socket s : accepted) {
                s.close();
            }
            accepted.clear();
        }

        BrokerNode node() {
            return new BrokerNode(2, "127.0.0.1", server.getLocalPort());
        }

        @Override
        public void close() throws IOException {
            server.close();
            dropAllConnections();
        }
    }

    private static void ping(PeerClient client) throws IOException {
        assertEquals(FrameType.HEARTBEAT_ACK, client.request(WireMessage.of(FrameType.HEARTBEAT)).type());
    }

    @Test
    void connectionIdleLongerThanTheEvictionWindowIsNotReused() throws Exception {
        try (StubPeer peer = new StubPeer(); PeerClient client = new PeerClient(peer.node(), 3_000, 50)) {
            ping(client);                       // opens connection #1 and parks it in the pool
            peer.dropAllConnections();          // server-side idle timeout closes it
            Thread.sleep(150);                  // client-side idle time now exceeds the 50 ms window

            assertDoesNotThrow(() -> ping(client), "must use a fresh connection, not the dead pooled one");
            assertEquals(2, peer.connectionsAccepted.get());
        }
    }

    @Test
    void afterOneStaleConnectionFailsTheRestOfThePoolIsDiscarded() throws Exception {
        try (StubPeer peer = new StubPeer(); PeerClient client = new PeerClient(peer.node(), 3_000, 60_000)) {
            // Park two connections in the pool: two overlapping requests need two sockets.
            peer.replyDelayMs = 200;
            Thread other = new Thread(() -> assertDoesNotThrow(() -> ping(client)));
            other.start();
            ping(client);
            other.join();
            peer.replyDelayMs = 0;
            assertEquals(2, peer.connectionsAccepted.get());

            peer.dropAllConnections();          // peer restarted: both pooled sockets are dead

            assertThrows(IOException.class, () -> ping(client), "the first request hits a dead socket");
            assertDoesNotThrow(() -> ping(client),
                    "the other dead socket must have been discarded with it, so this one reconnects");
        }
    }
}
