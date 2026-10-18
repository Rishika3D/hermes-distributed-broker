package io.hermes.core.net;

import io.hermes.core.cluster.BrokerNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The peer read timeout decides when a slow peer becomes a failed request. It must be
 * configurable: under disk saturation legitimate replies take longer than any single
 * hard-coded value suits every deployment.
 */
@Timeout(20)
class PeerClientTimeoutTest {

    /** A peer that accepts connections and then never says anything. */
    private static final class SilentPeer implements AutoCloseable {
        final ServerSocket server;
        final List<Socket> accepted = new ArrayList<>();

        SilentPeer() throws IOException {
            server = new ServerSocket(0);
            Thread t = new Thread(() -> {
                try {
                    while (true) {
                        accepted.add(server.accept());
                    }
                } catch (IOException closed) {
                    // shutting down
                }
            });
            t.setDaemon(true);
            t.start();
        }

        BrokerNode node() {
            return new BrokerNode(2, "127.0.0.1", server.getLocalPort());
        }

        @Override
        public void close() throws IOException {
            server.close();
            for (Socket s : accepted) {
                s.close();
            }
        }
    }

    @Test
    void silentPeerFailsTheRequestAtTheConfiguredReadTimeout() throws Exception {
        try (SilentPeer peer = new SilentPeer(); PeerClient client = new PeerClient(peer.node(), 300)) {
            long start = System.nanoTime();

            assertThrows(IOException.class, () -> client.request(WireMessage.of(FrameType.HEARTBEAT)));

            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            assertTrue(elapsedMs >= 250, "must wait for the configured timeout, took " + elapsedMs + "ms");
            assertTrue(elapsedMs < 2_000, "must not use the 3s default, took " + elapsedMs + "ms");
        }
    }

    @Test
    void defaultReadTimeoutIsThreeSeconds() throws Exception {
        try (SilentPeer peer = new SilentPeer(); PeerClient client = new PeerClient(peer.node())) {
            long start = System.nanoTime();

            assertThrows(IOException.class, () -> client.request(WireMessage.of(FrameType.HEARTBEAT)));

            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            assertTrue(elapsedMs >= 2_800 && elapsedMs < 6_000, "default is 3s, took " + elapsedMs + "ms");
        }
    }
}
