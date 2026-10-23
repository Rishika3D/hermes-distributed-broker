package io.hermes.core.net;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Peers open connections in bursts (every in-flight request needs its own socket). If more
 * connects arrive than the server's listen backlog holds, Linux silently drops the SYN and the
 * client retransmits one second later. PeerClient's connect timeout is also one second, so the
 * delayed connects fail and the request is reported as a replication/forwarding failure.
 * macOS does not behave this way, so this only runs on Linux (where production runs).
 */
@EnabledOnOs(OS.LINUX)
@Timeout(60)
class ListenBacklogTest {

    private static final int BURST = 500;
    /** A SYN retransmit costs ~1000 ms; anything near that means the backlog overflowed. */
    private static final long SYN_RETRANSMIT_THRESHOLD_MS = 900;

    @Test
    void burstOfSimultaneousConnectsIsNotDelayedBySynRetransmits() throws Exception {
        int port;
        try (ServerSocket probe = new ServerSocket(0)) {
            port = probe.getLocalPort();
        }
        BrokerServer server = new BrokerServer(port, request -> WireMessage.of(FrameType.HEARTBEAT_ACK));
        server.start();

        List<Long> connectMillis = Collections.synchronizedList(new ArrayList<>());
        List<Socket> sockets = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch ready = new CountDownLatch(BURST);
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(BURST);
        try {
            for (int i = 0; i < BURST; i++) {
                pool.submit(() -> {
                    Socket s = new Socket();
                    ready.countDown();
                    try {
                        go.await();
                        long t0 = System.nanoTime();
                        s.connect(new InetSocketAddress("127.0.0.1", port), 5_000);
                        connectMillis.add((System.nanoTime() - t0) / 1_000_000);
                        sockets.add(s);
                    } catch (IOException | InterruptedException e) {
                        connectMillis.add(Long.MAX_VALUE);
                    }
                    return null;
                });
            }
            ready.await();
            go.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        } finally {
            for (Socket s : sockets) {
                s.close();
            }
            server.close();
        }

        long slow = connectMillis.stream().filter(ms -> ms >= SYN_RETRANSMIT_THRESHOLD_MS).count();
        long worst = connectMillis.stream().mapToLong(Long::longValue).max().orElse(0);
        assertEquals(0, slow, slow + " of " + BURST + " connects took >= " + SYN_RETRANSMIT_THRESHOLD_MS
                + "ms (worst " + worst + "ms): the listen backlog overflowed");
    }
}
