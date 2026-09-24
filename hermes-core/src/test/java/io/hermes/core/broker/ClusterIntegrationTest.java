package io.hermes.core.broker;

import io.hermes.core.cluster.BrokerNode;
import io.hermes.core.group.JoinResult;
import io.hermes.core.model.Message;
import io.hermes.core.model.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Boots a real 3-broker cluster on loopback sockets (random free ports, a
 * shared cluster secret, temp data dirs) and exercises the full pipeline:
 * election, topic creation, cross-broker produce forwarding, replication,
 * fetch, consumer groups, offset commits and lag.
 */
@Timeout(90)
class ClusterIntegrationTest {

    private static final String TOPIC = "it-events";
    private static final String SECRET = "test-cluster-secret";

    @TempDir
    Path dataRoot;

    private final List<Broker> brokers = new ArrayList<>();

    @BeforeEach
    void startCluster() throws IOException {
        int[] ports = freePorts(3);
        List<BrokerNode> members = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            members.add(new BrokerNode(i + 1, "localhost", ports[i]));
        }
        for (int i = 0; i < 3; i++) {
            Broker broker = new Broker(new BrokerConfig(i + 1, dataRoot.resolve("broker-" + (i + 1)),
                    members, 2, 3, 64 * 1024, SECRET));
            broker.start();
            brokers.add(broker);
        }
        waitUntil("a controller is elected and visible everywhere",
                () -> brokers.stream().allMatch(b -> b.raft().leaderId() >= 0));
    }

    @AfterEach
    void stopCluster() throws IOException {
        for (Broker broker : brokers) {
            broker.close();
        }
        brokers.clear();
    }

    @Test
    void fullPipelineAcrossThreeBrokers() throws Exception {
        // topic creation via a broker that may not be the controller
        assertEquals(6, brokers.get(2).createTopic(TOPIC, 6).partitionCount());

        // keyed produces through every broker; forwarding routes to partition leaders
        int total = 90;
        for (int i = 0; i < total; i++) {
            ProduceResult result = brokers.get(i % 3).produce(TOPIC, "key-" + i, "value-" + i);
            assertEquals(TOPIC, result.topic());
        }

        // every message is accounted for in partition end offsets
        long sum = 0;
        for (int p = 0; p < 6; p++) {
            sum += brokers.get(0).endOffset(new TopicPartition(TOPIC, p));
        }
        assertEquals(total, sum);

        // fetch everything back through a single broker (local reads + forwards)
        Map<String, String> seen = new HashMap<>();
        for (int p = 0; p < 6; p++) {
            for (Message m : brokers.get(1).fetch(TOPIC, p, 0, 1000)) {
                seen.put(m.key(), m.value());
            }
        }
        assertEquals(total, seen.size());
        assertEquals("value-37", seen.get("key-37"));

        // consumer group: single member owns all partitions, commits, lag reflects it
        JoinResult member = brokers.get(2).joinGroup("it-group", TOPIC, null);
        assertEquals(6, member.partitions().size());
        brokers.get(2).commitOffset("it-group", TOPIC, 0, 5);
        assertEquals(5, brokers.get(0).committedOffsets("it-group", TOPIC).get(0));

        List<Map<String, Object>> lag = brokers.get(1).lag();
        assertEquals(6, lag.size());
        long totalEnd = lag.stream().mapToLong(e -> (long) e.get("endOffset")).sum();
        assertEquals(total, totalEnd);

        // a second member triggers a rebalance that splits the partitions
        JoinResult second = brokers.get(0).joinGroup("it-group", TOPIC, null);
        assertEquals(3, second.partitions().size());
    }

    @Test
    void invalidInputIsRejectedBeforeTouchingTheCluster() {
        assertThrows(IllegalArgumentException.class,
                () -> brokers.get(0).createTopic("bad,name", 3));
        assertThrows(IllegalArgumentException.class,
                () -> brokers.get(0).produce("t", "k", "x".repeat(70_000)));
        assertThrows(IllegalArgumentException.class,
                () -> brokers.get(0).fetch("no-such-topic", 0, 0, 10));
        assertThrows(IllegalArgumentException.class, () -> {
            brokers.get(0).createTopic("bounds", 3);
            brokers.get(0).fetch("bounds", 99, 0, 10);
        });
    }

    private static void waitUntil(String what, BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("interrupted while waiting for: " + what);
            }
        }
        fail("timed out waiting for: " + what);
    }

    private static int[] freePorts(int count) throws IOException {
        int[] ports = new int[count];
        List<ServerSocket> sockets = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ServerSocket socket = new ServerSocket(0);
            ports[i] = socket.getLocalPort();
            sockets.add(socket);
        }
        for (ServerSocket socket : sockets) {
            socket.close();
        }
        return ports;
    }
}
