package io.hermes.core.broker;

import io.hermes.core.cluster.BrokerNode;
import io.hermes.core.model.Message;
import io.hermes.core.model.ProduceRecord;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * End-to-end batch produce across a real 3-broker cluster: records spanning
 * many partitions (local + forwarded leaders) are all persisted with correct,
 * unique offsets, and durability is preserved (acks=all, RF=2, all alive).
 */
@Timeout(120)
class BatchProduceIntegrationTest {

    private static final String TOPIC = "batch-it";

    @TempDir
    Path dataRoot;

    private final List<Broker> brokers = new ArrayList<>();

    @BeforeEach
    void start() throws IOException {
        int[] ports = freePorts(3);
        List<BrokerNode> members = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            members.add(new BrokerNode(i + 1, "localhost", ports[i]));
        }
        for (int i = 0; i < 3; i++) {
            Broker b = new Broker(new BrokerConfig(i + 1, dataRoot.resolve("b" + (i + 1)),
                    members, 2, 6, 64 * 1024, "sec"));
            b.start();
            brokers.add(b);
        }
        waitUntil(() -> brokers.stream().allMatch(b -> b.raft().leaderId() >= 0));
    }

    @AfterEach
    void stop() throws IOException {
        for (Broker b : brokers) {
            b.close();
        }
    }

    @Test
    void batchAcrossPartitionsPersistsEveryRecordExactlyOnce() throws IOException {
        brokers.get(0).createTopic(TOPIC, 6);

        int n = 600;
        List<ProduceRecord> batch = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            batch.add(new ProduceRecord("key-" + i, "value-" + i)); // keys spread across all 6 partitions
        }

        // submit the whole batch through a single broker (mix of local + forwarded leaders)
        List<ProduceResult> results = brokers.get(2).produceBatch(TOPIC, batch);
        assertEquals(n, results.size(), "one result per input record");

        // every partition's end offsets must sum to n (nothing lost or duplicated)
        long sum = 0;
        for (int p = 0; p < 6; p++) {
            sum += brokers.get(0).endOffset(new io.hermes.core.model.TopicPartition(TOPIC, p));
        }
        assertEquals(n, sum);

        // read every record back and confirm all original values are present exactly once
        Map<String, Integer> seen = new HashMap<>();
        for (int p = 0; p < 6; p++) {
            for (Message m : brokers.get(1).fetch(TOPIC, p, 0, 10_000)) {
                seen.merge(m.value(), 1, Integer::sum);
            }
        }
        assertEquals(n, seen.size());
        assertEquals(1, seen.getOrDefault("value-321", 0), "a specific record is present exactly once");
        assertTrue(seen.values().stream().allMatch(c -> c == 1), "no record duplicated");
    }

    @Test
    void emptyBatchIsANoOp() throws IOException {
        brokers.get(0).createTopic(TOPIC, 6);
        assertEquals(0, brokers.get(0).produceBatch(TOPIC, List.of()).size());
    }

    private void waitUntil(java.util.function.BooleanSupplier c) {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            if (c.getAsBoolean()) return;
            try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        fail("cluster did not converge");
    }

    private static int[] freePorts(int n) throws IOException {
        int[] p = new int[n];
        List<ServerSocket> s = new ArrayList<>();
        for (int i = 0; i < n; i++) { ServerSocket ss = new ServerSocket(0); p[i] = ss.getLocalPort(); s.add(ss); }
        for (ServerSocket ss : s) ss.close();
        return p;
    }
}
