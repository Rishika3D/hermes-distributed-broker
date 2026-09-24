package io.hermes.core.cluster;

import io.hermes.core.model.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsistentHashRingTest {

    @Test
    void placementIsDeterministicAcrossInstances() {
        ConsistentHashRing a = new ConsistentHashRing(List.of(1, 2, 3));
        ConsistentHashRing b = new ConsistentHashRing(List.of(1, 2, 3));
        for (int p = 0; p < 50; p++) {
            String key = new TopicPartition("orders", p).toString();
            assertEquals(a.nodesFor(key, 2), b.nodesFor(key, 2));
        }
    }

    @Test
    void replicaSetsAreDistinctBrokers() {
        ConsistentHashRing ring = new ConsistentHashRing(List.of(1, 2, 3));
        for (int p = 0; p < 50; p++) {
            List<Integer> replicas = ring.nodesFor("topic-" + p, 3);
            assertEquals(3, replicas.size());
            assertEquals(3, replicas.stream().distinct().count());
        }
    }

    @Test
    void replicaCountIsCappedAtClusterSize() {
        ConsistentHashRing ring = new ConsistentHashRing(List.of(1));
        assertEquals(List.of(1), ring.nodesFor("anything", 3));
    }

    @Test
    void partitionsSpreadAcrossBrokers() {
        ConsistentHashRing ring = new ConsistentHashRing(List.of(1, 2, 3));
        Map<Integer, Integer> leaders = new HashMap<>();
        for (int p = 0; p < 300; p++) {
            int leader = ring.nodesFor("load-" + p, 1).get(0);
            leaders.merge(leader, 1, Integer::sum);
        }
        assertEquals(3, leaders.size(), "every broker should lead some partitions");
        leaders.values().forEach(count ->
                assertTrue(count > 30, "distribution too skewed: " + leaders));
    }
}
