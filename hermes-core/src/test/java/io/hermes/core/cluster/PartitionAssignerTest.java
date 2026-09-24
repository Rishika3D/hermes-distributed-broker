package io.hermes.core.cluster;

import io.hermes.core.model.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link PartitionAssigner}, the pure function mapping a
 * partition to its leader and replica set via the hash ring. Correctness here
 * is what lets every broker agree on placement without coordination, so the
 * invariants (leader == replicas[0], replica-set size, membership) are tested
 * directly and for determinism.
 */
class PartitionAssignerTest {

    private final PartitionAssigner assigner =
            new PartitionAssigner(new ConsistentHashRing(List.of(1, 2, 3)), 2);

    private final TopicPartition tp = new TopicPartition("orders", 0);

    @Test
    void leaderIsTheFirstReplica() {
        assertEquals(assigner.replicasFor(tp).get(0), assigner.leaderFor(tp));
    }

    @Test
    void replicaSetHonoursReplicationFactorWithDistinctBrokers() {
        List<Integer> replicas = assigner.replicasFor(tp);
        assertEquals(2, replicas.size());
        assertEquals(2, replicas.stream().distinct().count());
    }

    @Test
    void isReplicaAgreesWithReplicaSet() {
        List<Integer> replicas = assigner.replicasFor(tp);
        for (int broker = 1; broker <= 3; broker++) {
            assertEquals(replicas.contains(broker), assigner.isReplica(tp, broker));
        }
    }

    @Test
    void placementIsDeterministic() {
        PartitionAssigner other = new PartitionAssigner(new ConsistentHashRing(List.of(1, 2, 3)), 2);
        for (int p = 0; p < 20; p++) {
            TopicPartition x = new TopicPartition("t", p);
            assertEquals(assigner.replicasFor(x), other.replicasFor(x));
        }
    }

    @Test
    void replicationFactorIsCappedAtClusterSize() {
        PartitionAssigner over = new PartitionAssigner(new ConsistentHashRing(List.of(1, 2)), 5);
        assertEquals(2, over.replicasFor(tp).size());
    }

    @Test
    void everyBrokerLeadsSomePartitionsAcrossManyKeys() {
        boolean[] led = new boolean[4];
        for (int p = 0; p < 200; p++) {
            led[assigner.leaderFor(new TopicPartition("spread", p))] = true;
        }
        assertTrue(led[1] && led[2] && led[3]);
        assertFalse(led[0]);
    }
}
