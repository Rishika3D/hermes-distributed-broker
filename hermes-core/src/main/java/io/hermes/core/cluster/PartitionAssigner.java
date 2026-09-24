package io.hermes.core.cluster;

import io.hermes.core.model.TopicPartition;

import java.util.List;

/**
 * Maps partitions to brokers via the consistent hash ring. The assignment is a
 * pure function of the configured membership, so every broker independently
 * agrees on who leads and who replicates each partition without coordination.
 */
public final class PartitionAssigner {

    private final ConsistentHashRing ring;
    private final int replicationFactor;

    public PartitionAssigner(ConsistentHashRing ring, int replicationFactor) {
        this.ring = ring;
        this.replicationFactor = Math.max(1, replicationFactor);
    }

    /** Leader plus followers, in ring order; index 0 is the leader. */
    public List<Integer> replicasFor(TopicPartition tp) {
        return ring.nodesFor(tp.toString(), replicationFactor);
    }

    public int leaderFor(TopicPartition tp) {
        return replicasFor(tp).get(0);
    }

    public boolean isReplica(TopicPartition tp, int brokerId) {
        return replicasFor(tp).contains(brokerId);
    }
}
