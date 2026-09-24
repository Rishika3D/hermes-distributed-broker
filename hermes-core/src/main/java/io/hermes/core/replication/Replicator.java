package io.hermes.core.replication;

import io.hermes.core.cluster.ClusterState;
import io.hermes.core.metrics.BrokerMetrics;
import io.hermes.core.model.Message;
import io.hermes.core.model.TopicPartition;
import io.hermes.core.net.FrameType;
import io.hermes.core.net.PeerChannels;
import io.hermes.core.net.WireMessage;

import java.util.List;

/**
 * Leader-side replication: after a message is appended to the local log, it is
 * pushed synchronously to every alive follower in the partition's replica set.
 * The write is considered durable once a majority of the replica set (leader
 * included) holds it — the Raft quorum rule applied per partition.
 */
public final class Replicator {

    private final ClusterState cluster;
    private final PeerChannels peers;
    private final BrokerMetrics metrics;

    public Replicator(ClusterState cluster, PeerChannels peers, BrokerMetrics metrics) {
        this.cluster = cluster;
        this.peers = peers;
        this.metrics = metrics;
    }

    /** Returns true if a majority of the replica set acknowledged the message. */
    public boolean replicate(TopicPartition tp, int partitionCount, Message message, List<Integer> replicas) {
        return replicateBatch(tp, partitionCount, List.of(message), replicas);
    }

    /**
     * Replicates a whole batch as a single frame carrying all N records, so a
     * batched append costs one replication round-trip (and one follower fsync)
     * instead of N. Returns true once a majority of the replica set holds it.
     */
    public boolean replicateBatch(TopicPartition tp, int partitionCount,
                                  List<Message> messages, List<Integer> replicas) {
        int acks = 1; // the leader's own append
        for (int replicaId : replicas) {
            if (replicaId == cluster.localId() || !cluster.isAlive(replicaId)) {
                continue;
            }
            WireMessage request = WireMessage.of(FrameType.REPLICATE)
                    .header("topic", tp.topic())
                    .header("partitionCount", partitionCount)
                    .header("partition", tp.partition())
                    .records(messages);
            if (peers.tryRequest(replicaId, request).isPresent()) {
                acks++;
            }
        }
        int majority = replicas.size() / 2 + 1;
        boolean replicated = acks >= majority;
        if (!replicated) {
            metrics.recordUnderReplicated();
        }
        return replicated;
    }
}
