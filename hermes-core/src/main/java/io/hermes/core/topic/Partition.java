package io.hermes.core.topic;

import io.hermes.core.model.TopicPartition;
import io.hermes.core.storage.PartitionLog;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * A partition hosted on this broker: its write-ahead log plus the (static)
 * leader and replica assignment computed from the consistent hash ring.
 */
public final class Partition implements Closeable {

    private final TopicPartition id;
    private final PartitionLog log;
    private final int leaderId;
    private final List<Integer> replicaIds;

    public Partition(TopicPartition id, PartitionLog log, int leaderId, List<Integer> replicaIds) {
        this.id = id;
        this.log = log;
        this.leaderId = leaderId;
        this.replicaIds = List.copyOf(replicaIds);
    }

    public TopicPartition id() {
        return id;
    }

    public PartitionLog log() {
        return log;
    }

    public int leaderId() {
        return leaderId;
    }

    public List<Integer> replicaIds() {
        return replicaIds;
    }

    public boolean isLeader(int brokerId) {
        return leaderId == brokerId;
    }

    @Override
    public void close() throws IOException {
        log.close();
    }
}
