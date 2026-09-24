package io.hermes.core.topic;

import io.hermes.core.cluster.PartitionAssigner;
import io.hermes.core.model.TopicPartition;
import io.hermes.core.storage.PartitionLog;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Owns the lifecycle of topics on this broker: creates them, materializes the
 * partitions this broker hosts (leader or replica), persists topic metadata to
 * disk for restarts, and applies metadata gossiped by the Raft leader so all
 * brokers converge on the same topic set.
 */
public final class TopicRegistry implements Closeable {

    private final Path dataDir;
    private final Path metaFile;
    private final long segmentBytes;
    private final boolean groupCommit;
    private final long lingerMs;
    private final PartitionAssigner assigner;
    private final int localBrokerId;
    private final Map<String, Topic> topics = new ConcurrentHashMap<>();

    public TopicRegistry(Path dataDir, long segmentBytes, PartitionAssigner assigner, int localBrokerId)
            throws IOException {
        this(dataDir, segmentBytes, false, 0, assigner, localBrokerId);
    }

    public TopicRegistry(Path dataDir, long segmentBytes, boolean groupCommit, long lingerMs,
                         PartitionAssigner assigner, int localBrokerId) throws IOException {
        this.dataDir = dataDir;
        this.metaFile = dataDir.resolve("topics.meta");
        this.segmentBytes = segmentBytes;
        this.groupCommit = groupCommit;
        this.lingerMs = lingerMs;
        this.assigner = assigner;
        this.localBrokerId = localBrokerId;
        Files.createDirectories(dataDir);
        loadFromDisk();
    }

    private void loadFromDisk() throws IOException {
        if (!Files.exists(metaFile)) {
            return;
        }
        for (String line : Files.readAllLines(metaFile)) {
            String[] parts = line.trim().split(":");
            if (parts.length == 2) {
                materialize(parts[0], Integer.parseInt(parts[1]));
            }
        }
    }

    /** Creates the topic if absent (idempotent) and persists the metadata. */
    public synchronized Topic ensureTopic(String name, int partitionCount) {
        Topic existing = topics.get(name);
        if (existing != null) {
            return existing;
        }
        Topic topic = materialize(name, partitionCount);
        persistMetadata();
        return topic;
    }

    private Topic materialize(String name, int partitionCount) {
        Topic topic = topics.computeIfAbsent(name, n -> new Topic(n, partitionCount));
        for (int p = 0; p < topic.partitionCount(); p++) {
            TopicPartition tp = new TopicPartition(name, p);
            if (topic.localPartition(p) == null && assigner.isReplica(tp, localBrokerId)) {
                try {
                    PartitionLog log = PartitionLog.open(dataDir.resolve(tp.toString()), segmentBytes,
                            groupCommit, lingerMs);
                    topic.addLocalPartition(new Partition(tp, log,
                            assigner.leaderFor(tp), assigner.replicasFor(tp)));
                } catch (IOException e) {
                    throw new UncheckedIOException("failed to open log for " + tp, e);
                }
            }
        }
        return topic;
    }

    private void persistMetadata() {
        try {
            Files.writeString(metaFile, metadataString().replace(";", "\n") + "\n");
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist topic metadata", e);
        }
    }

    /** Compact form gossiped on Raft heartbeats: {@code name:partitions;name:partitions}. */
    public String metadataString() {
        StringJoiner joiner = new StringJoiner(";");
        for (Topic topic : topics.values()) {
            joiner.add(topic.name() + ":" + topic.partitionCount());
        }
        return joiner.toString();
    }

    /** Applies metadata received from the Raft leader, creating any missing topics. */
    public synchronized void applyMetadata(String metadata) {
        if (metadata == null || metadata.isBlank()) {
            return;
        }
        boolean changed = false;
        for (String entry : metadata.split(";")) {
            String[] parts = entry.split(":");
            if (parts.length == 2 && !topics.containsKey(parts[0])) {
                materialize(parts[0], Integer.parseInt(parts[1]));
                changed = true;
            }
        }
        if (changed) {
            persistMetadata();
        }
    }

    public Topic topic(String name) {
        return topics.get(name);
    }

    public Topic requireTopic(String name) {
        Topic topic = topics.get(name);
        if (topic == null) {
            throw new IllegalArgumentException("unknown topic " + name);
        }
        return topic;
    }

    public Partition localPartition(TopicPartition tp) {
        Topic topic = topics.get(tp.topic());
        return topic == null ? null : topic.localPartition(tp.partition());
    }

    public List<Topic> allTopics() {
        return new ArrayList<>(topics.values());
    }

    @Override
    public synchronized void close() throws IOException {
        for (Topic topic : topics.values()) {
            for (Partition partition : topic.localPartitions()) {
                partition.close();
            }
        }
    }
}
