package io.hermes.core.topic;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Topic metadata plus the partitions of it that live on this broker. A broker
 * only materializes a partition's log if it is the leader or a replica for it;
 * the rest exist only as metadata.
 */
public final class Topic {

    private final String name;
    private final int partitionCount;
    private final Map<Integer, Partition> localPartitions = new ConcurrentHashMap<>();

    public Topic(String name, int partitionCount) {
        this.name = name;
        this.partitionCount = partitionCount;
    }

    public String name() {
        return name;
    }

    public int partitionCount() {
        return partitionCount;
    }

    public void addLocalPartition(Partition partition) {
        localPartitions.put(partition.id().partition(), partition);
    }

    public Partition localPartition(int partition) {
        return localPartitions.get(partition);
    }

    public Collection<Partition> localPartitions() {
        return localPartitions.values();
    }
}
