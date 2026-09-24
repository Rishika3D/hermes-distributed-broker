package io.hermes.core.group;

import io.hermes.core.model.TopicPartition;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Coordinates all consumer groups on this broker. In Hermes the coordinator
 * runs on the Raft leader (the controller); other brokers forward group
 * operations to it. Dead members are expired lazily on every group access.
 */
public final class GroupCoordinator implements Closeable {

    public static final long SESSION_TIMEOUT_MS = 10_000;

    private final Map<String, ConsumerGroup> groups = new ConcurrentHashMap<>();
    private final OffsetStore offsets;

    public GroupCoordinator(OffsetStore offsets) {
        this.offsets = offsets;
    }

    public JoinResult join(String groupId, String topic, String memberId, int partitionCount) {
        ConsumerGroup group = groups.computeIfAbsent(groupId,
                id -> new ConsumerGroup(id, topic, partitionCount));
        if (!group.topic().equals(topic)) {
            throw new IllegalArgumentException(
                    "group " + groupId + " is bound to topic " + group.topic() + ", not " + topic);
        }
        group.expireMembers(SESSION_TIMEOUT_MS);
        String member = memberId == null || memberId.isBlank()
                ? "member-" + UUID.randomUUID().toString().substring(0, 8)
                : memberId;
        return group.join(member);
    }

    public JoinResult heartbeat(String groupId, String memberId) {
        ConsumerGroup group = requireGroup(groupId);
        group.expireMembers(SESSION_TIMEOUT_MS);
        return group.heartbeat(memberId);
    }

    public void leave(String groupId, String memberId) {
        ConsumerGroup group = groups.get(groupId);
        if (group != null) {
            group.leave(memberId);
        }
    }

    public void commit(String groupId, TopicPartition tp, long offset) throws IOException {
        offsets.commit(groupId, tp, offset);
    }

    /**
     * Validated commit: rejects a commit from a member that is not the current
     * owner of the partition at the given generation (a zombie that missed a
     * rebalance). Used whenever the caller supplies its member identity.
     */
    public void commit(String groupId, String memberId, int generation,
                       TopicPartition tp, long offset) throws IOException {
        ConsumerGroup group = groups.get(groupId);
        if (group == null || !group.canCommit(memberId, generation, tp.partition())) {
            throw new IllegalStateException("rejected stale/unauthorized commit: member " + memberId
                    + " gen " + generation + " does not own " + tp);
        }
        offsets.commit(groupId, tp, offset);
    }

    public long committed(String groupId, TopicPartition tp) {
        return offsets.committed(groupId, tp);
    }

    public List<ConsumerGroup> allGroups() {
        List<ConsumerGroup> all = new ArrayList<>(groups.values());
        all.forEach(g -> g.expireMembers(SESSION_TIMEOUT_MS));
        return all;
    }

    private ConsumerGroup requireGroup(String groupId) {
        ConsumerGroup group = groups.get(groupId);
        if (group == null) {
            throw new IllegalArgumentException("unknown consumer group " + groupId);
        }
        return group;
    }

    @Override
    public void close() throws IOException {
        offsets.close();
    }
}
