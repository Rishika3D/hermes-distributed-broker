package io.hermes.core.group;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One consumer group subscribed to one topic. Tracks members (in join order),
 * a generation counter bumped on every membership change, and the current
 * partition assignment computed by range assignment: partitions are split as
 * evenly as possible across members, earlier joiners taking the extras.
 */
public final class ConsumerGroup {

    private final String groupId;
    private final String topic;
    private final int partitionCount;
    private final LinkedHashMap<String, Long> memberLastSeen = new LinkedHashMap<>();
    private final Map<String, List<Integer>> assignments = new HashMap<>();
    private int generation = 0;

    public ConsumerGroup(String groupId, String topic, int partitionCount) {
        this.groupId = groupId;
        this.topic = topic;
        this.partitionCount = partitionCount;
    }

    public synchronized JoinResult join(String memberId) {
        boolean isNew = !memberLastSeen.containsKey(memberId);
        memberLastSeen.put(memberId, System.currentTimeMillis());
        if (isNew) {
            rebalance();
        }
        return resultFor(memberId);
    }

    /** Refreshes liveness; a member the group forgot (expired) triggers a rejoin. */
    public synchronized JoinResult heartbeat(String memberId) {
        return join(memberId);
    }

    public synchronized void leave(String memberId) {
        if (memberLastSeen.remove(memberId) != null) {
            rebalance();
        }
    }

    /** Drops members not seen within the timeout; returns true if the group rebalanced. */
    public synchronized boolean expireMembers(long sessionTimeoutMs) {
        long cutoff = System.currentTimeMillis() - sessionTimeoutMs;
        boolean removed = memberLastSeen.values().removeIf(lastSeen -> lastSeen < cutoff);
        if (removed) {
            rebalance();
        }
        return removed;
    }

    private void rebalance() {
        generation++;
        assignments.clear();
        List<String> members = new ArrayList<>(memberLastSeen.keySet());
        if (members.isEmpty()) {
            return;
        }
        int perMember = partitionCount / members.size();
        int extras = partitionCount % members.size();
        int nextPartition = 0;
        for (int i = 0; i < members.size(); i++) {
            int share = perMember + (i < extras ? 1 : 0);
            List<Integer> assigned = new ArrayList<>(share);
            for (int p = 0; p < share; p++) {
                assigned.add(nextPartition++);
            }
            assignments.put(members.get(i), assigned);
        }
    }

    private JoinResult resultFor(String memberId) {
        return new JoinResult(memberId, generation, List.copyOf(assignments.getOrDefault(memberId, List.of())));
    }

    /**
     * True if {@code memberId} may commit {@code partition}: it must be a
     * current member, at the current generation, and actually assigned that
     * partition. Rejects zombie members that missed a rebalance.
     */
    public synchronized boolean canCommit(String memberId, int generation, int partition) {
        return generation == this.generation
                && assignments.getOrDefault(memberId, List.of()).contains(partition);
    }

    public synchronized int memberCount() {
        return memberLastSeen.size();
    }

    public synchronized int generation() {
        return generation;
    }

    public String groupId() {
        return groupId;
    }

    public String topic() {
        return topic;
    }

    public int partitionCount() {
        return partitionCount;
    }
}
