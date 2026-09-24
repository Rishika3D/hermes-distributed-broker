package io.hermes.core.group;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsumerGroupTest {

    @Test
    void singleMemberOwnsAllPartitions() {
        ConsumerGroup group = new ConsumerGroup("g1", "orders", 6);
        JoinResult result = group.join("m1");
        assertEquals(6, result.partitions().size());
        assertEquals(1, result.generation());
    }

    @Test
    void joiningMemberTriggersRebalanceAndSplitsPartitions() {
        ConsumerGroup group = new ConsumerGroup("g1", "orders", 6);
        group.join("m1");
        JoinResult second = group.join("m2");
        assertEquals(2, second.generation());
        assertEquals(3, second.partitions().size());

        JoinResult first = group.heartbeat("m1");
        Set<Integer> covered = new HashSet<>();
        covered.addAll(first.partitions());
        covered.addAll(second.partitions());
        assertEquals(Set.of(0, 1, 2, 3, 4, 5), covered);
    }

    @Test
    void unevenPartitionCountsGiveEarlierJoinersTheExtras() {
        ConsumerGroup group = new ConsumerGroup("g1", "orders", 7);
        group.join("m1");
        group.join("m2");
        assertEquals(4, group.heartbeat("m1").partitions().size());
        assertEquals(3, group.heartbeat("m2").partitions().size());
    }

    @Test
    void leavingMemberReturnsPartitionsToTheGroup() {
        ConsumerGroup group = new ConsumerGroup("g1", "orders", 4);
        group.join("m1");
        group.join("m2");
        group.leave("m2");
        JoinResult result = group.heartbeat("m1");
        assertEquals(4, result.partitions().size());
        assertTrue(result.generation() >= 3);
    }

    @Test
    void expiredMembersAreDroppedOnRebalanceCheck() throws InterruptedException {
        ConsumerGroup group = new ConsumerGroup("g1", "orders", 4);
        group.join("stale");
        Thread.sleep(30);
        group.join("fresh");
        assertTrue(group.expireMembers(20), "stale member should have expired");
        assertEquals(1, group.memberCount());
        assertEquals(4, group.heartbeat("fresh").partitions().size());
    }
}
