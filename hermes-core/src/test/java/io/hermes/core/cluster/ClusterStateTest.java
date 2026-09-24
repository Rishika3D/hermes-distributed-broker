package io.hermes.core.cluster;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ClusterState} liveness and quorum arithmetic — the
 * inputs replication and Raft rely on. The local node is always considered
 * alive; peers are alive only within the liveness window after a heartbeat.
 */
class ClusterStateTest {

    private static final List<BrokerNode> MEMBERS = List.of(
            new BrokerNode(1, "localhost", 9091),
            new BrokerNode(2, "localhost", 9092),
            new BrokerNode(3, "localhost", 9093));

    private final ClusterState state = new ClusterState(1, MEMBERS);

    @Test
    void localNodeIsAlwaysAlive() {
        assertTrue(state.isAlive(1));
    }

    @Test
    void peerIsDeadUntilHeardFrom() {
        assertFalse(state.isAlive(2));
        state.markAlive(2);
        assertTrue(state.isAlive(2));
    }

    @Test
    void majorityIsHalfPlusOne() {
        assertEquals(2, state.majority());
        assertEquals(3, state.size());
    }

    @Test
    void peersExcludeTheLocalNode() {
        assertEquals(2, state.peers().size());
        assertFalse(state.peers().stream().anyMatch(n -> n.id() == 1));
    }

    @Test
    void constructingWithoutLocalMemberIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new ClusterState(99, MEMBERS));
    }

    @Test
    void unknownBrokerLookupIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> state.node(42));
    }
}
