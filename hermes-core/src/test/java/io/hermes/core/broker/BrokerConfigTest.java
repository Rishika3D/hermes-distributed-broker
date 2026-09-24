package io.hermes.core.broker;

import io.hermes.core.cluster.BrokerNode;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link BrokerConfig}: member-list parsing, the ack-policy
 * contract, and the normalisation applied in the compact constructor
 * (defaults, blank handling). These guards protect operators from silent
 * misconfiguration.
 */
class BrokerConfigTest {

    private static final List<BrokerNode> MEMBERS = List.of(
            new BrokerNode(1, "localhost", 9091), new BrokerNode(2, "localhost", 9092));

    private static BrokerConfig config(String acks) {
        return new BrokerConfig(1, Path.of("/tmp/x"), MEMBERS, 2, 3, 1024, null, acks);
    }

    @Test
    void parsesMemberListAndSkipsBlanks() {
        List<BrokerNode> members = BrokerConfig.parseMembers("1@localhost:9091, 2@localhost:9092 ,");
        assertEquals(2, members.size());
        assertEquals(2, members.get(1).id());
    }

    @Test
    void acksDefaultsToAllAndRequiresQuorum() {
        assertTrue(config(null).requireQuorum());
        assertTrue(config("").requireQuorum());
        assertTrue(config("all").requireQuorum());
    }

    @Test
    void acksLeaderDoesNotRequireQuorum() {
        assertFalse(config("leader").requireQuorum());
    }

    @Test
    void invalidAcksIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> config("some-nonsense"));
    }

    @Test
    void batchAndLingerDefaultsAreNormalised() {
        BrokerConfig c = new BrokerConfig(1, Path.of("/tmp/x"), MEMBERS, 2, 3, 1024,
                null, "all", 0, -5, true);
        assertEquals(BrokerConfig.DEFAULT_BATCH_MAX_RECORDS, c.batchMaxRecords(), "0 -> default");
        assertEquals(0, c.lingerMs(), "negative linger -> 0");
        assertTrue(c.groupCommit());
    }

    @Test
    void localNodeMustBePresentInMembers() {
        BrokerConfig missing = new BrokerConfig(99, Path.of("/tmp/x"), MEMBERS, 2, 3, 1024, null);
        assertThrows(IllegalArgumentException.class, missing::localNode);
    }

    @Test
    void localNodeResolvesFromMembers() {
        assertEquals(9091, config("all").localNode().port());
    }
}
