package io.hermes.core.cluster;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link BrokerNode#parse}, the parser for the {@code id@host:port}
 * member-list entries. This is untrusted config input, so its error paths matter
 * as much as the happy path — a malformed member list must fail fast and clearly.
 */
class BrokerNodeTest {

    @Test
    void parsesWellFormedSpec() {
        BrokerNode node = BrokerNode.parse("3@broker-3.local:9093");
        assertEquals(3, node.id());
        assertEquals("broker-3.local", node.host());
        assertEquals(9093, node.port());
    }

    @Test
    void toleratesSurroundingWhitespace() {
        assertEquals(new BrokerNode(1, "localhost", 9091), BrokerNode.parse("  1@localhost:9091 "));
    }

    @Test
    void rejectsMissingAtSeparator() {
        assertThrows(IllegalArgumentException.class, () -> BrokerNode.parse("1-localhost:9091"));
    }

    @Test
    void rejectsMissingPort() {
        assertThrows(IllegalArgumentException.class, () -> BrokerNode.parse("1@localhost"));
    }

    @Test
    void rejectsNonNumericIdOrPort() {
        assertThrows(NumberFormatException.class, () -> BrokerNode.parse("x@localhost:9091"));
        assertThrows(NumberFormatException.class, () -> BrokerNode.parse("1@localhost:port"));
    }
}
