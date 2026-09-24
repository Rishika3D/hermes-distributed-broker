package io.hermes.core.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validation and self-describing behaviour of the immutable value types.
 * A null value must be rejected at construction (it would corrupt the WAL
 * encoding), and {@link Message#sizeInBytes} must reflect the real payload so
 * segment rolling and byte-bounded fetch stay correct.
 */
class ModelValidationTest {

    @Test
    void messageRejectsNullValue() {
        assertThrows(IllegalArgumentException.class, () -> new Message(0, 1L, "k", null));
    }

    @Test
    void messageAllowsNullKey() {
        assertNull(new Message(0, 1L, null, "v").key());
    }

    @Test
    void messageFactoryStampsTimestamp() {
        long before = System.currentTimeMillis();
        Message m = Message.create(5, "k", "v");
        assertEquals(5, m.offset());
        assertTrue(m.timestamp() >= before);
    }

    @Test
    void sizeInBytesGrowsWithPayloadAndCountsNullKeyAsZero() {
        int keyed = new Message(0, 0L, "key", "value").sizeInBytes();
        int noKey = new Message(0, 0L, null, "value").sizeInBytes();
        assertTrue(keyed > noKey, "a key adds bytes");
        assertTrue(new Message(0, 0L, null, "longer-value").sizeInBytes() > noKey);
    }

    @Test
    void produceRecordRejectsNullValue() {
        assertThrows(IllegalArgumentException.class, () -> new ProduceRecord("k", null));
    }

    @Test
    void topicPartitionRendersStableString() {
        assertEquals("orders-4", new TopicPartition("orders", 4).toString());
    }
}
