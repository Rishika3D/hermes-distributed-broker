package io.hermes.core.storage;

import io.hermes.core.model.Message;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class RecordSerdeTest {

    private static Message roundTrip(Message original) throws IOException {
        ByteBuffer buffer = RecordSerde.serialize(original);
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return RecordSerde.read(new DataInputStream(new ByteArrayInputStream(bytes)));
    }

    @Test
    void roundTripsKeyedRecord() throws IOException {
        Message m = roundTrip(new Message(42, 1234567890L, "customer-7", "{\"a\":1}"));
        assertEquals(42, m.offset());
        assertEquals(1234567890L, m.timestamp());
        assertEquals("customer-7", m.key());
        assertEquals("{\"a\":1}", m.value());
    }

    @Test
    void roundTripsNullKeyAndUnicode() throws IOException {
        Message m = roundTrip(new Message(0, 1L, null, "नमस्ते ಬೆಂಗಳೂರು 🚦"));
        assertNull(m.key());
        assertEquals("नमस्ते ಬೆಂಗಳೂರು 🚦", m.value());
    }

    @Test
    void serializedSizeMatchesSizeInBytes() {
        Message m = new Message(7, 9L, "k", "value-φ");
        assertEquals(m.sizeInBytes(), RecordSerde.serialize(m).remaining());
    }

    @Test
    void tornTrailingWriteReadsAsEndOfLog() throws IOException {
        ByteBuffer buffer = RecordSerde.serialize(new Message(5, 5L, "key", "value"));
        byte[] full = new byte[buffer.remaining()];
        buffer.get(full);
        byte[] torn = Arrays.copyOf(full, full.length - 3);
        assertNull(RecordSerde.read(new DataInputStream(new ByteArrayInputStream(torn))),
                "a truncated record must terminate the read cleanly");
    }

    @Test
    void emptyStreamReadsAsEndOfLog() throws IOException {
        assertNull(RecordSerde.read(new DataInputStream(new ByteArrayInputStream(new byte[0]))));
    }
}
