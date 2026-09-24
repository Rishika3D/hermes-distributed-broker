package io.hermes.core.net;

import io.hermes.core.model.Message;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageCodecTest {

    private static WireMessage roundTrip(WireMessage original) throws IOException {
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        MessageCodec.encode(original, new DataOutputStream(sink));
        return MessageCodec.decode(new DataInputStream(new ByteArrayInputStream(sink.toByteArray())));
    }

    @Test
    void roundTripsHeadersAndRecords() throws IOException {
        WireMessage original = WireMessage.of(FrameType.PRODUCE)
                .header("topic", "orders")
                .header("partition", 4)
                .record(new Message(9, 100L, "k1", "v1"))
                .record(new Message(10, 101L, null, "v2"));
        WireMessage decoded = roundTrip(original);
        assertEquals(FrameType.PRODUCE, decoded.type());
        assertEquals("orders", decoded.get("topic"));
        assertEquals(4, decoded.getInt("partition"));
        assertEquals(2, decoded.records().size());
        assertEquals("k1", decoded.records().get(0).key());
        assertNull(decoded.records().get(1).key());
        assertEquals("v2", decoded.records().get(1).value());
    }

    @Test
    void roundTripsEmptyFrame() throws IOException {
        WireMessage decoded = roundTrip(WireMessage.of(FrameType.HEARTBEAT_ACK));
        assertEquals(FrameType.HEARTBEAT_ACK, decoded.type());
        assertTrue(decoded.headers().isEmpty());
        assertTrue(decoded.records().isEmpty());
    }

    @Test
    void errorFrameCarriesReason() throws IOException {
        WireMessage decoded = roundTrip(WireMessage.error("boom"));
        assertTrue(decoded.isError());
        assertEquals("boom", decoded.get("reason"));
    }

    @Test
    void unknownFrameCodeIsRejected() {
        byte[] garbage = {(byte) 250, 0, 0, 0, 0, 0, 0, 0, 0};
        assertThrows(IllegalArgumentException.class,
                () -> MessageCodec.decode(new DataInputStream(new ByteArrayInputStream(garbage))));
    }
}
