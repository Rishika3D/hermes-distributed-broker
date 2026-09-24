package io.hermes.core.broker;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WireEncodingTest {

    @Test
    void partitionsRoundTrip() {
        assertEquals(List.of(0, 3, 5), WireEncoding.decodePartitions(
                WireEncoding.encodePartitions(List.of(0, 3, 5))));
        assertTrue(WireEncoding.decodePartitions(WireEncoding.encodePartitions(List.of())).isEmpty());
        assertTrue(WireEncoding.decodePartitions(null).isEmpty());
    }

    @Test
    void offsetsRoundTrip() {
        Map<Integer, Long> offsets = new LinkedHashMap<>();
        offsets.put(0, 15L);
        offsets.put(1, -1L);
        offsets.put(2, 9000L);
        assertEquals(offsets, WireEncoding.decodeOffsets(WireEncoding.encodeOffsets(offsets)));
        assertTrue(WireEncoding.decodeOffsets("").isEmpty());
    }

    @Test
    void lagEntriesRoundTrip() {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("group", "g1");
        entry.put("topic", "orders");
        entry.put("partition", 2);
        entry.put("committed", 10L);
        entry.put("endOffset", 25L);
        entry.put("lag", 15L);
        entry.put("generation", 3);
        entry.put("members", 2);
        List<Map<String, Object>> decoded = WireEncoding.decodeLag(WireEncoding.encodeLag(List.of(entry)));
        assertEquals(1, decoded.size());
        assertEquals(entry, decoded.get(0));
        assertTrue(WireEncoding.decodeLag("").isEmpty());
    }
}
