package io.hermes.core.storage;

import io.hermes.core.model.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionLogTest {

    @TempDir
    Path dir;

    @Test
    void appendsAssignSequentialOffsets() throws IOException {
        try (PartitionLog log = PartitionLog.open(dir, 1024 * 1024)) {
            assertEquals(0, log.append("k1", "v1", 1L).offset());
            assertEquals(1, log.append(null, "v2", 2L).offset());
            assertEquals(2, log.endOffset());
        }
    }

    @Test
    void readsFromArbitraryOffsets() throws IOException {
        try (PartitionLog log = PartitionLog.open(dir, 1024 * 1024)) {
            for (int i = 0; i < 500; i++) {
                log.append("key-" + i, "value-" + i, i);
            }
            List<Message> records = log.read(250, 10);
            assertEquals(10, records.size());
            assertEquals(250, records.get(0).offset());
            assertEquals("value-250", records.get(0).value());
            assertEquals(259, records.get(9).offset());
        }
    }

    @Test
    void rollsSegmentsAndReadsAcrossThem() throws IOException {
        try (PartitionLog log = PartitionLog.open(dir, 512)) {
            for (int i = 0; i < 100; i++) {
                log.append("key-" + i, "value-" + i, i);
            }
            assertTrue(log.segmentCount() > 1, "expected the log to roll segments");
            List<Message> all = log.read(0, 1_000);
            assertEquals(100, all.size());
            assertEquals(99, all.get(99).offset());
        }
    }

    @Test
    void recoversFromTornWriteAtEndOfSegment() throws IOException {
        try (PartitionLog log = PartitionLog.open(dir, 1024 * 1024)) {
            for (int i = 0; i < 20; i++) {
                log.append("key-" + i, "value-" + i, i);
            }
        }
        // simulate a crash mid-write: chop bytes off the tail of the segment
        java.nio.file.Path segment;
        try (var files = java.nio.file.Files.list(dir)) {
            segment = files.filter(p -> p.toString().endsWith(".log")).findFirst().orElseThrow();
        }
        try (var channel = java.nio.channels.FileChannel.open(segment,
                java.nio.file.StandardOpenOption.WRITE)) {
            channel.truncate(channel.size() - 7);
        }
        try (PartitionLog recovered = PartitionLog.open(dir, 1024 * 1024)) {
            assertEquals(19, recovered.endOffset(), "torn record must be dropped");
            assertEquals("value-18", recovered.read(18, 1).get(0).value());
            assertEquals(19, recovered.append("k", "after-crash", 99L).offset());
        }
    }

    @Test
    void recoversStateAfterReopen() throws IOException {
        try (PartitionLog log = PartitionLog.open(dir, 512)) {
            for (int i = 0; i < 50; i++) {
                log.append("key-" + i, "value-" + i, i);
            }
        }
        try (PartitionLog reopened = PartitionLog.open(dir, 512)) {
            assertEquals(50, reopened.endOffset());
            List<Message> records = reopened.read(40, 100);
            assertEquals(10, records.size());
            assertEquals("value-40", records.get(0).value());
            assertNull(reopened.append(null, "after-restart", 99L).key());
            assertEquals(51, reopened.endOffset());
        }
    }
}
