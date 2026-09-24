package io.hermes.core.storage;

import io.hermes.core.model.Message;
import io.hermes.core.model.ProduceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the batching contract: a batch of N records is one fsync and
 * assigns contiguous offsets, and replayed batches are idempotent.
 */
class PartitionLogBatchTest {

    @TempDir
    Path dir;

    private static List<ProduceRecord> records(int n) {
        List<ProduceRecord> r = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            r.add(new ProduceRecord("k" + i, "v" + i));
        }
        return r;
    }

    @Test
    void batchOfNRecordsCostsExactlyOneFsync() throws IOException {
        try (PartitionLog log = PartitionLog.open(dir, 8 * 1024 * 1024)) {
            long before = log.totalFlushes();
            List<Message> stored = log.appendBatch(records(1000), 1L);
            long after = log.totalFlushes();
            assertEquals(1000, stored.size());
            assertEquals(1, after - before, "1000 records must cost exactly one fsync");
        }
    }

    @Test
    void singleAppendsCostOneFsyncEach_provingTheContrast() throws IOException {
        try (PartitionLog log = PartitionLog.open(dir, 8 * 1024 * 1024)) {
            long before = log.totalFlushes();
            for (int i = 0; i < 100; i++) {
                log.append("k" + i, "v" + i, i);
            }
            assertEquals(100, log.totalFlushes() - before, "100 single appends = 100 fsyncs");
        }
    }

    @Test
    void batchAssignsContiguousOffsetsAndPersists() throws IOException {
        try (PartitionLog log = PartitionLog.open(dir, 8 * 1024 * 1024)) {
            log.append("first", "0", 0);                 // offset 0
            List<Message> stored = log.appendBatch(records(50), 1L); // offsets 1..50
            assertEquals(1, stored.get(0).offset());
            assertEquals(50, stored.get(49).offset());
            assertEquals(51, log.endOffset());
            List<Message> readBack = log.read(1, 1000);
            assertEquals(50, readBack.size());
            assertEquals("v0", readBack.get(0).value());
            assertEquals("v49", readBack.get(49).value());
        }
    }

    @Test
    void replayedBatchIsIdempotent() throws IOException {
        List<Message> first;
        try (PartitionLog log = PartitionLog.open(dir, 8 * 1024 * 1024)) {
            first = log.appendBatch(records(30), 1L);
        }
        try (PartitionLog reopened = PartitionLog.open(dir, 8 * 1024 * 1024)) {
            assertEquals(30, reopened.endOffset());
            // re-applying the same already-stored offsets must be a no-op (idempotent replication)
            reopened.appendBatchAssigned(first);
            assertEquals(30, reopened.endOffset(), "duplicate offsets must be skipped");
        }
    }

    @Test
    void batchRollsSegmentsAndStaysReadable() throws IOException {
        try (PartitionLog log = PartitionLog.open(dir, 512)) { // tiny segments force rolls
            log.appendBatch(records(200), 1L);
            assertTrue(log.segmentCount() > 1, "batch spanning the size limit should roll segments");
            assertEquals(200, log.read(0, 10_000).size());
        }
    }
}
