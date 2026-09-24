package io.hermes.core.storage;

import io.hermes.core.model.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reproduces M5: a fetch must be bounded by bytes, not only record count, so a
 * client asking for max=10000 large records cannot force megabytes into heap.
 */
class FetchMemoryBoundTest {

    @TempDir
    Path dir;

    @Test
    void fetchStopsAtByteBudgetNotJustRecordCount() throws IOException {
        String oneKb = "x".repeat(1024);
        try (PartitionLog log = PartitionLog.open(dir, 8 * 1024 * 1024)) {
            for (int i = 0; i < 500; i++) {
                log.append("k" + i, oneKb, i);
            }

            // Count-only bound (old behaviour): all 500 records ~ 512 KB in heap.
            List<Message> unbounded = log.read(0, 10_000);
            assertEquals(500, unbounded.size());

            // Byte-bounded (fix): 16 KB budget yields ~16 records, far fewer than 10k.
            List<Message> bounded = log.read(0, 10_000, 16 * 1024);
            assertTrue(bounded.size() < 30,
                    "byte budget should cap the batch well below the record limit, got " + bounded.size());
            long bytes = bounded.stream().mapToLong(Message::sizeInBytes).sum();
            assertTrue(bytes <= 16 * 1024 + oneKb.length(),
                    "returned bytes must respect the budget (plus one straddling record): " + bytes);
            System.out.println("[M5] count-only=" + unbounded.size() + " records; "
                    + "byte-bounded(16KB)=" + bounded.size() + " records / " + bytes + " bytes");
        }
    }

    @Test
    void alwaysReturnsAtLeastOneRecordEvenIfItExceedsBudget() throws IOException {
        try (PartitionLog log = PartitionLog.open(dir, 8 * 1024 * 1024)) {
            log.append("k", "y".repeat(50_000), 0); // single record larger than the budget
            List<Message> result = log.read(0, 10_000, 1024);
            assertEquals(1, result.size(), "a single oversized record must not stall the reader");
        }
    }
}
