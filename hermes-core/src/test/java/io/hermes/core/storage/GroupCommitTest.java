package io.hermes.core.storage;

import io.hermes.core.model.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies group commit: concurrent single appends to one partition are
 * correct (unique contiguous offsets, all durable and readable) AND coalesce
 * their fsyncs (far fewer force() calls than appends). Also checks the
 * durability invariant survives a reopen.
 */
@Timeout(60)
class GroupCommitTest {

    @TempDir
    Path dir;

    @Test
    void concurrentAppendsAreCorrectAndCoalesceFsyncs() throws Exception {
        int threads = 8;
        int perThread = 500;
        int total = threads * perThread;   // 4000 concurrent appends to one partition
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Set<Long> offsets = ConcurrentHashMap.newKeySet();

        // linger 2ms: the flusher waits briefly so concurrent writers pile into one fsync
        try (PartitionLog log = PartitionLog.open(dir, 64 * 1024 * 1024, true, 2)) {
            CyclicBarrier barrier = new CyclicBarrier(threads);
            List<Thread> workers = new java.util.ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int id = t;
                Thread worker = new Thread(() -> {
                    try {
                        barrier.await(); // maximise contention
                        for (int i = 0; i < perThread; i++) {
                            Message m = log.append("k" + id, "t" + id + "-" + i, System.currentTimeMillis());
                            if (!offsets.add(m.offset())) {
                                failure.compareAndSet(null,
                                        new AssertionError("duplicate offset " + m.offset()));
                            }
                        }
                    } catch (Throwable e) {
                        failure.compareAndSet(null, e);
                    }
                });
                workers.add(worker);
                worker.start();
            }
            for (Thread w : workers) {
                w.join();
            }

            assertNull(failure.get(), () -> "worker failed: " + failure.get());
            assertEquals(total, offsets.size(), "every append must get a unique offset");
            assertEquals(total, log.endOffset(), "offsets must be contiguous 0..total-1");

            long fsyncs = log.totalFlushes();
            assertTrue(fsyncs < total / 3,
                    "group commit must coalesce: " + fsyncs + " fsyncs for " + total + " appends");
            System.out.println("[GROUP-COMMIT] " + total + " concurrent appends (" + threads
                    + " threads) used " + fsyncs + " fsyncs — "
                    + String.format("%.1fx", (double) total / fsyncs) + " coalescing");

            // every record must be durable and readable
            assertEquals(total, log.read(0, total + 10).size());
        }

        // durability across reopen: all records survive
        try (PartitionLog reopened = PartitionLog.open(dir, 64 * 1024 * 1024)) {
            assertEquals(total, reopened.endOffset());
        }
    }

    @Test
    void serialModeStillFsyncsEveryAppend() throws IOException {
        try (PartitionLog log = PartitionLog.open(dir, 64 * 1024 * 1024, false, 0)) {
            long before = log.totalFlushes();
            for (int i = 0; i < 200; i++) {
                log.append("k", "v" + i, i);
            }
            assertEquals(200, log.totalFlushes() - before,
                    "with group commit off, each append is its own fsync");
        }
    }
}
