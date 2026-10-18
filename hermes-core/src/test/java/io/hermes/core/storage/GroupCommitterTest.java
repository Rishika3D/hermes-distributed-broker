package io.hermes.core.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Fault-injection tests for {@link GroupCommitter}. A real {@code force()} cannot be made
 * to fail on demand, so the sync step is injected. The invariant under test: a writer is
 * NEVER released as durable unless a sync that covers its ticket completed successfully.
 */
@Timeout(30)
class GroupCommitterTest {

    @Test
    void failedSyncDoesNotReleaseTheWriterAsDurable() {
        AtomicInteger successfulSyncs = new AtomicInteger();
        GroupCommitter committer = new GroupCommitter(() -> {
            throw new IOException("injected EIO");
        }, 0);

        long ticket = committer.ticket();
        assertThrows(IOException.class, () -> committer.awaitDurable(ticket));

        // The ticket must still be undurable: a later call must hit the syncer again
        // (and fail again) instead of returning immediately as "already durable".
        assertThrows(IOException.class, () -> committer.awaitDurable(ticket),
                "ticket must not be treated as durable after a failed sync");
        assertEquals(0, successfulSyncs.get());
    }

    @Test
    void concurrentWaitersAreNotReleasedByAFailedSync() throws Exception {
        int writers = 8;
        AtomicInteger syncCalls = new AtomicInteger();
        AtomicInteger successfulSyncs = new AtomicInteger();
        CountDownLatch allTicketed = new CountDownLatch(writers);

        GroupCommitter committer = new GroupCommitter(() -> {
            if (syncCalls.incrementAndGet() == 1) {
                throw new IOException("injected EIO on first sync");
            }
            successfulSyncs.incrementAndGet();
        }, 0);

        List<Thread> threads = new ArrayList<>();
        List<Boolean> returnedNormally = java.util.Collections.synchronizedList(new ArrayList<>());
        List<Integer> successfulSyncsAtReturn = java.util.Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < writers; i++) {
            Thread t = new Thread(() -> {
                long ticket = committer.ticket();
                allTicketed.countDown();
                try {
                    allTicketed.await();
                    committer.awaitDurable(ticket);
                    successfulSyncsAtReturn.add(successfulSyncs.get());
                    returnedNormally.add(true);
                } catch (IOException e) {
                    returnedNormally.add(false);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            threads.add(t);
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }

        assertEquals(writers, returnedNormally.size());
        // Anyone who returned normally must have been covered by a *successful* sync.
        for (int seen : successfulSyncsAtReturn) {
            assertTrue(seen >= 1, "a writer returned as durable before any successful sync");
        }
    }

    @Test
    void healthySyncReleasesEveryCoveredWriterWithFewSyncs() throws Exception {
        int writers = 16;
        AtomicInteger syncs = new AtomicInteger();
        CountDownLatch allTicketed = new CountDownLatch(writers);
        GroupCommitter committer = new GroupCommitter(syncs::incrementAndGet, 0);

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < writers; i++) {
            Thread t = new Thread(() -> {
                long ticket = committer.ticket();
                allTicketed.countDown();
                try {
                    allTicketed.await();     // every ticket exists before anyone waits -> one sync covers all
                    committer.awaitDurable(ticket);
                } catch (IOException | InterruptedException e) {
                    throw new AssertionError(e);
                }
            });
            threads.add(t);
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
        assertEquals(1, syncs.get(), "all 16 writers were ticketed before the sync -> exactly one sync");
    }

    @Test
    void recoversAfterTransientFailure() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        GroupCommitter committer = new GroupCommitter(() -> {
            if (calls.incrementAndGet() == 1) {
                throw new IOException("transient");
            }
        }, 0);

        long first = committer.ticket();
        assertThrows(IOException.class, () -> committer.awaitDurable(first));
        committer.awaitDurable(first);               // retry succeeds on the second sync
        assertEquals(2, calls.get());
    }
}
