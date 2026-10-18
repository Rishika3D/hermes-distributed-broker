package io.hermes.core.storage;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Coalesces fsyncs across concurrent writers (group commit).
 *
 * <p>Protocol: a writer appends its bytes to the log, and <em>while still holding the
 * log's write monitor</em> takes a {@link #ticket()}; then, after releasing the monitor,
 * it calls {@link #awaitDurable(long)}. One waiting thread at a time becomes the
 * flusher and runs the supplied {@link Syncer}; everyone whose ticket it covers is
 * released by that single sync.
 *
 * <p>Safety argument: tickets are issued only after the corresponding bytes are in the
 * channel, and in write order. The flusher reads {@code issued} <em>before</em> syncing,
 * so every ticket &le; that value has its bytes written before the sync begins and is
 * therefore covered by it. A waiter is released only after a sync that began after its
 * ticket was issued has <b>completed successfully</b>.
 */
final class GroupCommitter {

    /** Makes everything written so far durable (e.g. {@code FileChannel.force}). */
    @FunctionalInterface
    interface Syncer {
        void sync() throws IOException;
    }

    private final Syncer syncer;
    private final long lingerMs;
    private final AtomicLong issued = new AtomicLong();

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition synced = lock.newCondition();
    private long durable;          // highest ticket known durable; guarded by lock
    private boolean flushing;      // a thread is currently running the syncer; guarded by lock

    GroupCommitter(Syncer syncer, long lingerMs) {
        this.syncer = syncer;
        this.lingerMs = lingerMs;
    }

    /** Call while holding the log write monitor, after the bytes have been written. */
    long ticket() {
        return issued.incrementAndGet();
    }

    /** Blocks until a successful sync covering {@code ticket} has completed. */
    void awaitDurable(long ticket) throws IOException {
        lock.lock();
        try {
            while (durable < ticket) {
                if (flushing) {
                    synced.awaitUninterruptibly();
                    continue;
                }
                flushing = true;
                lock.unlock();
                long target = 0;
                boolean succeeded = false;
                try {
                    if (lingerMs > 0) {
                        Thread.sleep(lingerMs);   // let more writers pile into this sync
                    }
                    target = issued.get();        // captured immediately before the sync
                    syncer.sync();
                    succeeded = true;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new InterruptedIOException("group commit interrupted");
                } finally {
                    lock.lock();
                    if (succeeded) {
                        durable = Math.max(durable, target);   // never advance on a failed sync
                    }
                    flushing = false;
                    synced.signalAll();           // on failure waiters wake, retry the sync themselves
                }
            }
        } finally {
            lock.unlock();
        }
    }
}
