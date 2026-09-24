package io.hermes.core.storage;

import io.hermes.core.model.Message;
import io.hermes.core.model.ProduceRecord;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

/**
 * The write-ahead log for one partition: an ordered chain of append-only
 * segments. Appends go to the active (last) segment and roll to a new segment
 * once it exceeds the size limit; reads locate the right segment by base
 * offset. On open, existing segments are discovered from disk and recovered.
 */
public final class PartitionLog implements Closeable {

    private final Path dir;
    private final long maxSegmentBytes;
    private final boolean groupCommit;
    private final long lingerMs;
    private final List<LogSegment> segments = new ArrayList<>();

    // Group-commit coordination. writeSeq counts completed channel writes (assigned
    // under the write monitor); flushedSeq is the highest writeSeq made durable.
    private final ReentrantLock flushLock = new ReentrantLock();
    private final Condition flushed = flushLock.newCondition();
    private long writeSeq;
    private long flushedSeq;
    private boolean flushing;

    private PartitionLog(Path dir, long maxSegmentBytes, boolean groupCommit, long lingerMs) {
        this.dir = dir;
        this.maxSegmentBytes = maxSegmentBytes;
        this.groupCommit = groupCommit;
        this.lingerMs = lingerMs;
    }

    public static PartitionLog open(Path dir, long maxSegmentBytes) throws IOException {
        return open(dir, maxSegmentBytes, false, 0);
    }

    public static PartitionLog open(Path dir, long maxSegmentBytes, boolean groupCommit, long lingerMs)
            throws IOException {
        PartitionLog log = new PartitionLog(dir, maxSegmentBytes, groupCommit, lingerMs);
        log.loadSegments();
        return log;
    }

    private void loadSegments() throws IOException {
        Files.createDirectories(dir);
        List<Long> baseOffsets = new ArrayList<>();
        try (Stream<Path> files = Files.list(dir)) {
            files.filter(p -> p.getFileName().toString().endsWith(".log"))
                    .forEach(p -> {
                        String name = p.getFileName().toString();
                        baseOffsets.add(Long.parseLong(name.substring(0, name.length() - 4)));
                    });
        }
        baseOffsets.sort(Comparator.naturalOrder());
        for (long baseOffset : baseOffsets) {
            segments.add(LogSegment.open(dir, baseOffset));
        }
        if (segments.isEmpty()) {
            segments.add(LogSegment.open(dir, 0L));
        }
    }

    /**
     * Appends a single record with a leader-assigned offset. With group commit
     * disabled this is a serial write + fsync under the monitor. With it
     * enabled, the write happens under the monitor but the fsync is coalesced
     * across concurrent appenders via {@link #groupFlush}, so N concurrent
     * single appends to this partition share far fewer than N fsyncs.
     */
    public Message append(String key, String value, long timestamp) throws IOException {
        if (!groupCommit) {
            synchronized (this) {
                Message message = new Message(endOffset(), timestamp, key, value);
                appendAssigned(message);
                return message;
            }
        }
        Message message;
        long mySeq;
        synchronized (this) {
            message = new Message(endOffset(), timestamp, key, value);
            writeAssigned(message);      // write bytes to the channel, no fsync (may roll + flush old seg)
            mySeq = ++writeSeq;
        }
        groupFlush(mySeq);
        return message;
    }

    /**
     * Coalesced fsync. A thread returns only once a {@code force()} that began
     * after its write completed has finished, so its bytes are durable.
     *
     * <p>Correctness: {@code force()} makes durable every byte written to the
     * channel before it was called. The flusher captures {@code target =
     * writeSeq} (>= the caller's seq) under the monitor, then forces the active
     * segment; because writes are serialized and assign their seq only after
     * writing their bytes, all writes with seq <= target are already in the
     * channel, so the force covers them. It then sets {@code flushedSeq =
     * target}. A waiter with seq S is released only when {@code flushedSeq >=
     * S} — i.e. by a force that started strictly after its write. Over-covering
     * (bytes with seq > target that raced in before the force) is harmless.
     */
    private void groupFlush(long mySeq) throws IOException {
        flushLock.lock();
        try {
            while (flushedSeq < mySeq) {
                if (flushing) {
                    flushed.awaitUninterruptibly();
                    continue;
                }
                flushing = true;
                flushLock.unlock();
                long target = 0;
                try {
                    if (lingerMs > 0) {
                        Thread.sleep(lingerMs);      // let more writers accumulate before forcing
                    }
                    // Capture target AFTER the linger, immediately before the force, so every
                    // write that landed during the window is covered by this one fsync.
                    LogSegment segment;
                    synchronized (this) {
                        target = writeSeq;
                        segment = activeSegment();
                    }
                    segment.flush();                 // the shared fsync, held under no lock
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("group flush interrupted", e);
                } finally {
                    flushLock.lock();
                    flushedSeq = Math.max(flushedSeq, target);
                    flushing = false;
                    flushed.signalAll();
                }
            }
        } finally {
            flushLock.unlock();
        }
    }

    /**
     * Appends a batch of records under one lock acquisition with a single fsync
     * for the whole batch, and returns the stored messages with their assigned
     * (contiguous) offsets. This is the core throughput lever: N records cost
     * one {@code force()} instead of N.
     */
    public List<Message> appendBatch(List<ProduceRecord> records, long timestamp) throws IOException {
        List<Message> stored = new ArrayList<>(records.size());
        long mySeq;
        synchronized (this) {
            for (ProduceRecord record : records) {
                Message message = new Message(endOffset(), timestamp, record.key(), record.value());
                writeAssigned(message);
                stored.add(message);
            }
            if (!groupCommit) {
                activeSegment().flush();      // one fsync for this batch, serial mode
                return stored;
            }
            mySeq = ++writeSeq;               // whole batch shares one write-sequence slot
        }
        // Group-commit mode: concurrent batches (including single-record produces from
        // the leader path) coalesce their fsyncs via the shared flusher.
        groupFlush(mySeq);
        return stored;
    }

    /**
     * Appends a message whose offset was already assigned by the partition
     * leader (the replica path). Offsets at or below the current end are
     * duplicates from replication retries and are ignored.
     */
    public synchronized void appendAssigned(Message message) throws IOException {
        writeAssigned(message);
        activeSegment().flush();
    }

    /**
     * Replica-side batch apply: writes all records with a single fsync,
     * mirroring {@link #appendBatch} so replication of a batch is also one
     * flush on the follower.
     */
    public synchronized void appendBatchAssigned(List<Message> messages) throws IOException {
        boolean wroteAny = false;
        for (Message message : messages) {
            if (message.offset() >= endOffset()) {
                writeAssigned(message);
                wroteAny = true;
            }
        }
        if (wroteAny) {
            activeSegment().flush();
        }
    }

    /**
     * Writes one record to the active segment (rolling if full) WITHOUT
     * flushing. Callers are responsible for a subsequent {@code flush()} to
     * make the write durable — this is what lets a batch share one fsync.
     * Duplicate/replayed offsets at or below the end are skipped.
     */
    private void writeAssigned(Message message) throws IOException {
        if (message.offset() < endOffset()) {
            return;
        }
        LogSegment active = activeSegment();
        if (active.sizeBytes() >= maxSegmentBytes) {
            active.flush();
            active = LogSegment.open(dir, endOffset());
            segments.add(active);
        }
        active.append(message);
    }

    public synchronized List<Message> read(long fromOffset, int maxRecords) throws IOException {
        return read(fromOffset, maxRecords, Long.MAX_VALUE);
    }

    /**
     * Reads up to {@code maxRecords} records from {@code fromOffset}, stopping
     * early once the accumulated encoded size would exceed {@code maxBytes}
     * (bounding heap use per fetch). At least one record is always returned if
     * any are available.
     */
    public synchronized List<Message> read(long fromOffset, int maxRecords, long maxBytes) throws IOException {
        List<Message> result = new ArrayList<>();
        long from = Math.max(fromOffset, 0);
        long remainingBytes = maxBytes;
        for (LogSegment segment : segments) {
            if (result.size() >= maxRecords || remainingBytes <= 0) {
                break;
            }
            if (segment.endOffset() <= from) {
                continue;
            }
            List<Message> batch = segment.read(Math.max(from, segment.baseOffset()),
                    maxRecords - result.size(), remainingBytes);
            for (Message m : batch) {
                remainingBytes -= m.sizeInBytes();
            }
            result.addAll(batch);
        }
        return result;
    }

    /** The offset that the next appended message will receive. */
    public synchronized long endOffset() {
        return activeSegment().endOffset();
    }

    public synchronized int segmentCount() {
        return segments.size();
    }

    /** Total fsyncs across all segments — one per append/batch/roll. */
    public synchronized long totalFlushes() {
        long total = 0;
        for (LogSegment segment : segments) {
            total += segment.flushCount();
        }
        return total;
    }

    private LogSegment activeSegment() {
        return segments.get(segments.size() - 1);
    }

    @Override
    public synchronized void close() throws IOException {
        for (LogSegment segment : segments) {
            segment.close();
        }
    }
}
