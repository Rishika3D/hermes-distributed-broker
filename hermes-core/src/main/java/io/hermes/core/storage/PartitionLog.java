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
    private final List<LogSegment> segments = new ArrayList<>();
    /** Non-null when group commit is enabled: coalesces fsyncs across concurrent appenders. */
    private final GroupCommitter committer;

    private PartitionLog(Path dir, long maxSegmentBytes, boolean groupCommit, long lingerMs) {
        this.dir = dir;
        this.maxSegmentBytes = maxSegmentBytes;
        this.committer = groupCommit ? new GroupCommitter(this::syncActiveSegment, lingerMs) : null;
    }

    /**
     * Forces the active segment. Earlier segments were already fsynced when the log rolled
     * (see {@link #writeAssigned}), so forcing the active one covers every outstanding write.
     */
    private void syncActiveSegment() throws IOException {
        LogSegment active;
        synchronized (this) {
            active = activeSegment();
        }
        active.flush();
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
     * Appends a single record with a leader-assigned offset and returns only once it is
     * durable. With group commit off this is a serial write + fsync under the monitor; with
     * it on, the write is still serialized but the fsync is shared with concurrent appenders
     * (see {@link GroupCommitter}), so N concurrent appends cost far fewer than N fsyncs.
     */
    public Message append(String key, String value, long timestamp) throws IOException {
        if (committer == null) {
            synchronized (this) {
                Message message = new Message(endOffset(), timestamp, key, value);
                appendAssigned(message);
                return message;
            }
        }
        Message message;
        long ticket;
        synchronized (this) {
            message = new Message(endOffset(), timestamp, key, value);
            writeAssigned(message);       // bytes into the channel; durable only after the shared sync
            ticket = committer.ticket();  // issued under the monitor => ticket order == write order
        }
        committer.awaitDurable(ticket);
        return message;
    }

    /**
     * Appends a batch of records under one lock acquisition with a single fsync
     * for the whole batch, and returns the stored messages with their assigned
     * (contiguous) offsets. This is the core throughput lever: N records cost
     * one {@code force()} instead of N.
     */
    public List<Message> appendBatch(List<ProduceRecord> records, long timestamp) throws IOException {
        List<Message> stored = new ArrayList<>(records.size());
        long ticket;
        synchronized (this) {
            for (ProduceRecord record : records) {
                Message message = new Message(endOffset(), timestamp, record.key(), record.value());
                writeAssigned(message);
                stored.add(message);
            }
            if (committer == null) {
                activeSegment().flush();      // one fsync for the whole batch
                return stored;
            }
            ticket = committer.ticket();      // the whole batch shares one ticket
        }
        committer.awaitDurable(ticket);       // concurrent batches/singles share one sync
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
