package io.hermes.core.storage;

import io.hermes.core.model.Message;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * One append-only segment file of a partition log. Records are written
 * sequentially and never mutated; a sparse in-memory index is rebuilt by
 * scanning the file on open, which also recovers cleanly from torn writes.
 */
public final class LogSegment implements Closeable {

    /** Every Nth record gets an index entry. */
    private static final int INDEX_INTERVAL = 64;

    private final Path file;
    private final long baseOffset;
    private final SparseIndex index = new SparseIndex();
    private final FileChannel writeChannel;

    private long nextOffset;
    private long sizeBytes;
    private long recordCount;
    private long flushCount;

    private LogSegment(Path file, long baseOffset, FileChannel writeChannel) {
        this.file = file;
        this.baseOffset = baseOffset;
        this.writeChannel = writeChannel;
        this.nextOffset = baseOffset;
    }

    public static LogSegment open(Path dir, long baseOffset) throws IOException {
        Files.createDirectories(dir);
        Path file = dir.resolve(fileName(baseOffset));
        FileChannel channel = FileChannel.open(file,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        LogSegment segment = new LogSegment(file, baseOffset, channel);
        segment.recover();
        return segment;
    }

    public static String fileName(long baseOffset) {
        return String.format("%020d.log", baseOffset);
    }

    /** Rebuilds the sparse index, record count and next offset by scanning the file. */
    private void recover() throws IOException {
        long position = 0;
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(new FileInputStream(file.toFile())))) {
            while (true) {
                Message message = RecordSerde.read(in);
                if (message == null) {
                    break;
                }
                if (recordCount % INDEX_INTERVAL == 0) {
                    index.add(message.offset(), position);
                }
                position += message.sizeInBytes();
                recordCount++;
                nextOffset = message.offset() + 1;
            }
        }
        sizeBytes = position;
    }

    public synchronized void append(Message message) throws IOException {
        if (recordCount % INDEX_INTERVAL == 0) {
            index.add(message.offset(), sizeBytes);
        }
        ByteBuffer buffer = RecordSerde.serialize(message);
        int written = buffer.remaining();
        while (buffer.hasRemaining()) {
            writeChannel.write(buffer);
        }
        sizeBytes += written;
        recordCount++;
        nextOffset = message.offset() + 1;
    }

    /** Forces buffered writes to the storage device. */
    public synchronized void flush() throws IOException {
        writeChannel.force(false);
        flushCount++;
    }

    /** Number of fsyncs performed on this segment — used to verify batching. */
    public synchronized long flushCount() {
        return flushCount;
    }

    public List<Message> read(long fromOffset, int maxRecords, long maxBytes) throws IOException {
        List<Message> result = new ArrayList<>();
        if (maxRecords <= 0 || fromOffset >= endOffset()) {
            return result;
        }
        long startPosition = index.floorPosition(fromOffset);
        long accumulatedBytes = 0;
        try (FileInputStream fileIn = new FileInputStream(file.toFile())) {
            long skipped = 0;
            while (skipped < startPosition) {
                long n = fileIn.skip(startPosition - skipped);
                if (n <= 0) {
                    break;
                }
                skipped += n;
            }
            DataInputStream in = new DataInputStream(new BufferedInputStream(fileIn));
            while (result.size() < maxRecords) {
                Message message = RecordSerde.read(in);
                if (message == null) {
                    break;
                }
                if (message.offset() >= fromOffset) {
                    // Stop before exceeding the byte budget, but always return at
                    // least one record so a single large message can't stall a reader.
                    if (!result.isEmpty() && accumulatedBytes + message.sizeInBytes() > maxBytes) {
                        break;
                    }
                    result.add(message);
                    accumulatedBytes += message.sizeInBytes();
                }
            }
        }
        return result;
    }

    public long baseOffset() {
        return baseOffset;
    }

    public synchronized long endOffset() {
        return nextOffset;
    }

    public synchronized long sizeBytes() {
        return sizeBytes;
    }

    @Override
    public void close() throws IOException {
        writeChannel.close();
    }
}
