package io.hermes.core.group;

import io.hermes.core.model.TopicPartition;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Durable store for committed consumer offsets: an append-only text WAL of
 * {@code group topic partition offset} lines, replayed into memory on open.
 * The latest line for a key wins, so commits are simply appended. Each commit
 * is fsync'd (like the message log) so an acknowledged offset survives a power
 * loss, not merely a process crash.
 */
public final class OffsetStore implements Closeable {

    private final Path file;
    private final Map<String, Long> committed = new ConcurrentHashMap<>();
    private final FileOutputStream fileOut;
    private final BufferedWriter writer;

    public OffsetStore(Path file) throws IOException {
        this.file = file.toAbsolutePath();
        Path parent = this.file.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        replay();
        this.fileOut = new FileOutputStream(file.toFile(), true);
        this.writer = new BufferedWriter(new OutputStreamWriter(fileOut, StandardCharsets.UTF_8));
    }

    private void replay() throws IOException {
        if (!Files.exists(file)) {
            return;
        }
        List<String> lines = Files.readAllLines(file);
        for (String line : lines) {
            String[] parts = line.trim().split(" ");
            if (parts.length != 4) {
                continue; // torn trailing write
            }
            committed.put(key(parts[0], new TopicPartition(parts[1], Integer.parseInt(parts[2]))),
                    Long.parseLong(parts[3]));
        }
    }

    public synchronized void commit(String group, TopicPartition tp, long offset) throws IOException {
        writer.write(group + " " + tp.topic() + " " + tp.partition() + " " + offset);
        writer.newLine();
        writer.flush();
        fileOut.getFD().sync(); // fsync: survive power loss, not just process crash
        committed.put(key(group, tp), offset);
    }

    /** The committed offset (next offset to consume), or -1 if never committed. */
    public long committed(String group, TopicPartition tp) {
        return committed.getOrDefault(key(group, tp), -1L);
    }

    private static String key(String group, TopicPartition tp) {
        return group + "|" + tp.topic() + "|" + tp.partition();
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
