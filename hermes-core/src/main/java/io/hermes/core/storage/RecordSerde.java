package io.hermes.core.storage;

import io.hermes.core.model.Message;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Binary on-disk encoding of a single record:
 *
 * <pre>
 *   offset      int64
 *   timestamp   int64
 *   keyLength   int32   (-1 encodes a null key)
 *   keyBytes    byte[keyLength]
 *   valueLength int32
 *   valueBytes  byte[valueLength]
 * </pre>
 */
public final class RecordSerde {

    private RecordSerde() {
    }

    public static ByteBuffer serialize(Message message) {
        byte[] key = message.key() == null ? null : message.key().getBytes(StandardCharsets.UTF_8);
        byte[] value = message.value().getBytes(StandardCharsets.UTF_8);
        int size = 8 + 8 + 4 + (key == null ? 0 : key.length) + 4 + value.length;
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putLong(message.offset());
        buffer.putLong(message.timestamp());
        buffer.putInt(key == null ? -1 : key.length);
        if (key != null) {
            buffer.put(key);
        }
        buffer.putInt(value.length);
        buffer.put(value);
        buffer.flip();
        return buffer;
    }

    /**
     * Reads the next record from the stream, or returns null at a clean end of file.
     * A truncated trailing record (torn write) also terminates the read cleanly, so
     * recovery after a crash simply ignores the incomplete tail.
     */
    public static Message read(DataInputStream in) throws IOException {
        long offset;
        try {
            offset = in.readLong();
        } catch (EOFException endOfLog) {
            return null;
        }
        try {
            long timestamp = in.readLong();
            int keyLength = in.readInt();
            String key = null;
            if (keyLength >= 0) {
                byte[] keyBytes = new byte[keyLength];
                in.readFully(keyBytes);
                key = new String(keyBytes, StandardCharsets.UTF_8);
            }
            int valueLength = in.readInt();
            byte[] valueBytes = new byte[valueLength];
            in.readFully(valueBytes);
            return new Message(offset, timestamp, key, new String(valueBytes, StandardCharsets.UTF_8));
        } catch (EOFException tornWrite) {
            return null;
        }
    }
}
