package io.hermes.core.model;

import java.nio.charset.StandardCharsets;

/**
 * An immutable message as stored in a partition log. The offset is assigned by
 * the partition leader at append time; key may be null, value never is.
 */
public record Message(long offset, long timestamp, String key, String value) {

    public Message {
        if (value == null) {
            throw new IllegalArgumentException("message value must not be null");
        }
    }

    public static Message create(long offset, String key, String value) {
        return new Message(offset, System.currentTimeMillis(), key, value);
    }

    /** Approximate wire/disk footprint, used for metrics and segment rolling. */
    public int sizeInBytes() {
        int keyBytes = key == null ? 0 : key.getBytes(StandardCharsets.UTF_8).length;
        return 8 + 8 + 4 + 4 + keyBytes + value.getBytes(StandardCharsets.UTF_8).length;
    }
}
