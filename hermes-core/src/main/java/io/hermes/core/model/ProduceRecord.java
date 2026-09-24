package io.hermes.core.model;

/**
 * A single record submitted by a producer, before the leader assigns it an
 * offset. Key may be null (round-robin routing); value must not be.
 */
public record ProduceRecord(String key, String value) {

    public ProduceRecord {
        if (value == null) {
            throw new IllegalArgumentException("message value must not be null");
        }
    }
}
