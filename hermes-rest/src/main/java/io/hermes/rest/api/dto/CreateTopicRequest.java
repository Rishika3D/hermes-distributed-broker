package io.hermes.rest.api.dto;

/** Body of {@code POST /api/topics}; partitions {@code <= 0} means use the broker default. */
public record CreateTopicRequest(String name, Integer partitions) {

    public int partitionsOrDefault() {
        return partitions == null ? 0 : partitions;
    }
}
