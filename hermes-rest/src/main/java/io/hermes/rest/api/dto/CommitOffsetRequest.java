package io.hermes.rest.api.dto;

/** Body of {@code POST /api/groups/{group}/offsets}: the next offset to consume. */
public record CommitOffsetRequest(String topic, int partition, long offset) {
}
