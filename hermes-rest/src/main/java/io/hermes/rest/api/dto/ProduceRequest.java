package io.hermes.rest.api.dto;

/** Body of {@code POST /api/topics/{topic}/messages}; a null key means round-robin routing. */
public record ProduceRequest(String key, String value) {
}
