package io.hermes.core.group;

import java.util.List;

/** Outcome of joining (or heartbeating) a consumer group: identity, generation and assignment. */
public record JoinResult(String memberId, int generation, List<Integer> partitions) {
}
