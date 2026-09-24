package io.hermes.rest.api.dto;

/** Body of group join/heartbeat calls; memberId is null on first join. */
public record JoinGroupRequest(String topic, String memberId) {
}
