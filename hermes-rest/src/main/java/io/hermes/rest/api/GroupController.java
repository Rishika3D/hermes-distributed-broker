package io.hermes.rest.api;

import io.hermes.core.broker.Broker;
import io.hermes.core.group.JoinResult;
import io.hermes.rest.api.dto.CommitOffsetRequest;
import io.hermes.rest.api.dto.JoinGroupRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/** Consumer group coordination: join, heartbeat, offset commits and lag. */
@RestController
public class GroupController {

    private final Broker broker;

    public GroupController(Broker broker) {
        this.broker = broker;
    }

    @PostMapping("/api/groups/{group}/join")
    public JoinResult join(@PathVariable String group, @RequestBody JoinGroupRequest request)
            throws IOException {
        if (request.topic() == null || request.topic().isBlank()) {
            throw new IllegalArgumentException("topic is required to join a group");
        }
        return broker.joinGroup(group, request.topic(), request.memberId());
    }

    @PostMapping("/api/groups/{group}/heartbeat")
    public JoinResult heartbeat(@PathVariable String group, @RequestBody JoinGroupRequest request)
            throws IOException {
        if (request.memberId() == null || request.memberId().isBlank()) {
            throw new IllegalArgumentException("memberId is required to heartbeat");
        }
        return broker.heartbeatGroup(group, request.memberId());
    }

    @PostMapping("/api/groups/{group}/offsets")
    public Map<String, String> commit(@PathVariable String group, @RequestBody CommitOffsetRequest request)
            throws IOException {
        broker.commitOffset(group, request.topic(), request.partition(), request.offset());
        return Map.of("status", "committed");
    }

    @GetMapping("/api/groups/{group}/offsets")
    public Map<Integer, Long> committed(@PathVariable String group, @RequestParam String topic)
            throws IOException {
        return broker.committedOffsets(group, topic);
    }

    @GetMapping("/api/lag")
    public List<Map<String, Object>> lag() throws IOException {
        return broker.lag();
    }
}
