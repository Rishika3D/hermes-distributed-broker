package io.hermes.rest.api;

import io.hermes.core.broker.Broker;
import io.hermes.core.topic.Topic;
import io.hermes.rest.api.dto.CreateTopicRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/** Topic lifecycle: create and list. */
@RestController
@RequestMapping("/api/topics")
public class TopicController {

    private final Broker broker;

    public TopicController(Broker broker) {
        this.broker = broker;
    }

    @PostMapping
    public Map<String, Object> create(@RequestBody CreateTopicRequest request) throws IOException {
        if (request.name() == null || request.name().isBlank()) {
            throw new IllegalArgumentException("topic name is required");
        }
        Topic topic = broker.createTopic(request.name(), request.partitionsOrDefault());
        return Map.of("name", topic.name(), "partitionCount", topic.partitionCount());
    }

    @GetMapping
    public List<Map<String, Object>> list() {
        return broker.topics().stream()
                .map(topic -> Map.<String, Object>of(
                        "name", topic.name(),
                        "partitionCount", topic.partitionCount()))
                .toList();
    }
}
