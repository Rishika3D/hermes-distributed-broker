package io.hermes.rest.api;

import io.hermes.core.broker.Broker;
import io.hermes.core.model.Message;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/** Consumer fetch endpoint: read a partition from a given offset. */
@RestController
public class ConsumeController {

    private final Broker broker;

    public ConsumeController(Broker broker) {
        this.broker = broker;
    }

    @GetMapping("/api/topics/{topic}/partitions/{partition}/messages")
    public Map<String, Object> fetch(@PathVariable String topic,
                                     @PathVariable int partition,
                                     @RequestParam(defaultValue = "0") long offset,
                                     @RequestParam(defaultValue = "100") int max) throws IOException {
        List<Message> records = broker.fetch(topic, partition, offset, max);
        long nextOffset = records.isEmpty() ? offset : records.get(records.size() - 1).offset() + 1;
        return Map.of(
                "topic", topic,
                "partition", partition,
                "records", records,
                "nextOffset", nextOffset);
    }
}
