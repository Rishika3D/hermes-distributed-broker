package io.hermes.rest.api;

import io.hermes.core.broker.Broker;
import io.hermes.core.broker.ProduceResult;
import io.hermes.core.model.ProduceRecord;
import io.hermes.rest.api.dto.ProduceRequest;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Producer endpoint: publish one message or a batch to a topic. */
@RestController
public class ProduceController {

    private final Broker broker;

    public ProduceController(Broker broker) {
        this.broker = broker;
    }

    @PostMapping("/api/topics/{topic}/messages")
    public ProduceResult produce(@PathVariable String topic, @RequestBody ProduceRequest request)
            throws IOException {
        if (request.value() == null) {
            throw new IllegalArgumentException("message value is required");
        }
        return broker.produce(topic, request.key(), request.value());
    }

    /**
     * Batch produce: all records in one request are grouped by partition and
     * each partition's sub-batch is appended with a single WAL write, one
     * fsync, and one replication frame. Durability is unchanged (acks=all still
     * fsyncs + quorum-replicates before returning); this only amortizes the
     * per-message overhead across the batch.
     */
    @PostMapping("/api/topics/{topic}/messages/batch")
    public List<ProduceResult> produceBatch(@PathVariable String topic,
                                            @RequestBody List<ProduceRequest> requests) throws IOException {
        List<ProduceRecord> records = new ArrayList<>(requests.size());
        for (ProduceRequest request : requests) {
            if (request.value() == null) {
                throw new IllegalArgumentException("message value is required");
            }
            records.add(new ProduceRecord(request.key(), request.value()));
        }
        return broker.produceBatch(topic, records);
    }
}
