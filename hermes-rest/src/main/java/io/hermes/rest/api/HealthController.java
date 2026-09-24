package io.hermes.rest.api;

import io.hermes.core.broker.Broker;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Liveness/readiness probe for orchestrators: 200 once the cluster has an
 * elected controller (the node can serve or forward every operation),
 * 503 while an election is still in flight.
 */
@RestController
public class HealthController {

    private final Broker broker;

    public HealthController(Broker broker) {
        this.broker = broker;
    }

    @GetMapping("/api/health")
    public ResponseEntity<Map<String, Object>> health() {
        boolean ready = broker.raft().leaderId() >= 0;
        Map<String, Object> body = Map.of(
                "status", ready ? "UP" : "ELECTING",
                "brokerId", broker.brokerId(),
                "role", broker.raft().role().name(),
                "leaderId", broker.raft().leaderId(),
                "term", broker.raft().term());
        return ResponseEntity.status(ready ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE).body(body);
    }
}
