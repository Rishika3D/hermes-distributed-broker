package io.hermes.rest.api;

import io.hermes.core.broker.Broker;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/** Broker-local metrics snapshot for the dashboard: throughput, latency, totals. */
@RestController
public class MetricsController {

    private final Broker broker;

    public MetricsController(Broker broker) {
        this.broker = broker;
    }

    @GetMapping("/api/metrics")
    public Map<String, Object> metrics() {
        return broker.metricsSnapshot();
    }
}
