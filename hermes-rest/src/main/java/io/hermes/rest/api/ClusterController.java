package io.hermes.rest.api;

import io.hermes.core.broker.Broker;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/** Cluster topology as this broker sees it: members, liveness, raft state, partition map. */
@RestController
public class ClusterController {

    private final Broker broker;

    public ClusterController(Broker broker) {
        this.broker = broker;
    }

    @GetMapping("/api/cluster")
    public Map<String, Object> cluster() {
        return broker.clusterInfo();
    }
}
