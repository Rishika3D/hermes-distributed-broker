package io.hermes.rest.api;

import io.hermes.core.broker.Broker;
import io.hermes.rest.config.WebConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Map;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Controller-slice tests for the read-only observability endpoints. */
@WebMvcTest(controllers = {MetricsController.class, ClusterController.class},
        excludeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = WebConfig.class))
class ObservabilityControllerTest {

    @Autowired
    private MockMvc mvc;

    @MockitoBean
    private Broker broker;

    @Test
    void metricsSnapshotIsServed() throws Exception {
        when(broker.metricsSnapshot()).thenReturn(Map.of(
                "brokerId", 1, "messagesInPerSec", 42.0, "produceLatencyP99Micros", 1500L));
        mvc.perform(get("/api/metrics")).andExpect(status().isOk())
                .andExpect(jsonPath("$.brokerId").value(1))
                .andExpect(jsonPath("$.messagesInPerSec").value(42.0));
    }

    @Test
    void clusterTopologyIsServed() throws Exception {
        when(broker.clusterInfo()).thenReturn(Map.of("leaderId", 2, "term", 5));
        mvc.perform(get("/api/cluster")).andExpect(status().isOk())
                .andExpect(jsonPath("$.leaderId").value(2))
                .andExpect(jsonPath("$.term").value(5));
    }
}
