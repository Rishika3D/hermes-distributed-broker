package io.hermes.rest.api;

import io.hermes.core.broker.Broker;
import io.hermes.core.group.JoinResult;
import io.hermes.rest.config.WebConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Controller-slice tests for consumer-group coordination endpoints. */
@WebMvcTest(controllers = GroupController.class,
        excludeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = WebConfig.class))
class GroupControllerTest {

    @Autowired
    private MockMvc mvc;

    @MockitoBean
    private Broker broker;

    @Test
    void joinReturnsAssignment() throws Exception {
        when(broker.joinGroup(eq("g1"), eq("orders"), any()))
                .thenReturn(new JoinResult("member-1", 3, List.of(0, 1, 2)));

        mvc.perform(post("/api/groups/g1/join").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"orders\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.memberId").value("member-1"))
                .andExpect(jsonPath("$.generation").value(3))
                .andExpect(jsonPath("$.partitions.length()").value(3));
    }

    @Test
    void joinWithoutTopicIsRejected() throws Exception {
        mvc.perform(post("/api/groups/g1/join").contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void heartbeatRequiresMemberId() throws Exception {
        mvc.perform(post("/api/groups/g1/heartbeat").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"orders\"}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void commitOffsetDelegatesToBroker() throws Exception {
        mvc.perform(post("/api/groups/g1/offsets").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"orders\",\"partition\":2,\"offset\":15}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("committed"));
        verify(broker).commitOffset("g1", "orders", 2, 15);
    }

    @Test
    void committedOffsetsReturnedAsMap() throws Exception {
        when(broker.committedOffsets("g1", "orders")).thenReturn(Map.of(0, 10L, 1, 20L));
        mvc.perform(get("/api/groups/g1/offsets?topic=orders"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.0").value(10));
    }

    @Test
    void lagIsServed() throws Exception {
        when(broker.lag()).thenReturn(List.of(Map.of("group", "g1", "lag", 5L)));
        mvc.perform(get("/api/lag")).andExpect(status().isOk())
                .andExpect(jsonPath("$[0].lag").value(5));
    }

    @Test
    void controllerNotReadyMapsToServiceUnavailable() throws Exception {
        when(broker.lag()).thenThrow(new IllegalStateException("no controller elected yet"));
        mvc.perform(get("/api/lag")).andExpect(status().isServiceUnavailable());
    }
}
