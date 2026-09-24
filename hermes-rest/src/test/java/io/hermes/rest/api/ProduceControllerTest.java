package io.hermes.rest.api;

import io.hermes.core.broker.Broker;
import io.hermes.core.broker.ProduceResult;
import io.hermes.core.replication.QuorumNotReachedException;
import io.hermes.rest.config.WebConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.io.IOException;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Controller-slice tests for the producer endpoints. The service layer
 * ({@link Broker}) is mocked, so these verify HTTP concerns in isolation:
 * request binding, input validation, batch handling, and — importantly — that
 * engine exceptions are mapped to the correct HTTP status by the shared
 * {@link ApiExceptionHandler}. No broker, cluster, or disk is involved.
 */
@WebMvcTest(controllers = ProduceController.class,
        excludeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = WebConfig.class))
class ProduceControllerTest {

    @Autowired
    private MockMvc mvc;

    @MockitoBean
    private Broker broker;

    @Test
    void producesAMessageAndReturnsItsOffset() throws Exception {
        when(broker.produce(eq("orders"), any(), eq("hello")))
                .thenReturn(new ProduceResult("orders", 2, 42L));

        mvc.perform(post("/api/topics/orders/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"key\":\"k1\",\"value\":\"hello\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.partition").value(2))
                .andExpect(jsonPath("$.offset").value(42));
    }

    @Test
    void missingValueIsRejectedAsBadRequest() throws Exception {
        mvc.perform(post("/api/topics/orders/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"key\":\"k1\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").exists());
    }

    @Test
    void malformedJsonIsBadRequestNotServerError() throws Exception {
        mvc.perform(post("/api/topics/orders/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("not json{{"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void quorumFailureMapsToServiceUnavailable() throws Exception {
        when(broker.produce(any(), any(), any()))
                .thenThrow(new QuorumNotReachedException("not durable"));

        mvc.perform(post("/api/topics/orders/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"value\":\"v\"}"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.error").value("not durable"));
    }

    @Test
    void upstreamIoErrorMapsToBadGateway() throws Exception {
        when(broker.produce(any(), any(), any())).thenThrow(new IOException("peer unreachable"));

        mvc.perform(post("/api/topics/orders/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"value\":\"v\"}"))
                .andExpect(status().isBadGateway());
    }

    @Test
    void invalidTopicNameFromEngineMapsToBadRequest() throws Exception {
        when(broker.produce(any(), any(), any()))
                .thenThrow(new IllegalArgumentException("topic name may only contain ..."));

        mvc.perform(post("/api/topics/bad,name/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"value\":\"v\"}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void batchProduceReturnsOneResultPerRecord() throws Exception {
        when(broker.produceBatch(eq("orders"), any()))
                .thenReturn(List.of(new ProduceResult("orders", 0, 0L),
                        new ProduceResult("orders", 1, 0L)));

        mvc.perform(post("/api/topics/orders/messages/batch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("[{\"value\":\"a\"},{\"value\":\"b\"}]"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2));
    }

    @Test
    void batchWithMissingValueIsRejected() throws Exception {
        mvc.perform(post("/api/topics/orders/messages/batch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("[{\"value\":\"a\"},{\"key\":\"b\"}]"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void acceptsLargeValuePayload() throws Exception {
        String big = "x".repeat(50_000);
        when(broker.produce(any(), any(), eq(big))).thenReturn(new ProduceResult("orders", 0, 1L));

        mvc.perform(post("/api/topics/orders/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"value\":\"" + big + "\"}"))
                .andExpect(status().isOk());
    }
}
