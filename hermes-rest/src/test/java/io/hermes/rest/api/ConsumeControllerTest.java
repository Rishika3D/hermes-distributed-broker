package io.hermes.rest.api;

import io.hermes.core.broker.Broker;
import io.hermes.core.model.Message;
import io.hermes.rest.config.WebConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.io.IOException;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Controller-slice tests for the consumer fetch endpoint. */
@WebMvcTest(controllers = ConsumeController.class,
        excludeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = WebConfig.class))
class ConsumeControllerTest {

    @Autowired
    private MockMvc mvc;

    @MockitoBean
    private Broker broker;

    @Test
    void fetchReturnsRecordsAndAdvancesNextOffset() throws Exception {
        when(broker.fetch(eq("orders"), eq(0), eq(5L), anyInt()))
                .thenReturn(List.of(new Message(5, 1L, "k", "v5"), new Message(6, 1L, null, "v6")));

        mvc.perform(get("/api/topics/orders/partitions/0/messages?offset=5&max=10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.records.length()").value(2))
                .andExpect(jsonPath("$.nextOffset").value(7));
    }

    @Test
    void emptyFetchReturnsSameOffsetAsNext() throws Exception {
        when(broker.fetch(eq("orders"), anyInt(), anyLong(), anyInt())).thenReturn(List.of());

        mvc.perform(get("/api/topics/orders/partitions/0/messages?offset=9"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.records.length()").value(0))
                .andExpect(jsonPath("$.nextOffset").value(9));
    }

    @Test
    void outOfRangePartitionMapsToBadRequest() throws Exception {
        when(broker.fetch(eq("orders"), eq(99), anyLong(), anyInt()))
                .thenThrow(new IllegalArgumentException("partition 99 out of range"));

        mvc.perform(get("/api/topics/orders/partitions/99/messages"))
                .andExpect(status().isBadRequest());
    }
}
