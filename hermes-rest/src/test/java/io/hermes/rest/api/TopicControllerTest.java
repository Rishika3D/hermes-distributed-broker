package io.hermes.rest.api;

import io.hermes.core.broker.Broker;
import io.hermes.core.topic.Topic;
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

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Controller-slice tests for topic create/list, with the engine mocked. */
@WebMvcTest(controllers = TopicController.class,
        excludeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = WebConfig.class))
class TopicControllerTest {

    @Autowired
    private MockMvc mvc;

    @MockitoBean
    private Broker broker;

    @Test
    void createsTopicAndEchoesPartitionCount() throws Exception {
        when(broker.createTopic(eq("orders"), anyInt())).thenReturn(new Topic("orders", 6));

        mvc.perform(post("/api/topics").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"orders\",\"partitions\":6}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("orders"))
                .andExpect(jsonPath("$.partitionCount").value(6));
    }

    @Test
    void blankNameIsRejected() throws Exception {
        mvc.perform(post("/api/topics").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"  \",\"partitions\":3}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").exists());
    }

    @Test
    void listsTopicsIncludingEmpty() throws Exception {
        when(broker.topics()).thenReturn(List.of());
        mvc.perform(get("/api/topics")).andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(0));

        when(broker.topics()).thenReturn(List.of(new Topic("a", 1), new Topic("b", 2)));
        mvc.perform(get("/api/topics")).andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[1].partitionCount").value(2));
    }
}
