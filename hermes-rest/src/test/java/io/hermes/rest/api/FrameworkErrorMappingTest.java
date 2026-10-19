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

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Mistakes made by the *client* (unknown route, wrong HTTP method, non-numeric path variable,
 * missing query parameter) must be reported as 4xx. They used to fall into the catch-all handler
 * and come back as 500 "internal error" with an ERROR log and a stack trace, which pages operators
 * for what is a typo in a URL.
 */
@WebMvcTest(controllers = {ConsumeController.class, ProduceController.class, GroupController.class},
        excludeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = WebConfig.class))
class FrameworkErrorMappingTest {

    @Autowired
    private MockMvc mvc;

    @MockitoBean
    private Broker broker;

    @Test
    void unknownRouteIs404WithJsonBody() throws Exception {
        mvc.perform(get("/api/does-not-exist"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").exists());
    }

    @Test
    void wrongHttpMethodIs405() throws Exception {
        mvc.perform(get("/api/topics/orders/messages"))   // produce is POST-only
                .andExpect(status().isMethodNotAllowed())
                .andExpect(jsonPath("$.error").exists());
    }

    @Test
    void nonNumericPartitionIs400() throws Exception {
        mvc.perform(get("/api/topics/orders/partitions/abc/messages"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").exists());
    }

    @Test
    void missingRequiredQueryParameterIs400() throws Exception {
        mvc.perform(get("/api/groups/g1/offsets"))        // ?topic= is required
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").exists());
    }
}
