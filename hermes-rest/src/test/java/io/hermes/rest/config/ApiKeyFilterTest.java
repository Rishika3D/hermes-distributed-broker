package io.hermes.rest.config;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the API-key authentication filter, exercised in isolation
 * (no Spring context) through its public {@code doFilter} behaviour. Covers the
 * security-critical paths: rejection without / with a wrong key, acceptance
 * with the right key, the health-probe and CORS pre-flight exemptions, and the
 * disabled (no key configured) pass-through. "Chain proceeded" is observed via
 * {@link MockFilterChain#getRequest()} being non-null after the call.
 */
class ApiKeyFilterTest {

    private static final String KEY = "s3cr3t-key";

    private record Outcome(int status, boolean proceeded) {
    }

    private Outcome run(ApiKeyFilter filter, String method, String uri, String key) throws Exception {
        MockHttpServletRequest req = new MockHttpServletRequest(method, uri);
        if (key != null) {
            req.addHeader(ApiKeyFilter.HEADER, key);
        }
        MockHttpServletResponse res = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();
        filter.doFilter(req, res, chain);
        return new Outcome(res.getStatus(), chain.getRequest() != null);
    }

    @Test
    void missingKeyIsRejectedAndChainNotInvoked() throws Exception {
        Outcome o = run(new ApiKeyFilter(KEY), "GET", "/api/topics", null);
        assertEquals(401, o.status());
        assertFalse(o.proceeded(), "request must not reach the controller");
    }

    @Test
    void wrongKeyIsRejected() throws Exception {
        assertEquals(401, run(new ApiKeyFilter(KEY), "GET", "/api/topics", "wrong").status());
    }

    @Test
    void correctKeyIsAccepted() throws Exception {
        Outcome o = run(new ApiKeyFilter(KEY), "GET", "/api/topics", KEY);
        assertTrue(o.proceeded(), "a valid key must reach the controller");
    }

    @Test
    void healthProbeIsExemptEvenWithoutKey() throws Exception {
        assertTrue(run(new ApiKeyFilter(KEY), "GET", "/api/health", null).proceeded());
    }

    @Test
    void corsPreflightIsExempt() throws Exception {
        assertTrue(run(new ApiKeyFilter(KEY), "OPTIONS", "/api/topics", null).proceeded());
    }

    @Test
    void noKeyConfiguredDisablesTheFilter() throws Exception {
        assertTrue(run(new ApiKeyFilter(""), "GET", "/api/topics", null).proceeded(),
                "with no key configured the filter is a pass-through");
    }

    @Test
    void rejectionBodyIsJson() throws Exception {
        MockHttpServletRequest req = new MockHttpServletRequest("GET", "/api/topics");
        MockHttpServletResponse res = new MockHttpServletResponse();
        new ApiKeyFilter(KEY).doFilter(req, res, new MockFilterChain());
        assertEquals("application/json", res.getContentType());
        assertNotNull(res.getContentAsString());
        assertTrue(res.getContentAsString().contains("error"));
    }
}
