package io.hermes.rest.config;

import jakarta.servlet.ServletInputStream;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for the request-body size guard (the OOM-via-one-request defence). */
class RequestSizeLimitFilterTest {

    private static final long LIMIT = 1_000;

    private MockHttpServletRequest post(byte[] body, boolean declareLength) {
        MockHttpServletRequest req = new MockHttpServletRequest("POST", "/api/topics/t/messages/batch");
        req.setContent(body);
        if (!declareLength) {
            req.removeHeader("Content-Length");
            req.setContent(body);
            // MockHttpServletRequest derives contentLength from content; force "unknown" (chunked)
            req.addHeader("Transfer-Encoding", "chunked");
        }
        return req;
    }

    @Test
    void declaredOversizeIsRejectedWith413AndChainNotInvoked() throws Exception {
        MockHttpServletResponse res = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();
        new RequestSizeLimitFilter(LIMIT).doFilter(post(new byte[2_000], true), res, chain);

        assertEquals(413, res.getStatus());
        assertFalse(chain.getRequest() != null, "oversized request must not reach the controller");
        assertTrue(res.getContentAsString().contains("error"));
    }

    @Test
    void bodyWithinLimitPasses() throws Exception {
        MockHttpServletResponse res = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();
        new RequestSizeLimitFilter(LIMIT).doFilter(post(new byte[500], true), res, chain);

        assertEquals(200, res.getStatus());
        assertTrue(chain.getRequest() != null);
    }

    @Test
    void bodyExactlyAtLimitPasses() throws Exception {
        MockFilterChain chain = new MockFilterChain();
        new RequestSizeLimitFilter(LIMIT).doFilter(post(new byte[(int) LIMIT], true),
                new MockHttpServletResponse(), chain);
        assertTrue(chain.getRequest() != null);
    }

    @Test
    void chunkedBodyCannotBypassTheLimit() throws Exception {
        MockHttpServletRequest req = new MockHttpServletRequest("POST", "/x") {
            @Override
            public long getContentLengthLong() {
                return -1;   // chunked: length not declared
            }
        };
        req.setContent(new byte[5_000]);
        MockFilterChain chain = new MockFilterChain();
        new RequestSizeLimitFilter(LIMIT).doFilter(req, new MockHttpServletResponse(), chain);

        ServletInputStream in = chain.getRequest().getInputStream();
        byte[] buf = new byte[4_096];
        IOException e = assertThrows(IOException.class, () -> {
            while (in.read(buf, 0, buf.length) >= 0) {
                // keep reading until the guard trips
            }
        });
        assertTrue(e.getMessage().contains("exceeds"));
    }

    @Test
    void chunkedBodyWithinLimitIsReadFully() throws Exception {
        MockHttpServletRequest req = new MockHttpServletRequest("POST", "/x") {
            @Override
            public long getContentLengthLong() {
                return -1;
            }
        };
        req.setContent(new byte[400]);
        MockFilterChain chain = new MockFilterChain();
        new RequestSizeLimitFilter(LIMIT).doFilter(req, new MockHttpServletResponse(), chain);

        assertEquals(400, chain.getRequest().getInputStream().readAllBytes().length);
    }
}
