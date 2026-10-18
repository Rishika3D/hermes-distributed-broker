package io.hermes.rest.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * Bounds the size of request bodies so a single oversized POST cannot exhaust the heap
 * (Jackson buffers the whole body before validation runs).
 *
 * <ul>
 *   <li>A declared {@code Content-Length} above the limit is rejected up front with 413,
 *       before any body bytes are read.</li>
 *   <li>A body with no declared length (chunked) is read through a counting stream that
 *       fails once the limit is crossed, so it cannot bypass the check.</li>
 * </ul>
 */
public final class RequestSizeLimitFilter extends OncePerRequestFilter {

    private final long maxBytes;

    public RequestSizeLimitFilter(long maxBytes) {
        this.maxBytes = maxBytes;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain chain) throws ServletException, IOException {
        long declared = request.getContentLengthLong();
        if (declared > maxBytes) {
            response.setStatus(HttpServletResponse.SC_REQUEST_ENTITY_TOO_LARGE);
            response.setContentType("application/json");
            response.getWriter().write("{\"error\":\"request body exceeds " + maxBytes + " bytes\"}");
            return;
        }
        chain.doFilter(declared < 0 ? new Limited(request, maxBytes) : request, response);
    }

    /** Wraps a request so reading more than {@code max} body bytes throws. */
    private static final class Limited extends HttpServletRequestWrapper {
        private final long max;

        Limited(HttpServletRequest request, long max) {
            super(request);
            this.max = max;
        }

        @Override
        public ServletInputStream getInputStream() throws IOException {
            ServletInputStream delegate = super.getInputStream();
            return new ServletInputStream() {
                private long read;

                @Override
                public int read() throws IOException {
                    int b = delegate.read();
                    if (b >= 0 && ++read > max) {
                        throw new IOException("request body exceeds " + max + " bytes");
                    }
                    return b;
                }

                @Override
                public int read(byte[] buf, int off, int len) throws IOException {
                    int n = delegate.read(buf, off, len);
                    if (n > 0 && (read += n) > max) {
                        throw new IOException("request body exceeds " + max + " bytes");
                    }
                    return n;
                }

                @Override
                public boolean isFinished() {
                    return delegate.isFinished();
                }

                @Override
                public boolean isReady() {
                    return delegate.isReady();
                }

                @Override
                public void setReadListener(ReadListener listener) {
                    delegate.setReadListener(listener);
                }
            };
        }
    }
}
