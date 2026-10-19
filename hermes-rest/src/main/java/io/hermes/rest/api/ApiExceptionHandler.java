package io.hermes.rest.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.io.IOException;
import java.util.Map;

/**
 * Maps every failure onto an HTTP status with a uniform {@code {"error": "..."}} body.
 *
 * <p>Extending {@link ResponseEntityExceptionHandler} gives the correct 4xx for Spring MVC's own
 * client errors (unknown route 404, wrong method 405, bad path variable / missing parameter 400,
 * unsupported media type 415, ...); without it the catch-all below turned those into 500s.
 */
@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> badRequest(IllegalArgumentException e) {
        return error(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Map<String, String>> unavailable(IllegalStateException e) {
        return error(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage());
    }

    @ExceptionHandler(io.hermes.core.replication.QuorumNotReachedException.class)
    public ResponseEntity<Map<String, String>> notDurable(
            io.hermes.core.replication.QuorumNotReachedException e) {
        // 503: the write is not durable; the client should retry.
        return error(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage());
    }

    @ExceptionHandler(IOException.class)
    public ResponseEntity<Map<String, String>> upstreamFailure(IOException e) {
        return error(HttpStatus.BAD_GATEWAY, e.getMessage());
    }

    /** Catch-all: never leak stack traces or internal class names to clients. */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, String>> unexpected(Exception e) {
        LOG.error("unhandled exception serving request", e);
        return error(HttpStatus.INTERNAL_SERVER_ERROR, "internal error");
    }

    /** Malformed or unparseable request body is a client error, not a server error. */
    @Override
    protected ResponseEntity<Object> handleHttpMessageNotReadable(HttpMessageNotReadableException ex,
                                                                  HttpHeaders headers, HttpStatusCode status,
                                                                  WebRequest request) {
        return body(HttpStatus.BAD_REQUEST, "malformed request body");
    }

    /** Renders all of Spring MVC's standard client errors in our body shape. */
    @Override
    protected ResponseEntity<Object> handleExceptionInternal(Exception ex, Object body, HttpHeaders headers,
                                                             HttpStatusCode status, WebRequest request) {
        String detail = body instanceof ProblemDetail problem && problem.getDetail() != null
                ? problem.getDetail()
                : reason(status);
        return body(status, detail);
    }

    private static ResponseEntity<Object> body(HttpStatusCode status, String message) {
        return ResponseEntity.status(status).body(Map.of("error", message));
    }

    private static ResponseEntity<Map<String, String>> error(HttpStatus status, String message) {
        return ResponseEntity.status(status)
                .body(Map.of("error", message == null ? status.getReasonPhrase() : message));
    }

    private static String reason(HttpStatusCode status) {
        HttpStatus known = HttpStatus.resolve(status.value());
        return known == null ? "error" : known.getReasonPhrase();
    }
}
