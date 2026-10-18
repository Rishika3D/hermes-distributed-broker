package io.hermes.core.net;

/**
 * Lets one event through per interval and counts the ones it swallowed, so a repeating
 * failure logs a single line ("... (+N similar)") instead of one line per request.
 * Thread-safe; the caller supplies the clock so it is trivially testable.
 */
final class LogThrottle {

    private final long intervalMs;
    private long lastAllowedMs;
    private boolean everAllowed;
    private int suppressed;

    LogThrottle(long intervalMs) {
        this.intervalMs = intervalMs;
    }

    /** True if this event should be logged; otherwise it is counted as suppressed. */
    synchronized boolean allow(long nowMs) {
        if (!everAllowed || nowMs - lastAllowedMs >= intervalMs) {
            everAllowed = true;
            lastAllowedMs = nowMs;
            return true;
        }
        suppressed++;
        return false;
    }

    /** Events suppressed since the last call; resets the count. */
    synchronized int takeSuppressed() {
        int count = suppressed;
        suppressed = 0;
        return count;
    }
}
