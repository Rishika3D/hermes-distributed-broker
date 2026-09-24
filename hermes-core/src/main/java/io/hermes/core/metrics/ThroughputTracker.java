package io.hermes.core.metrics;

/**
 * Per-second event counting over a one-minute wheel of buckets. Each bucket is
 * stamped with its epoch second and lazily reset when the wheel wraps, so no
 * background thread is needed. The reported rate averages the last five
 * completed seconds to smooth jitter.
 */
public final class ThroughputTracker {

    private static final int BUCKETS = 60;
    private static final int RATE_WINDOW_SECONDS = 5;

    private final long[] counts = new long[BUCKETS];
    private final long[] stamps = new long[BUCKETS];

    public synchronized void record(long events) {
        long second = System.currentTimeMillis() / 1_000;
        int index = (int) (second % BUCKETS);
        if (stamps[index] != second) {
            stamps[index] = second;
            counts[index] = 0;
        }
        counts[index] += events;
    }

    /** Average events/second over the last five completed seconds. */
    public synchronized double ratePerSecond() {
        long nowSecond = System.currentTimeMillis() / 1_000;
        long total = 0;
        for (long second = nowSecond - RATE_WINDOW_SECONDS; second < nowSecond; second++) {
            int index = (int) (second % BUCKETS);
            if (stamps[index] == second) {
                total += counts[index];
            }
        }
        return (double) total / RATE_WINDOW_SECONDS;
    }
}
