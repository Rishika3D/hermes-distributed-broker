package io.hermes.core.metrics;

import java.util.Arrays;

/**
 * Fixed-size ring buffer of recent latency samples (microseconds) from which
 * percentiles are computed on demand. Old samples are overwritten, so the
 * percentiles always reflect recent behavior.
 */
public final class LatencyTracker {

    private static final int CAPACITY = 2_048;

    private final long[] samples = new long[CAPACITY];
    private int next = 0;
    private int filled = 0;

    public synchronized void record(long micros) {
        samples[next] = micros;
        next = (next + 1) % CAPACITY;
        if (filled < CAPACITY) {
            filled++;
        }
    }

    /** The given percentile (0..100) over the current window, or 0 with no samples. */
    public synchronized long percentile(double p) {
        if (filled == 0) {
            return 0;
        }
        long[] window = Arrays.copyOf(samples, filled);
        Arrays.sort(window);
        int index = (int) Math.ceil(p / 100.0 * filled) - 1;
        return window[Math.max(0, Math.min(index, filled - 1))];
    }

    public long p50() {
        return percentile(50);
    }

    public long p99() {
        return percentile(99);
    }

    public synchronized int sampleCount() {
        return filled;
    }
}
