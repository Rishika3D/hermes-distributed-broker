package io.hermes.core.metrics;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * All broker-level counters and trackers in one place, snapshotted by the REST
 * layer. Producers record on the leader that performs the append; consumers
 * record on the broker that serves the fetch.
 */
public final class BrokerMetrics {

    private final long startedAtMillis = System.currentTimeMillis();

    private final LongAdder messagesIn = new LongAdder();
    private final LongAdder messagesOut = new LongAdder();
    private final LongAdder bytesIn = new LongAdder();
    private final LongAdder bytesOut = new LongAdder();
    private final LongAdder underReplicatedWrites = new LongAdder();

    private final ThroughputTracker inRate = new ThroughputTracker();
    private final ThroughputTracker outRate = new ThroughputTracker();
    private final LatencyTracker produceLatency = new LatencyTracker();

    public void recordProduce(int bytes, long latencyMicros) {
        messagesIn.increment();
        bytesIn.add(bytes);
        inRate.record(1);
        produceLatency.record(latencyMicros);
    }

    public void recordFetch(int messageCount, long bytes) {
        messagesOut.add(messageCount);
        bytesOut.add(bytes);
        outRate.record(messageCount);
    }

    public void recordUnderReplicated() {
        underReplicatedWrites.increment();
    }

    public Map<String, Object> snapshot() {
        Map<String, Object> snapshot = new LinkedHashMap<>();
        snapshot.put("messagesInTotal", messagesIn.sum());
        snapshot.put("messagesOutTotal", messagesOut.sum());
        snapshot.put("bytesInTotal", bytesIn.sum());
        snapshot.put("bytesOutTotal", bytesOut.sum());
        snapshot.put("messagesInPerSec", inRate.ratePerSecond());
        snapshot.put("messagesOutPerSec", outRate.ratePerSecond());
        snapshot.put("produceLatencyP50Micros", produceLatency.p50());
        snapshot.put("produceLatencyP99Micros", produceLatency.p99());
        snapshot.put("underReplicatedWrites", underReplicatedWrites.sum());
        snapshot.put("uptimeSeconds", (System.currentTimeMillis() - startedAtMillis) / 1_000);
        return snapshot;
    }
}
