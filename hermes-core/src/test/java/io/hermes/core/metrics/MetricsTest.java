package io.hermes.core.metrics;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetricsTest {

    @Test
    void latencyPercentilesAreOrderedAndBounded() {
        LatencyTracker tracker = new LatencyTracker();
        assertEquals(0, tracker.p99(), "no samples yields zero");
        for (long micros = 1; micros <= 1000; micros++) {
            tracker.record(micros);
        }
        assertEquals(500, tracker.p50(), 10);
        assertEquals(990, tracker.p99(), 10);
        assertTrue(tracker.p50() <= tracker.p99());
        assertTrue(tracker.percentile(100) <= 1000);
    }

    @Test
    void latencyWindowSlidesOverOldSamples() {
        LatencyTracker tracker = new LatencyTracker();
        for (int i = 0; i < 5000; i++) {
            tracker.record(1); // old regime
        }
        for (int i = 0; i < 3000; i++) {
            tracker.record(1000); // new regime overwrites the whole ring
        }
        assertEquals(1000, tracker.p50());
    }

    @Test
    void throughputCountsOnlyCompletedSeconds() throws InterruptedException {
        ThroughputTracker tracker = new ThroughputTracker();
        assertEquals(0.0, tracker.ratePerSecond());
        tracker.record(500);
        Thread.sleep(1100); // let the recording second complete
        assertTrue(tracker.ratePerSecond() > 0, "completed second should count toward the rate");
    }

    @Test
    void brokerMetricsSnapshotContainsEveryDashboardField() {
        BrokerMetrics metrics = new BrokerMetrics();
        metrics.recordProduce(128, 250);
        metrics.recordFetch(3, 384);
        metrics.recordUnderReplicated();
        Map<String, Object> snapshot = metrics.snapshot();
        for (String field : new String[]{"messagesInTotal", "messagesOutTotal", "bytesInTotal",
                "bytesOutTotal", "messagesInPerSec", "messagesOutPerSec",
                "produceLatencyP50Micros", "produceLatencyP99Micros",
                "underReplicatedWrites", "uptimeSeconds"}) {
            assertTrue(snapshot.containsKey(field), "missing " + field);
        }
        assertEquals(1L, snapshot.get("messagesInTotal"));
        assertEquals(3L, snapshot.get("messagesOutTotal"));
        assertEquals(1L, snapshot.get("underReplicatedWrites"));
    }
}
