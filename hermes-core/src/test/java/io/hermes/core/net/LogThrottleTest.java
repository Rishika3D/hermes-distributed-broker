package io.hermes.core.net;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A peer outage fails every request to that peer; logging each one would flood the log at
 * thousands of lines per second. The throttle lets one line through per interval and counts
 * what it suppressed so the line can say "(+N similar)".
 */
class LogThrottleTest {

    @Test
    void firstEventAlwaysPasses() {
        assertTrue(new LogThrottle(5_000).allow(1_000));
    }

    @Test
    void eventsInsideTheIntervalAreSuppressed() {
        LogThrottle throttle = new LogThrottle(5_000);
        assertTrue(throttle.allow(1_000));
        assertFalse(throttle.allow(2_000));
        assertFalse(throttle.allow(5_999));
    }

    @Test
    void eventAfterTheIntervalPassesAgain() {
        LogThrottle throttle = new LogThrottle(5_000);
        assertTrue(throttle.allow(1_000));
        assertTrue(throttle.allow(6_000));
    }

    @Test
    void reportsHowManyEventsWereSuppressedSinceTheLastAllowedOne() {
        LogThrottle throttle = new LogThrottle(5_000);
        throttle.allow(0);
        throttle.allow(1);
        throttle.allow(2);
        throttle.allow(3);
        assertEquals(3, throttle.takeSuppressed());
        assertEquals(0, throttle.takeSuppressed(), "taking resets the count");
    }
}
