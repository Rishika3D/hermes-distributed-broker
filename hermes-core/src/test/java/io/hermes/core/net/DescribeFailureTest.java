package io.hermes.core.net;

import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A peer failure that logs only "failed: null" is useless to an operator: EOFException (a peer
 * that closed the socket) has no message. The description must always name the exception.
 */
class DescribeFailureTest {

    @Test
    void exceptionWithoutAMessageIsIdentifiedByItsClass() {
        assertEquals("EOFException", PeerChannels.describe(new EOFException()));
    }

    @Test
    void exceptionWithAMessageShowsClassAndMessage() {
        assertEquals("SocketTimeoutException: Read timed out",
                PeerChannels.describe(new SocketTimeoutException("Read timed out")));
        assertEquals("ConnectException: Connection refused",
                PeerChannels.describe(new ConnectException("Connection refused")));
    }

    @Test
    void plainIoExceptionIsDescribedToo() {
        assertEquals("IOException: broker 3 rejected PRODUCE", PeerChannels.describe(
                new IOException("broker 3 rejected PRODUCE")));
    }
}
