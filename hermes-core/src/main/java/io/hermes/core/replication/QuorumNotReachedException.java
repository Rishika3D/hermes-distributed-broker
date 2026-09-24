package io.hermes.core.replication;

import java.io.IOException;

/**
 * Thrown when a produce under {@code acks=all} could not be persisted to a
 * majority of the partition's replica set. The write may exist on the leader
 * but is not durable, so the client must treat it as failed and retry.
 */
public class QuorumNotReachedException extends IOException {

    public QuorumNotReachedException(String message) {
        super(message);
    }
}
