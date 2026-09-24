package io.hermes.core.raft;

/** The three Raft roles a broker can hold. */
public enum RaftRole {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
