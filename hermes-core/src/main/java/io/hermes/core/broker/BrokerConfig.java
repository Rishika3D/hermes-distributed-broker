package io.hermes.core.broker;

import io.hermes.core.cluster.BrokerNode;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Static configuration of one broker process: identity, data directory and
 * the full cluster membership (which includes the local node). A non-blank
 * {@code clusterSecret} makes every wire frame carry and require the shared
 * secret, shutting out unauthenticated peers on the socket port.
 */
public record BrokerConfig(int brokerId,
                           Path dataDir,
                           List<BrokerNode> members,
                           int replicationFactor,
                           int defaultPartitions,
                           long segmentBytes,
                           String clusterSecret,
                           String acks,
                           int batchMaxRecords,
                           long lingerMs,
                           boolean groupCommit) {

    /** Produce acknowledgement policy: quorum-durable vs leader-only. */
    public static final String ACKS_ALL = "all";
    public static final String ACKS_LEADER = "leader";

    public static final int DEFAULT_BATCH_MAX_RECORDS = 10_000;

    public BrokerConfig {
        if (clusterSecret != null && clusterSecret.isBlank()) {
            clusterSecret = null;
        }
        if (acks == null || acks.isBlank()) {
            acks = ACKS_ALL;
        }
        if (!acks.equals(ACKS_ALL) && !acks.equals(ACKS_LEADER)) {
            throw new IllegalArgumentException("acks must be '" + ACKS_ALL + "' or '" + ACKS_LEADER + "'");
        }
        if (batchMaxRecords <= 0) {
            batchMaxRecords = DEFAULT_BATCH_MAX_RECORDS;
        }
        if (lingerMs < 0) {
            lingerMs = 0;
        }
    }

    /** Backward-compatible constructor defaulting to durable {@code acks=all}. */
    public BrokerConfig(int brokerId, Path dataDir, List<BrokerNode> members, int replicationFactor,
                        int defaultPartitions, long segmentBytes, String clusterSecret) {
        this(brokerId, dataDir, members, replicationFactor, defaultPartitions, segmentBytes,
                clusterSecret, ACKS_ALL, DEFAULT_BATCH_MAX_RECORDS, 0, false);
    }

    /** Backward-compatible constructor with an explicit ack policy. */
    public BrokerConfig(int brokerId, Path dataDir, List<BrokerNode> members, int replicationFactor,
                        int defaultPartitions, long segmentBytes, String clusterSecret, String acks) {
        this(brokerId, dataDir, members, replicationFactor, defaultPartitions, segmentBytes,
                clusterSecret, acks, DEFAULT_BATCH_MAX_RECORDS, 0, false);
    }

    public boolean requireQuorum() {
        return acks.equals(ACKS_ALL);
    }

    public BrokerNode localNode() {
        return members.stream()
                .filter(node -> node.id() == brokerId)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "broker " + brokerId + " not present in member list " + members));
    }

    /** Parses the comma-separated {@code id@host:port} member list. */
    public static List<BrokerNode> parseMembers(String spec) {
        List<BrokerNode> members = new ArrayList<>();
        for (String entry : spec.split(",")) {
            if (!entry.isBlank()) {
                members.add(BrokerNode.parse(entry));
            }
        }
        return members;
    }
}
