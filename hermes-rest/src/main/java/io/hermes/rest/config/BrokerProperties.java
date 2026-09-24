package io.hermes.rest.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Binds the {@code hermes.*} configuration for this broker node. */
@ConfigurationProperties(prefix = "hermes")
public class BrokerProperties {

    /** This node's broker id; must appear in the member list. */
    private int brokerId = 1;

    /** Full cluster membership as comma-separated {@code id@host:socketPort}. */
    private String members = "1@localhost:9091";

    /** Directory for log segments, topic metadata and committed offsets. */
    private String dataDir = "./data/broker-1";

    /** Copies of each partition, leader included. */
    private int replicationFactor = 2;

    /** Partition count used when a topic is auto-created. */
    private int defaultPartitions = 3;

    /** Size at which a log segment rolls to a new file. */
    private long segmentBytes = 1024 * 1024;

    /** Shared secret required on every broker-to-broker frame (empty = disabled). */
    private String clusterSecret = "";

    /** API key required as X-API-Key on REST calls except /api/health (empty = disabled). */
    private String apiKey = "";

    /** Comma-separated CORS origins allowed on /api/** ("*" = any). */
    private String corsOrigins = "*";

    /** Produce ack policy: "all" (quorum-durable) or "leader" (leader-only). */
    private String acks = "all";

    /** Coalesce fsyncs across concurrent single-message appends to a partition. */
    private boolean groupCommit = false;

    /** Optional wait (ms) to widen a group-commit batch before fsync (0 = piggyback only). */
    private long lingerMs = 0;

    /** Max records accepted in one batch produce request. */
    private int batchMaxRecords = 10000;

    public int getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(int brokerId) {
        this.brokerId = brokerId;
    }

    public String getMembers() {
        return members;
    }

    public void setMembers(String members) {
        this.members = members;
    }

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public int getDefaultPartitions() {
        return defaultPartitions;
    }

    public void setDefaultPartitions(int defaultPartitions) {
        this.defaultPartitions = defaultPartitions;
    }

    public long getSegmentBytes() {
        return segmentBytes;
    }

    public void setSegmentBytes(long segmentBytes) {
        this.segmentBytes = segmentBytes;
    }

    public String getClusterSecret() {
        return clusterSecret;
    }

    public void setClusterSecret(String clusterSecret) {
        this.clusterSecret = clusterSecret;
    }

    public String getApiKey() {
        return apiKey;
    }

    public void setApiKey(String apiKey) {
        this.apiKey = apiKey;
    }

    public String getCorsOrigins() {
        return corsOrigins;
    }

    public void setCorsOrigins(String corsOrigins) {
        this.corsOrigins = corsOrigins;
    }

    public String getAcks() {
        return acks;
    }

    public void setAcks(String acks) {
        this.acks = acks;
    }

    public boolean isGroupCommit() {
        return groupCommit;
    }

    public void setGroupCommit(boolean groupCommit) {
        this.groupCommit = groupCommit;
    }

    public long getLingerMs() {
        return lingerMs;
    }

    public void setLingerMs(long lingerMs) {
        this.lingerMs = lingerMs;
    }

    public int getBatchMaxRecords() {
        return batchMaxRecords;
    }

    public void setBatchMaxRecords(int batchMaxRecords) {
        this.batchMaxRecords = batchMaxRecords;
    }
}
