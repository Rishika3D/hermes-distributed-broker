package io.hermes.core.net;

/** Opcodes for frames on the broker-to-broker socket protocol. */
public enum FrameType {
    // Raft
    REQUEST_VOTE(1),
    VOTE_RESPONSE(2),
    HEARTBEAT(3),
    HEARTBEAT_ACK(4),

    // Replication
    REPLICATE(10),
    REPLICATE_ACK(11),

    // Forwarded client operations
    PRODUCE(20),
    PRODUCE_ACK(21),
    PRODUCE_BATCH(40),
    PRODUCE_BATCH_ACK(41),
    FETCH(22),
    FETCH_ACK(23),
    CREATE_TOPIC(24),
    CREATE_TOPIC_ACK(25),
    LOG_END(26),
    LOG_END_ACK(27),

    // Consumer group coordination (handled by the raft leader / controller)
    JOIN_GROUP(30),
    JOIN_ACK(31),
    GROUP_HEARTBEAT(32),
    GROUP_HEARTBEAT_ACK(33),
    COMMIT_OFFSET(34),
    COMMIT_ACK(35),
    FETCH_OFFSETS(36),
    FETCH_OFFSETS_ACK(37),
    GROUP_LAG(38),
    GROUP_LAG_ACK(39),

    ERROR(99);

    private final byte code;

    FrameType(int code) {
        this.code = (byte) code;
    }

    public byte code() {
        return code;
    }

    public static FrameType fromCode(byte code) {
        for (FrameType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new IllegalArgumentException("unknown frame type code " + code);
    }
}
