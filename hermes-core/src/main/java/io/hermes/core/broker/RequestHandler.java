package io.hermes.core.broker;

import io.hermes.core.group.JoinResult;
import io.hermes.core.model.Message;
import io.hermes.core.model.ProduceRecord;
import io.hermes.core.net.FrameType;
import io.hermes.core.net.PeerChannels;
import io.hermes.core.net.WireHandler;
import io.hermes.core.net.WireMessage;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;

/**
 * Dispatches inbound socket frames to the broker and its raft node, and shapes
 * the responses. Every failure becomes an ERROR frame so a misbehaving request
 * never kills the connection.
 */
public final class RequestHandler implements WireHandler {

    private final Broker broker;
    private final String clusterSecret;

    public RequestHandler(Broker broker, String clusterSecret) {
        this.broker = broker;
        this.clusterSecret = clusterSecret;
    }

    @Override
    public WireMessage handle(WireMessage request) {
        if (!authenticated(request)) {
            return WireMessage.error("unauthenticated frame rejected");
        }
        try {
            return dispatch(request);
        } catch (Exception e) {
            return WireMessage.error(e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
        }
    }

    /** Constant-time check of the shared cluster secret, when one is configured. */
    private boolean authenticated(WireMessage request) {
        if (clusterSecret == null) {
            return true;
        }
        String presented = request.get(PeerChannels.SECRET_HEADER);
        return presented != null && MessageDigest.isEqual(
                presented.getBytes(StandardCharsets.UTF_8),
                clusterSecret.getBytes(StandardCharsets.UTF_8));
    }

    private WireMessage dispatch(WireMessage request) throws Exception {
        return switch (request.type()) {
            case REQUEST_VOTE -> broker.raft().handleRequestVote(request);
            case HEARTBEAT -> broker.raft().handleHeartbeat(request);

            case REPLICATE -> {
                broker.applyReplicaBatch(request.get("topic"), request.getInt("partitionCount"),
                        request.getInt("partition"), request.records());
                yield WireMessage.of(FrameType.REPLICATE_ACK);
            }

            case PRODUCE -> {
                String key = request.getBoolean("hasKey") ? request.get("key") : null;
                ProduceResult result = broker.produceInto(request.get("topic"),
                        request.getInt("partitionCount"), request.getInt("partition"),
                        key, request.get("value"));
                yield WireMessage.of(FrameType.PRODUCE_ACK)
                        .header("partition", result.partition())
                        .header("offset", result.offset());
            }

            case PRODUCE_BATCH -> {
                List<ProduceRecord> records = new java.util.ArrayList<>();
                for (Message m : request.records()) {
                    records.add(new ProduceRecord(m.key(), m.value()));
                }
                long baseOffset = broker.produceBatchInto(request.get("topic"),
                        request.getInt("partitionCount"), request.getInt("partition"), records);
                yield WireMessage.of(FrameType.PRODUCE_BATCH_ACK)
                        .header("partition", request.getInt("partition"))
                        .header("baseOffset", baseOffset)
                        .header("count", records.size());
            }

            case FETCH -> {
                List<Message> records = broker.fetch(request.get("topic"), request.getInt("partition"),
                        request.getLong("offset"), request.getInt("max"));
                yield WireMessage.of(FrameType.FETCH_ACK).records(records);
            }

            case CREATE_TOPIC -> {
                var topic = broker.createTopic(request.get("name"), request.getInt("partitions"));
                yield WireMessage.of(FrameType.CREATE_TOPIC_ACK)
                        .header("partitions", topic.partitionCount());
            }

            case LOG_END -> {
                long end = broker.endOffset(new io.hermes.core.model.TopicPartition(
                        request.get("topic"), request.getInt("partition")));
                yield WireMessage.of(FrameType.LOG_END_ACK).header("endOffset", end);
            }

            case JOIN_GROUP -> {
                String memberId = request.get("memberId");
                JoinResult result = broker.coordinator().join(request.get("group"), request.get("topic"),
                        memberId == null || memberId.isBlank() ? null : memberId,
                        request.getInt("partitions"));
                yield joinAck(FrameType.JOIN_ACK, result);
            }

            case GROUP_HEARTBEAT -> {
                JoinResult result = broker.coordinator().heartbeat(request.get("group"), request.get("memberId"));
                yield joinAck(FrameType.GROUP_HEARTBEAT_ACK, result);
            }

            case COMMIT_OFFSET -> {
                broker.commitOffset(request.get("group"), request.get("topic"),
                        request.getInt("partition"), request.getLong("offset"));
                yield WireMessage.of(FrameType.COMMIT_ACK);
            }

            case FETCH_OFFSETS -> {
                Map<Integer, Long> offsets = broker.committedOffsets(request.get("group"), request.get("topic"));
                yield WireMessage.of(FrameType.FETCH_OFFSETS_ACK)
                        .header("offsets", WireEncoding.encodeOffsets(offsets));
            }

            case GROUP_LAG -> WireMessage.of(FrameType.GROUP_LAG_ACK)
                    .header("lag", WireEncoding.encodeLag(broker.lag()));

            default -> WireMessage.error("unsupported frame type " + request.type());
        };
    }

    private static WireMessage joinAck(FrameType type, JoinResult result) {
        return WireMessage.of(type)
                .header("memberId", result.memberId())
                .header("generation", result.generation())
                .header("assigned", WireEncoding.encodePartitions(result.partitions()));
    }
}
