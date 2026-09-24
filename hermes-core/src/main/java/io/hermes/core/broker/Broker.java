package io.hermes.core.broker;

import io.hermes.core.cluster.ClusterState;
import io.hermes.core.cluster.BrokerNode;
import io.hermes.core.cluster.ConsistentHashRing;
import io.hermes.core.cluster.PartitionAssigner;
import io.hermes.core.group.GroupCoordinator;
import io.hermes.core.group.JoinResult;
import io.hermes.core.group.OffsetStore;
import io.hermes.core.group.ConsumerGroup;
import io.hermes.core.metrics.BrokerMetrics;
import io.hermes.core.model.Message;
import io.hermes.core.model.Names;
import io.hermes.core.model.ProduceRecord;
import io.hermes.core.model.TopicPartition;
import io.hermes.core.net.BrokerServer;
import io.hermes.core.net.FrameType;
import io.hermes.core.net.PeerChannels;
import io.hermes.core.net.WireMessage;
import io.hermes.core.raft.RaftNode;
import io.hermes.core.replication.QuorumNotReachedException;
import io.hermes.core.replication.Replicator;
import io.hermes.core.topic.Partition;
import io.hermes.core.topic.Topic;
import io.hermes.core.topic.TopicRegistry;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The broker facade: everything a client (or a forwarding peer) can do goes
 * through here. Wires together storage, cluster assignment, raft, replication,
 * consumer groups and metrics, and transparently forwards any operation this
 * broker does not own — produces go to the partition leader, group operations
 * go to the Raft leader (the controller).
 */
public class Broker implements Closeable, RaftNode.MetadataChannel {

    /** writeUTF frames cap at 65,535 encoded bytes; leave headroom for headers. */
    public static final int MAX_VALUE_BYTES = 60_000;
    public static final int MAX_KEY_BYTES = 10_000;
    public static final int MAX_FETCH_RECORDS = 10_000;
    /** Heap ceiling per fetch response, independent of record count. */
    public static final long MAX_FETCH_BYTES = 8L * 1024 * 1024;
    public static final int MAX_PARTITIONS = 512;

    private final BrokerConfig config;
    private final ClusterState cluster;
    private final PartitionAssigner assigner;
    private final TopicRegistry registry;
    private final PeerChannels peers;
    private final RaftNode raft;
    private final Replicator replicator;
    private final GroupCoordinator coordinator;
    private final BrokerMetrics metrics = new BrokerMetrics();
    private final BrokerServer server;
    private final AtomicLong roundRobin = new AtomicLong();
    /** Fans per-partition sub-batches out concurrently; I/O-bound, so virtual threads. */
    private final ExecutorService batchDispatch = Executors.newVirtualThreadPerTaskExecutor();

    public Broker(BrokerConfig config) throws IOException {
        this.config = config;
        this.cluster = new ClusterState(config.brokerId(), config.members());
        List<Integer> memberIds = config.members().stream().map(BrokerNode::id).toList();
        this.assigner = new PartitionAssigner(new ConsistentHashRing(memberIds), config.replicationFactor());
        this.registry = new TopicRegistry(config.dataDir(), config.segmentBytes(),
                config.groupCommit(), config.lingerMs(), assigner, config.brokerId());
        this.peers = new PeerChannels(cluster, config.clusterSecret());
        this.raft = new RaftNode(cluster, peers, this);
        this.replicator = new Replicator(cluster, peers, metrics);
        this.coordinator = new GroupCoordinator(new OffsetStore(config.dataDir().resolve("offsets.log")));
        this.server = new BrokerServer(cluster.localNode().port(),
                new RequestHandler(this, config.clusterSecret()));
    }

    public void start() throws IOException {
        server.start();
        raft.start();
        System.out.println("[hermes] broker " + config.brokerId() + " listening on socket port "
                + cluster.localNode().port());
    }

    // ------------------------------------------------------------------ topics

    /**
     * Creates the topic (idempotent). The controller owns the authoritative
     * partition count: non-leaders ask it first and materialize with whatever
     * it answers, so two brokers can never end up modding keys by different
     * partition counts for the same topic.
     */
    public Topic createTopic(String name, int partitionCount) throws IOException {
        Names.requireValid(name, "topic name");
        if (partitionCount > MAX_PARTITIONS) {
            throw new IllegalArgumentException("partition count exceeds maximum of " + MAX_PARTITIONS);
        }
        int requested = partitionCount <= 0 ? config.defaultPartitions() : partitionCount;
        if (raft.isLeader()) {
            return registry.ensureTopic(name, requested);
        }
        int controller = requireController();
        WireMessage ack = peers.request(controller, WireMessage.of(FrameType.CREATE_TOPIC)
                .header("name", name)
                .header("partitions", requested));
        return registry.ensureTopic(name, ack.getInt("partitions"));
    }

    public List<Topic> topics() {
        return registry.allTopics();
    }

    // ----------------------------------------------------------------- produce

    /**
     * Routes the message to its partition (key hash, or round-robin for null
     * keys) and appends on the partition leader — locally if we lead it,
     * otherwise by forwarding over the socket protocol.
     */
    public ProduceResult produce(String topic, String key, String value) throws IOException {
        requireWireSafe(key, value);
        Topic meta = registry.topic(topic);
        if (meta == null) {
            meta = createTopic(topic, config.defaultPartitions());
        }
        int partition = key == null
                ? (int) Math.floorMod(roundRobin.getAndIncrement(), meta.partitionCount())
                : (int) Math.floorMod(ConsistentHashRing.hash(key), (long) meta.partitionCount());
        TopicPartition tp = new TopicPartition(topic, partition);
        int leader = assigner.leaderFor(tp);
        if (leader == cluster.localId()) {
            return appendAsLeader(tp, meta.partitionCount(), key, value);
        }
        WireMessage response = peers.request(leader, WireMessage.of(FrameType.PRODUCE)
                .header("topic", topic)
                .header("partition", partition)
                .header("partitionCount", meta.partitionCount())
                .header("hasKey", Boolean.toString(key != null))
                .header("key", key)
                .header("value", value));
        return new ProduceResult(topic, response.getInt("partition"), response.getLong("offset"));
    }

    /**
     * Leader-side entry for forwarded produces: appends to the exact partition
     * the forwarder resolved and never re-forwards, so a routing disagreement
     * between brokers fails fast instead of ping-ponging the message around
     * the cluster.
     */
    public ProduceResult produceInto(String topic, int partitionCount, int partition,
                                     String key, String value) throws IOException {
        registry.ensureTopic(topic, partitionCount);
        TopicPartition tp = new TopicPartition(topic, partition);
        if (assigner.leaderFor(tp) != cluster.localId()) {
            throw new IllegalStateException("broker " + cluster.localId() + " is not the leader of " + tp);
        }
        return appendAsLeader(tp, partitionCount, key, value);
    }

    private ProduceResult appendAsLeader(TopicPartition tp, int partitionCount, String key, String value)
            throws IOException {
        List<Message> stored = appendBatchAsLeader(tp, partitionCount,
                List.of(new ProduceRecord(key, value)));
        Message m = stored.get(0);
        return new ProduceResult(tp.topic(), tp.partition(), m.offset());
    }

    /**
     * Batched leader append: all records for one partition are written under a
     * single {@link PartitionLog} lock with one fsync, then replicated as one
     * frame. Durability is unchanged — the whole batch is fsynced on the leader
     * and quorum-replicated before this returns, so {@code acks=all} still
     * means every acked record is durable.
     */
    private List<Message> appendBatchAsLeader(TopicPartition tp, int partitionCount,
                                              List<ProduceRecord> records) throws IOException {
        long start = System.nanoTime();
        Partition partition = registry.localPartition(tp);
        if (partition == null) {
            throw new IllegalStateException("broker " + cluster.localId() + " does not host " + tp);
        }
        List<Message> stored = partition.log().appendBatch(records, System.currentTimeMillis());
        boolean quorum = replicator.replicateBatch(tp, partitionCount, stored, partition.replicaIds());
        if (config.requireQuorum() && !quorum) {
            throw new QuorumNotReachedException("batch of " + stored.size() + " to " + tp
                    + " did not reach a majority of replicas " + partition.replicaIds() + "; not durable");
        }
        long micros = (System.nanoTime() - start) / 1_000 / Math.max(1, stored.size());
        for (Message m : stored) {
            metrics.recordProduce(m.sizeInBytes(), micros);
        }
        return stored;
    }

    /**
     * Publishes a batch of records, grouping them by target partition so each
     * partition's sub-batch becomes one append + one fsync + one replication
     * frame on its leader (locally or forwarded). Returns one result per input
     * record, in input order.
     */
    public List<ProduceResult> produceBatch(String topic, List<ProduceRecord> records) throws IOException {
        if (records.isEmpty()) {
            return List.of();
        }
        if (records.size() > config.batchMaxRecords()) {
            throw new IllegalArgumentException("batch of " + records.size()
                    + " exceeds max of " + config.batchMaxRecords());
        }
        for (ProduceRecord r : records) {
            requireWireSafe(r.key(), r.value());
        }
        Topic meta = registry.topic(topic);
        if (meta == null) {
            meta = createTopic(topic, config.defaultPartitions());
        }
        int partitions = meta.partitionCount();

        // Group input indexes by target partition, preserving order within a partition.
        Map<Integer, List<Integer>> byPartition = new LinkedHashMap<>();
        for (int i = 0; i < records.size(); i++) {
            ProduceRecord r = records.get(i);
            int p = r.key() == null
                    ? (int) Math.floorMod(roundRobin.getAndIncrement(), partitions)
                    : (int) Math.floorMod(ConsistentHashRing.hash(r.key()), (long) partitions);
            byPartition.computeIfAbsent(p, k -> new ArrayList<>()).add(i);
        }

        ProduceResult[] results = new ProduceResult[records.size()];
        // Fan the per-partition sub-batches out concurrently: they touch disjoint
        // partitions (independent locks) and disjoint result indexes, and most are
        // forwarded to different leaders, so sequential dispatch would needlessly
        // serialize independent network round-trips.
        List<Callable<Void>> tasks = new ArrayList<>(byPartition.size());
        for (Map.Entry<Integer, List<Integer>> entry : byPartition.entrySet()) {
            int partition = entry.getKey();
            List<Integer> indexes = entry.getValue();
            tasks.add(() -> {
                List<ProduceRecord> sub = new ArrayList<>(indexes.size());
                for (int idx : indexes) {
                    sub.add(records.get(idx));
                }
                List<ProduceResult> subResults = produceSubBatch(topic, partitions, partition, sub);
                for (int j = 0; j < indexes.size(); j++) {
                    results[indexes.get(j)] = subResults.get(j);
                }
                return null;
            });
        }
        if (tasks.size() == 1) {
            runQuietly(tasks.get(0));
        } else {
            dispatchConcurrently(tasks);
        }
        return Arrays.asList(results);
    }

    /** Runs the per-partition sub-batch tasks concurrently and unwraps IOExceptions. */
    private void dispatchConcurrently(List<Callable<Void>> tasks) throws IOException {
        try {
            for (Future<Void> f : batchDispatch.invokeAll(tasks)) {
                f.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("batch produce interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException io) {
                throw io;
            }
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new IOException("batch sub-produce failed", cause);
        }
    }

    private static void runQuietly(Callable<Void> task) throws IOException {
        try {
            task.call();
        } catch (IOException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /** Appends one partition's sub-batch on its leader (local or forwarded). */
    private List<ProduceResult> produceSubBatch(String topic, int partitionCount, int partition,
                                                List<ProduceRecord> sub) throws IOException {
        TopicPartition tp = new TopicPartition(topic, partition);
        int leader = assigner.leaderFor(tp);
        if (leader == cluster.localId()) {
            List<Message> stored = appendBatchAsLeader(tp, partitionCount, sub);
            return toResults(topic, partition, stored);
        }
        WireMessage request = WireMessage.of(FrameType.PRODUCE_BATCH)
                .header("topic", topic)
                .header("partition", partition)
                .header("partitionCount", partitionCount);
        for (ProduceRecord r : sub) {
            // records carry the payload; offset/timestamp are placeholders assigned by the leader
            request.record(new Message(0, 0, r.key(), r.value()));
        }
        WireMessage response = peers.request(leader, request);
        long baseOffset = response.getLong("baseOffset");
        List<ProduceResult> results = new ArrayList<>(sub.size());
        for (int i = 0; i < sub.size(); i++) {
            results.add(new ProduceResult(topic, partition, baseOffset + i));
        }
        return results;
    }

    /** Leader-side apply of a forwarded produce batch; offsets are contiguous. */
    public long produceBatchInto(String topic, int partitionCount, int partition,
                                 List<ProduceRecord> records) throws IOException {
        registry.ensureTopic(topic, partitionCount);
        TopicPartition tp = new TopicPartition(topic, partition);
        if (assigner.leaderFor(tp) != cluster.localId()) {
            throw new IllegalStateException("broker " + cluster.localId() + " is not the leader of " + tp);
        }
        List<Message> stored = appendBatchAsLeader(tp, partitionCount, records);
        return stored.get(0).offset();
    }

    private static List<ProduceResult> toResults(String topic, int partition, List<Message> stored) {
        List<ProduceResult> results = new ArrayList<>(stored.size());
        for (Message m : stored) {
            results.add(new ProduceResult(topic, partition, m.offset()));
        }
        return results;
    }

    /** Follower-side apply of a replicated batch (one fsync for the batch). */
    public void applyReplicaBatch(String topic, int partitionCount, int partition, List<Message> messages)
            throws IOException {
        registry.ensureTopic(topic, partitionCount);
        TopicPartition tp = new TopicPartition(topic, partition);
        Partition local = registry.localPartition(tp);
        if (local == null) {
            throw new IllegalStateException("broker " + cluster.localId() + " is not a replica of " + tp);
        }
        local.log().appendBatchAssigned(messages);
    }

    /** Follower-side apply of a replicated message. */
    public void applyReplica(String topic, int partitionCount, int partition, Message message)
            throws IOException {
        registry.ensureTopic(topic, partitionCount);
        TopicPartition tp = new TopicPartition(topic, partition);
        Partition local = registry.localPartition(tp);
        if (local == null) {
            throw new IllegalStateException("broker " + cluster.localId() + " is not a replica of " + tp);
        }
        local.log().appendAssigned(message);
    }

    // ------------------------------------------------------------------- fetch

    /** Reads from the local copy when we host the partition, else from its leader. */
    public List<Message> fetch(String topic, int partition, long offset, int maxRecords) throws IOException {
        Topic meta = registry.requireTopic(topic);
        if (partition < 0 || partition >= meta.partitionCount()) {
            throw new IllegalArgumentException("partition " + partition + " out of range for topic "
                    + topic + " (0.." + (meta.partitionCount() - 1) + ")");
        }
        int limit = Math.min(Math.max(maxRecords, 0), MAX_FETCH_RECORDS);
        TopicPartition tp = new TopicPartition(topic, partition);
        Partition local = registry.localPartition(tp);
        if (local != null) {
            List<Message> records = local.log().read(offset, limit, MAX_FETCH_BYTES);
            long bytes = records.stream().mapToLong(Message::sizeInBytes).sum();
            metrics.recordFetch(records.size(), bytes);
            return records;
        }
        WireMessage response = peers.request(assigner.leaderFor(tp), WireMessage.of(FrameType.FETCH)
                .header("topic", topic)
                .header("partition", partition)
                .header("offset", offset)
                .header("max", limit));
        return response.records();
    }

    /** The next offset that will be written to the partition. */
    public long endOffset(TopicPartition tp) throws IOException {
        Partition local = registry.localPartition(tp);
        if (local != null) {
            return local.log().endOffset();
        }
        WireMessage response = peers.request(assigner.leaderFor(tp), WireMessage.of(FrameType.LOG_END)
                .header("topic", tp.topic())
                .header("partition", tp.partition()));
        return response.getLong("endOffset");
    }

    // ----------------------------------------------------------------- groups

    public JoinResult joinGroup(String group, String topic, String memberId) throws IOException {
        Names.requireValid(group, "group name");
        Names.requireValid(topic, "topic name");
        Topic meta = registry.topic(topic);
        if (meta == null) {
            meta = createTopic(topic, config.defaultPartitions());
        }
        if (raft.isLeader()) {
            return coordinator.join(group, topic, memberId, meta.partitionCount());
        }
        WireMessage response = peers.request(requireController(), WireMessage.of(FrameType.JOIN_GROUP)
                .header("group", group)
                .header("topic", topic)
                .header("memberId", memberId)
                .header("partitions", meta.partitionCount()));
        return decodeJoin(response);
    }

    public JoinResult heartbeatGroup(String group, String memberId) throws IOException {
        if (raft.isLeader()) {
            return coordinator.heartbeat(group, memberId);
        }
        WireMessage response = peers.request(requireController(), WireMessage.of(FrameType.GROUP_HEARTBEAT)
                .header("group", group)
                .header("memberId", memberId));
        return decodeJoin(response);
    }

    public void commitOffset(String group, String topic, int partition, long offset) throws IOException {
        Names.requireValid(group, "group name");
        Names.requireValid(topic, "topic name");
        if (raft.isLeader()) {
            coordinator.commit(group, new TopicPartition(topic, partition), offset);
            return;
        }
        peers.request(requireController(), WireMessage.of(FrameType.COMMIT_OFFSET)
                .header("group", group)
                .header("topic", topic)
                .header("partition", partition)
                .header("offset", offset));
    }

    public Map<Integer, Long> committedOffsets(String group, String topic) throws IOException {
        if (raft.isLeader()) {
            Topic meta = registry.requireTopic(topic);
            Map<Integer, Long> offsets = new LinkedHashMap<>();
            for (int p = 0; p < meta.partitionCount(); p++) {
                offsets.put(p, coordinator.committed(group, new TopicPartition(topic, p)));
            }
            return offsets;
        }
        WireMessage response = peers.request(requireController(), WireMessage.of(FrameType.FETCH_OFFSETS)
                .header("group", group)
                .header("topic", topic));
        return WireEncoding.decodeOffsets(response.get("offsets"));
    }

    /** Consumer lag per group and partition, computed on (or fetched from) the controller. */
    public List<Map<String, Object>> lag() throws IOException {
        if (raft.isLeader()) {
            return computeLag();
        }
        WireMessage response = peers.request(requireController(), WireMessage.of(FrameType.GROUP_LAG));
        return WireEncoding.decodeLag(response.get("lag"));
    }

    private List<Map<String, Object>> computeLag() {
        List<Map<String, Object>> entries = new ArrayList<>();
        for (ConsumerGroup group : coordinator.allGroups()) {
            for (int p = 0; p < group.partitionCount(); p++) {
                TopicPartition tp = new TopicPartition(group.topic(), p);
                long committed = coordinator.committed(group.groupId(), tp);
                long end;
                try {
                    end = endOffset(tp);
                } catch (IOException | RuntimeException unreachable) {
                    end = -1;
                }
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("group", group.groupId());
                entry.put("topic", group.topic());
                entry.put("partition", p);
                entry.put("committed", committed);
                entry.put("endOffset", end);
                entry.put("lag", end < 0 ? -1 : Math.max(0, end - Math.max(committed, 0)));
                entry.put("generation", group.generation());
                entry.put("members", group.memberCount());
                entries.add(entry);
            }
        }
        return entries;
    }

    // ------------------------------------------------------------ observability

    public Map<String, Object> metricsSnapshot() {
        Map<String, Object> snapshot = new LinkedHashMap<>();
        snapshot.put("brokerId", config.brokerId());
        snapshot.put("role", raft.role().name());
        snapshot.put("term", raft.term());
        snapshot.put("leaderId", raft.leaderId());
        snapshot.putAll(metrics.snapshot());
        return snapshot;
    }

    public Map<String, Object> clusterInfo() {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("brokerId", config.brokerId());
        info.put("role", raft.role().name());
        info.put("term", raft.term());
        info.put("leaderId", raft.leaderId());

        List<Map<String, Object>> brokers = new ArrayList<>();
        for (BrokerNode node : cluster.allNodes()) {
            Map<String, Object> b = new LinkedHashMap<>();
            b.put("id", node.id());
            b.put("host", node.host());
            b.put("port", node.port());
            b.put("alive", cluster.isAlive(node.id()));
            b.put("controller", node.id() == raft.leaderId());
            brokers.add(b);
        }
        info.put("brokers", brokers);

        List<Map<String, Object>> topics = new ArrayList<>();
        for (Topic topic : registry.allTopics()) {
            Map<String, Object> t = new LinkedHashMap<>();
            t.put("name", topic.name());
            t.put("partitionCount", topic.partitionCount());
            List<Map<String, Object>> partitions = new ArrayList<>();
            for (int p = 0; p < topic.partitionCount(); p++) {
                TopicPartition tp = new TopicPartition(topic.name(), p);
                Partition local = registry.localPartition(tp);
                Map<String, Object> pInfo = new LinkedHashMap<>();
                pInfo.put("partition", p);
                pInfo.put("leader", assigner.leaderFor(tp));
                pInfo.put("replicas", assigner.replicasFor(tp));
                pInfo.put("endOffset", local == null ? -1 : local.log().endOffset());
                partitions.add(pInfo);
            }
            t.put("partitions", partitions);
            topics.add(t);
        }
        info.put("topics", topics);
        return info;
    }

    // ---------------------------------------------------------------- plumbing

    /**
     * Rejects payloads that would overflow the wire framing before any bytes
     * are written — a mid-frame writeUTF failure would corrupt the connection
     * stream for whoever shares it.
     */
    private static void requireWireSafe(String key, String value) {
        if (value == null) {
            throw new IllegalArgumentException("message value is required");
        }
        if (value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > MAX_VALUE_BYTES) {
            throw new IllegalArgumentException("message value exceeds " + MAX_VALUE_BYTES + " bytes");
        }
        if (key != null && key.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > MAX_KEY_BYTES) {
            throw new IllegalArgumentException("message key exceeds " + MAX_KEY_BYTES + " bytes");
        }
    }

    private int requireController() {
        int controller = raft.leaderId();
        if (controller < 0 || controller == cluster.localId()) {
            throw new IllegalStateException("no controller elected yet, retry shortly");
        }
        return controller;
    }

    static JoinResult decodeJoin(WireMessage response) {
        return new JoinResult(response.get("memberId"),
                response.getInt("generation"),
                WireEncoding.decodePartitions(response.get("assigned")));
    }

    @Override
    public String metadataToShare() {
        return registry.metadataString();
    }

    @Override
    public void onLeaderMetadata(String metadata) {
        registry.applyMetadata(metadata);
    }

    public RaftNode raft() {
        return raft;
    }

    public GroupCoordinator coordinator() {
        return coordinator;
    }

    public int brokerId() {
        return config.brokerId();
    }

    @Override
    public void close() throws IOException {
        batchDispatch.shutdownNow();
        raft.close();
        server.close();
        peers.close();
        coordinator.close();
        registry.close();
    }
}
