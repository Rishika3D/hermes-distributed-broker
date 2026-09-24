package io.hermes.core.raft;

import io.hermes.core.cluster.ClusterState;
import io.hermes.core.cluster.BrokerNode;
import io.hermes.core.net.FrameType;
import io.hermes.core.net.PeerChannels;
import io.hermes.core.net.WireMessage;

import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * A simplified Raft node: terms, randomized election timeouts, majority voting
 * and leader heartbeats. The elected leader acts as the cluster controller —
 * it owns topic metadata (piggybacked on heartbeats so followers converge) and
 * hosts the consumer group coordinator. Log matching and commit indexes from
 * full Raft are intentionally omitted; data replication is handled per
 * partition by {@link io.hermes.core.replication.Replicator}.
 */
public final class RaftNode implements Closeable {

    /** Supplies metadata for outgoing heartbeats and applies metadata from the leader. */
    public interface MetadataChannel {
        String metadataToShare();

        void onLeaderMetadata(String metadata);
    }

    private static final long TICK_MS = 100;
    private static final long HEARTBEAT_INTERVAL_MS = 400;
    private static final long ELECTION_TIMEOUT_MIN_MS = 1_500;
    private static final long ELECTION_TIMEOUT_JITTER_MS = 1_500;

    private final ClusterState cluster;
    private final PeerChannels peers;
    private final MetadataChannel metadata;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "raft-" + System.nanoTime());
                t.setDaemon(true);
                return t;
            });

    private volatile RaftRole role = RaftRole.FOLLOWER;
    private volatile int currentTerm = 0;
    private volatile Integer votedFor = null;
    private volatile int leaderId = -1;
    private volatile long electionDeadline;
    private volatile long lastHeartbeatSent;

    public RaftNode(ClusterState cluster, PeerChannels peers, MetadataChannel metadata) {
        this.cluster = cluster;
        this.peers = peers;
        this.metadata = metadata;
    }

    public void start() {
        resetElectionDeadline();
        scheduler.scheduleAtFixedRate(this::tick, TICK_MS, TICK_MS, TimeUnit.MILLISECONDS);
    }

    private void tick() {
        try {
            if (role == RaftRole.LEADER) {
                if (System.currentTimeMillis() - lastHeartbeatSent >= HEARTBEAT_INTERVAL_MS) {
                    sendHeartbeats();
                }
            } else if (System.currentTimeMillis() > electionDeadline) {
                startElection();
            }
        } catch (Exception e) {
            System.err.println("[raft] tick failed on broker " + cluster.localId() + ": " + e.getMessage());
        }
    }

    /**
     * Never hold the node's monitor across network I/O: a vote request to a
     * peer that is itself mid-election would deadlock both nodes until socket
     * timeouts, storming terms upward. State transitions are locked; the
     * ballot round-trips are not.
     */
    private void startElection() {
        int term;
        synchronized (this) {
            role = RaftRole.CANDIDATE;
            currentTerm++;
            votedFor = cluster.localId();
            resetElectionDeadline();
            term = currentTerm;
        }
        int votes = 1;
        for (BrokerNode peer : cluster.peers()) {
            WireMessage ballot = WireMessage.of(FrameType.REQUEST_VOTE)
                    .header("term", term)
                    .header("candidateId", cluster.localId());
            Optional<WireMessage> response = peers.tryRequest(peer.id(), ballot);
            if (response.isEmpty()) {
                continue;
            }
            cluster.markAlive(peer.id());
            int responseTerm = response.get().getInt("term");
            if (responseTerm > term) {
                stepDown(responseTerm);
                return;
            }
            if (response.get().getBoolean("granted")) {
                votes++;
            }
        }
        boolean won;
        synchronized (this) {
            won = role == RaftRole.CANDIDATE && currentTerm == term && votes >= cluster.majority();
            if (won) {
                role = RaftRole.LEADER;
                leaderId = cluster.localId();
                System.out.println("[raft] broker " + cluster.localId() + " elected leader for term " + term);
            }
        }
        if (won) {
            sendHeartbeats();
        }
    }

    private void sendHeartbeats() {
        lastHeartbeatSent = System.currentTimeMillis();
        StringBuilder alive = new StringBuilder();
        for (BrokerNode node : cluster.allNodes()) {
            if (cluster.isAlive(node.id())) {
                if (alive.length() > 0) {
                    alive.append(',');
                }
                alive.append(node.id());
            }
        }
        WireMessage heartbeat = WireMessage.of(FrameType.HEARTBEAT)
                .header("term", currentTerm)
                .header("leaderId", cluster.localId())
                .header("alive", alive.toString())
                .header("topics", metadata.metadataToShare());
        for (BrokerNode peer : cluster.peers()) {
            peers.tryRequest(peer.id(), heartbeat).ifPresent(ack -> {
                cluster.markAlive(peer.id());
                int ackTerm = ack.getInt("term");
                if (ackTerm > currentTerm) {
                    stepDown(ackTerm);
                }
            });
        }
    }

    public synchronized WireMessage handleRequestVote(WireMessage request) {
        int term = request.getInt("term");
        int candidateId = request.getInt("candidateId");
        if (term > currentTerm) {
            stepDown(term);
        }
        boolean granted = term == currentTerm && (votedFor == null || votedFor == candidateId);
        if (granted) {
            votedFor = candidateId;
            resetElectionDeadline();
        }
        cluster.markAlive(candidateId);
        return WireMessage.of(FrameType.VOTE_RESPONSE)
                .header("term", currentTerm)
                .header("granted", Boolean.toString(granted));
    }

    public synchronized WireMessage handleHeartbeat(WireMessage request) {
        int term = request.getInt("term");
        int remoteLeader = request.getInt("leaderId");
        if (term >= currentTerm) {
            if (term > currentTerm) {
                votedFor = null;
            }
            currentTerm = term;
            role = RaftRole.FOLLOWER;
            leaderId = remoteLeader;
            resetElectionDeadline();
            cluster.markAlive(remoteLeader);
            adoptLivenessView(request.get("alive"));
            metadata.onLeaderMetadata(request.get("topics"));
        }
        return WireMessage.of(FrameType.HEARTBEAT_ACK).header("term", currentTerm);
    }

    /**
     * Followers only exchange frames with the leader, so without this they
     * would consider every other follower dead and skip replicating to it.
     * The leader learns liveness from heartbeat acks and gossips its view;
     * followers adopt it (at most one liveness window stale).
     */
    private void adoptLivenessView(String aliveCsv) {
        if (aliveCsv == null || aliveCsv.isBlank()) {
            return;
        }
        for (String id : aliveCsv.split(",")) {
            int brokerId = Integer.parseInt(id.trim());
            if (brokerId != cluster.localId()) {
                cluster.markAlive(brokerId);
            }
        }
    }

    private synchronized void stepDown(int newTerm) {
        currentTerm = newTerm;
        role = RaftRole.FOLLOWER;
        votedFor = null;
        resetElectionDeadline();
    }

    private void resetElectionDeadline() {
        electionDeadline = System.currentTimeMillis()
                + ELECTION_TIMEOUT_MIN_MS
                + ThreadLocalRandom.current().nextLong(ELECTION_TIMEOUT_JITTER_MS);
    }

    public boolean isLeader() {
        return role == RaftRole.LEADER;
    }

    public int leaderId() {
        return leaderId;
    }

    public int term() {
        return currentTerm;
    }

    public RaftRole role() {
        return role;
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
