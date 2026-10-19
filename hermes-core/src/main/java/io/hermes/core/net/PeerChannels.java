package io.hermes.core.net;

import io.hermes.core.cluster.BrokerNode;
import io.hermes.core.cluster.ClusterState;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The pool of peer connections, one {@link PeerClient} per remote broker.
 * Offers a throwing request for paths that must surface failures and a
 * best-effort variant for gossip-style traffic (heartbeats, votes).
 */
public final class PeerChannels implements Closeable {

    /** Header carrying the shared cluster secret on every authenticated frame. */
    public static final String SECRET_HEADER = "_secret";

    private static final System.Logger LOG = System.getLogger(PeerChannels.class.getName());
    private static final long FAILURE_LOG_INTERVAL_MS = 5_000;

    private final Map<Integer, PeerClient> clients = new LinkedHashMap<>();
    private final Map<Integer, LogThrottle> failureLogs = new ConcurrentHashMap<>();
    private final String clusterSecret;

    public PeerChannels(ClusterState cluster, String clusterSecret) {
        this(cluster, clusterSecret, PeerClient.DEFAULT_READ_TIMEOUT_MS);
    }

    public PeerChannels(ClusterState cluster, String clusterSecret, int readTimeoutMs) {
        this.clusterSecret = clusterSecret;
        for (BrokerNode peer : cluster.peers()) {
            clients.put(peer.id(), new PeerClient(peer, readTimeoutMs));
            failureLogs.put(peer.id(), new LogThrottle(FAILURE_LOG_INTERVAL_MS));
        }
    }

    public WireMessage request(int brokerId, WireMessage message) throws IOException {
        PeerClient client = clients.get(brokerId);
        if (client == null) {
            throw new IOException("no channel to broker " + brokerId);
        }
        if (clusterSecret != null) {
            message.header(SECRET_HEADER, clusterSecret);
        }
        WireMessage response = client.request(message);
        if (response.isError()) {
            throw new IOException("broker " + brokerId + " rejected " + message.type() + ": " + response.get("reason"));
        }
        return response;
    }

    /** Best-effort request: unreachable or failing peers yield empty. */
    public Optional<WireMessage> tryRequest(int brokerId, WireMessage message) {
        try {
            return Optional.of(request(brokerId, message));
        } catch (IOException failure) {
            logFailure(brokerId, message, failure);
            return Optional.empty();
        }
    }

    /** One WARN per peer per interval, carrying the cause and how many similar failures it hides. */
    private void logFailure(int brokerId, WireMessage message, IOException failure) {
        LogThrottle throttle = failureLogs.get(brokerId);
        if (throttle != null && throttle.allow(System.currentTimeMillis())) {
            int hidden = throttle.takeSuppressed();
            LOG.log(System.Logger.Level.WARNING, "request " + message.type() + " to broker " + brokerId
                    + " failed: " + describe(failure)
                    + (hidden > 0 ? " (+" + hidden + " similar suppressed)" : ""));
        }
    }

    /** Always names the exception: e.g. an EOFException (peer closed the socket) has no message. */
    static String describe(Throwable failure) {
        String name = failure.getClass().getSimpleName();
        return failure.getMessage() == null ? name : name + ": " + failure.getMessage();
    }

    @Override
    public void close() {
        clients.values().forEach(PeerClient::close);
    }
}
