package io.hermes.core.net;

import io.hermes.core.cluster.BrokerNode;
import io.hermes.core.cluster.ClusterState;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The pool of peer connections, one {@link PeerClient} per remote broker.
 * Offers a throwing request for paths that must surface failures and a
 * best-effort variant for gossip-style traffic (heartbeats, votes).
 */
public final class PeerChannels implements Closeable {

    /** Header carrying the shared cluster secret on every authenticated frame. */
    public static final String SECRET_HEADER = "_secret";

    private final Map<Integer, PeerClient> clients = new LinkedHashMap<>();
    private final String clusterSecret;

    public PeerChannels(ClusterState cluster, String clusterSecret) {
        this.clusterSecret = clusterSecret;
        for (BrokerNode peer : cluster.peers()) {
            clients.put(peer.id(), new PeerClient(peer));
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
        } catch (IOException unreachable) {
            return Optional.empty();
        }
    }

    @Override
    public void close() {
        clients.values().forEach(PeerClient::close);
    }
}
