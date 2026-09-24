package io.hermes.core.cluster;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks the configured membership and the observed liveness of each peer.
 * A peer counts as alive if we heard from it (heartbeat, ack, any frame)
 * within the liveness window; the local broker is always alive.
 */
public final class ClusterState {

    public static final long LIVENESS_WINDOW_MS = 4_000;

    private final int localId;
    private final Map<Integer, BrokerNode> nodes = new LinkedHashMap<>();
    private final Map<Integer, Long> lastSeenMillis = new ConcurrentHashMap<>();

    public ClusterState(int localId, Collection<BrokerNode> members) {
        this.localId = localId;
        for (BrokerNode node : members) {
            nodes.put(node.id(), node);
        }
        if (!nodes.containsKey(localId)) {
            throw new IllegalArgumentException("local broker " + localId + " missing from peer list");
        }
    }

    public void markAlive(int brokerId) {
        lastSeenMillis.put(brokerId, System.currentTimeMillis());
    }

    public boolean isAlive(int brokerId) {
        if (brokerId == localId) {
            return true;
        }
        Long seen = lastSeenMillis.get(brokerId);
        return seen != null && System.currentTimeMillis() - seen < LIVENESS_WINDOW_MS;
    }

    public int localId() {
        return localId;
    }

    public BrokerNode localNode() {
        return nodes.get(localId);
    }

    public BrokerNode node(int brokerId) {
        BrokerNode node = nodes.get(brokerId);
        if (node == null) {
            throw new IllegalArgumentException("unknown broker id " + brokerId);
        }
        return node;
    }

    public Collection<BrokerNode> allNodes() {
        return nodes.values();
    }

    public List<BrokerNode> peers() {
        List<BrokerNode> peers = new ArrayList<>();
        for (BrokerNode node : nodes.values()) {
            if (node.id() != localId) {
                peers.add(node);
            }
        }
        return peers;
    }

    public int size() {
        return nodes.size();
    }

    public int majority() {
        return nodes.size() / 2 + 1;
    }
}
