package io.hermes.core.cluster;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A consistent hash ring over broker ids. Each broker is placed on the ring at
 * many virtual points (to smooth the distribution); a key maps to the first
 * broker clockwise from its hash. The ring is immutable once built, so every
 * broker constructed with the same membership computes identical placements.
 */
public final class ConsistentHashRing {

    private static final int VIRTUAL_NODES_PER_BROKER = 128;

    private final TreeMap<Long, Integer> ring = new TreeMap<>();
    private final int brokerCount;

    public ConsistentHashRing(Collection<Integer> brokerIds) {
        if (brokerIds.isEmpty()) {
            throw new IllegalArgumentException("ring requires at least one broker");
        }
        for (int brokerId : brokerIds) {
            for (int v = 0; v < VIRTUAL_NODES_PER_BROKER; v++) {
                ring.put(hash("broker-" + brokerId + "#" + v), brokerId);
            }
        }
        this.brokerCount = (int) brokerIds.stream().distinct().count();
    }

    /**
     * The first {@code count} distinct brokers walking clockwise from the
     * key's position — index 0 is the primary, the rest are replica targets.
     * Iterates the tail of the ring and wraps to the head lazily; this sits on
     * the produce hot path, so it must not copy the ring per lookup.
     */
    public List<Integer> nodesFor(String key, int count) {
        int wanted = Math.min(count, brokerCount);
        List<Integer> nodes = new ArrayList<>(wanted);
        if (collectDistinct(ring.tailMap(hash(key)), nodes, wanted)) {
            return nodes;
        }
        collectDistinct(ring, nodes, wanted);
        return nodes;
    }

    private static boolean collectDistinct(SortedMap<Long, Integer> section,
                                           List<Integer> nodes, int wanted) {
        for (Map.Entry<Long, Integer> entry : section.entrySet()) {
            if (!nodes.contains(entry.getValue())) {
                nodes.add(entry.getValue());
                if (nodes.size() == wanted) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * FNV-1a 64-bit hash with a murmur3-style finalizer — stable across JVMs,
     * unlike String.hashCode. The finalizer matters: raw FNV-1a leaves nearly
     * identical keys ("orders-0", "orders-1", ...) correlated in the high bits
     * that determine ring position, which piles sequential partitions onto one
     * broker.
     */
    public static long hash(String key) {
        long h = 0xcbf29ce484222325L;
        for (byte b : key.getBytes(StandardCharsets.UTF_8)) {
            h ^= b & 0xffL;
            h *= 0x100000001b3L;
        }
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;
        return h;
    }
}
