package io.hermes.core.broker;

import io.hermes.core.cluster.BrokerNode;
import io.hermes.core.replication.QuorumNotReachedException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Reproduces H1: a produce whose replication misses quorum must NOT be
 * acknowledged as successful. Runs the same scenario under both ack policies:
 *  - acks=leader (the old behaviour) silently returns success — the bug.
 *  - acks=all (the fix) throws QuorumNotReachedException — durable semantics.
 */
@Timeout(120)
class DurabilityQuorumTest {

    private static final String TOPIC = "durability";
    private static final String SECRET = "dur-secret";

    @TempDir
    Path dataRoot;

    private final List<Broker> brokers = new ArrayList<>();

    @AfterEach
    void stop() throws IOException {
        for (Broker b : brokers) {
            b.close();
        }
        brokers.clear();
    }

    @Test
    void acksLeaderSilentlyAcceptsNonDurableWrite_thenAcksAllRejectsIt() throws Exception {
        int[] ports = freePorts(3);
        List<BrokerNode> members = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            members.add(new BrokerNode(i + 1, "localhost", ports[i]));
        }

        // --- Boot cluster with acks=LEADER to reproduce the original behaviour ---
        startCluster(members, BrokerConfig.ACKS_LEADER);
        waitForController();
        Broker any = brokers.get(0);
        any.createTopic(TOPIC, 6);

        // Find a partition whose leader and follower are distinct, then crash the follower.
        int[] leaderFollowerPartition = pickReplicatedPartition(any);
        int leaderId = leaderFollowerPartition[0];
        int followerId = leaderFollowerPartition[1];
        int partition = leaderFollowerPartition[2];
        brokerById(followerId).close();
        Thread.sleep(500);

        // BUG DEMONSTRATED: leader-only acks returns success though the follower is gone
        // and this RF=2 partition can no longer reach its majority of 2.
        ProduceResult result = assertDoesNotThrow(() ->
                        brokerById(leaderId).produceInto(TOPIC, 6, partition, "k", "not-durable"),
                "acks=leader accepts the write (documents the non-durable behaviour)");
        assertEquals(partition, result.partition());
        System.out.println("[H1-repro] acks=leader ACCEPTED offset " + result.offset()
                + " on " + TOPIC + "-" + partition + " with follower " + followerId + " down (NON-DURABLE)");

        stop();

        // --- Same scenario, acks=ALL: the write must be rejected as not durable ---
        startCluster(members, BrokerConfig.ACKS_ALL);
        waitForController();
        brokers.get(0).createTopic(TOPIC, 6);
        int[] lfp2 = pickReplicatedPartition(brokers.get(0));
        brokerById(lfp2[1]).close();
        Thread.sleep(500);

        QuorumNotReachedException error = assertThrows(QuorumNotReachedException.class,
                () -> brokerById(lfp2[0]).produceInto(TOPIC, 6, lfp2[2], "k", "must-fail"),
                "acks=all must reject a write that cannot reach quorum");
        System.out.println("[H1-fixed] acks=all REJECTED: " + error.getMessage());

        // Control: a partition whose replicas are all alive still succeeds under acks=all.
        int healthy = healthyPartition(brokers.get(0), lfp2[1]);
        if (healthy >= 0) {
            ProduceResult ok = brokerLeaderOf(healthy).produceInto(TOPIC, 6, healthy, "k", "durable");
            System.out.println("[H1-fixed] acks=all ACCEPTED durable write at offset " + ok.offset()
                    + " on healthy partition " + healthy);
        } else {
            System.out.println("[H1-fixed] (control skipped: crashed broker was a replica of all partitions)");
        }
    }

    // ---- helpers -----------------------------------------------------------

    private void startCluster(List<BrokerNode> members, String acks) throws IOException {
        for (int i = 0; i < 3; i++) {
            Broker broker = new Broker(new BrokerConfig(i + 1, dataRoot.resolve(acks + "-broker-" + (i + 1)),
                    members, 2, 6, 64 * 1024, SECRET, acks));
            broker.start();
            brokers.add(broker);
        }
    }

    private Broker brokerById(int id) {
        return brokers.stream().filter(b -> b.brokerId() == id).findFirst().orElseThrow();
    }

    @SuppressWarnings("unchecked")
    private Broker brokerLeaderOf(int partition) {
        Map<String, Object> info = brokers.get(0).clusterInfo();
        for (Map<String, Object> t : (List<Map<String, Object>>) info.get("topics")) {
            if (!TOPIC.equals(t.get("name"))) continue;
            for (Map<String, Object> p : (List<Map<String, Object>>) t.get("partitions")) {
                if ((int) p.get("partition") == partition) {
                    return brokerById((int) p.get("leader"));
                }
            }
        }
        throw new IllegalStateException("no leader for partition " + partition);
    }

    /** Returns {leaderId, followerId, partition} for a partition with 2 distinct replicas. */
    @SuppressWarnings("unchecked")
    private int[] pickReplicatedPartition(Broker broker) {
        Map<String, Object> info = broker.clusterInfo();
        for (Map<String, Object> t : (List<Map<String, Object>>) info.get("topics")) {
            if (!TOPIC.equals(t.get("name"))) continue;
            for (Map<String, Object> p : (List<Map<String, Object>>) t.get("partitions")) {
                List<Integer> replicas = (List<Integer>) p.get("replicas");
                if (replicas.size() >= 2 && !replicas.get(0).equals(replicas.get(1))) {
                    return new int[]{replicas.get(0), replicas.get(1), (int) p.get("partition")};
                }
            }
        }
        throw new IllegalStateException("no 2-replica partition found");
    }

    /** A partition whose replica set does NOT include the crashed broker. */
    @SuppressWarnings("unchecked")
    private int healthyPartition(Broker broker, int deadBrokerId) {
        Map<String, Object> info = broker.clusterInfo();
        for (Map<String, Object> t : (List<Map<String, Object>>) info.get("topics")) {
            if (!TOPIC.equals(t.get("name"))) continue;
            for (Map<String, Object> p : (List<Map<String, Object>>) t.get("partitions")) {
                List<Integer> replicas = (List<Integer>) p.get("replicas");
                if (!replicas.contains(deadBrokerId)) {
                    return (int) p.get("partition");
                }
            }
        }
        return -1;
    }

    private void waitForController() {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            if (brokers.stream().allMatch(b -> b.raft().leaderId() >= 0)) {
                return;
            }
            sleep();
        }
        fail("no controller elected");
    }

    private static void sleep() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static int[] freePorts(int count) throws IOException {
        int[] ports = new int[count];
        List<ServerSocket> sockets = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ServerSocket s = new ServerSocket(0);
            ports[i] = s.getLocalPort();
            sockets.add(s);
        }
        for (ServerSocket s : sockets) {
            s.close();
        }
        return ports;
    }
}
