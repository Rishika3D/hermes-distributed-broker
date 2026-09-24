package io.hermes.core.group;

import io.hermes.core.model.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reproduces M1: a validated commit must reject a zombie member — one at a
 * stale generation, or committing a partition it no longer owns after a
 * rebalance. The unvalidated path (internal) still works for compatibility.
 */
class GroupCommitValidationTest {

    @TempDir
    Path dir;

    private GroupCoordinator coordinator() throws IOException {
        return new GroupCoordinator(new OffsetStore(dir.resolve("offsets.log")));
    }

    @Test
    void staleGenerationCommitIsRejected() throws IOException {
        try (GroupCoordinator c = coordinator()) {
            JoinResult a = c.join("g", "orders", "A", 6);
            assertEquals(6, a.partitions().size());
            int gen1 = a.generation();

            // A commits a partition it owns at the current generation: accepted.
            c.commit("g", "A", gen1, new TopicPartition("orders", a.partitions().get(0)), 5);

            // A second member joins -> rebalance bumps the generation.
            JoinResult b = c.join("g", "orders", "B", 6);
            assertTrue(b.generation() > gen1);

            // A (zombie) tries to commit with its OLD generation: rejected.
            IllegalStateException stale = assertThrows(IllegalStateException.class,
                    () -> c.commit("g", "A", gen1, new TopicPartition("orders", 0), 9));
            System.out.println("[M1] stale-generation commit rejected: " + stale.getMessage());
        }
    }

    @Test
    void committingUnownedPartitionIsRejected() throws IOException {
        try (GroupCoordinator c = coordinator()) {
            c.join("g", "orders", "A", 6);
            JoinResult b = c.join("g", "orders", "B", 6);
            int gen = b.generation();

            JoinResult a = c.heartbeat("g", "A");
            int owned = a.partitions().get(0);
            int notOwned = -1;
            for (int p = 0; p < 6; p++) {
                if (!a.partitions().contains(p)) {
                    notOwned = p;
                    break;
                }
            }

            // Owned partition at current gen: accepted.
            c.commit("g", "A", a.generation(), new TopicPartition("orders", owned), 3);
            // Partition owned by B, committed by A: rejected.
            final int foreign = notOwned;
            IllegalStateException e = assertThrows(IllegalStateException.class,
                    () -> c.commit("g", "A", gen, new TopicPartition("orders", foreign), 3));
            System.out.println("[M1] cross-owner commit rejected: partition " + foreign + " -> " + e.getMessage());
        }
    }
}
