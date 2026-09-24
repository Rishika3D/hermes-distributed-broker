package io.hermes.core.group;

import io.hermes.core.model.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OffsetStoreTest {

    @TempDir
    Path dir;

    @Test
    void commitsAreReadBackAndLatestWins() throws IOException {
        TopicPartition tp = new TopicPartition("orders", 1);
        try (OffsetStore store = new OffsetStore(dir.resolve("offsets.log"))) {
            assertEquals(-1, store.committed("g", tp));
            store.commit("g", tp, 10);
            store.commit("g", tp, 25);
            assertEquals(25, store.committed("g", tp));
        }
    }

    @Test
    void replaysFromDiskAfterReopen() throws IOException {
        TopicPartition tp = new TopicPartition("orders", 0);
        try (OffsetStore store = new OffsetStore(dir.resolve("offsets.log"))) {
            store.commit("g1", tp, 100);
            store.commit("g2", tp, 7);
        }
        try (OffsetStore reopened = new OffsetStore(dir.resolve("offsets.log"))) {
            assertEquals(100, reopened.committed("g1", tp));
            assertEquals(7, reopened.committed("g2", tp));
        }
    }

    @Test
    void tornTrailingLineIsIgnoredOnReplay() throws IOException {
        Path file = dir.resolve("offsets.log");
        TopicPartition tp = new TopicPartition("orders", 2);
        try (OffsetStore store = new OffsetStore(file)) {
            store.commit("g", tp, 42);
        }
        Files.writeString(file, "g orders 2", StandardOpenOption.APPEND); // torn write, no offset
        try (OffsetStore reopened = new OffsetStore(file)) {
            assertEquals(42, reopened.committed("g", tp));
        }
    }
}
