package io.hermes.core.model;

/** Identity of a single partition within a topic; used as a map key everywhere. */
public record TopicPartition(String topic, int partition) {

    @Override
    public String toString() {
        return topic + "-" + partition;
    }
}
