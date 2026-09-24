package io.hermes.core.broker;

/** Where a produced message landed: which partition and at what offset. */
public record ProduceResult(String topic, int partition, long offset) {
}
