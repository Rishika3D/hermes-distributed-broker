#!/usr/bin/env python3
"""End-to-end Hermes demo: create a topic, produce from one thread, consume
through a consumer group from another, and print throughput plus a final
cluster summary. Run scripts/start-cluster.sh first, then:

    python3 clients/python/demo.py [messages]
"""

from __future__ import annotations

import sys
import threading
import time

from hermes_client import GroupConsumer, HermesClient, HermesError

BROKERS = [
    "http://localhost:8081",
    "http://localhost:8082",
    "http://localhost:8083",
]
TOPIC = "orders"
GROUP = "demo-consumers"


def produce(client: HermesClient, total: int) -> None:
    start = time.time()
    for i in range(total):
        client.produce(TOPIC, value=f'{{"order": {i}, "amount": {i % 97}}}', key=f"customer-{i % 20}")
        if (i + 1) % 200 == 0:
            rate = (i + 1) / (time.time() - start)
            print(f"[producer] {i + 1}/{total} sent ({rate:.0f} msg/s)")
    print(f"[producer] done: {total} messages in {time.time() - start:.1f}s")


def consume(client: HermesClient, total: int, done: threading.Event) -> None:
    consumer = GroupConsumer(client, GROUP, TOPIC)
    seen = 0
    idle_polls = 0
    while seen < total and idle_polls < 20:
        records = consumer.poll(max_records=200)
        if records:
            seen += len(records)
            idle_polls = 0
            first, last = records[0], records[-1]
            print(
                f"[consumer {consumer.member_id}] +{len(records)} "
                f"(total {seen}) e.g. p{first['partition']}@{first['offset']}: {first['value'][:40]}"
            )
        else:
            idle_polls += 1
            time.sleep(0.25)
    done.set()
    print(f"[consumer] finished with {seen} messages consumed")


def main() -> None:
    total = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000
    client = HermesClient(brokers=BROKERS)

    topic = None
    for _ in range(20):  # the cluster may still be electing a controller
        try:
            topic = client.create_topic(TOPIC, partitions=6)
            break
        except HermesError:
            time.sleep(0.5)
    if topic is None:
        raise SystemExit("[demo] cluster never became ready; check .logs/broker-*.log")
    print(f"[demo] topic ready: {topic}")

    done = threading.Event()
    producer = threading.Thread(target=produce, args=(client, total), daemon=True)
    consumer = threading.Thread(target=consume, args=(client, total, done), daemon=True)
    consumer.start()
    producer.start()
    producer.join()
    done.wait(timeout=60)

    print("\n[demo] consumer lag:")
    for row in client.lag():
        print(
            f"  {row['group']} {row['topic']}-{row['partition']}: "
            f"committed={row['committed']} end={row['endOffset']} lag={row['lag']}"
        )

    info = client.cluster()
    alive = sum(1 for b in info["brokers"] if b["alive"])
    print(f"\n[demo] cluster: {alive}/{len(info['brokers'])} brokers alive, "
          f"controller=broker {info['leaderId']}, term={info['term']}")
    print("[demo] open http://localhost:3000 to watch the dashboard")


if __name__ == "__main__":
    main()
