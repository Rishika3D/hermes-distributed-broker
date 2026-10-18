"""Hermes Python client — stdlib only, talks to any broker's REST API.

The broker forwards requests it does not own (produces go to the partition
leader, group operations to the controller), so a client may point at any
node in the cluster. Pass several base URLs for failover.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field


class HermesError(RuntimeError):
    """Raised when every configured broker rejects or fails a request."""


@dataclass
class HermesClient:
    """Thin producer/admin client over the Hermes REST API."""

    brokers: list[str] = field(default_factory=lambda: ["http://localhost:8081"])
    timeout: float = 5.0
    retries: int = 3            # extra full passes over the broker list after the first
    backoff: float = 0.2        # delay before the first retry; doubles each pass
    max_backoff: float = 2.0    # ceiling for the doubled delay

    # ------------------------------------------------------------- plumbing

    def _request(self, method: str, path: str, body: dict | list | None = None):
        """Send a request, failing over across brokers and retrying transient failures.

        Each pass tries every broker in order (immediate failover). If all fail with a
        transient error (5xx, connection refused, timeout), sleep with exponential backoff
        and try another pass, up to ``retries`` extra passes. 4xx errors are the caller's
        fault and are raised at once.

        Delivery is at-least-once: a produce that timed out after the broker stored it, or
        was refused with 503 for missing quorum, may be stored again on retry.
        """
        last_error: Exception | None = None
        for attempt in range(self.retries + 1):
            for base in self.brokers:
                url = base.rstrip("/") + path
                data = None if body is None else json.dumps(body).encode()
                req = urllib.request.Request(url, data=data, method=method)
                req.add_header("Content-Type", "application/json")
                try:
                    with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                        payload = resp.read()
                        return json.loads(payload) if payload else None
                except urllib.error.HTTPError as e:
                    detail = e.read().decode(errors="replace")
                    last_error = HermesError(f"{method} {url} -> {e.code}: {detail}")
                    if 400 <= e.code < 500:
                        raise last_error from None  # our fault; retrying won't help
                except OSError as e:  # connection refused, timeout, DNS...
                    last_error = e
            if attempt < self.retries:
                time.sleep(min(self.backoff * 2 ** attempt, self.max_backoff))
        raise HermesError(f"no broker reachable for {method} {path}") from last_error

    # --------------------------------------------------------------- admin

    def create_topic(self, name: str, partitions: int | None = None) -> dict:
        return self._request("POST", "/api/topics", {"name": name, "partitions": partitions})

    def topics(self) -> list[dict]:
        return self._request("GET", "/api/topics")

    def cluster(self) -> dict:
        return self._request("GET", "/api/cluster")

    def metrics(self) -> dict:
        return self._request("GET", "/api/metrics")

    def lag(self) -> list[dict]:
        return self._request("GET", "/api/lag")

    # ------------------------------------------------------------- produce

    def produce(self, topic: str, value: str, key: str | None = None) -> dict:
        return self._request("POST", f"/api/topics/{topic}/messages", {"key": key, "value": value})

    def produce_batch(self, topic: str, messages: list[tuple[str | None, str]]) -> list[dict]:
        body = [{"key": k, "value": v} for k, v in messages]
        return self._request("POST", f"/api/topics/{topic}/messages/batch", body)

    # ------------------------------------------------------------- consume

    def fetch(self, topic: str, partition: int, offset: int, max_records: int = 100) -> dict:
        return self._request(
            "GET",
            f"/api/topics/{topic}/partitions/{partition}/messages"
            f"?offset={offset}&max={max_records}",
        )

    def join_group(self, group: str, topic: str, member_id: str | None = None) -> dict:
        return self._request(
            "POST", f"/api/groups/{group}/join", {"topic": topic, "memberId": member_id}
        )

    def committed_offsets(self, group: str, topic: str) -> dict:
        return self._request("GET", f"/api/groups/{group}/offsets?topic={topic}")

    def commit_offset(self, group: str, topic: str, partition: int, offset: int) -> None:
        self._request(
            "POST",
            f"/api/groups/{group}/offsets",
            {"topic": topic, "partition": partition, "offset": offset},
        )


class GroupConsumer:
    """A consumer-group member: joins, polls its partitions, commits offsets.

    Rejoining on every poll doubles as the group heartbeat, and picks up any
    rebalanced assignment (generation changes) automatically.
    """

    def __init__(self, client: HermesClient, group: str, topic: str):
        self.client = client
        self.group = group
        self.topic = topic
        self.member_id: str | None = None
        self.generation = -1
        self.partitions: list[int] = []
        self._positions: dict[int, int] = {}

    def _sync_assignment(self) -> None:
        result = self.client.join_group(self.group, self.topic, self.member_id)
        self.member_id = result["memberId"]
        if result["generation"] != self.generation:
            self.generation = result["generation"]
            self.partitions = result["partitions"]
            committed = self.client.committed_offsets(self.group, self.topic)
            self._positions = {
                p: max(0, committed.get(str(p), -1)) for p in self.partitions
            }

    def poll(self, max_records: int = 100) -> list[dict]:
        """Fetches the next batch across owned partitions and commits as it goes."""
        self._sync_assignment()
        records: list[dict] = []
        for partition in self.partitions:
            position = self._positions.get(partition, 0)
            batch = self.client.fetch(self.topic, partition, position, max_records)
            for record in batch["records"]:
                record["partition"] = partition
                records.append(record)
            next_offset = batch["nextOffset"]
            if next_offset > position:
                self.client.commit_offset(self.group, self.topic, partition, next_offset)
                self._positions[partition] = next_offset
        return records
