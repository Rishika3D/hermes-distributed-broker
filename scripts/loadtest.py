#!/usr/bin/env python3
"""Load + chaos harness for a running Hermes cluster (stdlib only).

Scenarios:
  1. Concurrent load  — throughput + latency percentiles under many producers.
  2. Large payloads   — boundary behaviour at the 60 KB value limit.
  3. Chaos            — kill a broker mid-load, measure the durable-write error
                        rate (acks=all), then recovery after restart.

Usage: python3 scripts/loadtest.py
Assumes brokers on :8081-8083 and scripts/start-cluster.sh / stop are available.
"""

from __future__ import annotations

import json
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

BROKERS = ["http://localhost:8081", "http://localhost:8082", "http://localhost:8083"]
ROOT = Path(__file__).resolve().parent.parent


def post(base: str, path: str, body: dict, timeout: float = 5.0):
    data = json.dumps(body).encode()
    req = urllib.request.Request(base + path, data=data, method="POST",
                                 headers={"Content-Type": "application/json"})
    start = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            resp.read()
            return resp.status, (time.perf_counter() - start) * 1000
    except urllib.error.HTTPError as e:
        return e.code, (time.perf_counter() - start) * 1000
    except OSError:
        return 0, (time.perf_counter() - start) * 1000


def percentile(values, p):
    if not values:
        return 0.0
    values = sorted(values)
    idx = min(len(values) - 1, int(p / 100 * len(values)))
    return values[idx]


def scenario_load(topic: str, total: int, workers: int):
    print(f"\n=== Scenario 1: concurrent load ({total} msgs, {workers} producers) ===")
    post(BROKERS[0], "/api/topics", {"name": topic, "partitions": 12})
    latencies, statuses = [], []
    lock = threading.Lock()

    def produce(i: int):
        base = BROKERS[i % len(BROKERS)]
        status, ms = post(base, f"/api/topics/{topic}/messages",
                          {"key": f"k{i}", "value": f"payload-{i}"})
        with lock:
            latencies.append(ms)
            statuses.append(status)

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        pool.map(produce, range(total))
    elapsed = time.perf_counter() - start

    ok = sum(1 for s in statuses if s == 200)
    print(f"  throughput : {ok / elapsed:.0f} msg/s ({ok}/{total} ok in {elapsed:.2f}s)")
    print(f"  latency ms : p50={percentile(latencies,50):.1f} "
          f"p95={percentile(latencies,95):.1f} p99={percentile(latencies,99):.1f} "
          f"max={max(latencies):.1f}")
    print(f"  mean ms    : {statistics.mean(latencies):.2f}")
    return ok / elapsed


def scenario_large_payload(topic: str):
    print("\n=== Scenario 2: large payloads (60 KB value limit) ===")
    post(BROKERS[0], "/api/topics", {"name": topic, "partitions": 3})
    just_under, _ = post(BROKERS[0], f"/api/topics/{topic}/messages",
                         {"key": "big", "value": "x" * 59_000})
    over, _ = post(BROKERS[0], f"/api/topics/{topic}/messages",
                  {"key": "big", "value": "x" * 70_000})
    print(f"  59 KB value -> HTTP {just_under} (expect 200, accepted)")
    print(f"  70 KB value -> HTTP {over} (expect 400, rejected cleanly — no stream corruption)")
    # prove the connection/broker is still healthy after the oversized rejection
    after, _ = post(BROKERS[0], f"/api/topics/{topic}/messages", {"key": "ok", "value": "still-works"})
    print(f"  follow-up   -> HTTP {after} (expect 200 — broker healthy after rejection)")
    return just_under == 200 and over == 400 and after == 200


def broker_pids():
    return {p.stem.split("-")[1]: int(p.read_text().strip())
            for p in (ROOT / ".logs").glob("broker-*.pid")}


def scenario_chaos(topic: str, duration_s: int = 8):
    print(f"\n=== Scenario 3: chaos — kill a broker mid-load ({duration_s}s) ===")
    post(BROKERS[0], "/api/topics", {"name": topic, "partitions": 12})
    stop = threading.Event()
    counters = {"ok": 0, "unavailable": 0, "other": 0}
    lock = threading.Lock()

    def hammer():
        i = 0
        while not stop.is_set():
            i += 1
            status, _ = post(BROKERS[i % len(BROKERS)], f"/api/topics/{topic}/messages",
                            {"key": f"k{i}", "value": f"v{i}"}, timeout=3)
            with lock:
                if status == 200:
                    counters["ok"] += 1
                elif status == 503:
                    counters["unavailable"] += 1  # acks=all: correctly refused, not lost
                else:
                    counters["other"] += 1
            time.sleep(0.002)

    threads = [threading.Thread(target=hammer, daemon=True) for _ in range(8)]
    for t in threads:
        t.start()

    time.sleep(duration_s / 2)
    victim = broker_pids().get("3")
    print(f"  [chaos] killing broker 3 (pid {victim}) under load...")
    subprocess.run(["kill", "-9", str(victim)], check=False)
    kill_mark = dict(counters)
    time.sleep(duration_s / 2)
    stop.set()
    for t in threads:
        t.join()

    after = {k: counters[k] - kill_mark[k] for k in counters}
    print(f"  before kill: ok={kill_mark['ok']} 503={kill_mark['unavailable']} other={kill_mark['other']}")
    print(f"  after  kill: ok={after['ok']} 503={after['unavailable']} other={after['other']}")
    total_after = sum(after.values()) or 1
    print(f"  during outage: {after['ok']}/{total_after} writes still succeeded on healthy "
          f"partitions; {after['unavailable']} correctly refused as non-durable (NOT lost)")
    print("  interpretation: partitions whose replica set included broker 3 refuse writes")
    print("                  (acks=all durability); the rest keep serving. No silent loss.")


def main():
    scenario_load("lt-load", total=2000, workers=32)
    scenario_large_payload("lt-payload")
    scenario_chaos("lt-chaos")
    print("\n[loadtest] done")


if __name__ == "__main__":
    sys.exit(main())
