#!/usr/bin/env python3
"""Runs the batch-size sweep (1, 10, 100, 1000) at fixed concurrency against a
running cluster, for two workloads:
  - spread  (null keys, round-robin across 24 partitions — pessimal for batching)
  - keyed   (each request's records share a key → one partition → fsync amortized)
Writes a combined machine-readable results file plus a human-readable table.
Same methodology as the baseline."""
from __future__ import annotations

import json, os, sys, time
from bench import run

BROKERS = ["http://localhost:8081", "http://localhost:8082", "http://localhost:8083"]

def sweep(total, concurrency, keyed, label):
    print(f"\n=== {label} workload (total={total}, concurrency={concurrency}) ===")
    print(f"{'batch':>6} {'tput msg/s':>12} {'reqp50':>9} {'reqp99':>9} {'reqp99.9':>9} {'acked':>14}")
    runs = []
    for b in [1, 10, 100, 1000]:
        topic = f"sw_{label}_b{b}_{int(time.time())}"
        r = run(BROKERS, topic, total, b, concurrency, value_bytes=200, timeout=30.0, keyed=keyed)
        runs.append(r)
        L = r["request_latency_ms"]
        print(f"{b:>6} {r['throughput_msgs_per_sec']:>12,.0f} {L['p50']:>9.1f} {L['p99']:>9.1f} "
              f"{L['p999']:>9.1f} {r['acked_messages']:>7}/{total}")
    return runs

def main():
    total = int(sys.argv[1]) if len(sys.argv) > 1 else 20000
    concurrency = int(sys.argv[2]) if len(sys.argv) > 2 else 16
    out = {"total": total, "concurrency": concurrency,
           "spread": sweep(total, concurrency, False, "spread"),
           "keyed": sweep(total, concurrency, True, "keyed")}
    os.makedirs("benchmarks/results", exist_ok=True)
    with open("benchmarks/results/sweep.json", "w") as f:
        json.dump(out, f, indent=2)
    print("\nwrote benchmarks/results/sweep.json")

if __name__ == "__main__":
    main()
