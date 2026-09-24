#!/usr/bin/env python3
"""Hermes produce benchmark — runs against a real 3-node cluster.

Same methodology as the project baseline: concurrent producer threads, every
request's ack latency recorded, all messages must be acked (acks=all). Reports
throughput and p50/p95/p99/p99.9 request latency, and emits a machine-readable
JSON results file.

At batch=1 each request carries one message (POST /messages); at batch>1 each
request carries `batch` messages in one call (POST /messages/batch), which the
broker appends with a single fsync + one replication frame.

Usage:
  python3 benchmarks/bench.py --total 20000 --batch 100 --concurrency 16 \
      --brokers http://localhost:8081,http://localhost:8082,http://localhost:8083 \
      --out benchmarks/results/batch100.json
"""
from __future__ import annotations

import argparse, json, os, time, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor

def _post(url, payload, timeout):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, method="POST",
                                 headers={"Content-Type": "application/json"})
    t = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            r.read(); return True, (time.perf_counter() - t) * 1000
    except (urllib.error.HTTPError, OSError):
        return False, (time.perf_counter() - t) * 1000

def _pct(sorted_vals, p):
    if not sorted_vals: return 0.0
    return sorted_vals[min(len(sorted_vals) - 1, int(p / 100 * len(sorted_vals)))]

def run(brokers, topic, total, batch, concurrency, value_bytes, timeout, keyed=False):
    # ensure topic exists (24 partitions, like the baseline)
    _post(f"{brokers[0]}/api/topics", {"name": topic, "partitions": 24}, timeout)
    value = "x" * value_bytes
    n_requests = (total + batch - 1) // batch
    latencies = [0.0] * n_requests
    ok = [False] * n_requests

    def do_request(i):
        base = brokers[i % len(brokers)]
        # keyed=True concentrates each request's records onto one partition (shared key),
        # so a batch amortizes one fsync over all its records. keyed=False uses null keys
        # (round-robin), spreading a batch across up to 24 partitions (pessimal for batching).
        key = f"k{i}" if keyed else None
        if batch == 1:
            success, ms = _post(f"{base}/api/topics/{topic}/messages",
                                 {"key": key, "value": value}, timeout)
        else:
            body = [{"key": key, "value": value} for _ in range(batch)]
            success, ms = _post(f"{base}/api/topics/{topic}/messages/batch", body, timeout)
        latencies[i] = ms; ok[i] = success

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        list(pool.map(do_request, range(n_requests)))
    elapsed = time.perf_counter() - start

    acked_requests = sum(ok)
    acked_messages = acked_requests * batch
    lat = sorted(latencies)
    return {
        "topic": topic, "total_messages": total, "batch_size": batch,
        "concurrency": concurrency, "value_bytes": value_bytes,
        "requests": n_requests, "acked_requests": acked_requests,
        "acked_messages": acked_messages, "elapsed_sec": round(elapsed, 3),
        "keyed": keyed,
        "throughput_msgs_per_sec": round(acked_messages / elapsed, 1),
        "request_latency_ms": {
            "p50": round(_pct(lat, 50), 2), "p95": round(_pct(lat, 95), 2),
            "p99": round(_pct(lat, 99), 2), "p999": round(_pct(lat, 99.9), 2),
            "max": round(max(lat), 2) if lat else 0.0,
        },
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--brokers", default="http://localhost:8081,http://localhost:8082,http://localhost:8083")
    ap.add_argument("--topic", default=None)
    ap.add_argument("--total", type=int, default=20000)
    ap.add_argument("--batch", type=int, default=1)
    ap.add_argument("--concurrency", type=int, default=16)
    ap.add_argument("--value-bytes", type=int, default=200)
    ap.add_argument("--timeout", type=float, default=30.0)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    brokers = [b.strip() for b in args.brokers.split(",") if b.strip()]
    topic = args.topic or f"bench_b{args.batch}_{int(time.time())}"
    result = run(brokers, topic, args.total, args.batch, args.concurrency,
                 args.value_bytes, args.timeout)
    print(json.dumps(result, indent=2))
    if args.out:
        os.makedirs(os.path.dirname(args.out), exist_ok=True)
        with open(args.out, "w") as f:
            json.dump(result, f, indent=2)
        print(f"\nwrote {args.out}")

if __name__ == "__main__":
    main()
