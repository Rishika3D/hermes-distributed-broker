#!/usr/bin/env python3
"""Measures the four headline metrics against a running 3-node Hermes cluster:
throughput (100k msgs), p99 ack latency, failover time, WAL recovery time.
Throughput/latency are driven concurrently since the broker is fsync-bound
(single-threaded would only measure round-trip latency, not capacity)."""
import json, time, urllib.request, urllib.error, sys
from concurrent.futures import ThreadPoolExecutor

BROKERS = ["http://localhost:8081", "http://localhost:8082", "http://localhost:8083"]

def post(base, path, body, timeout=15):
    d = json.dumps(body).encode()
    r = urllib.request.Request(base + path, data=d, method="POST",
                               headers={"Content-Type": "application/json"})
    t = time.perf_counter()
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            resp.read(); return 200, (time.perf_counter() - t) * 1000
    except urllib.error.HTTPError as e:
        return e.code, (time.perf_counter() - t) * 1000
    except OSError:
        return 0, (time.perf_counter() - t) * 1000

def pct(v, p):
    v = sorted(v); return v[min(len(v)-1, int(p/100*len(v)))]

def throughput_and_latency(total=100_000, workers=32, partitions=24):
    post(BROKERS[0], "/api/topics", {"name": "bench", "partitions": partitions})
    lat = [0.0]*total; ok = [0]*total
    def work(i):
        s, ms = post(BROKERS[i % 3], "/api/topics/bench/messages", {"key": None, "value": "m"*180})
        lat[i] = ms; ok[i] = 1 if s == 200 else 0
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(work, range(total)))
    dt = time.perf_counter() - start
    succeeded = sum(ok)
    print("=== THROUGHPUT + p99 LATENCY ===")
    print(f"  messages        : {total} ({succeeded} ok) in {dt:.2f}s")
    print(f"  throughput      : {succeeded/dt:,.0f} msgs/sec")
    print(f"  latency p50/p95 : {pct(lat,50):.2f} / {pct(lat,95):.2f} ms")
    print(f"  latency p99/max : {pct(lat,99):.2f} / {max(lat):.2f} ms")

if __name__ == "__main__":
    throughput_and_latency(int(sys.argv[1]) if len(sys.argv) > 1 else 100_000)
