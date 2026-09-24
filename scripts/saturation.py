#!/usr/bin/env python3
"""Saturation sweep: ramp producer concurrency and record throughput + latency
until throughput stops rising, to locate the knee and identify the bottleneck."""
import json, time, urllib.request, urllib.error, statistics
from concurrent.futures import ThreadPoolExecutor

BROKERS = ["http://localhost:8081", "http://localhost:8082", "http://localhost:8083"]
TOPIC = "sat"

def post(base, path, body, timeout=10):
    data = json.dumps(body).encode()
    req = urllib.request.Request(base + path, data=data, method="POST",
                                 headers={"Content-Type": "application/json"})
    t = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            r.read(); return r.status, (time.perf_counter()-t)*1000
    except urllib.error.HTTPError as e:
        return e.code, (time.perf_counter()-t)*1000
    except OSError:
        return 0, (time.perf_counter()-t)*1000

def pct(v, p):
    v = sorted(v); return v[min(len(v)-1, int(p/100*len(v)))] if v else 0

def run(concurrency, per_worker=40):
    total = concurrency * per_worker
    lat, ok = [], 0
    def work(i):
        s, ms = post(BROKERS[i % 3], f"/api/topics/{TOPIC}/messages", {"key": f"k{i}", "value": "x"*200})
        return s, ms
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        for s, ms in pool.map(work, range(total)):
            lat.append(ms); ok += (s == 200)
    dt = time.perf_counter() - start
    return dict(conc=concurrency, tput=ok/dt, ok=ok, total=total,
               p50=pct(lat,50), p95=pct(lat,95), p99=pct(lat,99), mean=statistics.mean(lat))

def main():
    post(BROKERS[0], "/api/topics", {"name": TOPIC, "partitions": 24})
    print(f"{'conc':>5} {'tput/s':>9} {'p50ms':>7} {'p95ms':>7} {'p99ms':>7} {'ok':>7}")
    peak = 0
    for c in [1, 2, 4, 8, 16, 32, 64, 128, 256, 384]:
        r = run(c)
        peak = max(peak, r['tput'])
        print(f"{r['conc']:>5} {r['tput']:>9.0f} {r['p50']:>7.1f} {r['p95']:>7.1f} {r['p99']:>7.1f} {r['ok']:>7}")
    print(f"\npeak throughput ~ {peak:.0f} msg/s")

if __name__ == "__main__":
    main()
