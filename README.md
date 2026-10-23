# Hermes

[![CI](https://github.com/Rishika3D/hermes-distributed-broker/actions/workflows/ci.yml/badge.svg)](https://github.com/Rishika3D/hermes-distributed-broker/actions/workflows/ci.yml)
![License](https://img.shields.io/badge/license-MIT-blue)
![Java](https://img.shields.io/badge/Java-21-orange)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.5-green)
![Tests](https://img.shields.io/badge/tests-163%20passing-brightgreen)
![Coverage](https://img.shields.io/badge/coverage-core%2089.5%25%20%7C%20rest%2092.6%25-brightgreen)

A distributed message broker built from first principles in Java: topics, partitions, append-only
write-ahead-log segments, consistent-hash partition placement, simplified Raft leader election,
majority-ack replication, consumer groups with rebalancing, a Spring Boot REST layer, a Next.js
telemetry dashboard, and a stdlib-only Python client.

**No Kafka libraries, no messaging frameworks.** Broker-to-broker traffic runs over raw Java sockets
with a hand-rolled binary protocol. The `hermes-core` engine has **zero runtime dependencies**.

```
                         ┌────────────────────────────┐
   Next.js dashboard ───►│  REST API  (Spring Boot)   │   one per broker node
   (server-side poller)  │  hermes-rest :8081/2/3     │
   Python client    ───►│                            │
                         └─────────────┬──────────────┘
                                       │ direct Java calls
                         ┌─────────────▼──────────────┐
                         │  Broker facade             │
                         │  (hermes-core, plain Java) │
                         │                            │
                         │  TopicRegistry ── WAL      │
                         │  RaftNode ─── controller   │
                         │  Replicator ─ majority ack │
                         │  GroupCoordinator ─ groups │
                         └─────────────┬──────────────┘
                                       │ raw TCP sockets :9091/2/3
                          binary frames (votes, heartbeats,
                          replication, forwarded ops)
```

## Contents

[Measured results](#measured-results) · [Architecture](#architecture) · [Running locally](#running-locally) ·
[Configuration](#configuration-reference) · [Production deployment](#production-deployment-guide) ·
[Operations & troubleshooting](#operations--troubleshooting) · [Security](#security) ·
[Testing](#testing) · [Hardening log](#hardening-log) · [Known limitations](#known-limitations)

---

## Measured results

> Every number here came from the harness in [`benchmarks/`](benchmarks/), `autocannon`, JFR/`jcmd`,
> or the test suite. **Environment:** one 8-core / 8 GB macOS laptop where all three broker JVMs *and*
> the load generator share the same cores and a single SSD, so absolute numbers **understate**
> dedicated hardware, and run-to-run variance is real (interleaved A/B runs below agree within ~2%;
> runs hours apart, with different background load such as Spotlight indexing, differed by up to ~50%).
> **Important:** all *native* numbers are from macOS, where Java's `FileChannel.force` performs a full
> drive-cache flush (`F_FULLFSYNC`); I measured that at ~325 syncs/s for one writer and ~614/s for 8 on
> this laptop. A Linux production host with a real disk will differ in either direction.
> Use the *ratios and bottleneck analysis* as the reliable part. Numbers marked *(earlier session)*
> were measured under different background load.

### Throughput

| Workload | Result | Setup |
|---|---:|---|
| Batched produce, keyed, 1,000 msgs/request | **71,591 msgs/s** | 3 nodes, RF=2, `acks=all`, 20k msgs, 16 concurrent |
| Batched produce, 1,000 msgs/request *(earlier session)* | **~220,000–310,000 msgs/s** | `autocannon`, 1 node, RF=1, ~50 B values, 64–512 concurrent |
| Single-message durable produce *(earlier session)* | **4,419 msgs/s** (collapses past ~800 connections) | `autocannon`, 1 node, RF=1, group commit on |
| Single-message, hot partition, **group commit on** | **3,288 msgs/s** (p50 9.4 ms, p99 18.7 ms) | 1 node, RF=1, 32 concurrent producers |
| Read path `GET /api/health` *(earlier session)* | 36,844 req/s | `autocannon`, parallel workers |
| Read path `GET /api/metrics` *(earlier session)* | 20,260 req/s | same |

### Batching: where the speedup comes from

20,000 messages, concurrency 16, 3 nodes, RF=2, `acks=all`, 24 partitions (`benchmarks/sweep.py`):

| batch size | spread keys (round-robin) | keyed (one partition) |
|---:|---:|---:|
| 1 | 375 msgs/s | 193 msgs/s |
| 10 | 264 msgs/s *(slower than 1)* | 1,213 msgs/s |
| 100 | 594 msgs/s | 9,975 msgs/s |
| 1,000 | 5,404 msgs/s | **71,591 msgs/s (371× batch=1)** |

Batching only pays when records land in the **same partition**. With null keys spread over 24
partitions a batch of 10 becomes ~10 single-record appends *and* waits on the slowest of them, so it is
slower than not batching. A concentrated batch amortises one `fsync`, one replication frame and one HTTP
round-trip across every record. The sweep reproduces within ~8% of the first run (spread 379/286/626/5,095;
keyed 180/1,146/10,079/70,867).

### Group commit: sharing fsyncs between concurrent producers

Hot single-partition topic, 32 concurrent single-message producers (`benchmarks/bench.py`):

| config | group commit **off** | group commit **on** (linger 2 ms) | speedup |
|---|---:|---:|---:|
| 1 node, RF=1 | 338 msgs/s (p50 95 ms) | **3,288 / 3,283 msgs/s** (p50 9.4 ms) | **9.7×** |
| 3 nodes, RF=2 | 182 msgs/s (p50 123 ms) | **825 / 611 msgs/s** (p50 33–40 ms) | **3.4–4.5×** |

RF=2 gains less because replication is still one synchronous frame per append, which group commit does
not coalesce (only the leader's fsync is shared). Storage-layer proof (`GroupCommitTest`): **4,000
concurrent appends → 502 fsyncs (8.0× coalescing)**, every offset unique, contiguous and durable.
The coordination was refactored into its own class this release; an interleaved A/B against the previous
implementation showed **no performance change** (old 3,359 / 3,330 vs new 3,315 / 3,361 msgs/s).

### Inside Docker on Linux (Docker Desktop VM, kernel 6.12)

The same compose stack that you would deploy (3 brokers, `acks=all`, group commit on, RF=2, API key on):

| Test | Result |
|---|---|
| `fsync` on the container's data volume | 3.6 ms single writer; **3,271 syncs/s** with 8 concurrent writers |
| Keyed batch=1000, 20k msgs, 16 concurrent: 11 runs | **9,100 – 67,000 msgs/s** (median ≈ 24,000); 10 of 11 runs fully acknowledged, 1 lost 10% to timeouts |
| Spread batch=1000 / batch=100 (one sweep) | 17,157 / 4,704 msgs/s |
| Hot partition, 32 single-message producers | 82 – 150 msgs/s (p99 up to 9 s on a cold stack) |
| Idle 75 s, then 14 writes | **14/14 succeed** (was `200 502 502 …` before the stale-connection fix) |
| 800 simultaneous peer connects (Linux) | before the backlog fix **257 delayed ≥ 1 s**; after: **0** (worst 326 ms) |

**Read this honestly:** Docker Desktop on a laptop is a noisy environment: identical runs differed up to
~10×, the first sweep right after a restart was far worse than later ones, and I could not attribute the
bad runs to a single cause (not disk: container fsync is *faster* than native; not GC: pauses p50 ≈ 5.5 ms,
max 513 ms; not OOM: 0 restarts). What the failures *were*, shown by the new peer-failure warnings, is
`REPLICATE`/`HEARTBEAT` calls exceeding the 3 s peer timeout. Raising `HERMES_PEER_TIMEOUT_MS` to 15,000
went 6/6 fully acknowledged vs 5/6 at 3,000: suggestive but **not statistically meaningful**, so treat it as
a tuning recommendation, not a proven fix. These are **not** production capacity numbers; benchmark on
your target Linux hardware.

### Reliability & fault tolerance

| Scenario | Result |
|---|---|
| Leader failover (`kill -9` controller → new leader elected) | **1,568 ms** |
| Broker restart & WAL recovery (12 MB log, 16 segments) | **~2,009 ms**, JVM/Spring boot dominated: an empty-data-dir start took 3.9 s in the same test, so WAL replay of 12 MB is below measurement noise |
| 5,000 concurrent appends to one partition, 32 threads | **5,000 unique contiguous offsets, 0 duplicates**, all persisted |
| Network partition (`SIGSTOP` a broker, then heal) | **No split-brain**; writes to healthy partitions kept succeeding; full recovery on heal |
| Broker crash under load (`acks=all`) | Writes that cannot reach quorum are **rejected with 503**, never silently acknowledged |
| Protocol fuzzing | 50,000 malformed frames, **0 crashes, 0 hangs** |
| One 126 MB request against a 96 MB heap | **Before: broker killed (OOM). After: 413/closed, broker alive, `/api/health` 200** |

### Resource utilisation (profiled with JFR, `jcmd`, `lsof`, `-Xlog:gc`)

The write path is **disk-bound, not CPU-bound.**

| Measurement | Value |
|---|---|
| JVM CPU, durable single-message writes | **42–66% of the 800% available** (cores mostly idle) |
| JVM CPU, 1,000-msg batches | 76–186% avg (peak 459%) |
| JVM CPU, `GET /api/health` (max HTTP load) | 291% avg (peak 425%); whole machine 12.6% idle |
| Disk during a sustained write burst | **9,212 IOPS, 58.6 MB/s** with **27.9% CPU idle** |
| `fsync` latency (JFR, 16,699 events) | 2.88 ms isolated → **29.2 ms mean under contention** (3 JVMs, one disk) |
| Hash-ring routing (hot path) | **164 ns/op**, ~0.007% of a request |

Threads sit blocked in `FileChannel.force()` rather than on-CPU, which is why batching and group commit
(not more cores) are the levers that matter. On a production host with a dedicated NVMe per broker the
fsync term shrinks by roughly the 10× seen between the isolated and contended measurements.

**Soak (accelerated: 450 s, not 24 h)**, 3 brokers, 512 MB heap, sustained load at the saturation knee:

| Metric | Observed |
|---|---|
| Heap used | sawtooth 39–187 MB, no upward trend |
| Live set after forced full GC | **27 MB** |
| Threads | constant **72** |
| Open file descriptors | **87–107**, stable |
| GC pauses | 28 pauses, mean 9.9 ms, **max 51.8 ms** |

This rules out leaks that manifest within minutes. A real 12–24 h soak was **not** run (see limitations).

**Saturation sweep** (single-message produce, 3 nodes, RF=2, 24 partitions; *earlier session*):

| concurrency | msgs/s | p50 | p99 |
|---:|---:|---:|---:|
| 1 | 57 | 11 ms | 118 ms |
| 16 | **422** | 35 ms | 80 ms |
| 128 | 448 (peak) | 231 ms | 1,127 ms |
| 384 | 284 (collapse) | 835 ms | 3,057 ms |

Flat throughput with linearly rising latency past concurrency ≈16 is the signature of a serialised
resource (the fsync queue), not CPU.

---

## Architecture

### Storage: write-ahead log segments (`io.hermes.core.storage`)

Each partition is a `PartitionLog`: an ordered chain of append-only `LogSegment` files
(`data/broker-N/<topic>-<partition>/00000000000000000000.log`). Records are binary-encoded by
`RecordSerde` (offset, timestamp, key, value), appended sequentially, **fsynced**, and never mutated.
When the active segment exceeds `hermes.segment-bytes` (1 MiB default) it rolls to a new file named by
its base offset. On startup segments are re-scanned to rebuild an in-memory `SparseIndex` (every 64th
record → byte position), which also truncates torn trailing writes: reads seek to the nearest indexed
position and scan forward.

**Batching & group commit.** A batch of N records is written under one lock acquisition with a **single
`fsync`** and replicated as **one frame**. Independently, `GroupCommitter` coalesces fsyncs across
*concurrent* producers: writers append under a short lock and take a ticket; one waiting thread at a
time becomes the flusher and syncs; everyone whose ticket it covers is released. A writer is released
**only after a sync that began after its write has succeeded**. If the sync throws, nobody is released:
waiters wake and retry the sync themselves, so an I/O error surfaces as an error to every affected
producer instead of a false "durable" (fault-injection tested; see [Hardening log](#hardening-log)).

### Partition placement: consistent hashing (`io.hermes.core.cluster`)

`ConsistentHashRing` places every broker on a 64-bit ring at 128 virtual points (FNV-1a with a
murmur-style finalizer; raw FNV-1a clustered sequential partition names onto one broker).
`PartitionAssigner` maps `topic-partition` → the first N distinct brokers clockwise: index 0 leads, the
rest replicate. The ring is a pure function of the configured membership, so every broker computes
identical assignments with zero coordination; a produce arriving at any node is appended locally or
forwarded straight to the leader.

### Simplified Raft (`io.hermes.core.raft`)

`RaftNode` implements terms, randomized election timeouts (1.5–3 s), majority voting and leader
heartbeats (400 ms). The elected leader is the **cluster controller**: it owns topic metadata
(piggybacked on heartbeats) and hosts the consumer-group coordinator. Simplified relative to full Raft:
no replicated command log or commit indexes; data replication is per-partition, and metadata sync is
last-writer-wins gossip from the leader. Election I/O happens outside the node's monitor (holding it
across network calls deadlocked two simultaneous candidates in an early version).

### Replication (`io.hermes.core.replication`)

After the partition leader appends locally, `Replicator` pushes the record(s) synchronously to every
alive follower and counts acks. The write is durable once a **majority of the replica set** (leader
included) holds it. Followers apply records with leader-assigned offsets, so replication retries are
idempotent. Under `acks=all` a write that misses quorum **fails with 503** rather than being silently
acknowledged.

### Consumer groups (`io.hermes.core.group`)

`GroupCoordinator` lives on the controller; other brokers forward group frames to it. Each group tracks
members and a generation counter; every membership change (join, leave, 10 s session expiry) bumps the
generation and re-runs **range assignment**. Committed offsets go through `OffsetStore`, an append-only
WAL fsynced per commit and replayed on startup. Commits are validated against the member's generation
and ownership, so a zombie consumer that missed a rebalance cannot clobber another member's progress.

### Wire protocol (`io.hermes.core.net`)

Each broker runs a `BrokerServer` (virtual-thread-per-connection, bounded connection ceiling that sheds
load) speaking framed binary over `DataInput/OutputStream`: 1-byte opcode, string headers, optional
record batch (`MessageCodec`). `PeerClient` keeps a pool of persistent sockets per peer with strict
request/response semantics and a configurable read timeout. Peer-call failures are logged (throttled to
one line per peer per 5 s, with a suppressed-count) so a degraded peer is visible.

### Request routing

| Operation | Handled by | Non-owner behaviour |
|---|---|---|
| produce | partition leader | forward `PRODUCE` (with the resolved partition) to the leader |
| fetch | any replica of the partition | forward `FETCH` to leader |
| create topic | controller | ask controller for the authoritative partition count, then materialise |
| join / heartbeat / commit / lag | controller | forward to controller |

Two rules keep the cluster loop-free: forwarded produces carry the partition the first broker resolved
(the receiver appends exactly there or errors, never re-routes), and topic auto-creation always takes the
partition count from the controller.

### Observability

`BrokerMetrics` aggregates totals, per-second throughput and produce-latency percentiles, exposed at
`/api/metrics`, topology at `/api/cluster`, group lag at `/api/lag`. Logging goes through Logback
(`io.hermes` at `HERMES_LOG_LEVEL`, default `info`): startup, elections, load shedding, peer failures.
The dashboard's server-side poller merges all brokers' views.

### Dashboard

Next.js. The **server** polls the brokers (`/api/poll`) and the browser only talks to its own origin, so
the broker API key never reaches a client and brokers need not be browser-reachable. `/api/healthz` is a
liveness probe. If brokers are down the UI shows an explicit unreachable state.

## Repository layout

```
hermes-core/     broker engine: plain Java 21, zero runtime dependencies
hermes-rest/     Spring Boot REST layer (one process per broker node)
dashboard/       Next.js telemetry dashboard (port 3000) + node:test unit tests
clients/python/  stdlib-only client (retry/failover) + demo + unittest tests
benchmarks/      reproducible throughput/latency harness + results + analysis
scripts/         start/stop a local 3-node cluster, load/chaos/saturation scripts
```

## Running locally

Prereqs: Java 21+, Maven 3.9+, Node 18+, Python 3.10+.

```bash
mvn package                        # build (runs the full test suite)
./scripts/start-cluster.sh         # 3 brokers: REST :8081-8083, sockets :9091-9093
python3 clients/python/demo.py     # create topic, produce 1000 keyed msgs, consume via a group
cd dashboard && npm install && npm run dev      # http://localhost:3000
./scripts/stop-cluster.sh          # data survives in ./data; delete to reset
```

Watch failure modes live: `kill -9 $(cat .logs/broker-3.pid)` mid-demo; the dashboard flags the node,
produces keep flowing to surviving partition leaders, and after ~1.6 s a new controller term appears if
the controller died.

### REST API (any broker)

```
POST /api/topics                                    {"name","partitions"}
GET  /api/topics
POST /api/topics/{t}/messages                       {"key","value"} → {partition,offset}
POST /api/topics/{t}/messages/batch                 [{"key","value"},...]
GET  /api/topics/{t}/partitions/{p}/messages?offset=&max=
POST /api/groups/{g}/join                           {"topic","memberId"?} → assignment
POST /api/groups/{g}/heartbeat                      {"memberId"}
POST /api/groups/{g}/offsets                        {"topic","partition","offset"}
GET  /api/groups/{g}/offsets?topic=
GET  /api/lag | /api/metrics | /api/cluster
GET  /api/health          200 once a controller is elected, 503 while electing
```

Status codes: `400` invalid input · `401` bad/missing API key · `413` body over the size limit ·
`502` a peer failed or timed out · `503` not durable (no quorum) or no controller yet · `500` unexpected.

### Python client

```python
from hermes_client import HermesClient
client = HermesClient(brokers=["http://b1:8081", "http://b2:8081"])   # fails over across brokers
client.produce("orders", "payload", key="customer-7")
```

Transient failures (5xx, connection refused, timeouts) are retried with exponential backoff
(`retries=3`, `backoff=0.2 s`, doubling, capped at `max_backoff=2 s`) after failing over across brokers;
4xx is raised immediately. **Delivery is at-least-once**: a request that timed out or was refused for
missing quorum may already be stored, so a retry can duplicate it.

---

## Configuration reference

All broker settings are environment variables (see `hermes-rest/src/main/resources/application.yml`).

| Variable | Default | Meaning |
|---|---|---|
| `HERMES_BROKER_ID` | `1` | This node's id (must appear in `HERMES_MEMBERS`) |
| `HERMES_REST_PORT` | `8081` | REST port |
| `HERMES_MEMBERS` | `1@localhost:9091` | Full membership: `id@host:socketPort,...` |
| `HERMES_DATA_DIR` | `./data/broker-<id>` | WAL, topic metadata, committed offsets |
| `HERMES_REPLICATION_FACTOR` | `2` | Copies per partition (leader included) |
| `HERMES_DEFAULT_PARTITIONS` | `3` | Partitions for auto-created topics |
| `HERMES_SEGMENT_BYTES` | `1048576` | Segment roll size |
| `HERMES_ACKS` | `all` | `all` = fsync + quorum before ack; `leader` = ack on local append (can lose acked data) |
| `HERMES_GROUP_COMMIT` | `false` | Coalesce fsyncs across concurrent producers (recommended `true`) |
| `HERMES_LINGER_MS` | `0` | Extra wait to widen a group-commit sync (`2` was benchmarked) |
| `HERMES_BATCH_MAX_RECORDS` | `10000` | Max records per batch request |
| `HERMES_MAX_REQUEST_BYTES` | `16777216` | Max HTTP body; larger → `413` before parsing |
| `HERMES_PEER_TIMEOUT_MS` | `3000` | Wait for a peer's reply (forwarded produce, replication) |
| `HERMES_CLUSTER_SECRET` | *(empty)* | Shared secret on every broker-to-broker frame (**set in production**) |
| `HERMES_API_KEY` | *(empty)* | Required as `X-API-Key` on REST calls except `/api/health` (**set in production**) |
| `HERMES_CORS_ORIGINS` | `*` | Allowed browser origins (**restrict in production**) |
| `HERMES_SHUTDOWN_TIMEOUT` | `20s` | Drain time for in-flight requests on SIGTERM |
| `HERMES_LOG_LEVEL` | `info` | Level for `io.hermes.*` |

Dashboard: `HERMES_BROKERS` (comma-separated broker REST URLs, default the three localhost nodes) and
`HERMES_API_KEY` (sent server-side). Startup logs a warning for each insecure setting left at its
default (no API key, no cluster secret on a multi-node cluster, CORS `*`, `acks=leader`).

---

## Production deployment guide

### Docker Compose

```bash
cp .env.example .env     # set HERMES_CLUSTER_SECRET (required) and HERMES_API_KEY
docker compose up --build
```

Compose **refuses to start without `HERMES_CLUSTER_SECRET`** (no shipped default secret), binds all
ports to loopback, gives each broker a 1 GB memory limit with `MaxRAMPercentage=75` and
`ExitOnOutOfMemoryError` (an OOM exits so the orchestrator restarts a clean process), a 30 s stop grace
period (longer than the 20 s drain), named volumes for data, and a `/api/health` container healthcheck.
Put a TLS-terminating reverse proxy in front to expose the REST API or dashboard.

> **Verification status:** this exact compose stack was built and run on Docker Desktop (macOS, Linux VM) and
> smoke-tested (twice: before and after the library/image upgrades): 4 containers healthy; auth enforced (401/200); produce on broker 1 read back from broker 3;
> a 1,000-record batch (0.27 s); a hard-killed broker (writes to partitions needing it refused, the rest
> kept working) and its recovery (20/20 writes after restart); a full `down`/`up` with **data and topics
> preserved on the named volumes**; non-root `hermes` user owning its volume; `docker stop` drained and
> exited in ~1 s. The images were Trivy-scanned (see Security). It was **not** run on a bare-metal Linux host, and container
> load results are noisy (see *Inside Docker on Linux*).

### Checklist before going live

- [ ] `HERMES_CLUSTER_SECRET` and `HERMES_API_KEY` set; `HERMES_CORS_ORIGINS` restricted
- [ ] TLS at a reverse proxy; broker socket ports (9091…) reachable **only** between brokers
- [ ] `HERMES_ACKS=all`; `HERMES_GROUP_COMMIT=true`, `HERMES_LINGER_MS=2`
- [ ] **A dedicated fast disk per broker.** Throughput is fsync-bound, so disk latency sets your ceiling
- [ ] Data directory on a persistent volume with a backup/snapshot policy
- [ ] Health probe on `/api/health`; alert on repeated `503` and on the `failed:` peer warnings
- [ ] Clients retry with backoff and tolerate duplicates (at-least-once)
- [ ] Rolling restarts: one broker at a time, wait for `/api/health` = 200 before the next
- [ ] Smoke test: `docker compose up`, produce/consume, kill one broker, confirm recovery

### Sizing notes (from the measurements above)

- Plan around **batched writes** (hundreds to thousands of records per request, ideally keyed so a batch
  lands in few partitions); single-message durable writes top out in the low thousands per partition
  even with group commit.
- Keep producer concurrency near the measured knee; past it latency climbs and throughput collapses
  (single-message sweep above).
- Replication factor 3 raises quorum tolerance by design (majority 2 of 3) but was **not benchmarked or
  chaos-tested** here. A partition whose *leader* is down still rejects writes (no data-plane failover).

---

## Operations & troubleshooting

| Symptom | Likely cause | What to do |
|---|---|---|
| `503 … did not reach a majority of replicas … not durable` | A follower is down, partitioned, or too slow | Check `/api/cluster` liveness and the `failed:` peer warnings; retry. The record may already exist on the leader (at-least-once) |
| `502 Read timed out` | A peer reply took longer than `HERMES_PEER_TIMEOUT_MS` (disk saturated, long fsync queue) | Reduce concurrency or move to faster disks; raise `HERMES_PEER_TIMEOUT_MS` if latency is legitimately higher. The write may still have been applied |
| `503 no controller elected yet` | Election in progress or no majority up | Wait ~2–3 s; if persistent, fewer than a majority of brokers are alive |
| First write(s) after an idle period fail, then succeed | (fixed in this release) stale pooled peer connections | Upgrade; see hardening log #10 |
| `401` | Missing/wrong `X-API-Key` | Send the key (dashboard: set `HERMES_API_KEY` on the *dashboard* server) |
| `413` | Body larger than `HERMES_MAX_REQUEST_BYTES` | Send smaller batches |
| Dashboard says "cluster unreachable" | `HERMES_BROKERS` wrong, or brokers down / key mismatch | `curl <dashboard>/api/poll` shows what the server sees per broker |
| Container restarts in a loop | OOM (`ExitOnOutOfMemoryError`) or bad config | Check logs; raise memory; verify `HERMES_MEMBERS` contains `HERMES_BROKER_ID` |

---

## Security

- **Broker-to-broker:** with `HERMES_CLUSTER_SECRET` set, every wire frame must carry it
  (constant-time compare); unauthenticated frames are rejected before dispatch.
- **REST:** optional `HERMES_API_KEY` gates everything except `/api/health`. Input is validated:
  topic/group names `[A-Za-z0-9._-]`, values ≤ 60 KB, request bodies ≤ 16 MiB (413), fetches bounded by
  record count and an 8 MB byte budget, partition indexes bounds-checked, error bodies never leak stack
  traces.
- **Dashboard:** the API key lives only in the dashboard server's environment. It was verified to appear
  **0 times** in the page, the poll response and the client JS bundles.
- **Not provided** (use a gateway): TLS, per-client identity, rate limiting.

**Verification performed**

| Check | Result |
|---|---|
| OWASP-style endpoint tests (no/wrong/right key, traversal and injection names, malformed JSON, oversized value) | all rejected with 401/400 as expected |
| Protocol fuzzing, 50,000 random frames | 0 crashes / hangs |
| Oversized request (126 MB, 96 MB heap) | before: broker died; after: rejected, broker alive |
| Secret scan | no hardcoded secrets |
| Dependency CVEs (Maven) | Upgraded Spring Boot 3.3.5 → **3.5.16**, Tomcat → **10.1.61**, Jackson → **2.21.8** (Spring 6.2.19, Logback 1.5.34); all tests green after each bump |
| Dependency CVEs (npm) | Next.js 14.2.15 → **15.5.28**, React 19, patched `postcss`/`nanoid`: `npm audit` reports **0** production vulnerabilities |
| **Container image scan (Trivy, HIGH/CRITICAL)** | Broker image: **9 CRITICAL + 30 HIGH → 0** (with 2 documented exceptions, below). Dashboard image: **4 CRITICAL + 41 HIGH → 0**; 770 MB → 317 MB, Node 22, npm removed from the runtime image |
| SpotBugs (max effort, low threshold) | 19 findings, 0 critical/high; 1 real null-path bug fixed, the rest triaged (record-exposure style, locks already serialised by callers) |
| PMD (errorprone + multithreading) | 3 minor findings |

**Accepted risks** (`.trivyignore`, with reasons): CVE-2026-47884 (Spring `XsltView` SSRF/RCE) and
CVE-2026-47890 (SSE stream corruption with view fragments) are reported as CRITICAL against Spring
Framework 6.2.x and are fixed only in 7.0.9. Both need features Hermes does not have (it is a REST-only API
with no view layer or SSE), so they are not reachable. Without the ignore file Trivy still reports these two.
Closing them properly means migrating to Spring Boot 4 / Framework 7 (roadmap). Re-scan with:

```bash
docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -v "$PWD/.trivyignore":/.trivyignore:ro \
  aquasec/trivy:latest image --scanners vuln --severity HIGH,CRITICAL --ignorefile /.trivyignore <image>
```

---

## Testing

**163 tests: 151 Java (JUnit 5), 7 dashboard (`node:test`), 5 Python (`unittest`)**, all passing. One Java
test (`ListenBacklogTest`) runs only on Linux; the full Java suite was also run on a real Linux kernel with
JDK 21 (151/151). JaCoCo coverage: **hermes-core 89.5% instructions / 78.8% branches, hermes-rest 92.6% / 75.6%**.

```bash
mvn verify                                        # Java tests + JaCoCo
open hermes-core/target/site/jacoco/index.html    # coverage report
cd dashboard && npm test                          # dashboard unit tests (no extra dependencies)
python3 -m unittest discover -s clients/python    # client retry/failover tests
```

| Layer | Approach | Examples |
|---|---|---|
| Storage / WAL | Real temp files, crash/torn-write recovery, batching = 1 fsync | `PartitionLogTest`, `PartitionLogBatchTest`, `RecordSerdeTest` |
| Concurrency | Multi-threaded stress; **fault injection** for failed fsyncs | `GroupCommitTest`, `GroupCommitterTest`, `ConnectionCapTest` |
| Pure logic | Fast deterministic unit tests | `ConsistentHashRingTest`, `PartitionAssignerTest`, `ClusterStateTest`, `BrokerConfigTest`, `LogThrottleTest` |
| Networking | Stub peers (silent/slow/dropping), listen-backlog burst (Linux), round-trip and fuzz | `PeerClientTimeoutTest`, `PeerClientStaleConnectionTest`, `ListenBacklogTest`, `MessageCodecTest`, `FuzzAndAuthTest` |
| REST controllers | `@WebMvcTest` slices with the engine mocked | `*ControllerTest` |
| Filters / config | Isolated unit tests | `ApiKeyFilterTest`, `RequestSizeLimitFilterTest`, `InsecureSettingWarningsTest` |
| Cluster | Real 3-broker integration: election, forwarding, replication, groups, durability | `ClusterIntegrationTest`, `DurabilityQuorumTest`, `BatchProduceIntegrationTest` |
| Dashboard | Stub broker HTTP server incl. an API-key-protected one | `dashboard/test/poll.test.js` |
| Client | Scripted stub server; sleeps patched | `clients/python/test_hermes_client.py` |

New behaviour in the hardening pass was developed test-first (red → green) where practical. Two test
suites written alongside their code (`RequestSizeLimitFilterTest`, `poll.test.js`) were checked by
**mutation**: the implementation was deliberately broken and the relevant tests confirmed to fail, then
restored. No real network calls are made except to localhost stubs.

### CI

`.github/workflows/ci.yml` runs on every push/PR: Java build + tests + coverage summary and artifacts,
dashboard tests + production build, Python compile + tests, and a compose validation job (which also
asserts compose refuses to start without a cluster secret).

---

## Hardening log

Issues found in a project-wide review and while deploying, each with how it was verified.

| # | Issue | Evidence | Fix |
|---|---|---|---|
| 1 | **Group commit released waiters after a *failed* fsync**: the `finally` block advanced the durable marker even when `force()` threw, silently breaking `acks=all` on any I/O error | Fault-injection tests failed against the old semantics (`ticket must not be treated as durable after a failed sync`; the retry returned without syncing) | Coordination extracted into `GroupCommitter`; durable marker advances only on success; waiters retry the sync. Performance unchanged (interleaved A/B) |
| 2 | **One oversized request could kill a broker** (unbounded JSON body buffered in the heap) | 126 MB body vs 96 MB heap: broker **dead** | `RequestSizeLimitFilter` (413 on declared length; counting stream for chunked bodies); same request: broker **alive**, health 200 |
| 3 | **Dashboard showed a healthy cluster as unreachable whenever the API key was on** (browser calls carried no key; putting it in the bundle would leak it) | Old poller vs key-protected broker: 0/1 reachable, broker healthy | Server-side polling route holds the key; verified absent from page and bundles |
| 4 | Logging was `System.out/err` with no levels; `root: warn` would also have hidden new `INFO` lines | grep: 7 call sites | `System.Logger` in the dependency-free core (routed to Logback by Spring Boot), SLF4J in REST, `io.hermes` log level configurable |
| 5 | Insecure defaults were silent | n/a | Startup warnings for no API key / no cluster secret / CORS `*` / `acks=leader`, as a tested pure function |
| 6 | No graceful shutdown | n/a | `server.shutdown: graceful`, 20 s drain. SIGTERM verified to start graceful shutdown and exit cleanly (draining *under load* was not tested) |
| 7 | Docker: named volume would be root-owned (non-root broker could not write the WAL); a known default cluster secret shipped; no JVM OOM policy | Found by reading the Dockerfile/compose | Data dir created and chowned in the image; secret now required; `ExitOnOutOfMemoryError`, memory limit, loopback ports. **Not built here** |
| 8 | Python client gave up after one pass over the brokers | Tests failed red | Bounded retry with exponential backoff; 4xx never retried; documented at-least-once |
| 9 | Peer read timeout hard-coded; peer failures invisible in logs | One cold-start run showed `502 Read timed out` at 4.3 s and others `503 not durable` with nothing logged | `HERMES_PEER_TIMEOUT_MS`; throttled `failed:` warnings carrying the cause |

| 10 | **Writes failed after the cluster sat idle**: the server closes peer sockets idle > 60 s, but the client pool reused the dead sockets, so the first request on each failed (found only by deploying: `200 502 502 200 …`) | Reproduced on the Docker stack; unit tests failed when eviction/purge were disabled (mutation check) | Pool evicts connections idle > 30 s and discards the rest of the pool when a reused connection fails. After the fix: 14/14 writes OK after a 75 s idle |
| 11 | Peer failures logged as `failed: null` (an `EOFException` has no message); routine idle closes logged as WARN | Seen in the deployed logs | Failures always name the exception class; idle closes are DEBUG |
| 12 | **Client mistakes returned 500 + an ERROR stack trace** (unknown route, wrong method, non-numeric path variable, missing parameter), so a typo'd URL would page an operator | 4/4 new tests failed (red) | Handler extends Spring's `ResponseEntityExceptionHandler`: 404 / 405 / 400 with the same `{"error": …}` body |

| 13 | **Linux dropped connection bursts**: the server's listen backlog was Java's default 50, so on Linux overflow SYNs were retransmitted a second later and tripped the 1 s connect timeout. macOS hides this, and my first test there wrongly cleared it | New Linux-only test: **423 of 500** simultaneous connects delayed ≥ 900 ms (worst 2.2 s). In the deployed containers: 5 / 50 / 129 / 257 delayed at 100 / 200 / 400 / 800 | Backlog 1,024 (capped by `net.core.somaxconn`). Test green; deployed bursts: **0 delayed** at every size |
| 14 | **Vulnerable dependencies and images**: Trivy found 9 CRITICAL + 30 HIGH in the broker image and 4 CRITICAL + 41 HIGH in the dashboard image (including Next.js 14.2.15 CVE-2025-29927) | Trivy before/after, `npm audit` | Spring Boot 3.5.16, Tomcat 10.1.61, Jackson 2.21.8, Next.js 15.5.28 + React 19, Node 22 base, npm and an unused Go binary removed from runtime images, OS packages upgraded at build time, `output: standalone`. Two non-reachable Spring CVEs documented in `.trivyignore` |
| 15 | Compose did not pass `HERMES_PEER_TIMEOUT_MS`; no license | n/a | Exposed in compose and `.env.example`; MIT license added |

**Not reproduced (item 9 context).** In earlier cold-start overload runs ~2% of heavy batch requests failed (503 or
502); I could not reproduce them in six cold-start runs after item 9 (0 failures at both 3,000 and
10,000 ms), nor after a connection-burst test (400 simultaneous connects, max 83 ms), so I cannot claim a
root cause or a fix. The failures occurred right after a Maven build finished, which points to host
contention. They are now observable (item 9) and clients retry; if you see them in production, start with
the `failed:` warnings. (Items 10 and 13 are *different* bugs found afterwards. Item 13 (Linux backlog)
is a plausible contributor to failures under connection bursts on Linux, but the original failures were
observed on macOS, where it does not occur, so the cause of those remains unconfirmed.)

---

## Known limitations

Deliberate simplifications and honest gaps (also see `CERTIFICATION.md`):

- **Static membership; no data-plane leader failover.** Partitions *led* by a crashed or partitioned
  broker reject writes until it returns (measured). Raft re-elects the *controller* in ~1.6 s, but
  partition leadership follows the static ring.
- **No follower catch-up.** A broker that was offline is not back-filled with writes it missed.
- **Raft is simplified:** no replicated log; `term`/`votedFor` are in memory, not persisted;
  group membership resets if the controller changes (offsets are durable).
- **No high-watermark.** Fetches from a lagging follower can briefly miss the newest records.
- **At-least-once.** A request that fails after the leader stored the record (quorum miss or peer
  timeout) may duplicate on retry. A batch spanning several partitions is **not atomic**: some
  partitions' sub-batches can be stored while the call returns an error.
- **Fetches hold the partition lock** while reading, so a slow large fetch can delay writes to that
  partition (not measured).
- **Topics are auto-created on first produce** (a typo creates a topic).
- **Single-message durable throughput is bounded by fsync** (this is the stronger-than-Kafka-default
  durability trade-off); `writeUTF` framing caps values at ~60 KB.
- **No log retention/compaction:** disks fill; plan for it.
- **Not verified here:** a 12–24 h soak (a 450 s accelerated soak was run); chaos scenarios needing
  root (packet loss, disk-full, slow-disk injection, clock skew); bare-metal Linux performance (the
  containers ran in Docker Desktop's VM and were noisy); replication factor 3; a public/internet-facing
  deployment (only local Docker Compose was deployed).
- **Two CRITICAL Spring Framework CVEs are accepted, not fixed** (not reachable; see Security).

### Roadmap

0. Spring Boot 4 / Spring Framework 7 migration, to close the two accepted CVEs.
1. Follower catch-up via pull-based replication, a prerequisite for (2).
2. Data-plane leader failover: promote an in-sync replica when a partition leader dies.
3. Persisted Raft state; batched replication frames; lock-free reads; log retention.
4. A binary client protocol (expected to matter only at high batch sizes, where JSON parsing becomes
   visible; ~0 benefit for single-message writes, which are fsync-bound).

Each class in `hermes-core` has one responsibility and the package boundaries (`storage`, `cluster`,
`net`, `raft`, `replication`, `group`, `metrics`, `topic`, `broker`) match the architecture sections
above. The code is meant to be read.

## License

[MIT](LICENSE) © 2026 Rishika Kherniwal
