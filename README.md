# Hermes

[![CI](https://github.com/Rishika3D/hermes-distributed-broker/actions/workflows/ci.yml/badge.svg)](https://github.com/Rishika3D/hermes-distributed-broker/actions/workflows/ci.yml)
![Java](https://img.shields.io/badge/Java-21-orange)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.4-green)
![Tests](https://img.shields.io/badge/tests-118%20passing-brightgreen)
![Coverage](https://img.shields.io/badge/coverage-core%2089%25%20%7C%20rest%2095%25-brightgreen)

A distributed message broker built from first principles in Java — topics, partitions,
append-only log segments, consistent-hash partition placement, simplified Raft leader
election, majority-ack replication, consumer groups with rebalancing, a Spring Boot
REST layer, a Next.js telemetry dashboard, and a stdlib-only Python client.

**No Kafka libraries, no messaging frameworks.** Broker-to-broker traffic runs over raw
Java sockets with a hand-rolled binary protocol. The `hermes-core` engine has **zero
runtime dependencies**.

```
                         ┌────────────────────────────┐
   Next.js dashboard ───►│  REST API  (Spring Boot)   │   one per broker node
   Python client    ───►│  hermes-rest :8081/2/3      │
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

---

## Measured results

> Every number below was produced by the harness in [`benchmarks/`](benchmarks/) or the
> test suite — none are estimates. **Environment:** single 8-core / 8 GB macOS laptop
> with all 3 broker JVMs *and* the load generator sharing the same cores and disk, so
> these **understate** what dedicated hardware would give.

### Throughput

| Workload | Result |
|---|---:|
| Batched produce (1,000 msgs/request, durable) | **~220,000–310,000 msgs/sec** |
| Single-message produce (durable, `acks=all`, c=800) | **4,419 msgs/sec** |
| Read path — `GET /api/health` | **36,844 req/sec** |
| Read path — `GET /api/metrics` | **20,260 req/sec** |

### Why batching dominates — fsync amortisation

20,000 messages, concurrency 16, RF=2, 24 partitions:

| batch size | spread keys (round-robin) | keyed (same partition) |
|---:|---:|---:|
| 1 | 379 msgs/s | 180 msgs/s |
| 10 | 286 msgs/s *(worse than 1)* | 1,146 msgs/s |
| 100 | 626 msgs/s | 10,079 msgs/s |
| 1,000 | 5,095 msgs/s | **70,867 msgs/s (187×)** |

The honest finding: batching only helps when records land in the **same partition**. With
null keys spread across 24 partitions, a batch of 10 becomes 10 single-record appends and
is *slower* than no batching at all. Concentrated batches amortise one `fsync` across all
1,000 records — that is the entire win.

### Group commit — coalescing fsyncs across concurrent producers

Hot single-partition topic, 32 concurrent single-message producers:

| config | RF=1 | RF=2 |
|---|---:|---:|
| group commit **off** | 342 msgs/s | 180 msgs/s |
| group commit **on** (`linger=2ms`) | **2,127 msgs/s (6.2×)** | **694 msgs/s (3.9×)** |

Storage layer, verified in `GroupCommitTest`: **4,000 concurrent appends → 501 fsyncs
(8.0× coalescing)** with every offset unique, contiguous, and durable.

### Reliability & fault tolerance

| Scenario | Result |
|---|---|
| Leader failover (`kill -9` controller → new leader elected) | **1,568 ms** |
| Broker restart & WAL recovery (12 MB log, 16 segments) | **~2,009 ms** (JVM boot dominated; replay is negligible) |
| Concurrency correctness (5,000 concurrent appends, 32 threads) | **5,000 unique contiguous offsets, 0 duplicates** |
| Network partition (`SIGSTOP` a broker, then heal) | **No split-brain**, full recovery |
| Protocol fuzzing (50,000 malformed frames) | **0 crashes, 0 hangs** |
| Broker crash under load (`acks=all`) | Non-durable writes correctly **rejected with 503**, zero silent loss |

### Bottleneck analysis — it is disk, not CPU

| Measurement | Value |
|---|---|
| JVM CPU under peak write load | **66% of 800% available** (CPU largely idle) |
| Disk during sustained write burst | **9,212 IOPS, 58.6 MB/s** while 27.9% CPU idle |
| `fsync` latency (JFR, 16,699 events) | 2.88 ms isolated → **29.2 ms under contention** |
| Hash-ring routing (hot path) | **164 ns/op** (0.007% of request cost) |

Profiling with Java Flight Recorder showed threads almost never on-CPU — they block in
`force()`. The system is **fsync-bound**, which is why batching and group commit (not
more cores) are the levers that matter.

---

## Architecture

### Storage: write-ahead log segments (`io.hermes.core.storage`)

Each partition is a `PartitionLog`: an ordered chain of append-only `LogSegment`
files (`data/broker-N/<topic>-<partition>/00000000000000000000.log`). Records are
binary-encoded by `RecordSerde` (offset, timestamp, key, value), appended
sequentially, **fsynced**, and never mutated. When the active segment exceeds
`hermes.segment-bytes` (1 MiB default) it rolls to a new file named by its base
offset. On startup segments are re-scanned to rebuild an in-memory `SparseIndex`
(every 64th record → byte position), which also truncates torn trailing writes for
free — reads seek to the nearest indexed position and scan forward.

**Batching & group commit.** A batch of N records is written under one lock
acquisition with a **single `fsync`** and replicated as **one frame**. Independently,
`groupCommit` coalesces fsyncs across *concurrent* producers: writers append under a
short lock, then a single flusher performs one `force()` covering all of them. A writer
is released only once a flush that began *after* its write has completed — so durability
is never weakened, only amortised.

### Partition placement: consistent hashing (`io.hermes.core.cluster`)

`ConsistentHashRing` places every broker on a 64-bit ring at 128 virtual points
(FNV-1a hash with a murmur-style finalizer — the finalizer matters, see the class
comment). `PartitionAssigner` maps `topic-partition` → the first N distinct
brokers clockwise: index 0 leads, the rest replicate. Because the ring is a pure
function of the configured membership, every broker computes identical
assignments with zero coordination — a produce arriving at any node is either
appended locally or forwarded straight to the leader over a socket.

### Simplified Raft (`io.hermes.core.raft`)

`RaftNode` implements terms, randomized election timeouts (1.5–3 s), majority
voting and leader heartbeats (400 ms). The elected leader acts as the **cluster
controller**: it owns topic metadata (piggybacked on heartbeats so followers
converge) and hosts the consumer-group coordinator. What's simplified relative to
full Raft: there is no replicated command log with commit indexes — data
replication is per-partition instead (below), and metadata sync is
last-writer-wins gossip from the leader.

### Replication (`io.hermes.core.replication`)

After the partition leader appends locally, `Replicator` pushes the record
synchronously to every alive follower in the replica set and counts acks. The
write is durable once a **majority of the replica set** (leader included) holds
it — the Raft quorum rule applied per partition. Followers apply records with
leader-assigned offsets (`appendAssigned`), which makes replication retries
idempotent. Under `acks=all`, a write that misses quorum **fails with 503** rather
than being silently acknowledged.

### Consumer groups (`io.hermes.core.group`)

`GroupCoordinator` lives on the controller; other brokers forward group frames to
it. Each `ConsumerGroup` tracks members in join order and a generation counter;
every membership change (join, leave, 10 s session expiry) bumps the generation
and re-runs **range assignment** — partitions split as evenly as possible, earlier
joiners taking the extras. Clients detect a changed generation on join/heartbeat
and pick up their new partitions. Committed offsets go through `OffsetStore`, an
append-only WAL (fsynced per commit) replayed on startup. Offset commits are
validated against the member's generation and ownership, so a zombie consumer that
missed a rebalance cannot clobber another member's progress.

### Wire protocol (`io.hermes.core.net`)

Every broker runs a `BrokerServer` (virtual-thread-per-connection, with a bounded
connection ceiling that sheds load) speaking framed binary over
`DataInput/OutputStream`: 1-byte opcode, string headers, optional record batch
(`MessageCodec`). ~20 frame types cover votes, heartbeats, replication and forwarded
client operations. `PeerClient` maintains a pool of persistent, auto-reconnecting
sockets per peer with strict request/response semantics.

### Request routing

Any broker can serve any client request:

| Operation | Handled by | Non-owner behavior |
|---|---|---|
| produce | partition leader | forward `PRODUCE` (with the resolved partition) to the leader |
| fetch | any replica of the partition | forward `FETCH` to leader |
| create topic | controller | ask controller for the authoritative partition count, then materialize |
| join / heartbeat / commit / lag | controller | forward to controller |

Two routing rules exist specifically to keep the cluster loop-free: forwarded
produces carry the partition the first broker resolved (the receiver appends
exactly there or errors — it never re-routes), and topic auto-creation always
takes the partition count from the controller, so no two brokers can ever mod
keys by different partition counts.

### Observability

`BrokerMetrics` aggregates totals (LongAdders), per-second throughput (a
60-bucket wheel averaged over the last 5 s) and produce latency percentiles (a
2048-sample ring buffer → p50/p99). Exposed per broker at `/api/metrics`,
topology at `/api/cluster`, group lag at `/api/lag`. The dashboard polls all
three brokers every 2 s and merges their views (end offsets are taken as the max
any replica reports).

## Repository layout

```
hermes-core/     broker engine — plain Java 21, zero runtime dependencies
hermes-rest/     Spring Boot REST layer (one process per broker node)
dashboard/       Next.js telemetry dashboard (port 3000)
clients/python/  stdlib-only client + end-to-end demo
benchmarks/      reproducible throughput/latency harness + results
scripts/         start/stop a local 3-node cluster
```

## Running locally

Prereqs: Java 21+, Maven 3.9+, Node 18+, Python 3.10+.

```bash
# 1. build (runs the full test suite)
mvn package

# 2. start the 3-node cluster (REST :8081-8083, sockets :9091-9093)
./scripts/start-cluster.sh

# 3. run the demo: creates topic "orders" (6 partitions), produces 1000
#    keyed messages, consumes them through consumer group "demo-consumers"
python3 clients/python/demo.py

# 4. dashboard
cd dashboard && npm install && npm run dev
# open http://localhost:3000

# 5. tear down
./scripts/stop-cluster.sh          # data survives in ./data; delete to reset
```

Watch the failure modes live: `kill -9 $(cat .logs/broker-3.pid)` mid-demo —
the dashboard flags the node, produces keep flowing to surviving partition
leaders, and after ~1.6 s a new controller term appears if the controller died.

### Reproducing the benchmarks

```bash
./scripts/start-cluster.sh
python3 benchmarks/sweep.py 20000 16     # batch-size sweep, both workloads
python3 benchmarks/bench.py --batch 1000 --concurrency 16 \
        --out benchmarks/results/b1000.json
```

`benchmarks/OPTIMIZATION.md` documents the full before/after analysis, including the
bugs that measurement uncovered.

### Single node

```bash
java -jar hermes-rest/target/hermes-rest-1.0.0.jar   # defaults: id 1, :8081/:9091
```

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

Configuration (env vars): `HERMES_BROKER_ID`, `HERMES_REST_PORT`,
`HERMES_MEMBERS` (`id@host:socketPort,...`), `HERMES_DATA_DIR`,
`HERMES_REPLICATION_FACTOR` (2), `HERMES_DEFAULT_PARTITIONS` (3),
`HERMES_SEGMENT_BYTES` (1 MiB), `HERMES_CLUSTER_SECRET` (empty = wire auth off),
`HERMES_API_KEY` (empty = REST auth off; sent as `X-API-Key`),
`HERMES_CORS_ORIGINS` (`*`), `HERMES_ACKS` (`all` = quorum-durable, default;
`leader` = ack on local append only), `HERMES_GROUP_COMMIT` (`false`),
`HERMES_LINGER_MS` (`0`), `HERMES_BATCH_MAX_RECORDS` (`10000`).

**Durability (`acks=all`, default):** a produce returns success only once a
majority of the partition's replica set holds the record. If quorum cannot be
reached (e.g. a follower is down), the REST layer returns **503** and the client
must retry — the write is never silently accepted as durable. Set `acks=leader`
for lower-latency, weaker guarantees.

### Docker

```bash
docker compose up --build   # 3 brokers (REST :8081-8083) + dashboard (:3000)
```

Each broker container exposes `/api/health` as its Docker healthcheck; data
lives on named volumes. Set `HERMES_CLUSTER_SECRET` in the environment to
change the compose default.

### Security model

- **Broker-to-broker**: when `HERMES_CLUSTER_SECRET` is set, every wire frame
  must carry the shared secret (constant-time compared); unauthenticated
  frames are rejected before dispatch.
- **REST**: optional `HERMES_API_KEY` gates every endpoint except
  `/api/health`. CORS origins are configurable. Input is validated at the
  broker: topic/group names restricted to `[A-Za-z0-9._-]`, values ≤ 60 KB,
  fetches clamped by both record count and an 8 MB byte budget, partition
  indexes bounds-checked.
- Not provided (front with a real gateway if exposed publicly): TLS,
  per-client identity, rate limiting.

## Testing

`mvn verify` runs the full suite (**118 test methods**) and produces a JaCoCo
coverage report. Measured coverage: **hermes-core 89% instructions / 79%
branches, hermes-rest 95% / 82%**.

```bash
mvn verify                                       # run all tests + coverage
open hermes-core/target/site/jacoco/index.html   # core coverage report
open hermes-rest/target/site/jacoco/index.html   # REST coverage report
mvn -pl hermes-rest test -Dtest=ProduceControllerTest   # run one test class
```

**Testing strategy** — tests are matched to what each layer is responsible for:

| Layer | Approach | Examples |
|---|---|---|
| Storage / WAL | Unit tests on real temp files, incl. crash/torn-write recovery | `PartitionLogTest`, `RecordSerdeTest`, `OffsetStoreTest` |
| Concurrency | Multi-threaded stress with correctness + coalescing assertions | `GroupCommitTest`, `ConnectionCapTest` |
| Pure logic | Fast deterministic unit tests | `ConsistentHashRingTest`, `PartitionAssignerTest`, `ClusterStateTest`, `BrokerConfigTest`, `NamesTest` |
| Protocol | Round-trip + fuzz (50k random frames) | `MessageCodecTest`, `FuzzAndAuthTest` |
| REST controllers | `@WebMvcTest` slice tests with the engine **mocked** (`@MockitoBean`) — validation, error→status mapping, edge/large inputs, per endpoint | `*ControllerTest` |
| Security / auth | Isolated filter unit tests | `ApiKeyFilterTest` |
| Cluster | Full 3-broker integration (election → produce → replication → fetch → groups → lag → durability under crash) | `ClusterIntegrationTest`, `DurabilityQuorumTest`, `BatchProduceIntegrationTest`, `RestApiIntegrationTest` |

Controller tests mock the broker so no disk, socket, or cluster is touched — they
assert HTTP behaviour in isolation and run in milliseconds. There are **no real
network calls** in the test suite.

### CI

GitHub Actions (`.github/workflows/ci.yml`) runs on every push/PR: builds and
tests the broker on JDK 21, fails the build on any test failure, writes a
coverage summary to the run, and uploads the JaCoCo reports, test reports, and
the broker jar as artifacts. Separate jobs build the Next.js dashboard and
compile-check the Python client.

## Design decisions & known simplifications

Deliberate trade-offs to keep the system understandable end to end:

- **Static membership.** The cluster is defined by `HERMES_MEMBERS`; the hash
  ring never moves, so there is no partition reassignment/data migration. Broker
  death is handled by liveness-aware replication and Raft re-election, not by
  re-placing partitions.
- **No data-plane leader failover.** Partitions *led* by a crashed broker reject
  writes until it returns (measured directly during chaos testing). Raft re-elects
  the *controller* in ~1.6 s, but partition leadership follows the static ring.
- **No follower catch-up.** A broker that was offline is not back-filled with the
  writes it missed; replication is push-once. New writes become durable again as
  soon as it rejoins.
- **Metadata gossip instead of a replicated Raft log.** Topic creation converges
  via leader heartbeats; group *offsets* are durable on the controller's disk,
  but group *membership* resets if the controller changes (consumers transparently
  rejoin). Raft `term`/`votedFor` are held in memory, not persisted.
- **Fetches are served by any replica** with no high-watermark tracking, so a
  reader hitting a lagging follower can briefly see fewer messages than the leader.
- **`writeUTF` framing** caps individual values at ~60 KB; topic and group names
  are restricted to `[A-Za-z0-9._-]`.
- **fsync on every append** (amortised by batching and group commit). This is
  *stronger* per-message durability than Kafka's default, and is the primary reason
  single-message throughput is in the thousands rather than the millions.

### Roadmap

The two changes that would move this from "correct" to "highly available":

1. **Follower catch-up** via pull-based replication — lets a recovered broker
   self-heal, and is a prerequisite for (2).
2. **Data-plane leader failover** — promote an in-sync replica when a partition
   leader dies, instead of rejecting writes until it returns.

Then: persisted Raft state, batched replication frames, log retention/compaction,
and a binary client protocol replacing JSON/HTTP.

Each class in `hermes-core` carries one responsibility and the package
boundaries (`storage`, `cluster`, `net`, `raft`, `replication`, `group`,
`metrics`, `topic`, `broker`) match the architecture sections above — the code
is meant to be read.
