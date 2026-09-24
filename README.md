# Hermes

A distributed message broker built from first principles in Java — topics, partitions,
append-only log segments, consistent-hash partition placement, simplified Raft leader
election, majority-ack replication, consumer groups with rebalancing, a Spring Boot
REST layer, a Next.js telemetry dashboard, and a stdlib-only Python client.

No Kafka libraries. Broker-to-broker traffic runs over raw Java sockets with a
hand-rolled binary protocol.

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

## Architecture

### Storage: write-ahead log segments (`io.hermes.core.storage`)

Each partition is a `PartitionLog`: an ordered chain of append-only `LogSegment`
files (`data/broker-N/<topic>-<partition>/00000000000000000000.log`). Records are
binary-encoded by `RecordSerde` (offset, timestamp, key, value), appended
sequentially, flushed per append, and never mutated. When the active segment
exceeds `hermes.segment-bytes` (1 MiB default) it rolls to a new file named by its
base offset. On startup segments are re-scanned to rebuild an in-memory
`SparseIndex` (every 64th record → byte position), which also truncates torn
trailing writes for free — reads seek to the nearest indexed position and scan
forward.

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
idempotent. Writes that miss quorum are surfaced in metrics as
`underReplicatedWrites`.

### Consumer groups (`io.hermes.core.group`)

`GroupCoordinator` lives on the controller; other brokers forward group frames to
it. Each `ConsumerGroup` tracks members in join order and a generation counter;
every membership change (join, leave, 10 s session expiry) bumps the generation
and re-runs **range assignment** — partitions split as evenly as possible, earlier
joiners taking the extras. Clients detect a changed generation on join/heartbeat
and pick up their new partitions. Committed offsets go through `OffsetStore`, an
append-only text WAL replayed on startup.

### Wire protocol (`io.hermes.core.net`)

Every broker runs a `BrokerServer` (virtual-thread-per-connection) speaking
length-free framed binary over `DataInput/OutputStream`: 1-byte opcode, string
headers, optional record batch (`MessageCodec`). ~20 frame types cover votes,
heartbeats, replication and forwarded client operations. `PeerClient` keeps one
persistent, auto-reconnecting socket per peer with strict request/response
semantics.

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
scripts/         start/stop a local 3-node cluster
```

## Running locally

Prereqs: Java 21+, Maven 3.9+, Node 18+, Python 3.10+.

```bash
# 1. build (runs the core test suite)
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
leaders, and after ~2 s a new controller term appears if the controller died.

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
`leader` = ack on local append only).

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
  fetches clamped to 10k records, partition indexes bounds-checked.
- Not provided (front with a real gateway if exposed publicly): TLS,
  per-client identity, rate limiting.

### Testing

`mvn verify` runs the full suite (**118 test methods**) and produces a JaCoCo
coverage report. Measured coverage: **hermes-core 89% instructions / 79%
branches, hermes-rest 95% / 82%**.

```bash
mvn verify                                   # run all tests + coverage
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
- **Metadata gossip instead of a replicated Raft log.** Topic creation converges
  via leader heartbeats; group *offsets* are durable on the controller's disk,
  but group *membership* resets if the controller changes (consumers transparently
  rejoin).
- **Fetches are served by any replica** and replication acks on receipt, so a
  reader hitting a lagging follower can briefly see fewer messages than the
  leader has (no high-watermark tracking).
- **writeUTF framing** caps individual keys/values at 64 KiB; topic and group
  names must avoid `, ; : @` characters.
- **Flush-per-append, fsync on segment roll** — a middle ground between
  throughput and durability; tune in `PartitionLog.appendAssigned` if you want
  `force()` per write.

Each class in `hermes-core` carries one responsibility and the package
boundaries (`storage`, `cluster`, `net`, `raft`, `replication`, `group`,
`metrics`, `topic`, `broker`) match the architecture sections above — the code
is meant to be read.
