# Hermes Throughput Optimization — Measured Results

All numbers from `benchmarks/bench.py` / `sweep.py` against a real cluster on one
machine (3 JVMs sharing one disk). Durability unchanged: `acks=all` still fsyncs on
the leader and replicates to a quorum before acking. 57 tests pass.

## Baseline
Single-message produce, RF=2, spread keys: **~240–380 msg/s**, p99 ~350 ms.
Profiling (JFR) attributed cost to: HTTP-per-message, fsync-per-message, sync replication.

## Task 2 — Produce batching (one WAL write + one fsync + one replication frame per batch)

20,000 msgs, concurrency 16, RF=2, acks=all:

| batch | spread (null keys, 24 partitions) | keyed (records share a partition) |
|------:|----------------------------------:|----------------------------------:|
| 1     | 379 msg/s                         | 180 msg/s                         |
| 10    | 286 msg/s  (worse than 1)         | 1,146 msg/s                       |
| 100   | 626 msg/s                         | 10,079 msg/s                      |
| 1000  | 5,095 msg/s                       | **70,867 msg/s**                  |

**Honest finding:** batching's win is entirely fsync/replication/HTTP amortization,
which requires records landing in the **same partition**. With null keys round-robined
across 24 partitions, a batch of N spreads across min(N,24) partitions, so small batches
don't amortize — **batch=10 is worse than batch=1** (the request waits on the slowest of
10 sub-batches). Only batch ≫ partition_count helps the spread case. With keyed records
concentrating into one partition, batching delivers the expected win:
**batch=1000 → 70,867 msg/s, a 187× gain over batch=1**, inside the 10k–100k target range.

Fix applied mid-measurement: per-partition sub-batches are now dispatched concurrently
(was sequential), which lifted the spread case (batch=1000: 3,883 → 5,095).

## Task 3 — Group commit (coalesce fsyncs across concurrent single-message producers)

Hot single-partition topic, 32 concurrent single-message producers:

| config              | RF=1 (leader fsync only) | RF=2 (with replication) |
|---------------------|-------------------------:|------------------------:|
| group commit OFF    | 342 msg/s (p50 92 ms)    | 180 msg/s (p50 120 ms)  |
| group commit ON     | **2,127 msg/s (p50 14 ms)** | **694 msg/s (p50 39 ms)** |
| improvement         | **6.2×**                 | **3.9×**                |

Storage-layer proof (`GroupCommitTest`): 4,000 concurrent appends to one partition →
501 fsyncs = **8.0× coalescing**, all offsets unique/contiguous/durable.

**Bug found by measurement:** group commit initially showed **zero** end-to-end effect.
Investigation (server-side vs client-side latency were identical at 90 ms → not HTTP;
RF=1 also flat → not replication) revealed the real cause: Task 2 routed single produces
through `appendBatchAsLeader → PartitionLog.appendBatch()`, which did its own direct
flush and never called the group-commit path. Group commit was **dead code on the hot
path**. Fixing `appendBatch` to participate in `groupFlush` produced the 6.2×/3.9× above.

**Why RF=2 gains less than RF=1:** replication is still per-message and synchronous, so
the follower round-trip + follower fsync remain a serialized residual cost that group
commit (leader-side only) does not coalesce. Batching the replication frames — as Task 2
already does for client batches — is the remaining lever for the concurrent-single-producer
case.

## Correctness argument for group commit (defensible)

`force()` makes durable every byte written to the channel before it was called. Under the
write monitor, each write appends its bytes and then assigns `mySeq = ++writeSeq`; writes
are serialized, so all writes with seq ≤ S have their bytes in the channel once seq S is
assigned. A single flusher captures `target = writeSeq` under the monitor immediately
before `force()`, forces the active segment (earlier segments were fsynced at roll time),
then sets `flushedSeq = target`. A waiter with `mySeq` is released only when
`flushedSeq ≥ mySeq` — i.e. by a force that began strictly after its write completed — so
its bytes are guaranteed durable. Over-covering (bytes with seq > target racing in before
the force) is harmless. Verified under 4,000-way concurrency in `GroupCommitTest`.

## Reproduce
```bash
./scripts/start-cluster.sh                 # or with HERMES_GROUP_COMMIT=true HERMES_LINGER_MS=2
python3 benchmarks/sweep.py 20000 16       # batch sweep, both workloads
python3 benchmarks/bench.py --batch 1000 --concurrency 16 --out benchmarks/results/b1000.json
```
