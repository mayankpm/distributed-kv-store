# Distributed Key-Value Store

A sharded, replicated, linearizable key-value store written from scratch with no third-party dependencies.
Each shard is a Raft group; each replica persists its data in its own LSM-tree storage engine; keys are spread across
shards with consistent hashing. The test suite injects crashes, network partitions and lossy networks, then verifies
every recorded client history with a linearizability checker.

```
                client (kvcli / KvClient)
                  |  key -> partition -> leader (cached, retried on failure)
                  v
  +-----------------------------  kvnode  ------------------------------+
  |  partition 0 replica      partition 3 replica      partition 7 ...  |
  |  +-------------------+    +-------------------+                     |
  |  | Raft (log, votes) |<-->| Raft peers on the other replica nodes   |
  |  | KV state machine  |    +-------------------+                     |
  |  | LSM tree          |                                              |
  |  +-------------------+                                              |
  +----------------------------------------------------------------------+
```

## What is implemented

**Raft consensus** ([src/raft](src/raft))
* Leader election with randomized timeouts and the log up-to-date check.
* Heartbeat-based failure detection in both directions: followers start an election when heartbeats stop, and a leader
  steps down when it has not heard from a quorum for an election timeout (check-quorum).
* Append-only log replication with pipelining and the conflict-term fast backup optimization.
* Commit only by counting replicas for current-term entries (Figure 8 safety), with a no-op appended on election.
* Linearizable reads via ReadIndex: the leader confirms leadership with a quorum heartbeat round instead of writing reads
  to the log.
* Durable term, vote and log with torn-write recovery.

**LSM-tree storage engine** ([src/storage](src/storage)), modeled on LevelDB
* Write-ahead log with per-record CRC32; replay stops cleanly at a torn tail. Multi-key write batches are atomic.
* Memtable, frozen and flushed by a background thread to level-0 SSTables.
* SSTables with 4 KB checksummed data blocks, an in-memory index and a per-table bloom filter (10 bits/key, about 1%
  false positives).
* Leveled compaction: L0 to L1 by file count, deeper levels by byte budget (10x per level), round-robin key ranges,
  trivial moves, and tombstone removal once no deeper level can hold an older value.
* LRU block cache, MANIFEST-based crash recovery, and obsolete files deleted only after in-flight readers release them.

**Sharding and replication** ([src/kv](src/kv))
* Keys hash to a fixed number of partitions. Each partition's replica set is chosen by consistent hashing with virtual
  nodes, so adding a node moves about 1/N of the placement instead of reshuffling everything.
* Configurable replication factor. Writes commit on a majority of the partition's replicas; reads are confirmed by a
  majority (ReadIndex).
* Exactly-once writes: clients tag writes with (client id, sequence number), and the state machine persists the latest
  sequence per client in the same atomic batch as the write and the applied log index.

**Networking** ([src/net](src/net))
* TCP transport (Winsock and POSIX) with a reader thread and a writer queue per connection, plus reconnecting dialers,
  so Raft never blocks on the network while holding its lock.
* An in-process simulated network used by the tests to drop, delay, reorder and partition messages and to crash nodes.

## Testing

`kvtests` has 42 tests. The fault-injection tests run 5 concurrent clients against a 5-node, 3-partition, 3x-replicated
cluster while a nemesis thread repeatedly partitions the network into random halves, crashes nodes and restarts them from
disk, and makes the network drop 10% of messages and delay the rest by up to 25 ms. Every history is then checked by
[a linearizability checker](src/check/linearizability.cpp) (the Wing & Gong search with memoization used by Porcupine
and Knossos).

| Scenario | Operations checked per run | Result |
|---|---|---|
| No faults | 80,000 to 110,000 | linearizable |
| Random network partitions | 85,000 to 125,000 | linearizable |
| Crash and restart from disk | 60,000 to 70,000 | linearizable |
| Lossy, reordering network | about 300 | linearizable |
| Partitions + crashes + lossy network | about 200 | linearizable |
| Partitions + crashes, clients that act on the server's first answer | 30,000 to 60,000 | linearizable |

The lossy-network runs complete fewer operations because every dropped request or reply costs a client timeout.

Other suites cover Raft on its own (elections, re-election, agreement with failures, divergent log repair,
persistence, check-quorum, ReadIndex, and a randomized "Figure 8" test on an unreliable network, all cross-checking state
machine safety on every apply), the storage engine (randomized comparison against `std::map` through many flushes and
multi-level compactions, crash recovery, torn writes, corrupted blocks), consistent hashing, the checker itself, and a
TCP cluster that survives a node restart.

**The tests catch real bugs.** Three deliberately planted bugs were each caught:

| Planted bug | Caught by |
|---|---|
| Disable duplicate detection for retried writes | `retried_writes_apply_exactly_once` and the lossy-network linearizability test, every run |
| Acknowledge a write even though a new leader overwrote its log slot | `write_is_acknowledged_only_if_committed_in_its_term`, every run |
| Serve reads on the leader without confirming leadership (no ReadIndex) | the partition linearizability test, about 1 run in 3 |

## Benchmarks

AMD Ryzen 9 5900HS (8 cores), SSD, Windows 11, MinGW GCC 16 with `-O2`. Numbers are from single runs on a laptop,
so treat them as indicative.

**Storage engine** (`kvbench lsm`: 200,000 entries, 100-byte values, 4 MB memtable, 8 MB block cache, no fsync)

| Workload | Throughput | Latency |
|---|---|---|
| Sequential writes | 183,000 ops/s | 5.5 us |
| Random writes | 175,000 ops/s | 5.7 us |
| Random reads, 24 MB working set | 41,000 ops/s | 24.5 us |
| Random reads, hot 1% of keys (block cache) | 114,000 ops/s | 8.8 us |
| Lookups of absent keys, bloom filters on | 601,000 ops/s | 1.7 us |
| Lookups of absent keys, bloom filters off | 35,000 ops/s | 28.7 us |

Bloom filters skip 99% of table probes for absent keys (about 17x faster lookups). Compaction wrote 1.9x the user bytes.

**Cluster** (`kvbench cluster`: all nodes in one process over TCP loopback, 3x replication, 100-byte values, no fsync)

| Setup | Operation | Throughput | p50 | p99 |
|---|---|---|---|---|
| 3 nodes, 3 partitions, 16 clients | put | 36,700 ops/s | 0.41 ms | 0.84 ms |
| 3 nodes, 3 partitions, 16 clients | get | 51,200 ops/s | 0.31 ms | 0.53 ms |
| 5 nodes, 12 partitions, 32 clients | put | 29,100 ops/s | 0.55 ms | 6.64 ms |
| 5 nodes, 12 partitions, 32 clients | get | 49,400 ops/s | 0.30 ms | 5.15 ms |

By default writes survive process crashes (data is flushed to the OS before being acknowledged) but not power loss.
Run `kvnode --sync` to fsync the Raft log and WAL on every write.

## Build and run

Requires CMake 3.20+ and a C++20 compiler (GCC 11+, Clang 14+, or MSVC 2022).

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
./build/kvtests                  # all tests (about 1.5 minutes)
./build/kvtests raft. storage.   # or filter by name
```

Start a local 3-node cluster and talk to it:

```sh
scripts/run_local_cluster.sh examples/cluster3.conf        # Windows: scripts\run_local_cluster.ps1
./build/kvcli --config examples/cluster3.conf put hello world
./build/kvcli --config examples/cluster3.conf get hello
./build/kvcli --config examples/cluster3.conf              # interactive: get/put/append/del
```

Kill any one node and the cluster keeps serving. Restart it with the same `--id` and `--data` and it recovers from disk
and catches up. A cluster is described by a small config file:

```
partitions 6
replication 3
vnodes 64
node 1 127.0.0.1:7001
node 2 127.0.0.1:7002
node 3 127.0.0.1:7003
```

## Layout

```
src/common    encoding, CRC32, hashing, file and timer helpers
src/storage   WAL, memtable, SSTable, bloom filter, block cache, LSM tree
src/raft      Raft messages, durable log, consensus
src/net       simulated network, sockets, TCP transport
src/kv        consistent hashing, placement, state machine, replica, node, client
src/check     linearizability checker
apps          kvnode (server), kvcli (client), kvbench (benchmarks)
tests         unit, fault-injection and integration tests
```

## Limitations

* No Raft log compaction or snapshots: each Raft log grows without bound, and a replica that falls far behind catches up
  by replaying the log. The LSM tree already holds the applied state, so snapshots would ship its files.
* Membership and placement are static. Consistent hashing keeps the placement change small when a node is added, but
  there is no live partition migration or Raft membership change yet.
* Point operations only: no range scans or transactions across keys.
* Windows note: MinGW's timed waits round up to the 15.6 ms scheduler tick, so timer loops use a 1 ms `Sleep` instead
  (see [src/common/platform.h](src/common/platform.h)).
