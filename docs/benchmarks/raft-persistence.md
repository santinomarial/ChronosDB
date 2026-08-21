# Raft Persistent-Log Benchmark Harness

> **Status: implemented measurement harness; no results are published here.**
> `chronos-raftbench` measures production `RaftPersistentLog` batched append and node-local
> synchronization paths. Its smoke test proves execution, artifact emission, and exact reopen; that
> is not performance evidence and does not close the Phase 14 measurement gate.

## Scope and durability boundary

Each repetition creates one fresh segmented physical log and deterministically alternates complete
Raft group states across a declared number of logical groups. A batch contains consecutive
full-state records with one retained application entry per record. The harness measures two
separate modes:

- `APPEND_ONLY` ends each sample after every record has completed its operating-system write. It
  makes no durable acknowledgment and excludes the final cleanup synchronization from timing.
- `LOCAL_SYNC` ends each sample only after one production `synchronize()` covers the complete
  batch. This is a node-local data-synchronization boundary, not quorum commit or client
acknowledgment latency.

Production segment rotation remains inside `append`: it synchronizes the predecessor and durably
installs a successor before accepting the next record. An APPEND_ONLY case that rotates can
therefore include that production synchronization cost and leave part of its history durable. The
harness reports segment count but does not separately instrument rotation-internal sync calls.

The workload does not run the deterministic Raft core, transport, replication, application,
snapshot transfer, catch-up, or a client protocol. It isolates the physical full-state log and
therefore cannot be compared with commit latency. Immediate reopen does not qualify power-loss
behavior, device caches, controller firmware, hypervisors, filesystems, or network storage.

## Safe invocation

The recommended wrapper creates a dedicated Release build and captures review artifacts:

```sh
scripts/benchmark-raft.sh /absolute/path/outside/chronosdb/raft-log-run \
  --mode LOCAL_SYNC \
  --operations 10000 \
  --warmup-operations 1000 \
  --repetitions 3 \
  --batch-records 16 \
  --payload-bytes 256 \
  --logical-groups 8
```

The output path must be new, beneath an existing parent, and outside the source checkout. The
wrapper refuses a dirty tree. `CHRONOS_BENCHMARK_ALLOW_DIRTY=1` permits tracked changes only and
retains `source.diff`; untracked files remain a hard error because they cannot be represented by a
complete diff.

Direct controlled invocation is also available:

```sh
cmake --preset raft-benchmark
cmake --build --preset raft-benchmark --target chronos-raftbench
build/raft-benchmark/chronos-raftbench \
  --output-dir /new/path --mode LOCAL_SYNC
```

The executable refuses build-time dirty metadata and non-Release builds by default.
`--allow-dirty` and `--allow-non-release` exist for explicit smoke automation, not publishable
measurements.

## Workload and procedure

The CLI exposes measured and warm-up record counts, repetition count, batch size, deterministic
seed, application payload bytes, logical-group count, target segment size, and a conservative total
tool-artifact-size cap; wrapper-added reproducibility captures are outside that cap. Warm-up records
use the same fresh log immediately before measurement and an excluded cleanup sync establishes a
known boundary. Measured batches are closed-loop and sequential. The harness performs no cache
drop, affinity, tuning, cooldown, random run ordering, or post-observation outlier removal.

Every record has a node-global physical sequence and a deterministic group UUID. The selected
group receives a deterministic full-state image whose one entry payload varies with global physical
sequence while its logical term/index remain fixed. Consequently the benchmark includes complete
full-state encoding and checksumming rather than timing a synthetic `fsync` on unrelated bytes.
Operations and warm-up operations must be divisible by the batch size; logical groups are bounded
to 255; segment, codec, address-space, and artifact budgets are checked before generating run state.

## Correctness gate

A repetition is valid only when:

1. every append returns its exact expected physical sequence;
2. every `LOCAL_SYNC` batch advances the durable frontier through its final record;
3. closing and reopening with production recovery accepts every record without repair;
4. recovered record count, physical frontier, and durable frontier exactly equal the generated
   history; and
5. the recovered latest full state for every logical group exactly equals its generated state.

Any error invalidates the run. `manifest.json` begins with `validation_status: pending` and changes
to `passed` only after every repetition and artifact write succeeds. For `LOCAL_SYNC`, successful
immediate-reopen reconciliation reports zero acknowledged-write loss. For `APPEND_ONLY`, the field
is `null` because that mode makes no durable acknowledgment.

## Artifacts and interpretation

The executable writes:

- `manifest.json`, containing exact argv, source/build identity, workload, durability boundary,
  host discovery, procedure, correctness status, and limitations;
- `raw-latencies.csv`, containing every measured batch and its exact physical-sequence interval;
- `summary.json`, containing per-repetition elapsed/recovery time, record throughput, nearest-rank
  batch latency percentiles, physical bytes/segments, measured explicit `synchronize()` calls, CPU
  time, peak RSS, and Linux process I/O bytes when available; and
- `repetition-N/raft-log/`, the exact verified segmented log.

The wrapper additionally captures stdout/stderr, shell-escaped invocation, Git HEAD/status and
dirty diff when applicable, CMake cache, compile commands, an allowlisted environment snapshot,
and best-effort system/storage/mount inventory. Unknown fields remain unknown.

Percentiles use nearest rank over successful closed-loop batch samples within each repetition;
p99.9 is emitted only for at least 1,000 samples. Throughput is measured records divided by the
measured wall-clock interval. Recovery time and physical bytes cover warm-up plus measured records.
Do not publish a number without retaining and reviewing the complete artifact directory under the
[benchmark publication contract](benchmark-contract.md).
