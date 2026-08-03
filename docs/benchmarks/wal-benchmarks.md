# WAL Benchmark Harness

> **Status: implemented measurement harness; no results are published here.** `chronos-walbench`
> measures the production WAL append, acknowledgment, group-commit, rotation, shutdown, and recovery
> paths. A smoke run proves that the harness executes and emits its artifacts; it is not performance
> evidence.

## Scope and non-scope

The harness creates a fresh WAL for each repetition with `WalWriter::create_new`, submits one
application entry per operation through `WalCommitCoordinator`, waits for the requested `ASYNC` or
`LOCAL_SYNC` completion, shuts the coordinator down, and verifies the complete image with
`recover_wal`. It measures the current physical WAL subsystem only. It does not model schemas,
tables, logical batches, checkpoints, WAL deletion, replication, queries, or `QUORUM_SYNC`.

`ASYNC` and `LOCAL_SYNC` are separate workloads. `ASYNC` completion establishes complete write, not
durability. `LOCAL_SYNC` completion is released only after the production synchronization frontier
covers the record. A graceful recovery check does not turn `ASYNC` into a durable mode.

This harness does not qualify physical power-loss behavior, device firmware, controller caches,
filesystem bugs, hypervisor failures, or network filesystems. Use the subprocess
[crash harness](../testing/wal-crash-harness.md) for process-termination evidence and a separately
designed qualified-device campaign for stronger storage-stack claims.

## Safe invocation

The recommended wrapper builds a dedicated optimized preset and captures the reproducibility
artifacts required by the [benchmark publication contract](benchmark-contract.md):

```sh
scripts/benchmark-wal.sh /absolute/path/outside/chronosdb/wal-run \
  --mode LOCAL_SYNC \
  --operations 10000 \
  --warmup-operations 1000 \
  --repetitions 3 \
  --producers 4 \
  --payload-bytes 256
```

The output path must not exist, its parent must exist, and the wrapper requires it to be outside the
source checkout. The tool never deletes or overwrites a result directory. The wrapper refuses a
dirty tree by default. `CHRONOS_BENCHMARK_ALLOW_DIRTY=1` permits tracked modifications only and
retains `source.diff`; untracked files remain a hard error because a complete source diff could not
be recorded safely.

Direct invocation is supported for controlled automation:

```sh
cmake --preset wal-benchmark
cmake --build --preset wal-benchmark --target chronos-walbench
build/wal-benchmark/chronos-walbench --output-dir /new/path --mode LOCAL_SYNC
```

By default the executable refuses build-time dirty metadata and a build type other than `Release`.
`--allow-dirty` and `--allow-non-release` exist for explicit automation and smoke testing, not to
make such runs publication-quality.

## Workload controls

The full CLI is available with `chronos-walbench --help`. Important controls are:

- `--mode ASYNC|LOCAL_SYNC`;
- measured operations, warm-up operations, repetitions, and producer count;
- payload size, deterministic generator seed, and target segment size;
- bounded coordinator request/byte admission limits;
- group-sync request, byte, and maximum-delay limits; and
- a conservative maximum aggregate artifact size across all repetitions.

The generator uses a test-only application envelope, an explicit little-endian request identity,
and deterministic xorshift64* bytes. The minimum payload is 24 bytes. This does not allocate an
application kind for a future database engine. Producers are closed-loop, with at most one
outstanding request per producer. Latency is therefore end-to-end closed-loop acknowledgment
latency, not an open-loop or coordinated-omission-corrected service distribution.

Warm-up requests use the same fresh WAL immediately before measurement and remain part of the image
recovered at the end. They are excluded from latency samples and measured coordinator metric deltas.
Every repetition creates its own WAL identity and directory. Run order is sequential, cache state is
not controlled, and no cooldown or post-observation outlier removal is performed.

## Correctness gate

A repetition is valid only if all of the following hold:

1. every requested completion reports the exact requested/effective durability mode;
2. each `LOCAL_SYNC` completion carries a synchronization frontier and durable record sequence that
   cover its append;
3. measured completion sequences form the exact expected global interval with no gaps or duplicates;
4. coordinator metrics report every measured operation admitted, appended, and acknowledged in the
   selected mode, with zero rejections, failures, or failed synchronizations;
5. production recovery accepts the complete physical history without repair; and
6. recovery preflight and replay observe every warm-up and measured request identity exactly once in
   global record-sequence order.

Any error invalidates the run. The initial manifest says `validation_status: pending` and reports no
loss count. Only after all repetitions and artifact writes succeed is it rewritten with
`validation_status: passed` and an acknowledged-write loss count of zero. An interrupted or failed
run retains pending metadata and, when available, `failure.txt`.

## Artifacts and metrics

The executable writes:

- `manifest.json`: scenario version, exact argv, source/build identity, durability and coordinator
  configuration, workload, host/environment discovery, procedure, correctness gate, and explicit
  limitations;
- `raw-latencies.csv`: every measured operation's repetition, ordinal, request identity, latency,
  admission sequence, record sequence, and requested/effective mode;
- `summary.json`: per-repetition elapsed and recovery time, throughput, nearest-rank p50/p95/p99,
  p99.9 when at least 1,000 samples exist, WAL bytes/segments, sync/group counts, error counts,
  user/system CPU time, peak RSS, and Linux process I/O bytes when available; and
- `repetition-N/wal/`: the exact verified WAL image.

The wrapper additionally retains stdout/stderr, exact shell-escaped invocation, Git HEAD/status and
dirty diff when applicable, CMake cache, compile commands, an allowlisted environment capture, and
best-effort system/storage/mount inventory. Unknown host fields remain explicitly unknown; they are
not guessed.

Throughput is measured operations divided by wall-clock measured-phase duration. Percentiles use
nearest rank over successful per-operation samples within each repetition and are never averaged
across repetitions. The reported WAL byte count and recovery time include warm-up records because
recovery verifies the complete image. Device I/O operation counts, allocator counts, steady-state
RSS, frequency/power state, and authoritative storage power-loss-protection properties require
external instrumentation or operator evidence.

## Publication rule

Do not copy a number from a local run into project prose. Preserve the complete result directory,
review every repetition, explain unknown metadata and environmental controls, and apply the
[comparison rules](benchmark-contract.md#comparison-rules). The harness makes measurements
reproducible enough to review; it does not make a particular host stable or a comparison valid by
itself.
