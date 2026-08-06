# Manifest Flush Benchmark Harness

> **Status: implemented measurement harness; no results are published here.**
> `chronos-flushbench` measures the production sealed-head flush coordinator, immutable CSEG and
> Manifest installation, aggregate publication, concurrent snapshot acquisition, and repeated
> Manifest/WAL recovery. Its smoke run proves artifact and correctness behavior, not performance.

## Scope

Each repetition creates a fresh single-tablet database and synchronized WAL. Fixed-size
`TIMESTAMP_NS` batches fill successive mutable generations. One additional append rotates each full
generation into the bounded production flush queue. The single storage owner then invokes
`SealedHeadFlushCoordinator`, which performs the real sort/encode, part installation, Manifest
construction and installation, aggregate publication, tablet retirement, and queue completion.

The workload uses unique contiguous timestamps in descending order within each input batch, so
conversion does real ordering work. It records raw and Zstandard policies separately. There are no
duplicates, corrections, tombstones, SQL queries, networking, compaction, or replication. The
foreground workload is closed-loop acquisition and validation of the aggregate immutable database
snapshot while flushes run; it is a publication-interference probe, not a query benchmark.

## Safe invocation

Use the wrapper with a new output path outside the source checkout:

```sh
scripts/benchmark-flush.sh /absolute/path/outside/chronosdb/flush-run \
  --flushes 32 \
  --warmup-flushes 4 \
  --repetitions 3 \
  --rows-per-head 65536 \
  --snapshot-readers 4 \
  --baseline-snapshots 10000 \
  --compression ZSTD
```

The wrapper configures the dedicated `flush-benchmark` Release preset, builds only the harness,
refuses to overwrite an output path, rejects an untracked or dirty tree by default, and captures
the invocation, Git identity, CMake cache, compile commands, allowlisted environment, and best-effort
host/storage inventory. `CHRONOS_BENCHMARK_ALLOW_DIRTY=1` permits tracked modifications and retains
`source.diff`; it never permits untracked source because a complete diff could not be preserved.

Direct invocation is available to controlled automation. `--allow-dirty` and
`--allow-non-release` are explicit smoke-test overrides, not publication-quality settings.

## Procedure and metrics

Warmup flushes run in the same fresh database before measurement and remain in its durable image.
After warmup, the configured reader threads each acquire a fixed baseline sample count. Equivalent
threads then acquire snapshots continuously during the measured flush interval. Readers validate
that every observed epoch owns one complete allowed head boundary. All retained samples are written;
the configured total per-repetition bound is enforced before filesystem mutation. A deterministic
per-reader reservoir retains observations across the complete phase, and non-retained observations
are counted as dropped rather than growing memory without limit.

The summary reports:

- durable flush latency percentiles, rows per second, and encoded part bytes;
- baseline and during-flush aggregate-snapshot latency distributions and operation counts;
- selected Manifest size, retained Manifest-directory growth, part bytes, and total durable bytes;
- exact file-sync and directory-sync counts plus syncs per measured flush;
- largest successful single candidate image and its ratio to one head's logical value bytes;
- retained durable bytes divided by all warmup and measured flushed logical value bytes;
- first and repeated selected-Manifest open/validation time;
- first and repeated retained-WAL suffix verification/decode/replay time; and
- user/system CPU, peak RSS, and Linux process I/O bytes where the host exposes them.

The temporary metric is deterministic for the current one-at-a-time protocol: part and Manifest
candidates never coexist, so the maximum candidate image is the exact maximum temporary-file bytes
owned by one successful operation. It is not a sampled filesystem-block or device-write metric.
Durable space includes all immutable generations because Phase 6 deliberately implements no
Manifest or part reclamation.

## Correctness gate and artifacts

A repetition is valid only when every requested flush completes and:

1. component metrics agree on the exact completed part and Manifest counts;
2. the queue is empty and no sealed generation remains in tablet state;
3. the aggregate publication selects the exact generation, part/retry counts, and active head;
4. no recognized temporary remains and every final generation/part name is present;
5. a fresh storage owner exact-decodes and validates the selected Manifest and all referenced parts;
6. a second fresh owner selects byte-identical Manifest bytes; and
7. two WAL recoveries from the Manifest checkpoint preflight, decode, and replay the exact generated
   command and row counts in record-sequence order.

The initial `manifest.json` remains `pending` until every repetition and artifact write passes. A
failure retains `failure.txt` and never claims validated results. Successful output contains:

- `manifest.json`: complete workload/source/build/host/procedure/correctness metadata;
- `raw-flushes.csv`: every measured flush latency, bytes, and sync counts;
- `raw-foreground-snapshots.csv`: bounded baseline/during-flush reader latencies;
- `summary.json`: per-repetition distributions, resource counts, growth/amplification, and recovery;
  and
- `repetition-N/database/`: the exact verified CSEG, Manifest, and WAL images.

These artifacts implement a reproducible local measurement boundary under the
[benchmark publication contract](benchmark-contract.md). They do not qualify a storage device's
power-loss behavior or publish a performance claim. A reviewed campaign must retain the complete
wrapper output and explain cache, CPU, filesystem, mount, device, and power-loss-protection controls.
