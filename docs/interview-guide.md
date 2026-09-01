# ChronosDB Interview Guide

## The 30-second version

ChronosDB is a from-scratch C++23 analytical database prototype. The completed vertical slice owns
the full path from a native SQL client through parsing and bounded vectorized execution to a
checksummed WAL, mutable columnar storage, immutable parts, manifests, recovery, and restart. I
stopped at a demonstrable single-node system rather than claiming that the much broader distributed
roadmap is production-ready.

## Five-minute demonstration

The supported packaged server is Linux-only because it uses `epoll`:

```sh
cmake --preset dev
cmake --build --preset dev --target chronosd chronosctl
scripts/demo-single-node.sh
```

Point out these boundaries while it runs:

1. `chronosctl` is a separate process using ChronosDB's bounded native protocol; it is not calling
   an in-process test API.
2. `CREATE TABLE` installs durable schema and physical-policy metadata.
3. `INSERT` acknowledges in `LOCAL_SYNC` mode only after the WAL durability boundary.
4. `SELECT` uses the custom parser, binder, physical lowering, vector operators, and result-batch
   codec.
5. The daemon receives `SIGTERM`, drains and checkpoints, then reopens the same database root. The
   final SELECT proves process-level recovery rather than merely reading retained memory.

The script prints the retained temporary root. Its two daemon logs and durable files are useful
artifacts to inspect after the run.

## Architecture story

An INSERT crosses these ownership boundaries:

```text
chronosctl
  -> native Protocol v1 session
  -> chronosd request dispatch
  -> SQL parse/bind and columnar materialization
  -> retry reservation and WAL admission
  -> LOCAL_SYNC durability completion
  -> batch-atomic mutable-head publication
  -> later seal/flush into immutable CSEG
  -> manifest/checkpoint publication
```

The key design rule is that a success response is never allowed to outrun the state that makes the
success recoverable. Durable records and pages use explicit fixed-width encodings and checksums;
recovery validates complete history before exposing state and is designed to be idempotent.

## Strong discussion topics

### Why build the storage engine instead of wrapping SQLite or RocksDB?

The point of the project is to demonstrate the ownership boundaries that a wrapper would hide:
durable framing, group commit, recovery, columnar publication, immutable-part installation,
manifest replacement, snapshot lifetime pins, and vector execution. The repository explicitly
forbids substituting another database engine for those core subsystems.

### What happens if the process dies during a write?

The answer depends on the named durability mode. The demo uses `LOCAL_SYNC`: acknowledgment follows
the WAL synchronization boundary. Recovery validates the segmented WAL, rejects interior
corruption, handles only the specified incomplete-tail cases, replays committed operations in
order, and does not expose a partially installed durable generation.

### How do readers avoid dangling storage during flush or compaction?

Published snapshots retain ownership pins for the immutable bytes they reference. Manifest
publication installs a complete successor atomically; physical reclamation waits until older live
readers release their pins.

### Why is query execution bounded?

Operators receive an explicit query resource context. Variable-width materialization, grouping,
sorting, spill, result encoding, and distributed carriers have checked limits and explicit failure
states rather than relying on unbounded allocation.

### What would you build next?

For a product, the next work is release qualification around the demonstrated single-node path:
backup/restore, operational metrics, broader Linux hardware campaigns, packaging, and compatibility
policy. For research depth, the repository contains experimental Raft, temporal, distributed-query,
tablet-movement, and tiering slices, but they should be discussed as incomplete extensions—not as
features of the demo-ready product boundary.

## Honest scope statement

Safe claims:

- A real single-node daemon and CLI execute the demonstrated durable SQL lifecycle.
- The core WAL, columnar, immutable-part, manifest, query, and native-protocol subsystems are custom
  C++23 implementations with focused correctness evidence.
- The repository has unusually deep specifications, ADRs, learning notes, corruption tests, crash
  tests, sanitizers, and static-analysis gates for a student project.

Claims to avoid:

- production-ready, production-safe, or generally deployable;
- complete distributed SQL database;
- universal SQL support, ACID transactions, or general exactly-once delivery;
- benchmark numbers not reproduced on the machine and revision being discussed;
- macOS daemon support or a cross-platform power-loss durability guarantee.

## Resume-ready phrasing

- Built a from-scratch C++23 analytical database prototype spanning checksummed WAL/recovery,
  columnar storage, immutable parts/manifests, bounded vectorized SQL execution, and a native
  client/server path.
- Designed recoverability-first state transitions with idempotent replay, atomic manifest
  publication, reader-pinned reclamation, corruption classification, and explicit durability modes.
- Developed real-process restart coverage plus sanitizer, crash/fault-injection, fuzz, portability,
  and static-analysis workflows; documented architectural decisions and durable byte contracts.

Adjust these bullets to the exact work you can personally explain and defend.
