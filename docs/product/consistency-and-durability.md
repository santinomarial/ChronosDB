# Consistency and Durability Contract

> **Status: contract specified; single-node WAL coordination and an internal replicated proof are
> implemented.** This document
> refines [ADR 0006](../adr/0006-wal-durability-and-group-commit.md),
> [ADR 0013](../adr/0013-wal-v1-format-and-recovery.md), and the snapshot invariants. The writer and
> recovery paths implement the physical write, synchronization, verification, repair, and reopen
> boundaries. The bounded commit coordinator implements `ASYNC` and `LOCAL_SYNC` completion and
> group commit. The already-routed single-tablet append executor now waits for that physical
> boundary and completes logical tablet/retry publication. No query service, recovery application,
> native transport acknowledgment path exists. The Raft runtime can prove stable or joint
> configuration quorum persistence, and the tablet state machine composes it with application, but no client request mode
> exposes that proof yet.
> This document does not strengthen guarantees beyond what a process, operating system, filesystem,
> device, or future replica protocol can establish.

## Durability modes

| Mode | Acknowledgment boundary | Intended covered failures | Outside the guarantee | Group commit |
| --- | --- | --- | --- | --- |
| `ASYNC` | The complete WAL v1 record has been accepted successfully through the active WAL file write path after its segment installation boundary; acknowledgment does not wait for data synchronization. | Normal continued process operation; no crash-survival claim. | Process or OS crash, power loss, device loss, and any failure before bytes reach required stable media may lose acknowledged operations. It must never be described as durable. | Requests may share write and later sync work, but acknowledgment does not wait for that sync. |
| `LOCAL_SYNC` | The complete record has finished the write path and is covered by a successful WAL data synchronization after any required synchronized segment installation. | Process termination and ordinary OS crashes on that local node under the documented filesystem/device assumptions. | Device loss, controller/firmware lies, incomplete power-loss protection, filesystem/kernel defects, operator destruction, or failures excluded by the platform contract. | Multiple requests may share one captured sync frontier; each acknowledgment waits for the successful sync covering its record end. |
| `QUORUM_SYNC` | Internal proof implemented for stable and joint-consensus membership; client exposure remains unavailable. A stable majority, or majorities of both old and new configurations during a transition, synchronize persistent state containing the entry; the leader synchronizes the derived commit and tablet application covers the index before acknowledgment. | Loss tolerated by the active stable or joint quorums under the stated membership, independence, storage, authenticated transport, and crash-fault Raft assumptions. | Correlated quorum loss, faulty persistence below the stated assumptions, bypassed membership protocol, forged/Byzantine behavior, or disaster beyond the replica topology. | Entries from one or many groups may share physical synchronization, but each request waits for its own group/index proof and application frontier. |

The server must expose requested and effective mode in the acknowledgment. It must never silently downgrade. Required operational metrics are:

- acknowledged operations and bytes labeled by effective mode;
- pending group-commit requests/bytes and oldest wait age;
- sync/quorum wait count, duration distribution, and failures;
- current unsynchronized WAL bytes and oldest unsynchronized age for `ASYNC`;
- rejected or downgraded-mode attempts (downgrade remains an error); and
- recovery reconciliation counts, including acknowledged-write loss count when a controlled test can determine it.

The [WAL recovery design](../architecture/wal-recovery.md) fixes the Linux reference operations and
ordering: synchronized temporary-file installation, same-directory atomic rename, directory sync,
complete record write, and `fdatasync`/stronger data sync for `LOCAL_SYNC`. It also states the
filesystem/device assumptions and macOS limitation. The blocking POSIX operations and serialized
WAL writer implement the complete-write boundary and an explicit data-sync frontier. The commit
coordinator owns that writer on one worker, bounds unfinished requests and encoded bytes, preserves
FIFO admission order, completes `ASYNC` after write, and groups `LOCAL_SYNC` requests behind one
covering frontier subject to configured request, byte, and delay limits. Locked
recovery verifies the complete physical history, permits only explicit synchronized final-tail
repair, and reopens at the verified end after a startup synchronization barrier. The server's
default mode and deployment-specific group-limit tuning remain deferred; the coordinator requires
an explicit mode per request and never exposes `QUORUM_SYNC`. The joint-consensus Raft runtime now
produces an internal immutable receipt only after majority-derived commit and local synchronization;
tablet application supplies the additional visibility proof. Native request/response integration,
explicit receipt configuration identity, and replica crash reconciliation remain deferred. The [subprocess crash
harness](../testing/wal-crash-harness.md) reconciles
parent-received acknowledgments with recovered physical records after controlled process death; it
does not extend this contract to unqualified power-loss or storage-stack failures. Benchmarks must follow the
[benchmark contract](../benchmarks/benchmark-contract.md).

The writer's runtime segment target and maximum application-payload setting are admission and
rotation policies, not durability modes or durable-format fields. Both are validated before
filesystem mutation; the runtime target never exceeds the header's fixed 64 MiB v1 limit, and an
oversized application payload is rejected before sequence assignment or record I/O.

## Single-node read behavior

### Committed visibility

Readers observe only operations that have crossed the logical commit boundary for their mode and have been fully published. `ASYNC` changes may be query-visible before stable-media synchronization, but that visibility does not make them durable. Prepared, partially decoded, partially appended, or partially initialized rows are invisible.

### Snapshot acquisition

A single-node snapshot atomically captures or pins:

- the committed WAL/apply position `C`;
- the bound schema/catalog version;
- the visible row boundary and generation for each mutable or sealed head;
- the selected manifest generation and its installed CSEG parts; and
- the row-version rule and system-time boundary.

All rows in the result are evaluated against that descriptor even while later commits, seals, flushes, or compactions proceed. Snapshot acquisition failure returns an error; it cannot fall back to an inconsistent mixture.

### Heads and parts

- A mutable head contributes only fully initialized rows at or before its captured publication boundary.
- A sealed head remains visible and pinned exactly like its captured mutable predecessor while flush runs.
- A newly installed CSEG part becomes visible only through the atomic manifest version that names it. A snapshot selects either the old head/part set or the new part set, never both logical copies and never neither.
- During compaction, existing snapshots retain input parts; new snapshots before the manifest edit use inputs and snapshots after it use outputs. Reclamation waits for all pins.

### Recovery

Normal query service remains unavailable until recovery selects a complete manifest, validates required parts, and completes the [WAL recovery state machine](../architecture/wal-recovery.md). WAL recovery verifies every segment and physical record before replay, optionally performs only the explicitly authorized synchronized repair of an incomplete suffix in the highest segment, re-verifies, preflights semantic support, and idempotently applies records in sequence. Corruption, discontinuity, and unsupported required semantics fail before query service; no “best effort” query may skip them. Operations acknowledged only under `ASYNC` may be absent after restart, while a `LOCAL_SYNC` operation cannot be absent under the covered platform failures.

## Future distributed read modes

These names specify planned cluster behavior only; none is currently available.

- **`LEADER_LINEARIZABLE`:** route to or validate the current leader, establish a Raft read barrier/read index after all operations completed before the read began, wait until the state machine has applied at least that index, and acquire the snapshot there. Failure to prove leadership or quorum returns an error/redirect, never a stale result.
- **`FOLLOWER_BOUNDED_STALE`:** serve from a follower only when it can prove its applied position satisfies the request's explicit maximum position lag and/or maximum time lag relative to a recently validated leader commit observation. If the proof or bound is unavailable, reject or redirect. It is not linearizable.
- **`LOCAL_EVENTUAL`:** serve the local replica's latest applied committed state without contacting a leader and without a freshness bound. It never exposes uncommitted entries, but it may be arbitrarily stale and offers no cross-request monotonicity unless a later session token requires a minimum position.

Consistency mode, serving replica, snapshot positions, observed lag, and redirect/rejection counts must be exposed in results/metrics. Cross-tablet snapshot coordination and exact staleness proof mechanisms remain deferred.

## Idempotency contract

A **client identity** is a stable, authenticated UUID-scoped producer identity. A **client batch identity** is a UUID unique within that client. Their pair is the idempotency identity; the server also records a canonical request digest and prior outcome.

- A retry with the same identity and digest within the advertised deduplication horizon returns the original logical outcome and creates no duplicate input.
- Reuse with a different digest is a deterministic `IDEMPOTENCY_CONFLICT` error and never becomes an implicit correction.
- A correction or tombstone uses its own batch identity and explicit operation kind while targeting the table's logical [deduplication key](data-model.md#row-and-version-semantics).
- The deduplication horizon is a configured time and/or commit-position interval reported by the server. Clients must retain outcomes or reconcile once an identity is older than that horizon; ChronosDB does not promise infinite identity retention.
- For synchronized modes, identity, digest, result, and mutation become recoverable under the same durability boundary. For `ASYNC`, both may be lost within that mode's failure envelope. Recovery replays them idempotently and cannot retain a mutation while forgetting its protected identity.

Garbage collection may remove identity records only after the advertised horizon and any recovery,
replication, subscription, or backup pins permit it. The initial one-tablet columnar append digest
and outcome are fixed by [ADR 0015](../adr/0015-columnar-batch-v1-and-wal-append-command.md) and the
[ingestion architecture](../architecture/columnar-ingestion.md). Authentication, horizon defaults,
multi-tablet batch atomicity, and protocol error representation remain deferred.
