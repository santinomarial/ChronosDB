# Consistency and Durability Contract

> **Status: specified, not implemented.** This document refines [ADR 0006](../adr/0006-wal-durability-and-group-commit.md) and the snapshot invariants. It does not strengthen guarantees beyond what a process, operating system, filesystem, device, or future replica protocol can establish.

## Durability modes

| Mode | Acknowledgment boundary | Intended covered failures | Outside the guarantee | Group commit |
| --- | --- | --- | --- | --- |
| `ASYNC` | The operation is validated, appended to process/kernel-visible log state as later specified, committed for current-process visibility, and accepted without waiting for stable-media synchronization. | Normal continued process operation; no crash-survival claim. | Process or OS crash, power loss, device loss, and any failure before bytes reach required stable media may lose acknowledged operations. It must never be described as durable. | Requests may share write and later sync work, but acknowledgment does not wait for that sync. |
| `LOCAL_SYNC` | Relevant local WAL bytes and required ordering metadata have completed the documented platform synchronization sequence before acknowledgment. | Process termination and ordinary OS crashes on that local node under the documented filesystem/device assumptions. | Device loss, controller/firmware lies, incomplete power-loss protection, filesystem/kernel defects, operator destruction, or failures excluded by the platform contract. | Multiple requests may share one sync; each acknowledgment waits for the sync covering its log extent. |
| `QUORUM_SYNC` | Available only in the future replicated system. A majority of the tablet's voting replicas, including the committed leader protocol state, satisfy the documented persistence condition before acknowledgment. | Loss of a minority of replicas under the stated membership, independence, storage, and Raft assumptions. | Correlated majority loss, faulty persistence below the stated assumptions, unsafe membership, Byzantine behavior, or disaster beyond the replica topology. | Entries from one or many groups may share physical synchronization, but each request waits for its own group to satisfy quorum persistence and commit. |

The server must expose requested and effective mode in the acknowledgment. It must never silently downgrade. Required operational metrics are:

- acknowledged operations and bytes labeled by effective mode;
- pending group-commit requests/bytes and oldest wait age;
- sync/quorum wait count, duration distribution, and failures;
- current unsynchronized WAL bytes and oldest unsynchronized age for `ASYNC`;
- rejected or downgraded-mode attempts (downgrade remains an error); and
- recovery reconciliation counts, including acknowledged-write loss count when a controlled test can determine it.

The exact sync syscalls, directory-ordering requirements, default mode, group size/delay, and future replica persistence condition remain deferred. Benchmarks must follow the [benchmark contract](../benchmarks/benchmark-contract.md).

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

Normal query service remains unavailable until recovery selects a complete manifest, validates required parts, identifies the valid log end, and idempotently reapplies committed records through its recovered boundary. A partial final WAL record is treated as an incomplete tail; corruption before the valid durable end is a surfaced recovery failure. Operations acknowledged only under `ASYNC` may be absent after restart. No “best effort” query may silently skip corrupt required state.

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

Garbage collection may remove identity records only after the advertised horizon and any recovery, replication, subscription, or backup pins permit it. Exact digest encoding, authentication, horizon defaults, multi-tablet batch atomicity, and protocol error representation remain deferred.
