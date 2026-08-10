# Committed Raft Tablet Application

## Purpose and ownership

`RaftTabletStateMachine` is the bridge between one durable logical Raft group and the existing
columnar tablet publication path. It owns the `TabletState` and `RetryDirectory`, borrows one
`DurableMultiRaftRuntime`, and is single-thread-affine with that runtime. Readers may borrow snapshots
from its tablet after successful construction; a recovery failure returns no state-machine owner.

The command bytes are specified by [Raft Tablet Command v1](../formats/raft-tablet-command-v1.md),
and the ordering decision is recorded in [ADR 0073](../adr/0073-committed-raft-tablet-application.md).

## Apply sequence

```text
Raft committed prefix
  -> exact command decode and schema/tablet preflight
  -> global retry decision
  -> own decoded column buffers
  -> reserve tablet publication
  -> publish rows + retry outcome at (group, index)
  -> commit exact global retry outcome
  -> persist final applied_index through the durable runtime
```

The state machine never scans appended-but-uncommitted entries. A repeated client batch with the
same mutation is a no-row operation that still advances the outer tablet position; a conflicting
mutation fails closed. The shared `apply_committed_columnar_append` function keeps WAL replay and
Raft application on the same row/retry implementation.

## Recovery model

Raft's persisted `applied_index` says what an earlier process applied; it does not contain the
process-memory tablet. Therefore startup creates fresh owners and deterministically replays the
complete retained committed log. It may leave the durable applied index unchanged when it already
equals commit, because application memory has just been rebuilt from those exact bytes.

Without a supplied application-snapshot owner, this model intentionally rejects any nonzero Raft
snapshot boundary. A compacted prefix must atomically describe the omitted rows, retry outcomes,
schema binding, and included group/index before suffix replay can be safe.

Raft Tablet Application Snapshot v1 now defines the first half of that boundary. Its owned codec
binds one group/table/tablet and complete Raft snapshot metadata to the exact accepted application
commands at their original term/index positions. Checksummed entry payloads preserve row and retry
semantics for deterministic rebuild.

`RaftTabletSnapshotStorage` now supplies the local durable half: one group-scoped directory lock,
exact readback, file sync, immutable no-replace rename, directory sync, idempotent same-byte retry,
temporary cleanup, and revalidated latest selection.

Snapshot-backed `recover` exact-matches those application bytes with persistent Raft metadata,
preflights both the stored prefix and committed retained suffix, rebuilds fresh row/retry state from
original command positions, publishes membership-only frontier gaps, and persists the final applied
index only after success. The state-machine owner retains the snapshot lock.

`compact_applied_prefix` routes local snapshot creation through that same owner. It carries forward
the prior exact application prefix, appends applied retained-log commands, derives canonical Raft
term and membership metadata, durably installs the application bytes first, and only then compacts
Raft to the identical boundary. A crash between those steps leaves an unreferenced future file, not
an unrecoverable Raft prefix.

`install_recovered_tablet_movement_snapshot` is the follower-transfer bridge into that owner. It
accepts only a completed, authoritatively recovered movement; exact-decodes and canonicalizes the
transferred RTAS; binds table, tablet, snapshot coordinates, source voters, and group ownership; and
then invokes the established immutable installer. Once movement records promotion, the bridge only
verifies a preexisting exact RTAS and treats absence as corruption. It returns full snapshot
metadata for the still-separate Raft installation transition.

## Failure behavior and limits

Entry count and payload size are bounded by Raft limits; decoded batch rows, columns, and bytes are
bounded by columnar decode limits; tablet rows, sealed generations, schemas, and retries retain their
existing explicit bounds. Complete preflight detects malformed later bytes before the current apply
batch mutates state. Allocation and state-capacity errors return their precise status. Any error
after committed application begins fails the owned tablet closed; restart rebuilds from the retained
log.

`prove_applied_quorum_sync` composes the leader's committed/joint-membership durability receipt with
the Raft applied index and tablet group/index publication frontier. Client protocol exposure, Raft
metadata completion for transferred application snapshots, and physical-log reclamation remain
absent.

## Complexity and likely interview questions

Application is linear in committed commands plus decoded column bytes. Retry lookup is `O(log N)`
under the bounded map. Startup is linear in snapshot commands plus the retained committed suffix;
v1 snapshot extension is linear in the carried command prefix plus newly covered log entries.

- Why is a persisted Raft applied index insufficient to restore mutable tablet memory?
- Why must applied-index persistence follow row publication?
- How does an exact retry at a later log index avoid duplicate rows while preserving progress?
- Why must application-snapshot installation precede Raft prefix compaction?
- Which boundary prevents an uncommitted entry from becoming query-visible?
