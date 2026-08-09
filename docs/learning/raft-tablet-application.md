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

This model intentionally rejects any nonzero Raft snapshot boundary. Once log reclamation is added,
a tablet application snapshot must atomically describe the omitted rows, retry outcomes, schema
binding, and included group/index before suffix replay can be safe.

## Failure behavior and limits

Entry count and payload size are bounded by Raft limits; decoded batch rows, columns, and bytes are
bounded by columnar decode limits; tablet rows, sealed generations, schemas, and retries retain their
existing explicit bounds. Complete preflight detects malformed later bytes before the current apply
batch mutates state. Allocation and state-capacity errors return their precise status. Any error
after committed application begins fails the owned tablet closed; restart rebuilds from the retained
log.

Current local synchronization proves only that this node's applied index record is durable. It does
not implement `QUORUM_SYNC`, majority acknowledgment, application snapshots, or physical-log
reclamation.

## Complexity and likely interview questions

Application is linear in committed commands plus decoded column bytes. Retry lookup is `O(log N)`
under the bounded map. Startup is linear in the complete committed history until snapshots exist.

- Why is a persisted Raft applied index insufficient to restore mutable tablet memory?
- Why must applied-index persistence follow row publication?
- How does an exact retry at a later log index avoid duplicate rows while preserving progress?
- Why does this implementation reject a compacted Raft prefix?
- Which boundary prevents an uncommitted entry from becoming query-visible?
