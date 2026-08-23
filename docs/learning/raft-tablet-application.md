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
an unrecoverable Raft prefix. If a remote two-stage snapshot installation is already pending, the
core rejects local compaction until that installation is completed or rejected; two immutable
application-snapshot identities therefore cannot race for one Raft boundary.
Exact retransmissions also coalesce without re-entering the application owner. A competing remote
snapshot receives a negative response while the first transfer retains the sole completion
identity; after that identity resolves, a later request can be admitted normally.

`install_recovered_tablet_movement_snapshot` is the follower-transfer bridge into that owner. It
accepts only a completed, authoritatively recovered movement; exact-decodes and canonicalizes the
transferred RTAS; binds table, tablet, snapshot coordinates, source voters, and group ownership; and
then invokes the established immutable installer. Once movement records promotion, the bridge only
verifies a preexisting exact RTAS and treats absence as corruption. It returns full snapshot
metadata for the still-separate Raft installation transition.

`complete_recovered_tablet_movement_raft_snapshot` performs that transition. It requires the
catching-up movement, exact-matches the handoff's full metadata and movement source/target against
the pending Raft request and local group, and runs `CompleteSnapshotInstallOperation` through the
durable runtime. Its returned success response is therefore held until the new Raft snapshot state
is synchronized. A durable RTAS with no matching pending request remains an unreferenced safe file.

A real-process crash matrix freezes that ordering at every durable application-file operation, the
Raft state-record write and sync, and the success-release boundary. After `SIGKILL`, public reopen
removes pre-rename temporaries, accepts a complete post-rename RTAS only as an orphan until Raft
metadata agrees, and either resumes completion or answers an exact leader retry from recovered Raft
authority. This establishes process-restart behavior; it does not substitute for power-loss and
filesystem durability qualification.

Deterministic I/O injection separately proves every RTAS installation call and both temporary-
cleanup calls. Pre-rename failures retain a retryable live owner. A failed final directory sync
poisons that owner because name durability is uncertain, while a new public owner can revalidate the
complete immutable file and converge through an exact retry.

The following Raft persistence boundary is also failure-injected at both definite and ambiguous
write/sync outcomes. Completion never yields an acknowledgment report once the runtime reports
`EIO`. Reopen either finds no snapshot metadata and safely repeats the pending completion, or finds
the complete record and answers the leader retry idempotently. The immutable RTAS is valid in both
states and never becomes authority by itself.

Short-write coverage crosses both owners. An RTAS temporary containing only a prefix is recognized
and removed on reopen. A partial Raft completion record is different because it lives in the append
stream: strict reopen preserves and rejects it, while repair-authorized reopen truncates and
synchronizes only the structurally incomplete final suffix before the completion is retried.
Repair fault injection covers the preceding size check and every mutating durability boundary.
Whether the failed call leaves the 16-byte suffix present or already removed, ownership is released
and the next public reopen derives the same pre-completion Raft authority before an exact retry.
The composed mixed-owner matrix adds another restart boundary. RTAS first fails before publication
or after final rename, public reopen selects no file or the exact immutable file, and only then does
the retained Raft fault fail the next completion write. A third attempt succeeds from the recovered
product state; neither earlier attempt produces a success response.
The longer repeated schedule stops two RTAS temporary writes and two Raft completion-record writes
at 16 bytes each. Cleanup and repair restart from observed bytes every time, and the application
sees no success until the fifth attempt synchronizes the complete Raft record.
Reopen faults are composed too: RTAS cleanup can fail with its temporary present or already removed,
and later Raft repair can fail at size inspection, truncation, repaired-file sync, or repair-
directory sync. Subsequent owners revalidate each of the eight images and reach the same pre-
completion application/Raft product state.

After that durable transition,
`checkpoint_recovered_tablet_movement_catch_up` reconciles the target's exact persisted snapshot
when movement still reopens as catching-up. It creates a private ready candidate, installs the next
movement generation, and only then advances the live owner. The external-reference and legacy
self-contained representations are each preserved.

## Failure behavior and limits

Entry count and payload size are bounded by Raft limits; decoded batch rows, columns, and bytes are
bounded by columnar decode limits; tablet rows, sealed generations, schemas, and retries retain their
existing explicit bounds. Complete preflight detects malformed later bytes before the current apply
batch mutates state. Allocation and state-capacity errors return their precise status. Any error
after committed application begins fails the owned tablet closed; restart rebuilds from the retained
log.

`prove_applied_quorum_sync` composes the leader's committed/joint-membership durability receipt with
the Raft applied index and tablet group/index publication frontier. Client protocol exposure,
transport of completed snapshot responses, and automatic scheduling of the implemented node-wide
physical-log reclamation remain absent.

Application snapshot-file reclamation is explicit and Raft-authoritative. The tablet owner first
requires its adopted application snapshot to equal durable Raft metadata, exact-loads that file,
then removes every other canonical final and directory-syncs the cleanup. A higher-index file is not
preferred: it may be an orphan from a crash before Raft compaction. Snapshot decoding owns all
bytes, so no live tablet reader retains a file mapping or borrowed span that needs a reclamation pin.

The single-thread-affine storage owner retains saturating cleanup metrics. Successful temporary
removal is counted only after its directory sync; reclamation separately counts attempts and
failures, then credits reclaimed files and one directory sync only after the complete cleanup batch
is synchronized. A failed unlink or sync may have shortened the namespace without increasing the
success counters. `snapshot_cleanup_metrics()` forwards this process-local snapshot from the tablet
state machine only when recovery transferred snapshot-storage ownership.

## Complexity and likely interview questions

Application is linear in committed commands plus decoded column bytes. Retry lookup is `O(log N)`
under the bounded map. Startup is linear in snapshot commands plus the retained committed suffix;
v1 snapshot extension is linear in the carried command prefix plus newly covered log entries.

- Why is a persisted Raft applied index insufficient to restore mutable tablet memory?
- Why must applied-index persistence follow row publication?
- How does an exact retry at a later log index avoid duplicate rows while preserving progress?
- Why must application-snapshot installation precede Raft prefix compaction?
- Which boundary prevents an uncommitted entry from becoming query-visible?
