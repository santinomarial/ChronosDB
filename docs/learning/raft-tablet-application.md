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

A four-cut process-crash matrix freezes the live path after rows and retry state are published but
before the applied-index record write, after that write, after its sync, and after successful return.
The pre-write image retains applied index 0; every complete-record image recovers applied index 1.
All four rebuild the same two rows, one retry identity, and group/index frontier from the retained
committed entry, then remain identical on a second reopen. Persisted applied position is therefore a
progress marker, never a substitute for reconstructing application memory.

A deterministic five-case companion matrix fails before the applied-index record write, after a
record prefix, after a complete write, before synchronization, and after synchronization. The live
state machine and runtime fail closed in every case because application publication already
happened. Strict recovery rejects the partial tail until explicitly authorized repair; write-before
and repaired-partial images retain applied index 0, while the three complete-record images retain 1.
Retained-log recovery nevertheless reconstructs the same rows, retry identity, and frontier in all
five cases, advances a trailing progress marker, and stays identical on another reopen.

The five persistence faults are also crossed with four committed-range shapes: one command, three
distinct commands, a matching retry within the range, and an internal leader no-op followed by
commands and a retry. These 20 schedules prove that the single final applied-index record covers the
whole preflighted range, while reconstruction still derives two, four, or six visible rows, the exact
distinct retry identities, and the last command-or-internal index solely from retained log order.

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

A ten-cut local-compaction process matrix stops after every application-file durability transition,
the Raft state-record write and sync, and successful return. Public reopen sees no snapshot file, an
immutable future orphan, or exact application/Raft authority according to the completed prefix.
Recovery rebuilds the same two rows and retry identity, exact retry adopts an orphan when needed,
and reclamation plus a second reopen converge. This is process-restart evidence; power-loss and
device qualification remain separate.

Deterministic local-compaction injection separately fails all ten post-ownership application-file
stages, including a 16-byte partial temporary and final directory-sync ambiguity. The failed call
never reaches Raft compaction: its retained entry remains authority and the live tablet state does
not fail closed. Public reopen removes any temporary, rebuilds the same rows and retry identity from
Raft, and exact retry either installs a new file or adopts the complete post-rename orphan. Raft
persistence fault products then cross each of those application failures with five record outcomes:
write-before, 16-byte partial write, write-after, sync-before, and sync-after. The first failed
attempt never touches Raft; the retry installs or adopts the RTAS before the Raft error fails the
runtime closed. Reopen repairs only the partial tail and deterministically recovers either the
retained entry or the exact snapshot authority. Both paths reconstruct the same rows and retry
identity, converge through orphan adoption when needed, reclaim nothing current, and survive a
second reopen.

Cleanup ambiguity is crossed separately with those same five Raft outcomes. A partial RTAS is left
behind, then reopen fails either before its unlink or after unlink at the directory-sync boundary.
The next public reopen removes or confirms removal of the temporary, reconstructs from the retained
entry, and proceeds to the injected Raft failure. Tail repair, authority selection, exact retry,
reclamation, and repeated reopen then converge for all ten products; the temporary is never treated
as an application snapshot.

The corresponding eight-case reopen matrix combines both cleanup failures with incomplete-Raft-tail
repair failures at size inspection, truncate, file sync, and directory sync. Size inspection leaves
the 16-byte tail intact; the other failures occur after truncation. A later repairing reopen observes
the correct bytes in either case. Successful snapshot and runtime opens after the injected errors
also prove that both domain locks were released before exact orphan adoption and repeated recovery.

A repeated-fault lifecycle runs two 16-byte application partial writes, cleaning the prior
temporary before each retry, followed by two 16-byte Raft partial records, repairing the prior tail
before each retry. The installed RTAS remains exact and immutable through both Raft failures. One
final compaction adopts it, advances authority, reclaims nothing current, and survives reopen. This
guards against retry-local state leaking across attempts in either durable owner.

Immutable mismatch coverage plants a structurally valid index-1 RTAS that disagrees with local
derivation in one of seven fields: table, tablet, boundary term, Manifest generation, part-set
checksum, voters, or command payload. Compaction returns corruption while retaining index-1 Raft
authority and keeping both live owners usable. After applying index 2, compaction installs that
distinct boundary, then authoritative reclamation removes the conflicting orphan. Reopen preserves
the two-row, one-retry logical result.

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
the Raft applied index and tablet group/index publication frontier. The later worker-affine
application and term-bound completion owners carry that exact proof through Protocol 2.0 replicated
ingest acknowledgement. Transport of completed snapshot responses and automatic scheduling of the
implemented node-wide physical-log reclamation remain absent.

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

Deterministic reclamation injection covers all four authoritative-file preflight calls before
mutation, directory enumeration, each possible ordered obsolete-file unlink, and the final
directory sync. Eight middle-authority cases prove that the exact authority remains readable; five
zero-authority cases expose only the deletion prefix already completed. Every failed owner remains
usable, its failure-only metrics do not claim unsynchronized removals, and retry plus reopen
converges from the observed namespace.

A separate eleven-schedule process matrix stops reclamation after enumeration, each completed
unlink, the directory durability boundary, and success release for both middle and zero authority.
Public reopen sees exactly that deletion prefix; retry and a second reopen converge without deleting
the middle authority or retaining an orphan when no authority exists. This establishes process-
restart behavior while leaving power-loss and storage-device qualification separate.

## Complexity and likely interview questions

Application is linear in committed commands plus decoded column bytes. Retry lookup is `O(log N)`
under the bounded map. Startup is linear in snapshot commands plus the retained committed suffix;
v1 snapshot extension is linear in the carried command prefix plus newly covered log entries.

- Why is a persisted Raft applied index insufficient to restore mutable tablet memory?
- Why must applied-index persistence follow row publication?
- How does an exact retry at a later log index avoid duplicate rows while preserving progress?
- Why must application-snapshot installation precede Raft prefix compaction?
- Which boundary prevents an uncommitted entry from becoming query-visible?
