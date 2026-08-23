# ADR 0088: Owned Raft tablet snapshot creation and compaction

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB ingestion, storage, and distributed-systems maintainers
- **Extends:** [ADR 0086](0086-durable-raft-tablet-snapshot-installation.md) and
  [ADR 0087](0087-raft-tablet-snapshot-recovery-composition.md)

## Context

Recovery can consume an installed application snapshot, but allowing an independent caller to
compact Raft leaves the live tablet owner unable to prove that matching application bytes exist.
The opposite ordering is also unsafe: compacting the only retained commands before the application
snapshot is durable can make restart unrecoverable. Repeated compaction must preserve commands from
the already-compacted prefix as well as newly retained commands.

## Accepted decision

`RaftTabletStateMachine::compact_applied_prefix` is the only local tablet compaction path. It
requires snapshot-storage ownership transferred through `recover`, a newer boundary no later than
the durable applied index, nonzero Manifest generation, stable membership, and a tablet publication
frontier covering that boundary.

The owner exact-loads the current application snapshot when one exists, appends every supported
application command through the requested retained-log boundary, and omits only validated Raft
membership entries. It derives the included term from the exact retained entry and derives the
canonical voter/configuration checkpoint using the same stable committed state and final-membership
entries as the Raft core. The owner obtains that metadata from the core's read-only prefix
preparation before installing application bytes. A prefix ending between joint and final entries is
rejected; a stable prefix before a later completed change retains the membership commands and
checkpoints the older boundary-time voters.

The complete application snapshot is immutably installed and directory-synchronized first. Only
then does the owner submit the matching `CompactSnapshotOperation` to the durable Raft runtime. It
requires the resulting persistent `SnapshotMetadata` to equal the installed bytes before adopting
the new live boundary. The returned report names the file, metadata, application-entry count, and
whether installation was an exact idempotent retry.

## Consequences and alternatives

A crash after application installation but before Raft persistence leaves an unreferenced future
snapshot file; recovery selects the exact older Raft boundary, so the file is not mistaken for
authority. Retrying the same operation is byte-idempotent. A different snapshot for an already
installed index remains corruption by design; callers choose a later applied index rather than
reinterpreting immutable bytes.

Repeated compaction reloads the prior application prefix, so logical Raft-log reclamation does not
lose rows or retry identities. This command-preserving v1 representation favors correctness over
snapshot size. ADRs 0269 and 0270 subsequently add caller-triggered shared-log and application-file
reclamation after their exact durable authorities are validated.

Compacting Raft first was rejected because a crash could remove the only recovery source. Letting
callers supply voters, terms, or configuration indexes was rejected because those fields are
canonical Raft state. Reconstructing only visible rows was rejected because retry semantics require
the exact accepted commands.

## Affected invariants and validation

Invariants 1–8, 10, 11, 14, and 18 apply. The filesystem integration test now creates the first
owned snapshot, applies an exact retry, extends the snapshot to a second boundary, leaves a distinct
committed suffix unapplied, restarts, and reconstructs both the compacted prefix and suffix with
their exact row/retry counts and final group/index frontier. A ten-case application-I/O matrix fails
each post-ownership installation stage from temporary creation and validation through partial write,
readback, sync, close, rename, and final directory sync. Every failed attempt leaves the retained
Raft entry authoritative; public reopen cleans any temporary, exact retry installs or adopts the
post-rename orphan, and a second reopen reconstructs the exact rows and retry state. Crossing these
stages with Raft runtime-persistence failures remains deferred. A ten-cut real-process `SIGKILL`
matrix stops after the same application-file transitions, then after the Raft state-record write,
sync, and successful return. Reopen observes no application file, an immutable orphan, or the exact
Raft authority as appropriate; state-machine recovery, idempotent retry or adoption, reclamation,
and a second reopen converge with the exact rows and retry state. Obsolete-file reclamation,
follower transfer, and physical shared-log/application-file fault injection are tracked by their
extending ADRs. A membership-boundary integration test compacts an application entry before retained
joint/final commands and requires the installed application snapshot to carry the older voter set
while the live group retains the newer set.
