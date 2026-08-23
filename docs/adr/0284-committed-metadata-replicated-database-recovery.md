# ADR 0284: Committed-metadata replicated database recovery

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB service, runtime, metadata, Raft, and ingest maintainers

## Context

The owning replicated-ingest runtime requires complete tablet objects and exact resident group
configuration at construction. A packaged process cannot safely duplicate tablet/schema/policy
facts in command-line flags because committed metadata is their authority. Conversely, the durable
database bootstrap intentionally contains only node identity, metadata-group identity, and stable
resource limits; Raft voter membership remains deployment configuration.

Startup therefore needs a recovery stage that holds the database-root lock, reads committed
metadata, constructs local tablet owners, and only then exposes the asynchronous service runtime.

## Decision

`ReplicatedIngestDatabase::open_existing` owns the validated Database Bootstrap v1 root and accepts
the complete externally configured resident Raft group set. It opens the retained Raft log through a
temporary synchronous durable runtime, recovers the metadata state machine (including an optional
authoritative metadata application snapshot), takes an owning catalog projection, and closes that
temporary runtime before reopening the log asynchronously.

The projection selects each committed tablet-to-group binding whose group is resident in the
configured set. A binding for an unconfigured group is ignored only when its committed placement
does not include the local node; this permits a global metadata catalog to describe remote tablets.
Every configured non-metadata group must have one binding. Each selected tablet is reconstructed
from its complete active schema, consecutive retained schema definitions, complete retry policy,
bootstrap memory bounds, a fresh bounded retry directory, and optional group-specific application
snapshot storage. Application recovery then replays the exact committed retained suffix.

Startup does not require placement to equal configured voters. Persisted joint membership and
placement transitions must remain restartable; the existing coordinator revalidates stable current
membership against committed placement before admitting a write.

Shutdown first drains and closes the replicated-ingest runtime and then releases the root lock. The
first failure is retained across repeated shutdown calls.

An optional borrowed startup observer receives four synchronous, non-throwing callbacks on the
opening thread after root ownership, catalog recovery, tablet-owner preparation, and asynchronous
runtime readiness. The observer receives no partially constructed database and has no return
channel into startup status; blocking it directly blocks startup. These stages provide operational
timing and deterministic process-crash boundaries without changing durable state.

## Consequences and validation

Tablet shape is no longer an independent daemon truth. Existing provisioned roots can reconstruct
query-visible tablet state and retry outcomes using committed metadata plus Raft history. Fresh
cluster provisioning, the external group-configuration file/parser, process transport, elections,
queries, and native daemon advertisement remain higher lifecycle stages.

No durable or network bytes change. Focused tests provision a root, commit global metadata with one
local and one remote tablet, apply a local QUORUM_SYNC append, close everything, reconstruct only the
resident tablet under the root lock, recover its rows, and return a matching retry after a new term.
Schema-evolution recovery additionally commits a predecessor-schema write, activates its direct
successor, reconstructs the complete lineage on reopen, preserves the predecessor generation, then
applies the successor and proves both generations plus the exact retry across another reopen. An
additional lifecycle commits one write under voters `{1,2}`, commits the joint removal `{1,2} ->
{1}`, and restarts before finalization. Reopen reconstructs the rows, retry, old/new voter sets, and
finalization eligibility; a new joint election finishes membership, committed placement advances to
`{1}`, exact retry succeeds, and another reopen observes stable voter `{1}`. A two-tablet lifecycle
commits one write to each same-table resident group, reopens both owners, returns each group's exact
retry independently, and executes one four-row whole-table count from pins that outlive shutdown.
The application-snapshot lifecycle compacts the complete provisioning catalog and first tablet
append, commits one unapplied suffix in each group, and closes both owners. Packaged startup without
the authoritative snapshot configuration fails closed; configured startup reconstructs the catalog,
four rows, and two retry identities from both snapshots plus suffixes, returns an exact suffix retry,
and reproduces that state on another reopen. An omitted recovered resident group fails closed.
A real subprocess additionally provisions and cleanly reopens one packaged owner, reports readiness
only after its exact two-row/one-retry publication is visible, and is then stopped with `SIGKILL`.
The parent reacquires the database-root and subsystem locks, reconstructs the committed state,
applies an exact retry without adding rows, and reproduces the advanced application frontier on a
second reopen. A second child variant compacts the provisioning catalog and first tablet append,
commits one retained suffix in each group, opens the packaged owner from both authoritative
application snapshots, and then reaches the same `SIGKILL` boundary. Parent recovery reacquires both
snapshot locks, reconstructs four rows and two retry identities, replays the metadata suffix, and
survives an exact suffix retry plus another reopen. These are steady-state owner-death boundaries;
a third child dies after the QUORUM_SYNC coordinator has admitted its observation and write proposal
but before the caller observes a response. Recovery permits only the exact pre-write or fully
committed publication, and the same request then returns first-application or matching-retry
accordingly without duplicate rows. The observer's complete success order is tested, and a
four-case child matrix pauses after each of validated root ownership, catalog recovery, tablet-owner
preparation, and asynchronous runtime readiness. Every child dies by `SIGKILL`; the parent proves
exact recovery, retry, and repeated reopen. Syscall-level startup cuts, remaining write stages,
snapshot-install/compaction, and shutdown crash/syscall matrices, larger resident-set profiles, and
broader TSan coverage remain deferred.

## Affected invariants

Invariants 1, 4–6, 8, 9, 11, 14, 15, and 18 apply.

## References

- [Database Bootstrap v1](../formats/database-bootstrap-v1.md)
- [ADR 0278](0278-worker-affine-metadata-application.md)
- [ADR 0279](0279-authoritative-tablet-group-binding.md)
- [ADR 0282](0282-owning-replicated-ingest-runtime.md)
