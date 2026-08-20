# ADR 0110: Multi-tablet temporal WAL recovery routing

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB temporal recovery and storage maintainers
- **Extends:** [ADR 0084](0084-verified-temporal-checkpoint-overlap.md)

## Context

The Manifest v2 startup owner restored and verified one WAL tablet, while its underlying replay
state already routed Temporal Mutation Command v1 by table identity. A database-wide WAL reclaim
checkpoint is safe only when every selected WAL tablet has an independently recoverable durable
boundary. Rejecting all multi-tablet generations prevented that exact composition.

Temporal Mutation Command v1 names a table and schema but does not carry a tablet identity. It
therefore cannot distinguish two selected tablets of the same table. Inferring routing from row
values or part ranges during replay would make recovery depend on mutable partition policy.

## Accepted decision

`recover_manifest_temporal_wal` restores every selected Manifest v2 WAL tablet under one held
generation and filesystem lock when:

- the global WAL checkpoint exists and is no later than every tablet durable position;
- every tablet has one exact retained schema and WAL source binding;
- at most one selected tablet exists for each table identity; and
- every nonempty tablet has the caller's explicit database-wide retained-system-time proof.

Each provider records its tablet's durable position. WAL preflight still validates the complete
suffix before publication. Replay routes a command by its table identity, exact-verifies it when its
record sequence is at or below that tablet's durable boundary, and applies it only when later. The
WAL must extend beyond the greatest selected durable position. A fresh empty tablet provider may
start from a later suffix position because intervening WAL records targeted other tables.

The returned owner exposes table-keyed providers and an ordered per-tablet report. Its no-argument
provider convenience returns null for multi-tablet state, avoiding an arbitrary choice. Per-tablet
and aggregate verified/applied counts remain distinct. Any restore, preflight, verification, replay,
cleanup, or optional WAL reclamation failure destroys the complete unpublished composition.

Multiple selected tablets for one table fail explicitly with `NOT_SUPPORTED`. Supporting that shape
requires a versioned durable command/routing contract that names tablet identity; it is not inferred.
Raft tablets retain their application-snapshot recovery owner and are not mixed into the WAL owner.

## Consequences

- One physical WAL checkpoint can now recover multiple independently advanced, distinct-table
  temporal tablets without duplicate application or skipped suffix commands.
- Manifest v2, CSEG v2, Temporal Mutation Command v1, and WAL v1 bytes remain unchanged.
- Recovery memory is bounded by the existing per-provider, CSEG decode, and Manifest limits, though
  all reconstructed providers coexist until the complete startup transaction succeeds.
- Same-table multi-tablet routing and mixed WAL/Raft database publication remain explicit later
  contracts.

## Alternatives considered

- **Replay the suffix into every provider:** duplicates or misapplies commands.
- **Route from event time or shard keys:** partition rules can evolve and are not authenticated in
  Temporal Mutation Command v1.
- **Use the greatest tablet boundary globally:** skips commands for tablets with earlier durable
  boundaries.
- **Publish providers incrementally:** exposes partial database recovery when a later tablet fails.

## Affected invariants and validation

Invariants 1, 2, 4, 6–8, 10–14, and 18 apply. The focused real-filesystem test selects one retained
tablet plus one empty tablet, verifies covered commands only against the first, applies independent
suffix commands to both, checks per-tablet and aggregate reports, and reopens the shared writer at
the next global sequence. Exhaustive allocation sweeps cover canonical empty and single-CSEG
one-tablet startup plus the accepted two-tablet combination of one of each; every injected failure
permits reacquisition of both locks. Same-table routing, allocation faults during multi-part
restoration, crash injection, many-tablet skew, and mixed WAL/Raft publication remain in Phase 18
or later format work.
