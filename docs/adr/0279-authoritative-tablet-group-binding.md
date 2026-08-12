# ADR 0279: Authoritative tablet-to-Raft-group binding

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB metadata, Raft, routing, and recoverability maintainers

## Context

Committed placement metadata names a tablet, replicas, placement epoch, and advisory leader hint,
but does not name the tablet's Raft group. Replicated ingest requires that group before it can obtain
an ordered leadership observation or submit a term-bound proposal. Deriving a group from tablet UUID
bytes, trusting caller input, or using a leader hint would create authority absent from the metadata
log.

Metadata Command v1 and Metadata Application Snapshot 1.0 are frozen. Existing type-2 placement
bytes cannot gain a field in place.

## Decision

Logical metadata Raft entry type 4 contains one checksummed Tablet Group Binding v1 value: an exact
nonzero tablet UUID and nonzero group UUID. The metadata state machine accepts a binding only after
that tablet has committed placement. The first binding is immutable. An exact later binding advances
the ordered application index; a conflicting group fails without mutation.

The binding is separate from placement. Placement epochs may change replica membership and leader
hints without changing consensus history identity. Routing must join the binding with the current
placement and must still obtain current role/term from an ordered Raft observation.

Metadata Application Snapshot major 1 gains minor 1. The encoder emits minor 1 when any type-4 entry
is retained and minor 0 otherwise. Minor 0 decoding remains restricted to types 2 and 3. Minor 1
admits types 2, 3, and 4. Older readers reject minor 1, and new readers reject a type-4 entry
mislabeled as minor 0. Snapshot digest construction already binds entry type and exact payload, so
its identity algorithm is unchanged.

`MetadataCatalogSnapshot` publishes bindings in tablet-ID order alongside placements and node
endpoints. It never synthesizes a binding for legacy placed tablets; operators or bootstrap logic
must commit type 4 before replicated routing becomes available.

## Consequences

- Replicated routing can derive tablet group identity solely from committed metadata.
- Existing Metadata Command v1, schema, and Snapshot 1.0 bytes are unchanged.
- A placement can be temporarily unbound between its type-2 and type-4 commits. Routing fails closed
  during that interval.
- Group identity cannot be changed through ordinary movement. A future tablet replacement protocol
  must use a new tablet identity or an explicitly versioned migration design.
- Mixed-version clusters must not commit type 4 until every metadata application reader understands
  entry type 4 and snapshot minor 1.

## Affected invariants and validation

Invariants 4–6, 8, 10–12, 14, and 18 apply. Tests cover canonical deterministic round trip, every
truncation, checksum damage, unknown version, ordered/immutable application, retained-log reopen,
owning projection, Snapshot 1.1 round trip, minor-0 rejection, compaction, exact snapshot recovery,
and asynchronous publication before completion.

Fuzzing, mixed-binary deployment, administrative backfill, allocation/syscall crash injection, and
large-catalog routing profiles remain Phase 18 work.

## References

- [ADR 0010](0010-tablets-raft-and-multiplexed-log-storage.md)
- [ADR 0075](0075-durable-metadata-raft-commands.md)
- [ADR 0266](0266-metadata-application-snapshot-v1.md)
- [Tablet Group Binding v1](../formats/tablet-group-binding-v1.md)
