# ADR 0221: Global Multi-Tablet Vector Source

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB query, tablet, and service maintainers

## Context

ADR 0217 placed all sealed and active mutable-head generations from one tablet beneath one physical
pipeline. Metadata and the single-node owner can represent more than one local tablet for a table.
Running that physical pipeline once per tablet and combining final chunks would produce incorrect
global aggregate, sort, latest, and limit semantics.

## Decision

`instantiate_tablet_states_pipeline` accepts a nonempty stable span of tablet snapshots, requires
that every snapshot belongs to the lineage table, and rejects nil or duplicate tablet identities.
It validates the destination physical shape once, creates head scans for each tablet's sealed
generations followed by its active generation, concatenates all scans in a bounded serial source,
then instantiates the physical pipeline exactly once above the combined source.

The existing single-tablet function delegates to this table-wide function. Tablet order is the
caller's deterministic input order; semantic ordering remains the responsibility of an explicit
physical sort/latest stage, as it was for multiple generations.

## Consequences

One vector query can now compute correct table-wide results over every supplied mutable tablet.
Snapshot construction and tablet enumeration remain service-owner responsibilities. Combining
mutable tablets with Manifest/CSEG data still requires the aggregate database snapshot path.

Validation of tablet identities uses a temporary sorted vector, making construction
`O(tablets log tablets + columns + generations)`. Pull execution remains linear in scanned chunks
plus generation transitions. The serial source keeps its existing finite query-memory reservation,
cancellation checks, eager child release, and fail-closed error behavior.

## Validation

Focused tests prove one global count across two tablet publications, the existing sealed-plus-active
global count, physical-shape rejection, and duplicate-tablet rejection. Allocation fault injection,
large tablet counts, concurrent publication schedules, and scalar/vector differential coverage
remain deferred.

## References

- [ADR 0217](0217-vectorized-tablet-state-query-source.md)
- [ADR 0022](0022-pull-based-vector-operator-memory-contract.md)
- [Tablet-state vector query pipeline](../learning/tablet-state-query-pipeline.md)
