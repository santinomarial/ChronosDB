# ADR 0326: Authority-bound grouped FLOAT64 fragment

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, manifest, and distributed-systems maintainers
- **Extends:** [ADR 0166](0166-authority-bound-distributed-fragment-construction.md),
  [ADR 0325](0325-distinct-grouped-float64-fragment-intent.md)

## Context

Grouped Fragment Intent v1 can name a projected key but its value type does not prove that the
projection, schema, snapshot, placement, group, and read admission came from one authority set.
Reimplementing those checks for grouping would risk divergence from the existing executable
aggregate binder.

## Decision

`DistributedGroupedFloat64FragmentBinding` contains the complete existing
`DistributedAggregateFragmentBinding` plus one group-key input index.
`bind_distributed_grouped_float64_fragment` first delegates to
`bind_distributed_aggregate_fragment`, preserving its admission, committed placement, Raft-group,
Manifest v2 durable-boundary, recovery-schema, projection, aggregate-FLOAT64, and predicate gates.

Only after that succeeds does the grouped binder check the key index against the returned owned
projection and prove that the selected ordinal is FLOAT64 under the same borrowed destination
schema. The key may be nullable or nonnullable as declared by that schema, and its input index may
equal the aggregate input index. The result owns the exact Raft group and grouped fragment intent;
it retains no borrowed span or authority reference.

The function performs no I/O, publication, dispatch encoding, or storage access. The returned
group-plus-intent value is not an executable network format; a later distinct group-scoped dispatch
must carry it to a revalidating grouped worker.

## Consequences and validation

Grouped construction cannot bypass or fork the proven ungrouped authority checks. The incremental
work after base binding is constant time and allocation-free; allocation/failure behavior remains
that of the base owning projection.

One focused real-Manifest-v2 case proves exact group/query/database/projection ownership and an
encodable grouped intent, permits one FLOAT64 column to serve as both key and aggregate input, and
rejects a projected timestamp key plus an out-of-bounds key index. All seven focused binding cases
pass, and the installed-consumer gate references the new public binder.

Worker-side local revalidation and real-CSEG grouping, authenticated grouped transport,
multi-key/non-FLOAT64 state, ordering/top-N/LIMIT, and broad failure evidence remain incomplete.
Canonical group-scoped dispatch bytes are the accepted follow-up in
[ADR 0327](0327-group-scoped-grouped-float64-dispatch.md), and the packaged constructor that moves
the validated values directly into that dispatch is accepted in
[ADR 0329](0329-packaged-authority-bound-grouped-dispatch.md). No Phase 16 exit gate is claimed.

Invariants 4–6, 10, 11, 14, and 18 apply.

## References

- [Authority-bound distributed fragment
  construction](0166-authority-bound-distributed-fragment-construction.md)
- [Distinct grouped FLOAT64 fragment intent](0325-distinct-grouped-float64-fragment-intent.md)
- [Distributed Grouped FLOAT64 Fragment Intent
  v1](../formats/distributed-grouped-float64-fragment-intent-v1.md)
