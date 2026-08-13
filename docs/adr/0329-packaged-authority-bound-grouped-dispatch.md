# ADR 0329: Packaged authority-bound grouped dispatch

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query and distributed-systems maintainers
- **Extends:** [ADR 0326](0326-authority-bound-grouped-float64-fragment.md),
  [ADR 0327](0327-group-scoped-grouped-float64-dispatch.md)

## Context

The grouped authority binder returns one owned Raft group and grouped fragment intent, while the
canonical grouped dispatch requires those same two values. Requiring callers to assemble that
dispatch separately leaves an unnecessary substitution point between authority validation and the
executable request value.

## Decision

`bind_distributed_grouped_float64_fragment_dispatch` is the packaged constructor from one borrowed
`DistributedGroupedFloat64FragmentBinding` to one owned
`DistributedGroupedFloat64FragmentDispatch`. It delegates all admission, Manifest, placement,
group, schema, projection, aggregate, predicate, and grouped-key checks to
`bind_distributed_grouped_float64_fragment`, then moves that exact validated group and intent into
the canonical dispatch value.

The constructor performs no I/O and defines no new durable or network bytes. Encoding remains an
explicit operation through the accepted Grouped FLOAT64 Fragment Dispatch v1 codec. Validation
failures preserve the delegated status without partial publication.

## Consequences and validation

Callers that need an executable grouped request no longer join authority-bound values themselves.
The lower-level grouped binder remains available for non-dispatch ownership and inspection; both
paths share one authority implementation. Successful packaging adds no allocation beyond the
existing owned projection and performs only moves plus one group copy.

The focused real-Manifest-v2 binding case proves exact group preservation and successful canonical
dispatch encoding, and proves that an unsupported key type remains `NOT_SUPPORTED`. All seven
focused binding cases pass, the public header is self-contained, and the installed-consumer gate
references the packaged constructor.

Authenticated grouped transport, packaged multi-tablet grouped execution, general vector-plan
fragments, and broad failure/measurement evidence remain incomplete. No Phase 16 exit gate is
claimed.

Invariants 4–6, 10, 11, 14, 15, and 18 apply.

## References

- [Authority-bound grouped FLOAT64 fragment](0326-authority-bound-grouped-float64-fragment.md)
- [Group-scoped grouped FLOAT64 dispatch](0327-group-scoped-grouped-float64-dispatch.md)
- [Distributed Grouped FLOAT64 Fragment Dispatch
  v1](../formats/distributed-grouped-float64-fragment-dispatch-v1.md)
