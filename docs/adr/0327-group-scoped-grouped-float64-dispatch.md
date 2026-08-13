# ADR 0327: Group-scoped grouped FLOAT64 dispatch

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query and distributed-systems maintainers
- **Extends:** [ADR 0165](0165-group-scoped-distributed-fragment-dispatch.md),
  [ADR 0326](0326-authority-bound-grouped-float64-fragment.md)

## Context

The authority-bound grouped result owns a Raft group and grouped intent, but no canonical network
format binds those values together. Reusing ungrouped Dispatch v1 would require its decoder to
guess the nested protocol and would change a frozen executable request boundary.

## Decision

Distributed Grouped FLOAT64 Fragment Dispatch v1 is a distinct checksummed envelope around one
exact Grouped FLOAT64 Fragment Intent v1 frame. It retains the proven dispatch header shape: exact
outer version/length, nonnil Raft group UUID, exact nested length, 28 zero reserved bytes, header
CRC, and complete CRC. Its distinct `CHDGDSP1` magic prevents reinterpretation as ungrouped
`CHDFDSP1`.

Header integrity is checked before peer lengths control slicing. Exact outer and nested length
relationships plus complete integrity are checked before grouped-intent decoding. The nested decoder
retains the caller's projection limit and its own inner Fragment v1 integrity/proof checks. Inner
failures preserve their status classification.

This is the only canonical executable request format for the first grouped path. A worker must
still exact-match the group and locally revalidate every nested route, placement, schema, snapshot,
and proof field before storage access. CRC32C is not authentication.

## Consequences and validation

Ungrouped dispatch and fragment bytes remain unchanged. The grouped dispatch owns at most 16,732
bytes; exact decoding returns owned group, intent, and nested projection state. Encoding and
decoding add one bounded allocation/copy plus one linear integrity pass around the nested format.

Two focused grouped cases freeze magic/group/length/integrity fields, exact-round-trip the nested
intent, prove grouped and ungrouped dispatches reject each other, and reject truncation, trailing
bytes, outer/header or nested damage, checksum-valid future versions, and nil encoder groups. All
four dispatch cases pass, and the installed-consumer gate covers both grouped codec symbols.

Grouped request/response transport, stream discrimination, multi-key/non-FLOAT64 state,
ordering/top-N/LIMIT, and broader failure evidence remain incomplete. Worker-side real-CSEG grouped
execution is the accepted follow-up in
[ADR 0328](0328-proof-revalidated-grouped-float64-worker.md). No Phase 16 exit gate is claimed.

Invariants 4–6, 10, 11, 14, 15, and 18 apply.

## References

- [Distributed Grouped FLOAT64 Fragment Dispatch
  v1](../formats/distributed-grouped-float64-fragment-dispatch-v1.md)
- [Group-scoped distributed fragment dispatch](0165-group-scoped-distributed-fragment-dispatch.md)
- [Authority-bound grouped FLOAT64 fragment](0326-authority-bound-grouped-float64-fragment.md)
