# ADR 0166: Authority-bound distributed fragment construction

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB query, manifest, and distributed-systems maintainers
- **Extends:** [ADR 0115](0115-proof-bound-distributed-read-admission.md), [ADR 0165](0165-group-scoped-distributed-fragment-dispatch.md)

## Context

Canonical fragment bytes do not prove that a coordinator assembled their fields from one coherent
authority set. Admission, placement, schema, Raft source, durable position, and Manifest generation
can each be valid in isolation while referring to different epochs. Manifest v1 query snapshots
are WAL-specific and cannot represent the replicated source identity required here.

## Decision

`bind_distributed_aggregate_fragment` is the only implemented constructor from runtime authority to
an executable aggregate dispatch. It borrows one acquire-loaded Manifest v2 snapshot, one committed
tablet placement, one destination schema, the exact Raft group, the planned admission, and the
requested projection for the duration of the call. The result owns all emitted values.

Binding reruns proof-bound admission validation. It then requires the serving node to be a member
of a canonical placement, rejects a conflicting leader hint for a leader-linearizable read, and
copies that placement epoch. The selected Manifest v2 tablet must exact-match database, table,
tablet, Raft group, admitted applied position, and destination recovery schema ID/version. A later
or earlier durable position is not substituted for the admitted snapshot.

The current specialized execution path requires all four pushdowns, a nonempty unique in-range
projection, and a projected Float64 aggregate input. An event-time predicate with no bounds is not
canonical. Binding performs no I/O or publication.

## Consequences and validation

Distributed requests cannot be assembled by mixing Manifest v1/WAL state with Raft admission or by
copying independent live fields without a snapshot pin. A follower whose durable Manifest lags its
admission fails unavailable until the exact boundary is published; reading newer durable state for
an older admission also fails rather than weakening snapshot identity.

Focused tests build a real pinned Manifest v2 publication and prove successful dispatch encoding,
then reject mismatched applied position, group, replica placement, aggregate type, and duplicate
projection. Full query, sanitizer, and installed-consumer checks cover integration.

The committed-placement argument is still a borrowed value. The caller must obtain it from an
acquire-consistent committed metadata view; the worker independently reproves its local current
placement before touching storage.

The first single-key grouped constructor reuses this complete boundary and adds FLOAT64 key-type
proof in [ADR 0326](0326-authority-bound-grouped-float64-fragment.md); it does not weaken or replace
the ungrouped executable dispatch.

Invariants 4–6, 10, 11, 14, and 18 apply.

## Migration and rollback

Callers constructing executable requests directly must migrate to the binder. Rollback may disable
remote execution, but must not fall back to Manifest v1 or accept a merely comparable Raft index.

## References

- [Distributed Aggregate Fragment v1](../formats/distributed-aggregate-fragment-v1.md)
- [Distributed Aggregate Fragment Dispatch v1](../formats/distributed-aggregate-fragment-dispatch-v1.md)
- [Explicit WAL and Raft commit identities](0072-explicit-wal-and-raft-commit-identities.md)
