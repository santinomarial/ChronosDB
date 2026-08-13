# ADR 0355: Compatible multi-tablet vector snapshot

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, manifest, and distributed-systems maintainers
- **Extends:** [ADR 0170](0170-compatible-multi-tablet-manifest-snapshot-binding.md),
  [ADR 0354](0354-authority-bound-distributed-vector-fragment.md)

## Context

Binding vector fragments independently allows individually valid requests to name different
Manifest publications. A general multi-tablet query needs one owner that pins the complete database
epoch behind every plan-ordered dispatch and rejects mixed order before work starts.

## Decision

`bind_compatible_distributed_vector_snapshot` accepts one `DistributedVectorQueryPlan`, one owning
`TemporalDatabaseStorageSnapshot`, and one authority binding per planned tablet. It requires exact
binding count and order, unique nonzero tablet identities, finite fragment count, and a bounded sum
of projection ordinals. Every entry delegates to the single-fragment vector authority binder
against the same Manifest snapshot.

The move-only `CompatibleDistributedVectorSnapshot` owns that snapshot and the plan-ordered dispatch
vector. Every dispatch must repeat the owner's database identity and generation. Different Raft
groups retain their independently proved applied positions; compatibility does not invent a
cross-group log index or transaction instant.

The default aggregate projection budget is 65,536 ordinals. Callers may raise it only to the hard
product of maximum fragments and schema columns. Binding performs no I/O or publication.

## Consequences and validation

The existing two-tablet Manifest-backed test now also constructs a compatible vector owner, proves
the Manifest generation remains pinned, and checks exact plan/tablet/group order in both dispatches.
Reversed bindings and a total projection budget one ordinal below demand fail before publication.
All existing aggregate/grouped/metadata binding cases pass unchanged. Header self-containment and
installed consumption cover the public owner.

Metadata-backed vector batch construction, worker execution, result coordination, request partial
I/O, authenticated transport, and process integration remain incomplete. No Phase 16 exit gate is
claimed.

Invariants 4–6, 11, 14, 15, and 18 apply.

## References

- [Compatible multi-tablet Manifest snapshot binding](0170-compatible-multi-tablet-manifest-snapshot-binding.md)
- [Authority-bound distributed vector fragment](0354-authority-bound-distributed-vector-fragment.md)
- [Manifest v2](../formats/manifest-v2.md)
