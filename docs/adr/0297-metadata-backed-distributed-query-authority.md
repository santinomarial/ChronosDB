# ADR 0297: Metadata-backed distributed query authority

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB query, metadata, and distributed-systems maintainers
- **Extends:** [ADR 0170](0170-compatible-multi-tablet-manifest-snapshot-binding.md),
  [ADR 0279](0279-authoritative-tablet-group-binding.md)

## Context

The compatible distributed aggregate binder pins one Manifest v2 generation, but its caller had to
assemble a parallel vector of schema, placement, and tablet-group references. Those values could be
borrowed from different metadata publications even when every individual value was valid. The API
also accepted an admission assembled independently from the Raft observation that selected the
serving replica.

## Decision

`bind_metadata_backed_distributed_aggregate_snapshot` accepts one committed
`MetadataCatalogSnapshot`, one plan-ordered runtime proof per fragment, and the aggregate projection.
It resolves the active schema, tablet placement, and immutable tablet-to-group binding from that
single catalog. Canonical catalog ordering and uniqueness are checked before binary lookup.

Each runtime observation must exact-match the resolved group, carry ordered indexes, and report
stable voters and committed voters identical to the committed placement. Joint, finalizing, or
pending membership is unavailable. The binder derives the admission by policy:

- leader-linearizable requires the planned node to be the observed current leader and an applied
  read barrier from that exact term;
- follower-bounded-stale requires a noncandidate replica, an in-placement observed leader, and an
  explicit leader-commit position no earlier than the replica's local committed position; and
- local-eventual permits the observed local applied state but rejects stronger proof fields.

The derived values enter the existing compatible snapshot binder, which revalidates policy,
Manifest generation, exact durable position, schema, placement, projection, and group source. The
result owns only the pinned Manifest epoch and dispatch values; metadata and observation references
are borrowed for the binding call and never escape.

## Consequences

A coordinator no longer assembles executable distributed aggregate authority from independently
borrowed metadata fields. Runtime barrier and leader-commit acquisition remain caller-owned because
they require asynchronous group interaction. Endpoint resolution also remains a transport concern.

Binding is `O(catalog validation + fragments log catalog + projected columns)`. It performs no I/O,
publishes no state, and adds no durable or network format. Allocation is bounded by the existing
fragment and projection limits and fails explicitly.

## Alternatives considered

- **Trust caller-assembled references:** preserves a mixed-publication authority hazard.
- **Copy the complete catalog into the compatible snapshot:** unnecessary after dispatch values are
  constructed and would retain unrelated metadata.
- **Infer a linearizable proof from leader role:** leadership alone does not prove a current quorum
  read or applied coverage.
- **Treat a follower's commit index as a leader-commit observation:** this can understate actual lag
  and falsely satisfy bounded-stale admission.

## Failure modes and operations

Malformed catalog ordering or identity is corruption. Missing active schema, placement, binding, or
stable proof is unavailable. Invalid policy/proof combinations are rejected before any worker
request exists. Operators should therefore distinguish transient leadership/reconfiguration from a
corrupt metadata publication; this pure binding layer emits no independent metrics.

## Validation

Focused tests bind two distinct groups from one catalog and Manifest generation, then cover exact
schema/group/placement projection, bounded-stale and local-eventual derivation, stale barrier terms,
joint membership, and missing group authority. Existing fragment tests continue to reject durable
position, schema, source, projection, and placement mismatches.

Invariants 4–6, 11, 14, and 18 apply.

## Migration and rollback

Production coordinators should use the metadata-backed binder instead of constructing
`DistributedAggregateSnapshotFragmentBinding` vectors. The lower-level binder remains available for
focused composition and tests. Rolling back is wire-compatible but restores the mixed-publication
risk and is not recommended.

## References

- [Compatible multi-tablet Manifest snapshot binding](0170-compatible-multi-tablet-manifest-snapshot-binding.md)
- [Proof-bound distributed read admission](0115-proof-bound-distributed-read-admission.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
- [Phase 16 roadmap](../roadmap.md#phase-16--distributed-query-execution-and-rebalancing)
