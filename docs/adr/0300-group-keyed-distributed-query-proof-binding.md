# ADR 0300: Group-keyed distributed query proof binding

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB query, metadata, and replicated-service maintainers
- **Extends:** [ADR 0297](0297-metadata-backed-distributed-query-authority.md),
  [ADR 0299](0299-correlated-replicated-read-authority.md)

## Context

The replicated barrier returns authority in canonical Raft-group order, while a distributed plan is
in pruned tablet order. The metadata-backed aggregate binder previously required one proof already
reordered to match each plan fragment. A caller could accidentally pair a valid leader barrier and
observation from one group with a tablet bound to another group.

The barrier vector may also include non-tablet groups, especially the metadata group used by the
packaged query gate. Requiring its shape to equal the fragment set would couple two independent
authority scopes.

## Decision

`DistributedAggregateGroupReadAuthority` is the shared owning value for one exact
`GroupReadBarrier` and the ordered `RaftGroupObservation` that validated it. The replicated service
aliases and returns this query-layer type directly, avoiding a conversion or reference-lifetime
seam.

`bind_group_backed_distributed_aggregate_snapshot` accepts a canonical unique group-sorted
authority span. For each planned tablet it resolves the immutable tablet-to-group binding from the
same committed metadata snapshot, binary-selects that exact group's authority, and constructs the
plan-ordered proof vector internally. Authority for unrelated groups is ignored. Missing selected
groups, missing committed tablet bindings, duplicate/reordered groups, mismatched barrier and
observation groups, or mismatched terms fail before dispatch construction.

The selected proofs then enter the existing metadata-backed binder. That layer still validates
current leader identity, stable membership equal to placement, applied barrier coverage, active
schema, Manifest v2 source/durable position, and projection. The new join neither weakens nor
duplicates those gates.

## Consequences

The complete leader-linearizable authority chain is now group-keyed from Raft completion through
tablet metadata to plan-ordered dispatches. Callers no longer manually reorder proofs. The group
authority vector owns all observations and barriers; the binder borrows it only for the call and
returns dispatch-owned values.

The additional join is `O(fragments log bindings + fragments log authorities)` and allocates one
bounded temporary proof vector. It performs no I/O and changes no durable or wire format.

## Alternatives considered

- **Return barriers in tablet order:** rejected because the Raft owner is configured by groups and
  does not own tablet metadata or query pruning.
- **Require exact vector shape/order equality:** rejected because metadata and other non-tablet
  groups may legitimately share the read gate.
- **Use tablet IDs as Raft group IDs:** rejected because ADR 0279 defines a separate immutable
  committed identity bridge.
- **Expose observation references from the service:** rejected because the completed authority must
  outlive single-thread-affine runtime state safely.

## Failure modes and operations

Noncanonical caller authority is invalid. Missing committed tablet binding or selected group proof
is unavailable. Later metadata, membership, schema, or Manifest disagreement retains the existing
corruption/unavailable classification. No partial compatible snapshot is returned.

## Validation

A two-tablet test supplies a group-sorted authority vector containing an unrelated metadata group,
binds both tablet groups in plan order, and verifies exact dispatch group identity. Reversed
authority and a vector missing one selected group fail closed. Existing metadata-backed tests cover
term, role, membership, schema, placement, durable position, and policy failures; service tests
prove the exact shared authority type is returned in both local and transported modes.

Invariants 4–6, 11, and 18 apply.

## Migration and rollback

Leader-linearizable distributed coordinators should pass the `await_authority` result directly to
the group-backed binder. Lower-level plan-ordered binders remain available for bounded-stale and
local-eventual embeddings. Rolling back restores the caller-owned group-to-tablet join and its
misbinding risk.

## References

- [Authoritative tablet-to-Raft-group binding](0279-authoritative-tablet-group-binding.md)
- [Metadata-backed distributed query authority](0297-metadata-backed-distributed-query-authority.md)
- [Correlated replicated read authority](0299-correlated-replicated-read-authority.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
