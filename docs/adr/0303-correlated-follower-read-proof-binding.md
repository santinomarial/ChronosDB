# ADR 0303: Correlated follower read proof binding

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB query, Raft, and distributed-systems maintainers
- **Extends:** [ADR 0115](0115-proof-bound-distributed-read-admission.md),
  [ADR 0297](0297-metadata-backed-distributed-query-authority.md)

## Context

Follower-bounded-stale binding accepted a follower observation and a separate scalar
leader-commit position. The binder validated the follower against committed placement, but the
scalar did not structurally identify the group, leader, term, or membership that produced it. A
caller could therefore pair an otherwise valid follower snapshot with an unrelated commit frontier.

## Decision

`DistributedAggregateFollowerReadAuthority` owns one leader observation and one follower
observation. `bind_follower_group_backed_distributed_aggregate_snapshot` accepts a canonical unique
group-sorted vector of those pairs and supports only follower-bounded-stale plans.

Every pair must identify the same nonnil group and nonzero term, name distinct nodes, show the
leader as self-led and the follower as following that exact leader, carry ordered indexes with the
leader commit no earlier than the follower commit, have no joint or pending membership, and report
identical voters and committed voters. For each planned tablet the binder resolves its immutable
group from the committed catalog, selects the exact pair, and derives the leader-commit frontier
from the leader observation itself.

The resulting plan-ordered follower proofs enter the existing metadata-backed binder. That layer
still requires follower membership equal to placement, exact Manifest durable coverage of the
follower applied index, an in-placement current leader, the declared lag bound, active schema, and
projection compatibility.

## Consequences

The bounded-stale binder no longer accepts a naked leader-commit scalar at its group-keyed public
boundary. Observation transport and freshness acquisition remain embedding responsibilities; this
change proves their correlation once supplied. Unrelated authority groups may share the vector and
are ignored after canonical structural validation.

Validation is linear in the authority vector plus `O(fragments log bindings + fragments log
authorities)` lookup and one bounded temporary proof vector. It performs no I/O and changes no
durable or wire format. Allocation failure is explicit resource exhaustion.

## Validation

A follower-bounded-stale test supplies a same-term leader/follower pair, derives the leader commit
without a caller scalar, binds the exact follower Manifest position, and verifies the serving node
and commit frontier. A term mismatch fails before dispatch construction. Existing tests continue to
cover lag excess, missing commit evidence, placement, schema, and local-eventual behavior.

Invariants 4–6, 11, 14, and 18 apply.

## Migration and rollback

Follower coordinators should prefer the group-backed pair binder after acquiring observations.
Rolling back restores the scalar seam without changing persisted or network bytes.

## References

- [Proof-bound distributed read admission](0115-proof-bound-distributed-read-admission.md)
- [Metadata-backed distributed query authority](0297-metadata-backed-distributed-query-authority.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
