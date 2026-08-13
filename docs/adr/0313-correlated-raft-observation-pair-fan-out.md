# ADR 0313: Correlated Raft observation pair fan-out

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB cluster, query, and networking maintainers
- **Extends:** [ADR 0312](0312-finite-multi-address-raft-observation-acquisition.md),
  [ADR 0303](0303-correlated-follower-read-proof-binding.md)

## Context

One finite remote observation acquisition still left embeddings to coordinate the selected leader
and follower. Sequential or ad hoc glue could expose one side early, fail to cancel the survivor,
pair different groups or terms, reuse a correlation identity, or sleep past either child's network
or retry deadline.

## Decision

`RaftObservationTcpPairAcquisition` is a move-only single-threaded poll owner for two independently
bounded acquisitions: one selected leader and one selected follower. Their requests must share one
source and group, name distinct target nodes, and use distinct nonzero correlation identities. Each
child retains its own authenticated route, address rotation, retry budget, deadlines, and metrics.

Every pair poll first drives both children without blocking, then waits on both active descriptors
with a timeout shortened to the earliest child connect, handshake, exchange, or retry-backoff
deadline. It drives both children again after readiness. Thus both targets start before either can
consume the caller's blocking wait, and at most two descriptors are active.

Failure of either child cancels the running survivor before becoming terminal. Success is
all-or-nothing: both complete observations must pass the same shared
`is_valid_distributed_aggregate_follower_read_authority` predicate used by the query binder. That
requires a nonnil same group, distinct leader/follower nodes, matching nonzero term and leader,
ordered indexes, leader commit no earlier than follower commit, identical stable voter membership,
and no joint or pending membership. Only then does the owner publish one owning authority pair.

## Consequences and validation

Network and retained memory remain bounded by two child acquisitions. Independent retries cannot
combine partial observations, alter either selected node, or expose one successful side after the
other fails. A legitimate role, term, leader, membership, or index transition during acquisition
fails the pair as `UNAVAILABLE`; a caller must reacquire a fresh pair rather than infer stability.

A focused two-server mutual-TLS test proves both attempts start together, both ordered services run
once, no partial result is visible, and one complete same-term pair is returned with exact child
metrics. A second two-server test returns individually valid observations from different terms and
proves the pair fails closed only after both exact responses. A third test holds one follower TLS
handshake open while the leader returns a nonretryable status and proves the follower descriptor is
cancelled. These tests require approved host execution where sandbox policy forbids loopback bind.

Automatic selection of all required tablet pairs from committed placement, multi-pair acquisition,
packaged bounded-stale query construction, process integration, and broader failure matrices remain
incomplete.

Invariants 4–6, 10, 11, 14, 15, and 18 apply.

## References

- [Finite multi-address Raft observation acquisition](0312-finite-multi-address-raft-observation-acquisition.md)
- [Correlated follower read proof binding](0303-correlated-follower-read-proof-binding.md)
- [Authenticated Raft observation transport](0306-authenticated-raft-observation-transport.md)
