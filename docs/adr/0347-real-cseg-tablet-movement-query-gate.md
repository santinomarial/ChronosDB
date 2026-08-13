# ADR 0347: Real-CSEG tablet-movement query gate

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB service, query, storage, cluster, and Raft maintainers
- **Extends:** [ADR 0154](0154-physical-ownership-gated-tablet-movement-readiness.md),
  [ADR 0338](0338-owned-real-cseg-grouped-query-tcp-service.md)

## Context

The existing query/movement/query gate exercised learner-first movement and real mutual TLS, but its
workers returned deterministic aggregate constants. Separately, the production inbound owner read a
real installed CSEG, but only on one node without movement. Neither gate proved that the exact
checksummed bytes accepted by movement could be reopened from a distinct target database root and
queried under the post-promotion placement authority.

## Decision

The focused integration gate uses a canonical Raft-sourced temporal CSEG as the movement snapshot
payload. `TabletMovement` validates its whole CRC, reaches catch-up, records externally committed
target promotion and source removal, and exposes the exact completed bytes. The test installs those
bytes and the matching Manifest in a distinct target root, reopens them through `ManifestStorage`,
and acquires a pinned publication from that target storage.

A separate production `ReplicatedDistributedGroupedQueryTcpServer` on target node 13 obtains that
owning target snapshot and validates the new placement epoch before executing the grouped worker.
The existing mTLS client sends a dispatch rebound to node 13 and epoch 14. Result publication remains
terminal-only and exact-correlated; no query result is inferred from movement metadata.

## Consequences and validation

The focused service test now proves that the checksummed CSEG bytes carried through learner-first
movement reopen from a distinct target root and produce the same key and full aggregate state over
the production authenticated TCP worker stack. It also proves target certificate authentication,
fresh target authority acquisition, one completed target connection, and deterministic shutdown.

The test runs two server owners in one process and still models promotion/removal as externally
committed milestones. It does not supply the packaged three-process native SQL/data-plane workflow,
process loss/failover, automatic metadata refresh, or broad failure/measurement evidence. No Phase
16 exit gate is claimed. No durable or network format changes.

Invariants 2–6, 10, 11, 14, and 18 apply.

## References

- [Physical-ownership-gated tablet movement readiness](0154-physical-ownership-gated-tablet-movement-readiness.md)
- [Owned real-CSEG grouped-query TCP service](0338-owned-real-cseg-grouped-query-tcp-service.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
- [Architecture invariants](../architecture/invariants.md)
