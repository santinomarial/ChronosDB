# ADR 0411: Mixed WAL/Raft subscription snapshot

- **Status:** accepted
- **Date:** 2026-08-20
- **Owners:** ChronosDB live-query, query, storage, and Raft maintainers
- **Extends:** [ADR 0102](0102-exact-multi-tablet-subscription-snapshot.md) and
  [ADR 0410](0410-raft-subscription-snapshot-and-prefix-reclamation.md)

## Context

Source-tagged subscription state can bind one canonical continuation vector containing both WAL
record sequences and Raft group indexes. Historical execution nevertheless accepted only
homogeneous source sets. Treating separately published WAL and Raft state as one scalar database
epoch would invent an authority that neither subsystem provides, while executing one complete plan
per authority would change global aggregate, ordering, latest, and limit semantics.

## Accepted decision

The exact registered source vector is the mixed historical boundary. Registration occurs before
any publication acquisition. All WAL members must exact-match their components in one aggregate
database storage publication, including database, table, tablet, WAL identity, and applied record
sequence. Every Raft member is acquired independently from the worker-hosted application and must
exact-match table, tablet, group, and applied log index. Acquisition or validation failure abandons
the complete registration before READY; no subset is emitted.

After the complete vector validates, the query adapter creates one raw source per canonical member.
WAL children share one query-accounted reservation for the aggregate publication. Raft children
retain their immutable tablet generations. The adapter concatenates children in canonical tablet
order and instantiates the checked physical pipeline exactly once above them. The service requires
both storage and Raft adapters for a mixed source set and otherwise uses the existing homogeneous
paths.

This decision does not claim that the WAL publication and Raft publications share a scalar time or
transaction epoch. Their product vector is the only cross-authority identity and remains the
continuation boundary carried by subscription state and tokens.

## Consequences and alternatives

Mixed snapshots now preserve global SQL semantics and the gap-free historical-to-live handoff
without weakening either source authority. Sequential acquisition can observe components at
different wall-clock instants, which is valid because each must equal the component registered
before acquisition. Post-registration commits remain buffered by the coordinator.

Fabricating an aggregate epoch, accepting at-least boundaries, joining independently finalized
results, and reading mutable Raft state from the subscription thread were rejected. Cross-source
transactional atomicity would require a separate accepted distributed commit/snapshot contract; it
is not implied here.

## Affected invariants and validation

Invariants 1, 4, 6, 8, 10, 11, 12, 15, and 17 apply. Focused tests combine a real applied Raft
append with an exact WAL publication under one global aggregate, reject a stale Raft vector
component with allocation-independent abandonment, and route a new mixed query through the
reactor-facing service. Header self-containment, full suites, sanitizers, formatting, and configured
static analysis remain part of the implementation verification record.
