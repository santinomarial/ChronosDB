# ADR 0003: Single-Node-First Development Order

- **Status:** accepted
- **Date:** 2026-08-01
- **Owners:** ChronosDB storage and distributed-systems maintainers

## Context

Replication preserves and orders state-machine inputs; it cannot repair a state machine that applies the same input nondeterministically, installs partial files, loses acknowledged writes, or compacts rows incorrectly. Building Raft while WAL, recovery, CSEG, manifests, version visibility, and query semantics are still unstable would multiply failure states and make bugs difficult to attribute.

Distribution nevertheless affects early identities and interfaces: tables need tablet boundaries, committed positions need room for replicated histories, and storage apply must be deterministic. The project therefore needs an ordering rule, not a ban on forward-compatible design.

## Accepted decision

ChronosDB will validate WAL, recovery, CSEG, manifests, flush/checkpointing, compaction, SQL semantics, and snapshot visibility in a single-node engine before adding Raft.

Distribution must reuse the same logical storage state machine and durable part/manifest model. It may introduce replicated ordering and distributed coordination, but it must not create a separate persistence engine with different row, recovery, or visibility semantics.

Distributed interfaces and identity requirements may be documented early. Distributed implementation cannot bypass incomplete single-node correctness or weaken a phase gate. Every roadmap phase has correctness and testing gates, and compilation alone is never an exit condition.

## Detailed rationale

A deterministic, crash-safe single-node state machine provides a tractable oracle for later Raft simulation: committed log entries can be replayed into the same apply interface and compared across replicas. It also isolates filesystem crash consistency from consensus safety before combining them. This sequence reduces the state space at each step while keeping tablet identity and commit-position concepts visible from the beginning.

Adding Raft first creates a serious diagnostic risk: divergent replicas could result from consensus, nondeterministic application, undefined behavior, corrupt recovery, or incompatible snapshots. It also encourages treating quorum replication as a substitute for local correctness, even though replicas can faithfully reproduce the same storage bug.

## Alternatives considered

- **Build Raft and storage concurrently:** might expose interfaces early, but couples two immature failure domains and makes deterministic diagnosis substantially harder.
- **Start with a distributed key-value layer:** accelerates a demo but replaces ChronosDB's storage state machine and prevents validation of its defining CSEG, manifest, and temporal contracts.
- **Use a separate replicated persistence engine later:** duplicates recovery and visibility semantics and creates two sources of truth.
- **Delay all distributed thinking:** simplifies early code but risks identities and interfaces that cannot express tablet histories or replicated commit positions. Documentation and interface review are allowed before implementation.

## Consequences

- Raft implementation waits until the single-node milestones and relevant roadmap gates pass.
- Single-node apply/recovery interfaces must be deterministic and suitable for simulation.
- Tablet and commit-position identities may include future-proof fields even while operating locally, when justified by a specification.
- Distribution does not excuse reopening settled storage semantics without a superseding ADR.
- The schedule favors slower early breadth in exchange for attributable failures and reusable correctness evidence.

## Affected invariants

This ordering directly supports invariants [1–8, 11, and 16](../architecture/invariants.md) before replication expands their state space. It prepares invariants 4 and 5 for deterministic Raft application and preserves invariant 18 by preventing a distributed shortcut around correctness gates.

## Validation plan

- Require all applicable single-node roadmap gates and crash suites to pass before Phase 14 implementation begins.
- Define one deterministic logical apply model used by normal execution, WAL recovery, and later Raft simulation.
- Replay identical committed operation sequences into independent instances and compare manifests, parts, visible rows, and applied positions.
- Record phase evidence beyond compilation: fault injection, corruption handling, differential queries, sanitizers, and reproducible measurements.

## Deferred decisions

The precise apply interface, tablet partitioning scheme, commit-position encoding, snapshot transfer representation, Raft protocol details, membership changes, and multi-Raft scheduling remain deferred. Their designs must reuse rather than bypass the validated storage state machine.

## Migration or reversal implications

This is a development-order constraint, so no deployed data migration exists. Introducing distribution early requires a superseding ADR demonstrating equivalent single-node evidence and an isolation plan for combined failures. Once replicated storage exists, replacing the shared state machine would be a major format and protocol migration.

## References

- [Roadmap](../roadmap.md)
- [Architecture overview](../architecture/overview.md)
- [Product vision milestones](../product/vision.md)
