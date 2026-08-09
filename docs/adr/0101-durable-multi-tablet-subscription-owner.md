# ADR 0101: Durable multi-tablet subscription owner

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB live-query and recovery maintainers
- **Extends:** [ADR 0100](0100-durable-subscription-checkpoint-generations.md)

## Context

Atomic checkpoint files alone do not ensure that coordinator state, generation advancement, and
source-log retention move as one recovery contract. A service owner must restore the exact retained
admission order and must not release source history based on state that failed to install.

## Accepted decision

`DurableMultiTabletSubscription` exclusively composes one `MultiTabletSubscriptionManager` with one
locked checkpoint store for the same database, table, plan, schema/version, and canonical
tablet/WAL set. Configuration with disagreeing identities fails before state is served.

A new owner begins dirty and has no published durable retention frontier. Each successfully applied
committed change marks it dirty. Checkpointing captures exact coordinator state, preallocates the
candidate per-source expiry vector, and installs the next bound generation. Only after installation
and directory synchronization succeed does the owner publish the new generation and retention
frontiers. A clean checkpoint call is a byte-identical idempotent retry of the current generation.

Reopen selects the latest valid durable generation and restores the coordinator at its recorded
per-source latest positions. Those positions are the replay boundary; newer external committed-log
entries must be reapplied consecutively afterward. Caller-provided initial sequence values do not
override a selected checkpoint. Subscriber socket/session state is ephemeral, while authenticated
tokens resume against the restored retained suffix.

The owner also starts ADR 0102 historical execution directly from a prepared or ADR 0103 recovered
plan. It retains mutable-manager encapsulation: plan/schema validation, registration, boundary
capture, snapshot execution, END_STREAM, and READY occur without exposing a path that could publish
committed changes while bypassing checkpoint dirty tracking. The durable owner and query resource
context outlive the returned snapshot driver.

## Consequences and alternatives

The returned durable expiry positions are safe coordinator recovery frontiers, not permission by
themselves to delete an upstream log. The eventual retention owner must also account for Raft/WAL,
snapshots, active readers, and topology transitions.

Persisting active subscription queues was rejected: external delivery is at-least-once and tokens
already name acknowledged safe positions. Advancing retention from the in-memory manager before a
successful install was rejected because a crash could then remove the only suffix needed to resume.
Treating newer configured source tails as restored state was rejected because it would skip the
unreplayed interval after the checkpoint.

## Affected invariants and validation

Invariants 4, 8, 11, 12, 15, and 17 apply. Focused tests cover exact generation recovery,
admission-order token replay, idempotent checkpoint retry, omission of uncheckpointed tail state,
continued consecutive application, no retention-frontier advancement on install failure, and one
recovered-plan-to-global-snapshot composition without mutable-manager escape.
Concurrent source replay, service shutdown, real retention-manager wiring, and crash/fault schedules
remain Phase 18 work.
