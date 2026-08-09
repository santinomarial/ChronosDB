# ADR 0104: Schema-change subscription terminal boundary

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB live-query, recovery, schema, and networking maintainers
- **Extends:** [ADR 0068](0068-live-handoff-and-resume-token-v1.md),
  [ADR 0094](0094-native-protocol-1-1-subscriptions.md), and
  [ADR 0099](0099-multi-tablet-subscription-checkpoint-v1.md)

## Context

A subscription plan is bound to one exact schema identity and version. The managers previously
treated a committed change from another schema like subscriber overflow, while also retaining that
change in coordinator state. Overflow is resumable resource exhaustion, not semantic
incompatibility, and a mismatched retained change cannot be encoded in the schema-bound durable
checkpoint. A restart also must not make an invalid old plan resumable again.

## Accepted decision

The first consecutive committed change whose schema differs from the plan advances that source to
the observed committed position and terminally invalidates the coordinator. Active snapshot or live
subscribers enter the distinct `kSchemaChanged` phase, buffered output is discarded, and poll returns
`NOT_SUPPORTED`. Protocol termination must represent this phase as the already assigned Protocol
1.1 `SCHEMA_CHANGED` reason; it cannot be mislabeled as overflow. The terminal frame retains the last
acknowledged safe token for diagnostics and at-least-once accounting, but the invalidated manager
rejects that token and all new registrations. A newly prepared plan and snapshot are required.

A multi-tablet invalidation drops the entire old-plan retained suffix and expires every component
through its current latest position. This fail-closed global boundary is necessary because one
schema-bound physical plan covers the complete source vector. The incompatible change itself is not
a logical result under the old plan and is never delivered or retained as one.

Multi-tablet Subscription Checkpoint major 1 minor 1 records terminal invalidation in the first
previously reserved header byte. Such state is canonical only with no retained changes and every
expiry equal to latest. Minor-0 compatible checkpoints remain byte-identical and readable; the
durable generation envelope remains v1.0. Reopen restores terminal incompatibility before accepting
registration or resume.

## Consequences and alternatives

The invalidated owner stops accepting later source changes; its service owner must replace it with a
catalog-reprepared coordinator. Durable retention can advance through the installed invalidation
boundary but remains subject to the upstream log, topology, snapshot, and reader owners.

Treating schema changes as overflow was rejected because it advertises a resumable resource failure.
Silently applying a new schema under the old fingerprint was rejected because it changes query
semantics. Persisting the incompatible change in the old-plan suffix was rejected because its bytes
cannot satisfy the checkpoint schema contract. Reusing an ambiguous empty checkpoint without an
explicit flag was rejected because recovery could incorrectly reactivate the plan.

## Affected invariants and validation

Invariants 4, 8, 12, 14, 15, and 17 apply. Focused tests cover single- and multi-tablet terminal
phases, precise wire reason validation, old-token and new-registration rejection, minor-0 stable
bytes, minor-1 round trip, terminal checkpoint restoration, and durable reopen. Catalog/replay races,
process-crash cut points, mixed-binary compatibility, allocation sweeps, and topology-driven owner
replacement remain in the Phase 18 ledger.
