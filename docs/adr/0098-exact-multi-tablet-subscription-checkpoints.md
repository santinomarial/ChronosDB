# ADR 0098: Exact multi-tablet subscription checkpoints

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB live-query maintainers
- **Extends:** [ADR 0068](0068-live-handoff-and-resume-token-v1.md) and
  [ADR 0095](0095-multi-tablet-subscription-delivery-order.md)

## Context

The multi-tablet coordinator records a replayable cross-tablet admission order in memory. Reopening
only the independent tablet logs cannot reconstruct that interleaving. Before defining durable
bytes and installation, recovery needs one exact logical state boundary that can be validated
without silently filling gaps.

## Accepted decision

`MultiTabletSubscriptionCheckpoint` binds database, table, plan fingerprint, schema identity and
version; captures the canonical source vector with each latest and expired-through sequence; and
copies every retained immutable logical change in coordinator admission order. It excludes token
MAC keys, active subscriber buffers, socket state, and unacknowledged delivery attempts. Those are
external or reconstructible from an authenticated client token and the retained suffix.

Checkpoint capture runs on the coordinator owner and copies one coherent state. Restore requires a
caller-proven source configuration whose canonical tablet/WAL membership and latest sequences match
exactly. It validates every expiry frontier, schema, operation, result-key/payload rule, configured
byte/count bound, and consecutive per-source sequence while preserving the recorded global order.
Every source must end exactly at its declared latest sequence. A missing, duplicated, reordered
within-source, foreign, oversized, or incompatible change fails the whole restore.

No subscriptions are restored implicitly. A client presents its authenticated Resume Token; the
reconstructed manager validates it and assigns replay delivery ordinals from its safe sequence over
the exact retained admission order.

## Consequences and alternatives

This is a logical recovery checkpoint, not yet a durable-format or crash-safety claim. The next
storage layer must version and checksum exact bytes, bind database/table/plan/schema/source identity,
install atomically, and never expose a partial checkpoint.

Re-merging tablet logs by record sequence was rejected because it invents a different order.
Persisting active subscriber buffers was rejected because acknowledged client state is already
represented by Resume Tokens and in-flight delivery is at least once. Accepting a checkpoint suffix
with holes was rejected because recovery could then omit a committed result.

## Affected invariants and validation

Invariants 4, 8, 11, 12, 15, and 17 apply. Focused tests checkpoint A/B/A/B admission order after a
partial acknowledgement, reconstruct a fresh manager, resume from the pre-checkpoint token, and
observe exactly B/A/B with the original delivery ordinals. Removing one retained record makes
restore fail. Durable codec corruption, crash installation, generation selection, allocation
sweeps, and restart concurrency remain in the Phase 18 ledger.
