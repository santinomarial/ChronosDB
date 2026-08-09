# ADR 0068: Bounded live handoff and Resume Token v1

- **Status:** accepted
- **Date:** 2026-08-08
- **Owners:** ChronosDB live-query maintainers
- **Extended by:** [ADR 0089](0089-exact-logical-materialized-view-checkpoints.md) and
  [ADR 0094](0094-native-protocol-1-1-subscriptions.md) and
  [ADR 0095](0095-multi-tablet-subscription-delivery-order.md)

## Context

Phase 11 needs a gap-free committed snapshot boundary, bounded slow-consumer behavior, and a token
that cannot be silently moved across a database, plan, schema, or source lineage.

## Accepted decision

One shard-affine `SubscriptionManager` registers before selecting its latest committed source
position, buffers every subsequent committed logical change during snapshot delivery, and releases
that suffix only after snapshot completion. Poll is at least once until acknowledgment. Overflow
disconnects the subscriber at its last safe position without rejecting the source commit.

Resume Token v1 is the authenticated format in
[`resume-token-v1.md`](../formats/resume-token-v1.md). HMAC-SHA256 uses the existing maintained
OpenSSL boundary. Materialized views reuse the same committed-position model. Numeric window state
retains exact removable extrema and endpoints; watermarks never determine commit visibility.

## Consequences and alternatives

The current manager is single-source and in-memory. A wall-clock-only token and lossy overflow were
rejected because both violate continuity. Exactly-once external effects are not claimed. ADR 0089
defines exact logical view checkpoint state; durable bytes/installation and multi-tablet merge
ordering require later integration without changing v1 bytes silently.

## Affected invariants and validation

Invariants 4, 8, 11–13, 15, and 17 apply. Focused tests cover token round trip/tamper, snapshot-race
buffering, acknowledgment/resume, retained replay, overflow, removable aggregates, sliding/tumbling
windows, corrections, and finalization. Restart, network delivery, fan-out, and sustained retention
campaigns are deferred in `docs/development/deferred-validation.md`.
