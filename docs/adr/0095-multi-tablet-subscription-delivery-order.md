# ADR 0095: Multi-tablet subscription delivery order

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB live-query and distributed-systems maintainers
- **Extends:** [ADR 0010](0010-tablets-raft-and-multiplexed-log-storage.md),
  [ADR 0068](0068-live-handoff-and-resume-token-v1.md), and
  [ADR 0094](0094-native-protocol-1-1-subscriptions.md)

## Context

Independent tablet WAL/Raft histories have authoritative per-tablet order but no global database
commit index. Sorting unrelated record sequences or wall clocks would invent an order that cannot be
proved without blocking an active tablet behind an idle one. A multi-tablet subscription still needs
one reproducible delivery sequence, an exact snapshot boundary vector, and gap-free retained replay.

## Accepted decision

`MultiTabletSubscriptionManager` is one thread-affine coordinator for one exact database, table,
plan fingerprint, schema version, and fixed tablet/WAL source set. It canonicalizes sources by tablet
identity and begins each at a caller-proven committed sequence. Registration atomically captures the
coordinator's complete latest position vector before any later owner call can publish a change.

Each tablet's published changes must be consecutive in its own committed log. Cross-tablet calls are
serialized by the coordinator; that call/admission order becomes the authoritative subscription
delivery order and is retained with immutable changes. It is explicitly not a database commit order,
event-time order, or statement about causality between tablets.

Acknowledgement advances each source component only when its delivered change leaves the buffered
prefix. Resume tokens carry the canonical complete vector plus the safe delivery sequence. Recovery
validates the exact database, plan, schema, source membership, and WAL lineage, rejects positions
ahead of committed state, then filters the retained admission-order log against each safe source
component. If any source has evicted a change beyond the token's component, resume fails as expired;
it never starts at the current tail.

The coordinator is bounded by the same subscription, retention, change, and per-consumer limits as
the single-tablet manager. Subscriber allocation or buffer failure overflows only that subscriber;
retention admission failure rejects the publish attempt for retry before advancing source state.
Protocol 1.1 uses the existing vector Resume Token and per-change complete source position.

## Consequences and alternatives

Different executions may observe independent tablet commits in different coordinator admission
orders, but one admitted history and every replay of it use the same delivery sequence. A durable
service restart therefore needs either this retained order durably recorded or a new handoff/snapshot;
per-tablet logs alone cannot reconstruct the prior interleaving.

The source set is immutable for one coordinator. A split, merge, or lineage change creates a new
plan/coordinator and snapshot; placement movement that preserves logical source identity does not
invent a new ordering coordinate.

Lexicographic `(record_sequence, tablet_id)` merge was rejected because an idle tablet can later
produce a smaller sequence and would block safe output indefinitely. Wall-clock merge was rejected
because clocks are not commit authority. A global Raft group was rejected by ADR 0010 because it
would serialize unrelated tablets merely to manufacture total order.

## Affected invariants and validation

Invariants 4, 8, 11, 12, 15, and 17 apply. Focused tests start sources at different committed
boundaries, register from unsorted configuration into a canonical vector, publish an A/B/A order
during snapshot, acknowledge through B, publish B, and resume the exact remaining A/B delivery
sequence. They also reject per-source gaps, wrong plan/source lineage, and expiry of only one vector
component. Cross-owner service wiring, durable admission-order restart, topology transitions,
allocation sweeps, fan-out, and deterministic scheduling campaigns remain in the Phase 18 ledger.
