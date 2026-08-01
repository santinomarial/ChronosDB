# ADR 0007: Event Time, System Time, and Row Versioning

- **Status:** accepted
- **Date:** 2026-08-01
- **Owners:** ChronosDB temporal-semantics and storage maintainers

## Context

An event can describe when something happened, arrive later, be retried, and then be corrected after users have queried it. Collapsing these moments into one timestamp makes windowing, replay, audit, and “as known then” questions mutually inconsistent. Immutable parts also make in-place correction both unsafe and contrary to the storage model.

## Accepted decision

Each event table designates exactly one event-time column. Ingestion or receive time may be stored separately as ordinary or managed metadata, but neither determines commit visibility.

Every committed logical version has a system commit position and, if exposed by the later temporal specification, a system timestamp derived under a documented rule. Commit position is authoritative for ordering and resume boundaries; wall-clock equality must not create ambiguous visibility.

A deduplicated logical entity may have multiple physical versions. At a query snapshot, a current-state query selects the latest visible non-tombstoned version according to the entity identity and committed version order. Corrections append new versions rather than altering installed parts. Deletions append tombstone versions.

`FOR SYSTEM_TIME AS OF` is a planned first-class query capability. ChronosDB must answer both “what happened in event time?” and “what did the database know at this system-time boundary?” Event-time watermarking estimates source-time completeness for live operators; it does not commit, hide, or make a row visible in system time.

Compaction may discard an obsolete version only when no active snapshot, retained system-time history, subscription/recovery position, backup, or other declared retention owner can observe or require it. It may rewrite versions into new immutable parts but cannot alter their logical temporal meaning.

## Detailed rationale

Separating the two time axes gives late events their original business time while preserving an audit trail of when ChronosDB learned or corrected them. Append-only versions align with WAL replay and immutable CSEG installation. An authoritative commit position avoids depending on synchronized clocks for visibility and later maps naturally to per-tablet log order.

The latest-version rule provides ordinary current queries without erasing history. Tombstones make deletion participate in the same ordering, snapshot, compaction, and replication model as corrections.

## Alternatives considered

- **Only event time:** cannot express when a correction became visible or reproduce an earlier database view.
- **Only ingestion/system time:** destroys source-domain window and ASOF meaning for delayed events.
- **In-place updates:** conflict with immutable parts, active snapshots, crash-safe replacement, and audit history.
- **Store only the latest deduplicated row:** reduces space but makes system-time queries, correction audit, and snapshot-safe compaction impossible.
- **Use wall-clock timestamp alone for version order:** clock ties and regressions make boundaries ambiguous; commit positions remain authoritative.

## Consequences

- Storage carries entity identity, version order, and tombstone state in addition to event-time sort dimensions.
- Current scans must resolve visible versions across heads, base parts, and delta parts.
- System-time retention increases space and compaction work and must be explicit.
- Indexes and zone maps may need both event-time and version-aware metadata.
- Live windows may emit corrections after watermark progress according to a future delivery contract.

## Affected invariants

This decision is central to invariants [4, 6, 7, 9, 11, 12, 13, and 17](../architecture/invariants.md): ordered versions, stable snapshots, compaction equivalence, retry identity, retention safety, deterministic boundaries, dual-time corrections, and historical-to-live continuity.

## Validation plan

- Build a bitemporal reference model covering originals, duplicates, corrections, tombstones, clock ties, and late arrivals.
- Differentially test current and system-time-as-of queries at every commit boundary before and after flush/compaction.
- Pin old snapshots while compacting and assert all observable versions remain available.
- Verify watermark changes cannot alter system-time visibility.
- Compare live correction streams and historical recomputation for supported operators.

## Deferred decisions

Entity-key declaration, system timestamp representation/mapping, SQL boundary syntax and inclusivity, tombstone payload, correction conflict rules, history retention defaults, watermark/finalization policy, and cross-tablet system-time coordination remain deferred.

## Migration or reversal implications

Once CSEG and SQL expose versions, removing system-time history or changing boundary semantics would require a format/query migration and superseding ADR. Retention can be tightened only under an explicit policy that does not invalidate promised snapshots or resume positions. Additional temporal metadata may be added through versioned formats.

## References

- [Product vision: two dimensions of time](../product/vision.md)
- [Representative workloads](../product/workloads.md)
- [Architecture compaction and live plane](../architecture/overview.md)
- [Roadmap Phase 13](../roadmap.md)
