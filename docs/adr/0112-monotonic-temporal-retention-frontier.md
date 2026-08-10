# ADR 0112: Monotonic temporal retention frontier

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB temporal-history maintainers

## Context

`TemporalSnapshotProvider::compact_history` retained one physical predecessor per logical identity,
but removed every system-time index entry before the advertised retention boundary. A query exactly
at that boundary could therefore select commit position zero and appear empty even though the
required predecessor row remained. The provider also accepted later calls that moved either
retention boundary backward, falsely promising history already discarded by an earlier compaction.

Retention uses two independent lower bounds: the oldest commit position still observable by pinned
or coordinated readers, and the earliest system time still promised by policy. A version can be
discarded only when older than both, while the newest predecessor required to evaluate the exact
system-time boundary remains queryable.

## Accepted decision

The provider records the greatest accepted oldest-observable commit position as well as its existing
earliest-retained system time. `compact_history` rejects either frontier moving backward or moving
ahead of current committed state. For each identity it retains the existing newest predecessor
logic. In the time index it now retains the greatest timestamp-to-position entry strictly before
the new time frontier plus every entry at or after it. Thus an exact-boundary request resolves the
state produced by the last earlier commit, while any earlier request still fails `NOT_FOUND` before
index lookup.

Compaction returns `TemporalHistoryCompactionReport`, including prior and installed frontiers plus
removed and retained version counts. Validation completes before mutation. Erasure performs no
allocation, and copied query snapshots remain independent of provider storage.

This boundary does not authorize durable CSEG/Manifest deletion by itself. A higher retention owner
must still derive the supplied frontiers from policy, active snapshots, subscriptions, backups, and
source-specific recovery requirements before invoking it.

## Consequences

- The earliest promised system-time query returns the correct predecessor state.
- Repeated or advancing compaction is idempotent/monotonic; history promises cannot silently widen.
- Operators receive exact observability for removed/retained versions and frontier changes.
- Durable temporal part replacement, generation pins, and file reclamation remain separate proof
  and installation work.

## Alternatives considered

- **Delete every earlier time index entry:** caused the exact-boundary empty-state defect.
- **Keep the entire time index:** correct but retains unbounded obsolete indexing state.
- **Permit frontier regression when no versions were removed:** outcome-dependent promises are hard
  to audit and can become unsafe after a prior successful removal.
- **Infer the time predecessor from retained row histories:** duplicates a global commit-time index
  scan on every query and complicates tables whose identities have different predecessor commits.

## Affected invariants and validation

Invariants 6–8, 11, 13, and 18 apply. Focused tests prove exact-boundary resolution after first and
advancing compaction, removal counts, retained counts, precise pre-boundary expiry, and rejection of
both position and time regression without state loss. Multi-owner authorization, durable CSEG v2
replacement, active generation pins, crash injection, and long generated histories remain later
feature/hardening work.
