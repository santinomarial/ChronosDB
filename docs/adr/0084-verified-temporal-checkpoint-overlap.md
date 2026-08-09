# ADR 0084: Verified temporal checkpoint overlap

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB temporal recovery and storage maintainers
- **Extends:** [ADR 0083](0083-manifest-v2-temporal-wal-recovery.md)

## Context

ADR 0083 composed one selected Manifest v2 tablet with its WAL only when the global physical
checkpoint exactly equaled the tablet durable position. A valid global reclaim checkpoint can trail
that per-tablet boundary. Applying the intervening commands would duplicate CSEG-restored versions,
while silently skipping them would fail to prove that the selected Manifest and WAL describe the
same committed history. Retention can also reclaim only some rows from a pre-boundary command while
preserving predecessor rows that remain visible at the earliest supported system time.

## Accepted decision

The single-WAL-tablet startup owner now accepts a global checkpoint at or behind the selected
tablet's durable position. WAL recovery starts from the exact physical checkpoint and retains its
two-pass structural and schema preflight. During replay:

1. commands in `(global checkpoint, tablet durable position]` are materialized with their enclosing
   WAL identity and sequence and verified without publication;
2. every physically retained row must match the restored timestamp, logical identity, source
   coordinate, operation metadata, and scalar storage exactly;
3. only physically absent rows from commands before the explicit retained-system-time boundary may
   be treated as reclaimed; and
4. commands after the tablet durable position are applied in WAL order as the live suffix.

Recovery additionally proves that the reopened WAL reaches beyond the tablet durable position.
The report records the global checkpoint, tablet durable position, verified covered-command count,
and applied suffix-command count. WAL segment reclamation remains bounded by the global checkpoint,
not the later tablet position.

## Consequences and alternatives

An equal checkpoint remains the zero-overlap special case. No durable format changes. A mismatched
retained row, incomplete retained row set at or after the retention boundary, malformed covered
command, or WAL ending before the durable boundary fails recovery before publication.

Replaying overlap was rejected because it duplicates durable history. Trusting the Manifest without
reading available covered WAL records was rejected because it would not detect a mismatched durable
pair. Requiring all pre-boundary command rows to remain was rejected because authorized retention
may have reclaimed them; ignoring retained predecessor rows was rejected because those rows remain
query-visible at the retention boundary.

The owner remains deliberately limited to one WAL tablet and Temporal Mutation Command v1 records.
Multiple-tablet routing, mixed application dispatch, Raft application snapshots, and topology epochs
need a database-level application-snapshot contract rather than inference inside this adapter.

## Affected invariants and validation

Invariants 1–8, 10–14, and 18 apply. The real-filesystem recovery test now places the global
checkpoint at record 7, verifies one reclaimed and one retained command through tablet position 9,
applies only record 10, and returns the locked writer at sequence 11. An otherwise identical WAL
whose retained record 9 disagrees with CSEG fails as corruption. Provider tests separately reject
changed retained predecessors, changed retained commits, and incomplete retained row coverage while
accepting a genuinely absent pre-boundary row.
