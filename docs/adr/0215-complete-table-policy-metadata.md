# ADR 0215: Complete Table Policy Metadata

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB catalog, ingestion, live-query, and retention maintainers

## Context

SQL `CREATE TABLE` binds partition interval, event-data retention, system-history retention, and
allowed lateness. Metadata Command v1 kind 4 persisted only system-history retention and a retry
position count. A restarted database could therefore reconstruct its schema but not the behavior
that governed partitioning, history, or event-time finalization. A local side file would compete
with the metadata Raft log, while changing kind-4 bytes would violate its frozen layout.

## Decision

Metadata Command v1 adds kind `5`, complete table policy, with an exact 64-byte payload. It carries
the table identity, positive partition interval, positive event-data retention, positive
system-history retention, nonnegative allowed lateness, nonzero retry-retention positions, and
eight reserved zero bytes.

The command applies only after a complete schema for the table. Applying it atomically publishes
both the complete policy and the existing partial-retention lookup derived from the same values.
Kind 4 remains decodable for old logs and may precede kind 5 during migration. Once kind 5 exists,
a later kind-4 command must match its system-history and retry fields exactly or application fails
closed. Complete policy updates remain ordered metadata commands; no local last-writer-wins path is
introduced.

## Detailed rationale

An additive command kind preserves every accepted Metadata Command v1 byte while keeping all table
behavior under the same committed catalog authority as schema and placement. Maintaining the
legacy lookup avoids a flag-day API migration without allowing it to diverge from the complete
record.

## Alternatives considered

- Reinterpreting kind 4 was rejected because old 32-byte payloads must remain valid and unambiguous.
- Extending Schema Definition v1 was rejected because schema generations are immutable while table
  policy can change independently.
- Re-parsing original DDL on restart was rejected because SQL text is not currently durable catalog
  state and is not the canonical bound policy.
- A standalone policy file was rejected because it would require cross-authority atomic commit and
  recovery precedence.

## Consequences

A daemon can reconstruct partition, retention, history, lateness, and retry policy without defaults
silently replacing user intent. Existing logs remain readable. Older binaries reject kind 5 as
unsupported and cannot safely apply a log containing it. Database namespaces, policy ALTER syntax,
drop/tombstone semantics, and physical partition creation remain separate work.

## Affected invariants

This decision strengthens invariants 3, 4, 8, 9, 13, 14, and 18 by retaining one durable policy
authority, applying it only in committed order, bounding its fields, and preventing incompatible
partial state from becoming visible after restart.

## Validation plan

Focused codec tests round-trip every command kind deterministically. State tests require schema
precedence, derived legacy retention, consecutive indexes, and rejection of divergent partial
updates. Durable-runtime tests close and reopen the physical Raft log and compare recovered policy
fields. A twelve-case real-filesystem matrix now places legacy-before-schema migration, first
complete policy, complete-policy replacement, and matching legacy projection on either side of a
Metadata Application Snapshot boundary, then commits each divergent legacy field in the retained
suffix. Accepted schedules reconstruct the exact complete and derived legacy authorities after
reopen; divergent suffixes poison live application and fail restart recovery closed. Golden
fixtures, fuzzing, allocation/crash injection, and large-catalog qualification remain in the Phase
18 ledger.

## Migration or rollback considerations

Writers may emit kind 5 only after all metadata-group participants understand it. A pre-kind-5 log
requires no conversion. Rolling back across the first committed kind-5 entry is unsupported because
an old process must fail closed instead of inventing missing table behavior.

## Unresolved questions

The SQL surface and authorization for policy alteration, retry-directory reclamation algorithm,
database namespaces, and metadata application snapshots remain unresolved.

## References

- [ADR 0075](0075-durable-metadata-raft-commands.md)
- [ADR 0214](0214-durable-complete-schema-definitions.md)
- [Metadata Command v1](../formats/metadata-command-v1.md)
- [Live-query semantics](../product/live-query-semantics.md)
