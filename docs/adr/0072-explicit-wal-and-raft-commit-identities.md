# ADR 0072: Explicit WAL and Raft commit identities

- **Status:** accepted
- **Date:** 2026-08-08
- **Owners:** ChronosDB storage and distributed-systems maintainers

## Context

The single-node ingestion path historically represented every logical commit position as
`(wal_id, record_sequence)`. Applying a committed tablet Raft entry through the same mutable-head,
tablet, retry, and query-publication machinery would otherwise require inventing a WAL identifier
or silently using a nil value. Either choice aliases distinct histories and makes row versions,
retries, resume boundaries, and recovery ambiguous.

CSEG v1 and Manifest v1 deliberately encode WAL identifiers. Their frozen bytes have no source tag
or Raft group identifier, so they cannot faithfully persist Raft-sourced row versions.

## Accepted decision

Every in-memory head/tablet commit position has an explicit source. A WAL position is
`(WAL, wal_id, record_sequence)`; a Raft position is `(RAFT, group_id, log_index)`. Exactly one
source-specific identifier must be valid, and every successor must advance strictly within the same
source and logical history. Row-version identities and committed retry outcomes preserve the source
and source-specific identifier.

Existing field names remain available during the staged migration to avoid an unrelated broad API
rewrite. Factory functions construct valid source-specific positions, and validation rejects nil,
mixed, non-advancing, or source-changing positions.

CSEG v1, Manifest v1, WAL recovery seeds, and their flush/publication boundaries remain WAL-only.
They must reject a Raft position before serialization rather than encoding a nil or fabricated WAL
identifier. A later versioned durable format must add an explicit commit source and source-specific
log identity before replicated rows can be flushed or checkpointed through that format.

## Consequences and alternatives

The common in-memory application path can now publish committed Raft entries without losing their
identity, and uncommitted entries still cannot reach the path. The staged restriction means a
Raft-backed mutable head cannot yet cross the v1 sealed-head durable boundary; this is an explicit
availability limit, not silent corruption.

Reusing the Raft group UUID as a WAL ID was rejected because equal-width bytes do not imply equal
namespaces or recovery semantics. Adding only a Boolean to retry outcomes was rejected because row
metadata and successor validation would remain ambiguous. Reinterpreting the frozen v1 system
column was rejected because existing readers would misclassify replicated history.

## Affected invariants and validation

Invariants 3, 4, 5, 6, 8, 11, and 14 apply. Focused tests prove Raft group/index preservation in
mutable-head rows and retry outcomes, rejection of source changes within one head history, and
fail-closed CSEG v1 serialization of Raft identities. The versioned replicated durable-row format,
manifest recovery, query row-version columns, compaction, and migration compatibility remain exit
work for the replicated storage integration.
