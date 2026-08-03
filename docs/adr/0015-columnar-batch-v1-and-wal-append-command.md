# ADR 0015: Columnar Batch v1 and WAL Append Command

- **Status:** accepted
- **Date:** 2026-08-03
- **Owners:** ChronosDB ingestion, WAL, and storage maintainers

## Context

The WAL exit gate deliberately froze a physical record and a generic 16-byte application envelope,
but assigned no application kind. Mutable-table recovery now needs a self-contained typed batch and
a command that atomically records mutation bytes, target tablet, retry identity, canonical digest,
and replayable outcome without changing WAL v1 framing.

## Accepted decision

The [columnar-batch v1 format](../formats/columnar-batch-v1.md) is the canonical immutable batch
encoding. It is independently length-delimited, typed, checksummed, and decodable without native
struct layout or a catalog. Catalog validation is still required before application.

The existing WAL v1 `APPLICATION_ENTRY` envelope is unchanged. This ADR assigns:

| Field | Assigned value |
| --- | --- |
| `application_format` | `1` |
| `application_kind` | `2` (`COLUMNAR_APPEND`) |
| `application_flags` | `0` |

The pair `(application_format=1, application_kind=1)` remains unassigned to production semantics.
Existing WAL codec, crash-harness, and benchmark fixtures use that pair as synthetic opaque physical
test data; this decision does not retroactively reinterpret those bytes.

Its kind-specific body is the fixed header and exact embedded batch defined in
[columnar ingestion](../architecture/columnar-ingestion.md#columnar-append-command-v1). One command
targets exactly one table, one tablet, one schema version, and one nonempty batch. Rows from one
command are never split across tablets or mutable heads. Multi-tablet atomicity is not provided.

The mutation digest is SHA-256 over the exact canonical request preimage defined by the command
specification. SHA-256 is commodity cryptography permitted by
[ADR 0011](0011-dependency-and-build-versus-buy-policy.md); a later implementation must select and
record a maintained dependency rather than invent a cryptographic implementation. CRC32C remains
the integrity checksum for accidental byte corruption and is not substituted for the retry digest.

The command records a nonzero client identity, nonzero client batch identity, digest, applied
outcome, and complete mutation together. The first successful command publishes a tablet-owned
retry outcome and a database-wide identity-directory entry keyed by the identity pair. A matching
retry returns the original logical outcome without another WAL entry or row; a different digest or
target returns `IDEMPOTENCY_CONFLICT` without a WAL entry.
The initial command represents only `APPEND_ROWS`. Replacement and tombstone mutations require a
new explicit operation assignment and are not inferred from a conflicting retry.

Recovery validates the physical WAL first, preflights support and all stateless command/batch
semantics for the complete log, then replays into fresh unpublished table state in global WAL
record-sequence order. A repeated identity with the same digest is a deterministic no-op; the same
identity with another digest is a state-machine error and causes recovery to discard the fresh
state and fail. The row-version identity for row ordinal `i` is the tuple `(TableId, TabletId,
wal_id, record_sequence, i)`. WAL position and row ordinal are derived during apply and are not
duplicated inside the batch.

No transport may acknowledge a command until its requested WAL durability boundary has succeeded
and the complete logical transition—rows, retry entry, and applied position—has been published. If
WAL persistence succeeds but publication or response delivery does not, the command is durable but
unacknowledged and recovery/retry returns its one original outcome. `ASYNC` retains exactly the
existing weaker crash envelope.

## Detailed rationale

Keeping the batch self-describing permits independent codec work, hostile-input validation, future
protocol reuse, and stable digesting. Binding the exact bytes and identities into one WAL record
prevents recovery from retaining a mutation while forgetting its retry protection. A one-tablet
command fits the accepted single-owner and future per-tablet Raft state-machine model.

The command does not encode its WAL position because the writer assigns that position. Replay has
the authoritative physical header and WAL identity and can reconstruct the logical outcome without
a circular digest or duplicated source of truth.

## Alternatives considered

- **Change WAL v1 framing:** unnecessary and incompatible; the accepted application envelope was
  designed for this allocation.
- **Store a row-oriented command:** simpler for small inserts but would force conversion and a
  second canonical form on the primary ingestion path.
- **Use only the WAL CRC as the request digest:** detects ordinary corruption but is not an
  appropriate collision-resistant idempotency identity.
- **Write retries again and deduplicate only on replay:** wastes WAL space and allows concurrent
  attempts to report different commit positions.
- **Allow one command to span tablets:** would require a cross-tablet commit protocol explicitly
  outside the single-node Phase 4 contract.

## Consequences

- The first application envelope allocation and batch bytes become durable compatibility
  contracts.
- Producers must retain immutable encoded bytes or an equivalent owned batch through admission and
  processing.
- Runtime admission must account for the WAL maximum after envelope and command overhead.
- Recovery needs an immutable schema registry and fresh/resettable state before it can apply.
- The system must retain retry entries for the advertised idempotency horizon; reclamation is a
  future checkpoint/catalog concern.

## Affected invariants

This decision directly supports invariants [1, 4, 6, 8, 9, 10, 12, 14, and
16](../architecture/invariants.md): named durability, ordered deterministic apply, snapshot
stability, idempotent recovery/retry, integrity and bounded decoding, unambiguous positions,
versioned formats, and batch-complete publication.

## Validation plan

- Golden, round-trip, property, corruption, truncation, cross-endian, and fuzz tests will cover the
  batch and command codecs, including every length/offset/type/flag boundary.
- Model tests will compare admission, matching retry, conflicting retry, replay, repeated replay,
  schema switches, and head sealing with a serial state machine.
- Crash tests will kill before/after append, sync, publication, and response and reconcile
  `ASYNC`/`LOCAL_SYNC` outcomes with rows and retry state.
- Deterministic concurrency and ThreadSanitizer runs will hold snapshots at each publication and
  sealing transition.

## Deferred decisions

Network framing, authentication, protocol error representation, idempotency-horizon defaults and
garbage collection, correction/tombstone commands, multi-tablet commit, catalog durability,
system-timestamp mapping, CSEG encoding, and future Raft command wrapping are deferred.

## Migration or reversal implications

Changing a type code, byte interpretation, digest preimage, or assigned application value requires
a new version or kind and an explicit compatibility path. WAL v1 physical framing remains frozen.

## References

- [WAL v1](../formats/wal-v1.md)
- [Columnar batch v1](../formats/columnar-batch-v1.md)
- [Columnar ingestion](../architecture/columnar-ingestion.md)
- [Mutable-head publication](../architecture/mutable-head-publication.md)
- [ADR 0013](0013-wal-v1-format-and-recovery.md)
