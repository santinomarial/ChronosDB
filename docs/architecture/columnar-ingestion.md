# Columnar Ingestion Architecture

> **Status: accepted design, partially implemented.** The logical schema foundation, canonical
> immutable in-memory vector/batch model, standalone Columnar Batch v1 codec, generic WAL
> application-envelope codec, pure in-memory `COLUMNAR_APPEND` v1 command/digest codec, and bounded
> process-local retry reservation directory exist. A bounded `chronos_head` generation now provides
> pre-WAL capacity/descriptor preparation, batch-atomic materialization/publication, stable owning
> snapshots, hidden row identity, and sealing. A bounded `chronos_ingest` tablet state now adds the
> authoritative live tablet retry table, whole-batch generation switching, sealed-generation
> backpressure, and joint rows/retry/applied-position publication. The blocking single-tablet
> `execute_columnar_append` path now composes canonical encoding, global retry reservation, bounded
> WAL admission, the requested `ASYNC` or `LOCAL_SYNC` completion, tablet publication, and exact
> outcome-pointer commit. `TabletState` now accepts a bounded registered linear schema lineage and
> seals the active generation before the first successor append. `recover_columnar_append_wal` now
> verifies and preflights a complete existing WAL, resolves every command against the supplied
> retained lineage, rebuilds fresh tablet and global retry state in sequence order, permits exact
> ancestor-schema retry no-ops, and returns the locked writer at the exact next sequence only after
> the complete recovery succeeds. Catalog/routing admission, per-row deduplication, retry pruning,
> and flush handoff remain unimplemented. This
> document
> fixes the logical state-machine and WAL command contracts for Phase 4; the physical
> [WAL v1](../formats/wal-v1.md) framing and application envelope remain unchanged.

## Scope

The ingestion unit is one immutable, schema-shaped
[columnar-batch v1](../formats/columnar-batch-v1.md) targeting one tablet. The owning shard worker
serializes validation, retry lookup, WAL admission, logical application, publication, and sealing.
This design covers original row appends only. It does not implement network framing, catalog
persistence, corrections, tombstones, cross-tablet atomicity, CSEG, manifests, checkpoints,
replication, or SQL execution.

## Identities and positions

The nominal identities and schema rules are fixed by
[ADR 0014](../adr/0014-logical-types-schema-identity-and-evolution.md). A command carries one
`TableId`, `TabletId`, `SchemaId`, and schema version. It also carries a nonzero authenticated
client UUID and a nonzero client batch UUID. Their ordered pair is the retry identity across the
single-node database, not merely within one tablet. A global identity directory serializes each key
and points a committed key to its owning tablet's published retry outcome. The tablet publication
stores the authoritative digest/outcome state. The directory may be partitioned internally, but a
reuse routed to another table or tablet is still detected and conflicts.

Phase 4 assumes one immutable routing/tablet map for the lifetime of a retained WAL history. Tablet
split, merge, key-range movement, or hash-function change is forbidden until a later command records
the routing epoch needed for replay. The command's `TabletId` is authoritative after admission;
replay validates that the retained catalog recognizes that table/tablet pair and never reroutes old
rows through a newer policy.

For the current single-node log, a command's commit position is `(wal_id, record_sequence)`. The
logical row-version identity at zero-based row ordinal `i` is:

```text
(TableId, TabletId, wal_id, record_sequence, i)
```

The `record_sequence` is assigned by the WAL writer. It is not event time, and it is not unique
without the WAL identity. All rows in a batch share the command commit position; row ordinal makes
their row-version identities distinct. The per-tablet applied stream is the subsequence of global
WAL sequence whose commands name that tablet. A tablet position can advance across a duplicate
no-op even when its visible row count does not.

## Columnar append command v1

The command occupies the kind-specific body after the frozen 16-byte WAL application envelope.
The envelope assignment is `application_format = 1`, `application_kind = 2`, and
`application_flags = 0`. All command integers are little-endian; all identifiers use the raw
16-byte representation from ADR 0014.

### Fixed 160-byte command header

| Body offset | Size | Field | v1 rule |
| ---: | ---: | --- | --- |
| 0 | 4 | `command_header_length` | `160`. |
| 4 | 4 | `command_flags` | `0`. |
| 8 | 4 | `mutation_kind` | `1` (`APPEND_ROWS`). |
| 12 | 4 | `digest_algorithm` | `1` (`SHA256`). |
| 16 | 16 | `client_id` | Nonzero authenticated client UUID. |
| 32 | 16 | `client_batch_id` | Nonzero UUID unique within that client. |
| 48 | 16 | `table_id` | Nonzero target `TableId`. |
| 64 | 16 | `tablet_id` | Nonzero target `TabletId`. |
| 80 | 16 | `schema_id` | Nonzero immutable `SchemaId`. |
| 96 | 8 | `schema_version` | Positive version for `schema_id`. |
| 104 | 4 | `row_count` | Nonzero; equals the embedded batch. |
| 108 | 4 | `batch_length` | Exact embedded batch length. |
| 112 | 32 | `request_digest` | SHA-256 result in conventional 32-byte digest order. |
| 144 | 4 | `outcome_code` | `1` (`APPLIED`). |
| 148 | 4 | `outcome_flags` | `0`. |
| 152 | 4 | `outcome_row_count` | Equals `row_count`. |
| 156 | 4 | `reserved` | Zero. |

Exactly `batch_length` bytes of canonical columnar-batch v1 data immediately follow the header;
there are no command-body trailing bytes. The header's table/schema/version and row count must
equal the batch header. The resolved catalog schema must match all batch descriptors. Every batch
row must route to `tablet_id` under that immutable schema and the catalog tablet map used for
admission. A mixed-tablet batch is rejected before WAL submission; callers may split it into
separate commands but receive no atomicity across them.

The maximum accepted WAL application payload is `16,777,172` bytes. After the 16-byte application
envelope and 160-byte command header, a canonical batch can be at most `16,776,992` bytes: the next
four nominal payload bytes would require WAL alignment that exceeds the 16 MiB physical record
limit. Runtime configuration may impose a smaller maximum.

### Canonical request digest

`request_digest` is SHA-256 over the following exact byte concatenation:

```text
ASCII bytes "ChronosDB.ColumnarAppend.v1\0"
u32 little-endian application_format (1)
u32 little-endian application_kind (2)
u32 little-endian mutation_kind (1)
16 raw table_id bytes
16 raw tablet_id bytes
16 raw schema_id bytes
u64 little-endian schema_version
u32 little-endian batch_length
exact batch bytes, including its CRC trailer
```

The digest intentionally excludes `client_id`, `client_batch_id`, encoded outcome, WAL position,
and requested durability: it identifies the canonical logical mutation protected by a retry key.
The server computes and compares it; a client-supplied digest is never trusted without
recomputation. The batch format's canonical encoding eliminates alternate byte representations of
the same accepted v1 batch.

The durable logical outcome is `APPLIED`, the derived commit position, and applied row count.
Requested/effective durability and response-delivery status are attempt metadata, not mutation
bytes. A retry returns the original logical outcome while reporting durability according to the
eventual transport contract; it cannot fabricate a new commit position.

## Admission and apply state machine

Only the tablet's owning shard worker mutates its state. The intended ordered transition is:

1. Take ownership of an immutable/reference-counted encoded batch or owned vectors from the bounded
   ingress queue. Borrowed network or reactor scratch storage is not sufficient.
2. Decode with configured bounds; resolve the exact immutable schema; validate types, nullability,
   UTF-8, decimal domains, event time, logical identity, authorization, and one-tablet routing.
3. Recompute the canonical digest and reserve or consult the database-wide retry identity. A
   committed entry resolves to the owning tablet's published retry outcome; an in-flight entry is
   handled within a bounded wait/reject policy and is never treated as absent.
4. For a matching identity and digest, return the stored outcome without appending. For a different
   digest, return `IDEMPOTENCY_CONFLICT`. Neither case mutates the WAL or head.
5. For a previously unseen identity, require the batch schema to be the table's current active
   ingest schema. This check occurs after retry lookup so a valid retry of a retained ancestor
   schema can still return its original outcome.
6. Ensure the complete batch can fit one empty head generation. If the active head cannot fit it,
   ensure sealing/handoff capacity exists, then seal it and prepare a new schema-bound generation.
   Capacity or allocation failure before WAL admission returns without a logical mutation.
7. Retain the global identity reservation and reserve all head, tablet retry-state, and publication
   resources needed to make post-WAL application nonallocating in the expected path. Submit the
   exact application payload to the production WAL coordinator.
8. When the WAL operation succeeds at the requested `ASYNC` or `LOCAL_SYNC` boundary, derive the
   commit position from the returned WAL identity and record sequence.
9. Initialize every user and hidden-system column slot, construct the retry outcome, and atomically
   publish rows, tablet retry state, and the new applied position as specified by
   [mutable-head publication](mutable-head-publication.md). Complete the global reservation by
   making it point to that same published outcome; observers of an in-flight reservation cannot
   return success before this linearization.
10. Only after publication may the transport report logical success or committed changes feed live
   consumers.

The current WAL coordinator's completion proves only its named physical persistence boundary. The
implemented single-tablet executor adds logical publication and global retry commit before a new
mutation returns success. A future transport must acknowledge only after that return and must
define durability reporting for a matching retry, which performs no new WAL operation. An
unexpected error after WAL success places the tablet in a failed state: it publishes nothing,
acknowledges no logical success, stops accepting later mutations, and requires replay into fresh
state. The durable command may therefore be unacknowledged and is recovered exactly once.

`ASYNC` publication is allowed after its complete-write boundary, but a crash may remove both the
rows and retry entry under that mode's documented envelope. `LOCAL_SYNC` acknowledgment waits for
the covering sync and publication; a crash after sync but before publication/response reconstructs
the durable command during recovery.

### Global retry reservation states

For each retry identity, the single-node directory has exactly three logical states: absent,
in-flight reservation, or committed pointer to an immutable tablet outcome. Only the owner of an
absent-to-in-flight transition may submit a WAL command for that identity. A pre-WAL rejection
removes its reservation. A successful tablet publication changes it to committed. A failure after
WAL I/O begins leaves the identity/tablet failed and blocks further use until recovery; it is never
made absent while later processing continues. Matching or conflicting contenders that see
in-flight state wait within a declared bound or receive a retryable busy result. They do not submit
a second record. Recovery reconstructs this directory in global record order before publishing it.

The concrete directory may use a correctness-first lock or a later proven partitioned design. Its
linearization and bounded ownership are required; calling it lock-free or weakening its ordering
requires a separate proof and evidence.

The current `chronos::ingest::RetryDirectory` is that correctness-first process-local primitive. It
uses one mutex, requires an explicit entry bound, returns in-flight immediately instead of waiting,
removes abandoned reservations only before their owner marks WAL I/O started, and stores the exact
immutable outcome pointer supplied after publication. `execute_columnar_append` connects it to one
already-routed `TabletState` and the WAL coordinator. Retained-lineage recovery reconstructs a
fresh instance through those same reservation/commit transitions. It intentionally has no pruning
API or retention policy yet. See the
[retry reservation directory guide](../learning/retry-reservation-directory.md).

## APPEND_ROWS semantics

The batch contributes one original physical row version per row in batch ordinal order. Hidden
head metadata records the derived commit position, row ordinal, operation `APPEND_ROWS`, and stable
row-version identity. Event-time values remain producer data and do not affect commit order.

For a table with a deduplication key, no two rows in one batch may have the same typed key. A new
`APPEND_ROWS` command that targets a logical identity already present is a logical conflict unless
it is the matching request-level retry. Replacement and tombstone intent require future explicit
mutation kinds. For a table without a deduplication key, the derived row-version identity is also
the row's logical identity.

Request idempotency and table deduplication are separate checks: the former protects a whole client
batch from retransmission; the latter defines row-version relationships.

## Recovery and reopening

The existing WAL recovery state machine remains authoritative:

1. discover and verify the entire physical history;
2. optionally repair only a qualifying incomplete final tail and reverify;
3. invoke semantic preflight for every record before replay;
4. replay in increasing global record sequence into fresh, unpublished state; and
5. publish recovered table state only after the complete pass succeeds.

Columnar preflight validates the envelope assignment, command header, reserved fields, lengths,
digest, batch CRC and structure, supported type/encoding set, catalog schema availability, and
table/schema agreement. It has no visible side effects. State-dependent conflicts found during
replay fail the pass and cause the fresh state to be discarded.

Replay processes commands through the same schema, routing, retry, and append semantics, except it
uses the record's existing position and performs no new WAL write. For an unseen database-wide
retry identity it appends once and installs the recorded outcome and target. For the same identity
and digest it verifies that the target and stored outcome are compatible and performs a no-row
state-machine no-op. Because the target participates in the digest, reuse against another tablet
conflicts. A different digest, impossible outcome, decreasing per-tablet position, or
schema/tablet mismatch fails recovery. No record is skipped to continue service.

For first-time commands, schema versions for a table are nondecreasing in WAL order and every change
must follow the accepted parent lineage. The first command under a successor seals the ancestor
head; a later first-time command under an ancestor is invalid. A same-digest retry no-op may refer to
its retained old schema because it adds no rows and returns the already published outcome.

Whole-log recovery always starts from a fresh/resettable target under the current WAL replay API.
Repeating recovery from the same bytes produces the same logical rows, identities, outcomes, and
applied positions. Physical head packing may depend on an explicitly supplied capacity policy, but
it cannot change visible rows or commit boundaries. Reopening the WAL continues at the exact next
global sequence already determined by physical recovery; head state never assigns WAL sequence.

The implemented `recover_columnar_append_wal` boundary realizes this contract for a caller-supplied
retained linear schema lineage per configured tablet. Each direct successor and its bounded empty
generation capacity are registered before WAL opening. Whole-log preflight resolves every command
to one retained immutable schema; replay lets the first-time successor command rotate the tablet
and rejects a first-time ancestor regression. Its owner and replay sink stay private until physical
recovery, whole-log semantic preflight, ordered application, active-segment synchronization, and
the startup directory barrier all succeed. A first command copies the borrowed decoded physical
vectors into an owned immutable batch before tablet publication. A matching duplicate, including
one under a retained ancestor schema, reuses the exact original outcome pointer, adds no rows or
retry entries, and advances only the tablet's outer applied position. Any replay failure destroys
the entire fresh owner. The returned move-only state owns the reconstructed global directory and
tablets plus a once-releasable locked `WalWriter`, allowing live coordination to continue at the
recovered next sequence.

Catalog schemas referenced by retained WAL must be recoverably available before preflight. This
document does not invent a catalog file or schema command. Until catalog persistence exists, tests
and embedding code must supply the exact immutable registry and must not advertise standalone
database restart.

## Backpressure, limits, and failure ownership

Every stage is bounded: network frame, decoded batch, queue slots, coordinator pending bytes,
command size, head capacity, sealed-head retention, and global/tablet retry state. An overload is
explicit before WAL admission when possible. There is no unbounded overflow queue and no splitting
of an admitted batch to make progress.

The shard worker owns mutable tablet state, but the production commit coordinator owns the WAL
writer on its worker thread. Submission copies or retains immutable bytes according to the
coordinator contract. Completion transfers only an outcome; it does not transfer tablet ownership.
Shutdown stops admission, drains or deterministically rejects published ingress work, resolves all
accepted WAL completions, publishes successful commands in order, then quiesces tablet and head
owners.

## Explicitly deferred

- correction and tombstone mutation bodies;
- generated logical identities and system timestamp assignment;
- catalog persistence and schema/tablet-map edit commands or routing epochs;
- idempotency-horizon defaults, durable pruning, and checkpoint coverage;
- CSEG format, flush installation, manifest edits, and WAL deletion;
- network representation and multi-tablet request semantics; and
- Raft wrapping, quorum acknowledgment, and cross-tablet coordination.
