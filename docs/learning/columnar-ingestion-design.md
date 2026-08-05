# Columnar Ingestion Design Guide

> **Status: design guide; schema, canonical batch, codecs, and retry directory implemented.**
> The identity/type/schema layer is documented in [logical schema foundation](schema-foundation.md),
> and immutable vectors, batches, and byte codec in
> [columnar memory model](columnar-memory-model.md). The pure in-memory WAL command layer is
> documented in [COLUMNAR_APPEND command codec](columnar-append-command.md). The bounded live
> identity primitive is documented in
> [retry reservation directory](retry-reservation-directory.md). WAL submission, recovered/tablet
> retry state, replay, and mutable-head implementation remain pending. The normative
> sources are
> [columnar batch v1](../formats/columnar-batch-v1.md),
> [columnar ingestion](../architecture/columnar-ingestion.md),
> [mutable-head publication](../architecture/mutable-head-publication.md), and
> [ADRs 0014](../adr/0014-logical-types-schema-identity-and-evolution.md) and
> [0015](../adr/0015-columnar-batch-v1-and-wal-append-command.md).

## What this subsystem is for

The Phase 4 subsystem turns an owned, typed columnar request into one replayable tablet-state
transition. Its job is narrower than a complete database write path: it validates immutable schema
bytes, protects client retries, records one mutation in the existing WAL, and publishes complete
recent rows to snapshot readers. It does not yet persist a catalog or CSEG part.

The central invariant is easy to state and surprisingly demanding:

> A reader sees either the complete batch and its commit position, or the state immediately before
> the batch; recovery and retry never create a second logical copy.

## Contract map

```text
immutable schema + owned vectors
              │
              ▼
canonical columnar-batch v1 bytes
              │ validate, route, digest, retry lookup
              ▼
WAL application envelope (format 1, kind 2)
              │ append / ASYNC or LOCAL_SYNC boundary
              ▼
single shard-owned apply
              │ initialize unpublished column slots + retry outcome
              ▼
release-published tablet descriptor
              │
       acquire-pinned snapshots
              │
              └── sealed-head ownership → future CSEG flush
```

Each layer has one authority. The batch codec owns value bytes. The schema owns meaning and column
roles. The command owns target, retry identity, digest, and outcome. WAL owns physical order and
durability. The tablet state machine owns deduplication and rows. The publication descriptor owns
reader visibility and lifetime.

## Why the batch is self-describing

The descriptor table repeats each column identity, type, parameters, and nullability rather than
assuming the current catalog. That supports safe independent decoding, precise corruption errors,
stable digest input, and recovery diagnostics. It does not make the catalog optional: a valid batch
can still name a schema whose authoritative definition differs, has been retired too early, or
routes elsewhere. Decode answers “are these bytes well formed?”; schema validation answers “may
these bytes be applied here?”

V1 uses one canonical PLAIN encoding. This is intentionally less compact than a menu of encodings.
It makes the first codec, fuzz oracle, and retry digest reviewable. Dictionary IDs are unsuitable
for `SYMBOL` at this boundary because their scope would leak into equality and replay; the batch
carries UTF-8 values.

## Owned and borrowed vector expectations

A later API should distinguish ownership visibly:

- an **owned batch** owns header/descriptor/value storage and is immutable after construction;
- a **borrowed batch view** contains spans into one complete immutable owner and cannot outlive it;
- an **owned column vector** owns validity, offsets, and values for one logical type; and
- a **borrowed column vector** references the corresponding regions with the owner's lifetime.

Construction validates sizes and domains before returning an immutable value. Borrowed views do
not allocate on success. Queue publication retains an owning batch reference; a view into a network
receive buffer or stack frame cannot cross the reactor-to-shard boundary. Decoding hostile lengths
must not allocate before header CRC, configured bounds, and checked arithmetic succeed.

## Two identities called “deduplication”

Request retry identity and logical row identity solve different problems.

The pair `(client_id, client_batch_id)` means “have I already executed this whole request anywhere
in this database?” A global identity directory serializes the key and resolves it to the owning
tablet's published retry outcome. The SHA-256 digest distinguishes a retransmission from conflicting
reuse, including a different target tablet. It protects all rows and the original logical outcome
together.

The schema `DEDUP KEY` means “which physical versions describe the same logical entity?” Initial
`APPEND_ROWS` accepts only a new entity; later explicit correction/tombstone commands will add
versions. Reusing a request identity is never a shorthand for correcting a row.

This distinction matters after a crash. The WAL record carries both the full mutation and the
request identity/digest. Replay cannot recover the rows while forgetting that a matching client
retry must be a no-op.

## Commit and acknowledgment timeline

The existing coordinator completion is a WAL persistence result, not by itself an externally
visible table success. The integrated order is:

```text
validate and reserve
  < no WAL mutation yet >
append record
  < ASYNC physical boundary >
optional covering sync
  < LOCAL_SYNC physical boundary >
initialize head + retry state
release-publish logical transition
return success / emit committed change
```

A crash after append can leave an unacknowledged record. A crash after synchronization but before
publication or response can leave a durable unacknowledged record. Recovery applies either exactly
once. `ASYNC` may disappear after crash, including its reconstructed retry entry, because its
physical contract never promised persistence.

Preparing capacity before WAL admission is essential. Expected out-of-memory or head-full errors
must occur while rejection is still harmless. An unexpected failure after a successful WAL
operation fails the tablet closed; continuing with later commands would create a gap between log
and state.

## Publication proof in plain language

The writer fills only row slots that no reader is allowed to inspect. It fills every column,
validity byte, offset, variable byte, and system field. Then it publishes one new immutable
descriptor with release semantics.

A reader acquires a descriptor. Seeing the old descriptor limits it to old rows. Seeing the new
descriptor establishes that all prior writes are visible and permits the whole new range. Fixed
capacity prevents relocation; a pin prevents destruction. Byte-per-row mutable null and Boolean
storage avoids a subtle C++ data race where setting a new bit modifies the same byte an older
snapshot reads.

Sealing applies the same idea at generation scale. The old generation becomes immutable and stays
pinned; a publication switches the active reference without making old rows disappear. Future
flush cannot remove it until a manifest transition substitutes durable rows exactly once.

## Replay model

Physical recovery first proves a valid WAL prefix. Semantic preflight then proves that every
columnar command is supported and statelessly valid before table mutation begins. Replay applies
the global sequence to fresh unpublished state and uses each record's actual WAL identity and
sequence as the commit position.

The reference state for one tablet is conceptually:

```text
applied_position
global_retry[(client_id, batch_id)] = (target_tablet, digest, outcome)
row_versions in (commit_position, row_ordinal) order
published head generations
```

An unseen retry key adds its rows and outcome. A same-digest key adds no rows. A different digest or
contradictory outcome fails the recovery. Publishing recovered state only after the complete pass
prevents a later corrupt or unsupported record from exposing a verified prefix as a database.

Running recovery again from the same WAL starts with fresh state and produces the same logical
model. Head capacities may change physical generation boundaries, but not rows, identities,
positions, or snapshot truth.

## Complexity and performance questions

For `R` rows, `C` columns, and `B` variable bytes, validation and plain encoding are `O(R×C + B)`;
append materialization has the same lower bound. The current correctness-first retry directory uses
an ordered map, so lookup and transition cost are `O(log N)` for `N` bounded retained identities;
it has no retention policy yet. Snapshot acquisition is proportional to the number of visible
generation references unless a later persistent descriptor structure reduces it.

Performance work must measure, not assume:

- owned versus retained encoded input;
- validity representation and scan cost;
- batch width, row count, and variable-value distribution;
- reservation/allocation count and memory overhead;
- head capacity, sealing frequency, and pinned-reader pressure;
- `ASYNC` versus `LOCAL_SYNC` and group-commit settings; and
- replay throughput with retry hits and schema transitions.

No benchmark may omit CRC, digest, validation, publication, or requested durability and still be
labeled as the complete ingestion path.

## Failure checklist

- Invalid bytes, unknown features, or schema mismatch: reject before WAL.
- Retry key with different digest: `IDEMPOTENCY_CONFLICT`, no WAL append.
- Queue/head/sealed-generation bound reached: explicit backpressure before WAL.
- WAL append or sync failure: writer/coordinator failure rules apply; no publication.
- Unexpected apply failure after WAL: fail tablet, publish nothing, recover fresh.
- Corrupt or unsupported replay record: fail startup, discard reconstructed state.
- Snapshot pin remains: do not reclaim its head.
- Flush unavailable: retain within the bound or backpressure; never drop rows.

## Likely review and interview questions

**Why repeat schema details in a batch?** To make byte decoding and corruption diagnosis independent
of mutable catalog state while still requiring authoritative schema validation.

**Why is WAL record sequence not stored in the command?** The writer assigns it. Replay receives it
from the authenticated physical record, avoiding two sources of truth and a circular digest.

**Why does the digest exclude the retry identity?** It identifies the mutation protected by that
identity. The retry-table key scopes comparison; including the key would not add mutation meaning.

**Can a batch cross heads?** No. It either fits the active head, causes a pre-admission seal and fits
a fresh head, or is rejected. This preserves one publication boundary.

**What makes a snapshot safe while appends continue?** Release/acquire publication, immutable
captured boundaries, stable storage, and owning pins.

**What happens if a durable command was never acknowledged?** Replay applies it once and restores
its retry outcome; the client can safely retry with the same identity and digest.

**Why not write CSEG directly?** Phase 4 establishes typed ingestion and recent snapshot state.
Sorted compressed immutable installation, manifests, and checkpoints have distinct crash protocols
in later phases.
