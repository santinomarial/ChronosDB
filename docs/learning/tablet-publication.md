# Tablet Publication

> **Status: bounded live tablet owner implemented.** `chronos_ingest::TabletState` composes the
> mutable-head generation and retry-outcome primitives into one generation-schema-bound,
> single-writer tablet
> publication boundary. `execute_columnar_append` now composes it with live WAL submission and the
> global retry directory. Bounded direct-successor registration and activation are implemented;
> durable catalog ownership, retry pruning, routing, flush handoff, CSEG, and transport
> acknowledgment remain outside this primitive.

## Purpose and boundary

A mutable generation can publish a complete batch internally, but a tablet snapshot must also agree
on which generations are visible, how far WAL application has advanced, and which client retry
outcomes exist. `TabletState` supplies that outer boundary:

> One acquired tablet snapshot sees the old rows, applied position, generation set, and retry table,
> or it sees all four from the new command. It never combines fields from different epochs.

The class is a pure in-memory state owner. It accepts an already validated immutable
`OwnedColumnarBatch`, its `RetryIdentity`, and its `ColumnarAppendMutationIdentity`. It does not
encode or submit a WAL record. The implemented single-tablet executor reserves the global retry
identity, submits the accepted `COLUMNAR_APPEND` bytes, obtains the successful
`(wal_id, record_sequence)`, publishes the prepared tablet append, and then commits the exact
returned outcome pointer into the global directory.

## Public interfaces and ownership

`TabletState::create` binds one immutable starting schema and tablet identity. Its configuration
includes the starting mutable-head capacity plus maximum registered-schema, retained
sealed-generation, and tablet retry-entry counts. Every ownership bound is explicit and nonzero
because durable catalog, flush, and retry-pruning policy do not exist yet.

`register_schema` is a shard-writer catalog handoff before WAL work. It accepts one immutable direct
v1 successor and records the capacity for an empty generation of that shape. Registration is
bounded, preserves linear parent/version order and lineage-wide identity/name rules, and publishes
no tablet epoch. Head-specific capacity validation occurs when the first append prepares that
generation, still before WAL admission. Registration does not decide catalog activation; the
caller must still admit only the active ingest schema.

`prepare_append` is a shard-writer operation. It verifies table/tablet/schema agreement, rejects a
published retry identity reuse, checks the retry bound, and asks the active generation whether the
whole batch fits. It then allocates the head publication, immutable retry table copy, outcome,
outer descriptor, and move-only `PreparedTabletAppend`. No row or retry outcome becomes logically
visible.

The handle sequence is:

1. `mark_wal_started()` closes the safe-cancellation boundary for both tablet and head state.
2. The external WAL integration obtains the successful append position.
3. `publish(position)` validates monotonic position order, materializes and publishes the inner
   head range, initializes the reserved retry outcome, and release-publishes the outer tablet
   descriptor.
4. The caller passes `TabletAppendResult::outcome` unchanged to
   `RetryReservation::commit_published()`.

`TabletSnapshot` is a copyable owning pin. Its active `HeadSnapshot`, sealed `HeadSnapshot` values,
applied position, and retry map belong to one outer descriptor. Generation column/cell views obey
the inner snapshot lifetime rule. A returned retry outcome has independent shared ownership and is
the exact object installed in the tablet map.

## Rotation and bounded retention

An admitted batch is never split across generations. If it does not fit a nonempty active head but
does fit a fresh head, preparation allocates the next generation, prepares the append there, seals
the old generation, and publishes a topology-only tablet epoch containing the old sealed snapshot
and new empty active snapshot. This topology transition retains the same logical applied position
and retry table. Cancelling the later append leaves that safe empty generation active.

If the batch does not fit an empty generation, or if another sealed generation would exceed the
configured bound, preparation returns `RESOURCE_EXHAUSTED` before WAL. Sealed generations remain
query-visible and pinned because no flush/install path exists. The implementation never silently
drops one to make room.

A batch under the next registered direct successor forces the same rotation even when the active
ancestor still has capacity. The ancestor is sealed; a nonempty ancestor is retained in the visible
generation set, while an empty ancestor is closed but not retained. The descendant generation uses
its registered per-column capacity. A first-time batch may advance across registered intermediate
versions that received no rows, but cannot return to an ancestor. Exact ancestor retries are
resolved before tablet preparation and therefore remain valid no-row outcomes. Registration
supplies physical lineage knowledge, not active-schema catalog admission.

## Exact publication and memory-order proof

The outer atomic object is the `shared_ptr<const TabletPublication>` stored in tablet state. One
shard writer owns its mutation. A snapshot performs an acquire load of that shared pointer; command
publication performs a release store of the prepared pointer.

Before the release store, the writer:

- completes the mutable head's own release publication;
- initializes hidden row metadata through that head publication;
- writes the reserved outcome's WAL identity and record sequence;
- replaces the unpublished outer descriptor's active `HeadSnapshot` with the new exact boundary;
  and
- binds the same applied position while its immutable retry map already names that outcome.

The release/acquire synchronization makes those prior writes visible to a reader observing the new
outer pointer. A reader observing the old pointer owns old `HeadSnapshot` values with old immutable
inner descriptors; it does not reacquire the live head. Therefore the inner head may publish first
without exposing new rows through the old tablet epoch. The controlled interleaving test pauses
exactly between inner and outer publication and verifies that rows, position, and retry entry all
remain absent.

Atomic shared-pointer operations are not claimed to be lock-free. They are the exact lifetime and
synchronization primitive used today. Replacing them requires benchmark evidence and an equally
explicit reclamation proof.

## Failure behavior

Expected validation, capacity, generation allocation, retry-map allocation, and descriptor
allocation failures occur during preparation, before WAL. Pre-WAL cancellation or handle
destruction releases the reservation. A topology-only rotation may remain visible after such a
cancellation, but it contains the same logical rows, position, and retry state.

After `mark_wal_started`, an invalid/nonadvancing position, unexpected inner publication failure, or
dropped handle fails the tablet closed and leaves the prior outer descriptor visible. The prepared
generation also fails closed. Partially initialized unpublished memory is unreachable and is
discarded only when the failed state is replaced during future recovery.

Status classification is deliberately narrow:

- identity/schema/configuration errors and conflicting retry reuse are `INVALID_ARGUMENT`;
- a matching already-published retry identity is `ALREADY_EXISTS` so the caller can use its earlier
  lookup outcome rather than append;
- row, byte, schema-version, retry, sealed-generation, token, or allocation bounds are
  `RESOURCE_EXHAUSTED`;
- an outstanding append or failed state is `UNAVAILABLE`; and
- impossible ownership/publication inconsistencies are `INTERNAL` and fail closed after WAL.

The `TabletState` API does not acknowledge writes. The executor validates the WAL coordinator's
exact requested/effective durability and covering frontier, then returns only after tablet and
global retry publication. A future transport may treat that successful return as its logical
response eligibility boundary.

## Complexity and tradeoffs

For `R` rows, `C` columns, `B` value bytes, `S` registered schemas, `T` retained retry entries, and
`G` sealed generations:

- normal preparation is `O(C + T)` because the correctness-first immutable retry map is copied;
- publication is `O(C × R + B)` for head materialization plus constant outer descriptor updates;
- snapshot acquisition is `O(1)` plus reference-count operations;
- retry lookup is `O(log T)`;
- visible-row counting is `O(G)`; and
- rotation adds `O(configured head capacity + G)` initialization/copy work before WAL; and
- successor registration validates `O(S × C)` retained lineage metadata before storing it.

Memory is bounded by configured active/sealed head capacities, `T` retry entries, `O(C)` prepared
head metadata, and immutable descriptors retained by live snapshots. Copying the retry map on every
append is intentionally simple and auditable, not a final throughput choice. A persistent map,
chunked table, or shard-owned arena needs allocation and benchmark evidence before replacing it.

## Verification and measurement

`chronos_ingest_tests` covers invalid bounds, the exact empty epoch, invisible preparation,
pre-WAL cancellation, joint rows/position/retry publication, exact pointer handoff to the global
retry directory, whole-batch rotation, registered rename/tail-add successor activation, stable
ancestor snapshots, empty-ancestor elision, first-time ancestor rejection, exact recovered ancestor
retry advancement, oversized-batch rejection, schema/sealed/retry backpressure,
duplicate/conflicting identities, post-WAL fail-closed position validation, a controlled
inner/outer publication pause, and concurrent readers accepting only complete epochs. The public
header compiles alone and the installed external consumer checks the registration method signature.

The tests run in the ordinary, AddressSanitizer/UndefinedBehaviorSanitizer, and applicable
ThreadSanitizer configurations. `chronos_ingest_benchmarks` separately measures first publication,
outer snapshot acquisition, topology-only capacity rotation, and registered schema transition
across declared batch sizes. A separate
single-tablet execution benchmark retains canonical encoding, global retry reservation, real WAL
write or `fdatasync`, and tablet publication while excluding per-iteration WAL setup. All are local
microbenchmarks; routing, catalog admission, transport, and recovery remain excluded.

Likely review questions include:

- Why is the inner head allowed to publish before the outer tablet pointer?
- Which exact snapshot object prevents old readers from reacquiring a new inner boundary?
- Why does rotation publish an empty generation even if the prepared append later cancels?
- Which allocations are guaranteed to happen before WAL admission?
- Why must the global directory retain the exact tablet-published outcome pointer?
- What backpressure occurs before flush and retry-pruning policies exist?
