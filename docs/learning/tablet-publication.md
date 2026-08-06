# Tablet Publication

> **Status: bounded live tablet owner implemented.** `chronos_ingest::TabletState` composes the
> mutable-head generation and retry-outcome primitives into one schema-bound, single-writer tablet
> publication boundary. WAL submission, recovery/replay, retry pruning, active-schema changes,
> routing, flush handoff, CSEG, and acknowledgment orchestration remain outside this library.

## Purpose and boundary

A mutable generation can publish a complete batch internally, but a tablet snapshot must also agree
on which generations are visible, how far WAL application has advanced, and which client retry
outcomes exist. `TabletState` supplies that outer boundary:

> One acquired tablet snapshot sees the old rows, applied position, generation set, and retry table,
> or it sees all four from the new command. It never combines fields from different epochs.

The class is a pure in-memory state owner. It accepts an already validated immutable
`OwnedColumnarBatch`, its `RetryIdentity`, and its `ColumnarAppendMutationIdentity`. It does not
encode or submit a WAL record. The integration layer must reserve the global retry identity, submit
the accepted `COLUMNAR_APPEND` bytes, obtain the successful `(wal_id, record_sequence)`, publish the
prepared tablet append, and then commit the exact returned outcome pointer into the global
directory.

## Public interfaces and ownership

`TabletState::create` binds one immutable schema and tablet identity. Its configuration includes a
fixed mutable-head capacity, a maximum retained sealed-generation count, and a maximum tablet retry
entry count. Both ownership bounds are explicit and nonzero because flush and retry-pruning policy
do not exist yet.

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

The current implementation keeps one schema for the tablet lifetime. Schema activation must later
reuse the sealing path but also needs catalog admission and replay rules, so it is not inferred from
a mismatching input batch.

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
- row, byte, retry, sealed-generation, token, or allocation bounds are `RESOURCE_EXHAUSTED`;
- an outstanding append or failed state is `UNAVAILABLE`; and
- impossible ownership/publication inconsistencies are `INTERNAL` and fail closed after WAL.

The API does not acknowledge writes. Durability mode and response eligibility belong to the future
orchestration that composes the WAL coordinator, tablet state, and global retry directory.

## Complexity and tradeoffs

For `R` rows, `C` columns, `B` value bytes, `T` retained retry entries, and `G` sealed generations:

- normal preparation is `O(C + T)` because the correctness-first immutable retry map is copied;
- publication is `O(C × R + B)` for head materialization plus constant outer descriptor updates;
- snapshot acquisition is `O(1)` plus reference-count operations;
- retry lookup is `O(log T)`;
- visible-row counting is `O(G)`; and
- rotation adds `O(configured head capacity + G)` initialization/copy work before WAL.

Memory is bounded by configured active/sealed head capacities, `T` retry entries, `O(C)` prepared
head metadata, and immutable descriptors retained by live snapshots. Copying the retry map on every
append is intentionally simple and auditable, not a final throughput choice. A persistent map,
chunked table, or shard-owned arena needs allocation and benchmark evidence before replacing it.

## Verification and measurement

`chronos_ingest_tests` covers invalid bounds, the exact empty epoch, invisible preparation,
pre-WAL cancellation, joint rows/position/retry publication, exact pointer handoff to the global
retry directory, whole-batch rotation, stable old snapshots, oversized-batch rejection, sealed and
retry backpressure, duplicate/conflicting identities, post-WAL fail-closed position validation, a
controlled inner/outer publication pause, and concurrent readers accepting only complete epochs.
The public header compiles alone and the installed external consumer includes its configuration.

The tests run in the ordinary, AddressSanitizer/UndefinedBehaviorSanitizer, and applicable
ThreadSanitizer configurations. `chronos_ingest_benchmarks` separately measures first publication,
outer snapshot acquisition, and topology-only rotation across declared batch sizes. These are local
microbenchmarks excluding WAL I/O, global retry reservation, routing, admission, and acknowledgments.

Likely review questions include:

- Why is the inner head allowed to publish before the outer tablet pointer?
- Which exact snapshot object prevents old readers from reacquiring a new inner boundary?
- Why does rotation publish an empty generation even if the prepared append later cancels?
- Which allocations are guaranteed to happen before WAL admission?
- Why must the global directory retain the exact tablet-published outcome pointer?
- What backpressure occurs before flush and retry-pruning policies exist?
