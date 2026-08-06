# Mutable-Head Generation

> **Status: bounded generation primitive implemented.** `chronos_head` implements one fixed-capacity,
> schema-bound mutable generation, batch preparation, batch-atomic publication, stable owning
> snapshots, hidden row metadata, and idempotent sealing. Tablet-level generation sets, retry-state
> publication. The single-tablet executor now composes live WAL submission above that boundary;
> replay and flush handoff remain future integration.

## Purpose and boundary

The mutable head turns an already validated immutable `ColumnarBatch` into recent in-memory rows
that concurrent readers can scan. Its correctness boundary is one batch:

> A snapshot observes either the previous publication or the complete new batch and commit
> position. It never observes a partially copied column, row, offset, or hidden identity.

The library is deliberately independent of WAL I/O. A caller supplies the successful WAL identity
and record sequence only when publishing, after controlling when a prepared append crosses the WAL
boundary. The class does
not route rows, reserve client retry identities, submit records, acknowledge writes, select a new
generation, or replay commands.

## Public interfaces

`MutableHead::create` binds one immutable schema, tablet identity, nonzero process-local generation,
row capacity, and one variable-value byte capacity per schema ordinal. Fixed-width and `BOOL`
columns require a zero variable capacity.

`prepare_append` takes shared ownership of one immutable batch. It checks exact schema equality,
complete-batch row capacity, and each variable column's byte capacity. It also allocates the next
immutable publication descriptor and move-only `PreparedHeadAppend` handle. It neither needs the
not-yet-known WAL position nor copies values or changes reader visibility. `check_append` performs
the same schema and capacity checks without allocation or reservation so a tablet owner can decide
whether rotation is required.

The owner then uses the handle in this order:

1. `mark_wal_started()` records that cancellation is no longer safe.
2. The external integration obtains WAL success at its requested durability boundary.
3. `publish(position)` validates that the successful position advances within one WAL history,
   copies into the reserved stable range, writes hidden metadata, and performs one release
   publication.

A handle may be cancelled or dropped before step 1. Dropping it after step 1 fails the head closed,
preserving the last complete publication. `snapshot()` acquire-loads one publication. `seal()`
idempotently closes the generation at its exact published boundary.

## Storage layout

All generation storage is allocated by `create` and never resized afterward:

- nullable validity uses one `uint8_t` per row;
- `BOOL` values use one `uint8_t` per row;
- fixed-width values use one contiguous canonical-byte slot per row;
- variable values use a fixed byte arena and native in-memory `uint32_t` offsets; and
- hidden metadata stores `(wal_id, record_sequence)`, batch row ordinal, and operation kind per row.

The byte-per-row validity and Boolean representation is intentional. An append writes only new row
elements, so a reader of an earlier snapshot never races on a packed byte shared with unpublished
rows. Variable append starts at the prior byte frontier and writes offsets strictly after the old
published row boundary; even writing the same numeric old boundary again would be a data race and
is forbidden.

`MutableHeadMetrics::retained_storage_bytes` counts the exact logical capacity of value, validity,
offset, variable, and hidden-row storage. It excludes allocator/container overhead and the small
immutable publication descriptors. Variable capacity and published variable bytes are reported
separately.

## Publication and memory-order proof

Exactly one shard writer calls prepare, mark, publish, cancel, and seal. Those methods are not
internally serialized. Readers may concurrently call `snapshot` and scan returned views.

The exact atomic object is the `shared_ptr<const HeadPublication>` stored in the generation state.
The writer initializes only storage beyond the current row/byte frontiers, then uses an atomic
release store of the prepared descriptor. A reader uses an atomic acquire load of that same shared
pointer. Observing the new descriptor therefore makes all prior ordinary storage writes visible;
observing the old descriptor limits every view to the old frontiers.

`HeadSnapshot` owns both the generation state and its exact immutable publication descriptor.
Column and cell views borrow storage pinned by that snapshot. The head object can be destroyed while
a snapshot remains readable; storage reclamation occurs only after the final owning reference is
released. Moving or destroying the head concurrently with writer or reader entry is outside the
contract.

## Hidden identities

Every published row records the command position, zero-based row ordinal within that command, and
`APPEND_ROWS` operation. `row_version_identity` derives the stable tuple:

```text
(TableId, TabletId, wal_id, record_sequence, row_ordinal)
```

This metadata is process memory, not a new durable format. The enclosing `COLUMNAR_APPEND` command
and WAL record remain the durable source reconstructed during future replay.

## Failure behavior

- invalid configuration, schema mismatch, and pre-WAL misuse return `INVALID_ARGUMENT`;
- row, variable-byte, or allocation bounds return `RESOURCE_EXHAUSTED`;
- sealing, failure, and an outstanding preparation return `UNAVAILABLE` where retry may require
  owner action; and
- impossible ownership or publication-boundary violations return `INTERNAL` and fail closed once
  WAL work has started.

Expected capacity and descriptor-allocation failure happens during preparation, before WAL. The
successful post-WAL materialization path binds and validates the returned position, performs bounded
copies into existing storage, and does not allocate. An invalid/nonadvancing post-WAL position,
unexpected failure, or abandoned handle after the WAL boundary leaves the last publication intact
and prevents subsequent appends until fresh recovery.

## Complexity

For `R` appended rows, `C` columns, and `B` input value bytes:

- creation is `O(total configured capacity)` because storage is value-initialized;
- preparation is `O(C)` plus publication-descriptor allocation;
- publication is `O(C × R + B)` and has no per-row owning allocation;
- snapshot acquisition is `O(1)` plus one reference-count operation;
- construction of one column view and access to one fixed/Boolean cell are `O(1)`; and
- a returned variable cell is `O(1)` plus whatever work the caller performs on its byte slice.

Memory use is bounded by configured row/variable capacities plus `O(C)` publication metadata and
snapshot reference-count owners.

## Verification and measurement

`chronos_head_tests` covers configuration, exact capacity rejection before WAL, one outstanding
preparation, cancellation, post-WAL fail-closed abandonment, complete publication, hidden identity,
stable old snapshots, nonrewritten variable boundaries, sealing, lifetime pinning, every frozen
logical type and nullable shape, controlled snapshot acquisition after each column and hidden
metadata write, and concurrent readers that accept only complete batch boundaries. The public
header is compiled alone; install/export tests link `chronos::head` from a staged package.

The deterministic concurrency test runs under ThreadSanitizer, while the ordinary suite also runs
under AddressSanitizer and UndefinedBehaviorSanitizer. `chronos_head_benchmarks` measures:

- preparation plus materialization plus release publication by batch size, excluding generation
  arena construction and WAL I/O; and
- acquire snapshot plus construction of all borrowed column views.

The measurements are local microbenchmarks, not an end-to-end ingestion claim. Later Phase 4
evidence must add allocation failpoints, tablet generation switching, seal/handoff cost, scan
throughput, memory overhead including allocator effects, reader contention, retry integration, and
ordered replay.

## Tradeoffs and likely review questions

Byte-per-row validity and Boolean storage costs more memory than compact bitmaps but supplies a
short, auditable race-freedom proof during append. Preallocating maximum storage can waste capacity
but gives stable addresses and makes expected post-WAL application nonallocating. Atomic
reference-counted publication is not claimed to be lock-free; correctness and snapshot lifetime are
the current priority, and optimization requires benchmark evidence.

Useful review questions are:

- Why must the old variable offset boundary never be rewritten?
- Which exact release/acquire pair makes new column bytes visible?
- Why is a row counter alone insufficient to pin storage and byte frontiers?
- What is safe to cancel before WAL, and why does abandonment after WAL fail closed?
- Which work still belongs to the future tablet publication descriptor rather than this generation?
