# Mutable-Head Publication and Snapshot Contract

> **Status: accepted design, bounded in-memory publication implemented.** `chronos_head` now
> provides one bounded schema-bound generation, pre-WAL append preparation, batch-atomic
> release/acquire publication, stable owning snapshots, hidden row identities, and idempotent
> sealing. `chronos_ingest` now provides a bounded schema-bound tablet owner, generation rotation,
> one outer rows/position/retry/generation-set publication, exact retry-outcome handoff to the
> global directory, and bounded direct-successor registration. The first append under a registered
> successor seals the ancestor and publishes a new schema-bound generation. WAL replay rebuilds
> mixed-schema publications into an owner returned only after complete recovery, including
> position-only outer publication for matching ancestor-schema duplicates. Retry pruning,
> catalog/routing admission, and flush handoff remain unimplemented. This document
> refines [ADR 0005](../adr/0005-columnar-heads-and-immutable-cseg-parts.md) for Phase 4 without
> specifying CSEG bytes or flush installation.

## State and ownership

One mutable head belongs to one `(TableId, TabletId, SchemaId, schema_version)` and one process-local
monotonic generation number. Exactly one shard worker owns mutation. A head stores schema-shaped
user columns plus hidden system columns for commit position, row ordinal, operation kind, and stable
row-version identity. Its row order is command commit order followed by batch row ordinal; physical
sorting for future CSEG output is not a visibility rule.

A generation is created with fixed row capacity and bounded per-column variable-byte capacity.
Published storage never moves or reallocates. Fixed-width value slots and variable offsets occupy
distinct element storage. Mutable-head validity and Boolean storage use at least one independently
addressable byte per row; a packed bit representation that lets an append write the same memory
location a reader may access is forbidden. Compact bitmaps may be created only after sealing or by
a representation with an explicit race-free proof.

Variable-width bytes are owned by the head, not borrowed from the ingress batch. Each published
offset points into stable owned storage. Readers never dereference beyond the variable-byte frontier
captured for their published row boundary. The implementation may instead retain immutable owned
batch chunks if it provides equivalent bounded ownership, batch-atomic publication, and scan
semantics; no representation may depend on producer scratch lifetime.

For an offsets array, the offset at the old published row boundary is already reader-visible and is
never rewritten. An append writes only subsequent unpublished offsets and new variable bytes; the
initial zero offset is initialized before the first publication. Writing the same numeric value
again is still a concurrent C++ write and is not permitted without synchronization.

## Preparing and publishing one batch

An admitted batch must fit wholly in one generation. Before WAL admission, the owner validates
capacity and reserves every row slot, variable byte, retry entry, and immutable publication
descriptor needed by the expected post-WAL path. It does not advance a reader-visible boundary.

After the WAL operation reaches its requested persistence boundary, the owner:

1. copies or materializes all user-column values and validity into the reserved unpublished range;
2. writes every variable offset and byte and all hidden system metadata;
3. constructs the retry entry and logical outcome for the command;
4. verifies the completed end offsets and invariants; and
5. release-publishes one immutable tablet-publication descriptor containing the new complete row
   boundary, variable-byte frontiers where the representation needs them, applied WAL position,
   active generation reference, and the visible sealed-generation references.

There is one publication event for the command, not one per row or column. The tablet retry-table
update is part of the same shard-owned logical transition and must be established before the
descriptor makes the new applied position/rows observable. The database-wide identity reservation
then becomes a committed pointer to this published outcome; another submitter observing the
reservation cannot report success or append a second command while that pointer is not committed.
A transport success and a live-operator handoff happen only after both linearization steps.

If any expected validation or allocation can still fail, it occurs before WAL admission. An
unexpected post-WAL failure does not roll the visible boundary forward; the tablet enters failed
state and rejects later mutations until fresh recovery. Unpublished slots may contain arbitrary
partial bytes but are unreachable and are destroyed with the failed state.

## Memory-order argument

The single writer performs ordinary initialization only in unpublished, nonoverlapping memory. Its
release publication is sequenced after every value, validity byte, offset, variable byte, hidden
field, retry-state write, and descriptor initialization for the command.

A snapshot reader acquire-loads the tablet-publication descriptor. If it observes the new
descriptor, the release/acquire synchronization makes all preceding initialization visible. It may
read only the referenced generations and each captured boundary. If it observes the previous
descriptor, it reads only the previous boundary and cannot access the reserved range. Thus a
snapshot sees either every row in the batch and the new applied position or none of them.

Single-writer ownership provides mutation order; acquire/release provides cross-thread visibility.
Relaxed publication, a plain shared row counter, moving storage, `vector<bool>`-style shared bit
updates, or reading an uncaptured live boundary does not satisfy this argument. A concrete C++
implementation must document the exact atomic object, ownership/pinning mechanism, and reclamation
edge and must pass ThreadSanitizer plus deterministic interleaving tests before claiming this
contract.

The implemented single-generation boundary atomically release-stores an immutable
`shared_ptr<const HeadPublication>` after materialization. `HeadSnapshot` acquire-loads and owns
that same descriptor plus the generation state, so its row count, byte frontiers, applied position,
and storage lifetime come from one epoch. The tablet owner atomically release-stores a distinct
`shared_ptr<const TabletPublication>` after inner-head publication and retry-outcome initialization.
`TabletSnapshot` acquire-loads and owns that descriptor. Old outer descriptors retain exact old
`HeadSnapshot` values rather than reacquiring the live generation, so the earlier inner publication
cannot leak new rows before the outer store.

## Tablet snapshots

A tablet snapshot is an immutable, owning descriptor that captures:

- table/tablet identity and the bound catalog/schema view;
- the tablet's applied commit position;
- every visible sealed head reference and its complete row boundary;
- the active head generation reference and published row/byte boundary; and
- the row-version visibility policy needed by the query.

Acquisition is one acquire observation of a tablet publication, or an equivalent retry loop that
proves all fields came from one publication epoch. It never combines a new applied position with an
old row boundary. Database-wide snapshot coordination across tablets remains a later contract; a
single-tablet snapshot is exact at its captured position.

The descriptor owns pins. Writers may append after its active boundary, seal the active head, or
publish a new generation, but cannot mutate bytes within the snapshot's visible range and cannot
reclaim referenced storage. Readers scan only the captured range. Releasing the final snapshot pin
permits reclamation only if no tablet publication, flush handoff, or other owner still references
the generation.

## Sealing and handoff

Sealing is triggered before an append when the whole next batch does not fit, when the active
schema changes, or by an explicit owner policy. A batch larger than an empty generation is rejected;
it is never divided after admission. The owner performs these steps in order:

1. close the active generation to reservation;
2. freeze its exact published row and byte boundaries and mark it sealed;
3. retain it in the tablet's visible generation set;
4. transfer an owning immutable reference and coverage metadata to the future bounded flush
   pipeline, or retain it locally when no flush implementation exists;
5. create a new mutable generation; and
6. release-publish a tablet descriptor that names the sealed generation and new active generation.

A head is never removed from query visibility merely because it was handed to flush. Future CSEG
installation must atomically replace covered heads through a manifest/snapshot transition so a
snapshot sees the logical rows exactly once. That replacement, checkpoint coverage, and durable
reclamation belong to Phase 6.

If the sealed-head/flush handoff bound is full and a new generation cannot be retained safely, the
owner applies backpressure before admitting the append to WAL. It never drops a sealed head or
allows unbounded retention. Schema activation uses the same sealing path: generations never mix
schemas.

An empty head need not be handed to flush. Sealing is idempotent: repeating it does not change the
frozen boundary or enqueue a second ownership transfer. A crash loses all in-memory generations;
WAL replay reconstructs fresh heads according to the same logical command order.

## Retry no-ops and applied positions

A matching retry normally returns from the live retry table without a WAL append or publication.
If replay encounters a same-digest duplicate command already present in the WAL, it adds no rows
and retains the original outcome, but the tablet records that the later physical command was
processed. It may therefore publish an advanced applied position with the same row boundary. A
conflicting digest publishes nothing and fails recovery.

Rows, tablet retry entries, global identity-directory entries, and applied positions are never
allowed to disagree. A checkpoint or future flush may cover them only as one logical state-machine
prefix; pruning retry state while covered mutations remain retry-protected is forbidden.

## Representation constraints and non-decisions

This contract deliberately does not choose an allocator, vector class, cache-line layout, chunk
size, sort/reorder structure, scan ABI, or flush queue implementation. Those choices require tests
and benchmark evidence. Any implementation must still provide:

- no per-row owning heap allocation on the append path;
- checked capacity accounting before writes;
- stable addresses or immutable owned chunks for pinned readers;
- race-free null, Boolean, offset, and variable-byte access;
- one publication event per command;
- bounded sealed-generation and retry-state ownership; and
- deterministic logical results independent of physical head packing.
