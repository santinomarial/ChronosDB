# Database Storage Publication

## Purpose and boundary

`DatabaseStoragePublisher` is the Phase 6 query-visibility boundary between mutable in-memory heads
and installed CSEG parts. A reader must see either the old sealed head with Manifest generation
`N`, or its exact replacement part with generation `N + 1`. It must never assemble independently
loaded tablet and Manifest pointers that could expose the rows twice or omit them.

This component performs no filesystem I/O, WAL mutation, flush scheduling, query execution, or
part deletion. Its durable input is an owning `LoadedManifestGeneration` returned after
`ManifestStorage` selected and validated a directory-synchronized generation.

## Public interfaces and ownership

`chronos/manifest/publication.hpp` provides:

- `DatabaseStoragePublisher`, a move-only single-writer owner;
- `DatabaseStorageSnapshot`, a copyable owning pin for one acquire-observed database epoch;
- conservative complete retained-memory reporting for that epoch;
- `PublishedTabletStorage`, the exact sealed and active head pins for one tablet;
- `publish_tablet_snapshot`, which refreshes one complete monotonic tablet epoch without changing
  durable state; and
- `publish_manifest`, which selects exactly the next durable generation and retires explicitly
  matched sealed heads from new snapshots; and
- non-forgeable `SealedGenerationRetirementReceipt` values issued by that successful aggregate
  publication and consumed idempotently by the shard-owned `TabletState`.

The internal immutable `DatabaseStoragePublication` owns a shared selected Manifest generation,
copies of every exact `HeadSnapshot`, and one shared private lifetime identity per selected part.
Manifest descriptors and encoded bytes borrow the retained Manifest owner. Head cells borrow their
retained head pins. Part identities are carried into every tablet-only or Manifest epoch that
retains them. Old `DatabaseStorageSnapshot` objects therefore keep old Manifest bytes, retired head
arenas, and selected-part reclamation pins alive after publication and after the live publisher is
destroyed.

## Invariants and validation

Tablet epochs are canonical by `TabletId`. Their sealed heads are generation ordered and precede
one active head. Every live row belongs to the selected WAL history and has a record sequence later
than that tablet's durable Manifest boundary. Refresh rejects an applied-position regression, a
dropped sealed generation, a changed retained row identity, an active-generation regression, or a
rotation that does not retain the former active generation.

A durable replacement requires exact generations `N` then `N + 1`, matching database/WAL identity,
and add-only retention of prior tablets, parts, and retry outcomes. Every newly selected part must
map one-to-one to an explicitly named nonempty sealed head. Publication compares table, tablet,
schema identity/version, row count, WAL sequence extrema, and event-time extrema. It then verifies
that the new durable row count includes exactly those rows and that every remaining head row lies
strictly after the new durable boundary.

Visible-row arithmetic is checked. A failure after a durable successor is handed to
`publish_manifest` fails the publisher closed: continuing with its old in-memory epoch could
contradict the selected durable truth. A rejected pre-durable tablet refresh leaves the owner usable
and its current epoch unchanged.

## Atomic object and memory ordering

The exact atomic object is one `shared_ptr<const DatabaseStoragePublication>`. The single writer
allocates and completely initializes the successor, invokes the test-only prepublication hook, then
performs a release store. `snapshot()` performs one acquire load and returns an owning copy of that
pointer.

Initialization of the Manifest owner, descriptor arrays, head pins, and aggregate counts is
sequenced before the release store. A reader that observes the pointer through the acquire load can
therefore read every field with ordinary immutable accesses. Reference counts provide lifetime,
not logical synchronization; the release/acquire pointer exchange establishes visibility. The
implementation does not claim the shared-pointer atomic operation is lock-free.

## Complexity and tradeoffs

Snapshot acquisition is `O(1)` plus shared ownership accounting. A tablet refresh copies the
database tablet descriptor vector and validates retained head rows, so its cost is
`O(T + R)` for tablets and inspected retained rows. A Manifest replacement copies the same vector,
validates add-only durable descriptors, and inspects the retired and remaining head rows; it is
`O(T + P + Q + R)`. This work is intentionally off the row append's inner mutable-head storage
path. Descriptor copying favors a simple auditable immutable epoch over a more complex persistent
tree before profiles justify one.

The descriptor retains Manifest identities rather than entire CSEG file images. Phase 7
reclamation now watches weak per-part lifetime pins, not one immediate-predecessor publication:
older tablet-refresh epochs can select the same part without retaining that immediate object.
`SnapshotPartImage` closes the descriptor-to-file-open interval by copying the exact publication
token into a fully revalidated owned image.

## Evidence and benchmark method

Tests cover tablet rotation, stale-epoch rejection, exact part substitution, old-snapshot lifetime,
a writer paused immediately before the release store, hostile replacement identity, fail-closed
behavior, and prevention of durable-row reintroduction. The same focused cases run under
ASan/UBSan and TSan. Receipt integration additionally proves that pre-publication backpressure
remains, post-publication retirement releases it, repeated consumption is harmless, and a paused
tablet retirement exposes only the complete old or new outer epoch. A compaction regression holds
an older same-Manifest tablet-refresh epoch and a snapshot-loaded image until reclamation proves
both lifetimes have drained. The external-consumer test compiles the installed public API.

`chronos_manifest_benchmarks` measures the acquire-load snapshot hot path, complete tablet-epoch
descriptor/pin construction and refresh, one-candidate pinned retirement checks, and idempotent
post-reclamation verification. Fixture creation and filesystem setup occur outside timed loops.
Results are local microbenchmarks, not a production latency claim.

## Likely review questions

- Why is one atomic pointer necessary? Independently observing a new Manifest and old head set can
  duplicate rows; the opposite combination can omit them.
- Why validate row identities during a tablet refresh? Generation numbers alone cannot prove that a
  retained prefix still names the same committed row versions.
- Why does a durable-publication error fail closed? The directory-synchronized generation is
  recoverable truth, so silently continuing an older live view would create two authorities.
- Why are old heads not immediately freed? Readers may still own the prior database epoch; shared
  ownership delays reclamation until its last snapshot is released.
- What remains? The end-to-end coordinator and filesystem/publication crash matrix are implemented.
  Persistent catalog reconstruction, service activation, and reviewed device campaigns remain
  outside this in-memory publication primitive.
