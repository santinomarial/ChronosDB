# Snapshot-Bound CSEG Loading

## Purpose and phase boundary

This layer joins three previously separate proofs: an aggregate database snapshot authorizes an
exact part descriptor, `ManifestStorage` revalidates and owns that file's bytes, and the query CSEG
source retains both through every returned chunk. It is the first safe bridge from a published
Manifest part to physical query execution.

The bridge scans one part. It does not choose or order multiple parts, read mutable heads, apply
zone-map pruning, resolve row versions, lower SQL, perform asynchronous I/O, or schedule morsels.

## Public interfaces

`chronos/manifest/storage.hpp` exposes:

- `SnapshotPartImage`, a move-only exact image with database/WAL/generation provenance, selected
  `PartDescriptor`, immutable bytes, publication retention token, and conservative retained count;
  and
- `ManifestStorage::load_snapshot_part_images`, the locked read path for a strictly sorted selected
  subset of one `DatabaseStorageSnapshot`.

`chronos/query/database_cseg_scan.hpp` exposes:

- `pin_snapshot_cseg_part`, the trusted conversion to `CsegPartPin`; and
- `create_snapshot_cseg_scan`, the identity/schema preflight plus existing single-part source
  factory.

`chronos::query` now publicly links `chronos::manifest` because its installed adapter accepts the
public snapshot image type.

## Ownership and reclamation

```text
DatabaseStoragePublication
  ├── selected Manifest and head pins
  └── shared PartRetentionPin for every selected PartId
            ▲ carried through every epoch that retains that part

SnapshotPartImage
  ├── DatabaseStorageRetentionToken ──► exact publication and PartRetentionPin
  ├── selected descriptor/provenance
  └── owned validated CSEG bytes
             │
             ▼
        CsegPartPin
             │
             ▼
       returned VectorChunk backing
```

A compaction retirement holds weak pointers to the removed inputs' per-part pins. It remains
pending while any old publication, explicit token, snapshot image, scan source, or returned chunk
can still name those inputs. New tablet-only epochs share the same pins; a compaction successor
omits removed pins. Expiry therefore means no reader can later reacquire the old identity through
the publisher.

This fixes a subtle whole-publication mistake. Watching only the immediate predecessor object is
insufficient because an older tablet-refresh object under the same Manifest can remain live without
owning that immediate object.

## Load and validation sequence

The single-threaded `ManifestStorage` owner:

1. rejects empty or non-strictly-sorted part identities;
2. holds the supplied aggregate snapshot for the complete call;
3. scans and classifies the locked namespace without following symlinks;
4. finds each exact descriptor in that snapshot rather than the current Manifest;
5. requires the exact final part name to remain present;
6. resolves the descriptor's exact retained tablet/schema lineage;
7. reads exactly the declared file length under configured limits;
8. repeats complete CSEG content, WAL, table, tablet, schema, row-count, sequence, and event-time
   validation; and
9. returns an image containing its own publication token before the caller can release the input
   snapshot.

A held predecessor snapshot remains valid after a newer Manifest becomes namespace maximum. The
loader intentionally does not fall back to a different descriptor or generation.

## Query admission and failure behavior

The image's retained count conservatively includes the complete aggregate epoch, Manifest decoded
state, mutable-head arenas, part-pin bookkeeping, owned file image, objects, and allocator
allowances. Multiple images and chunks independently repeat the shared epoch charge. This can
over-admit less work but prevents any one result from retaining uncharged storage memory.

The query adapter validates selected descriptor length, target tablet, source schema identity and
version, and destination table before calling `CsegScanOperator`. A mismatch is `INVALID_ARGUMENT`
with no reservation. Budget failure is `RESOURCE_EXHAUSTED` before metadata/page decode. Filesystem
absence or malformed authoritative bytes fail through the storage loader; CSEG page corruption
found during a selective pull retains the existing `CORRUPTION`/cancellation behavior.

No failed call changes a Manifest, part, publication, or reclamation record.

## Complexity and measurement

Loading is `O(namespace entries + requested parts + requested file bytes + full validation)` and
owns one byte vector per requested part. Pin conversion is `O(1)`. Single-part pull complexity is
unchanged from the pinned CSEG source.

The existing publication benchmark includes one-candidate weak-pin checks and tablet epoch
refresh; the existing CSEG scan benchmark measures the delegated raw/Zstandard pull. Fixture setup,
snapshot loading, and file I/O remain outside that pull benchmark. No timing claim is made for the
thin adapter until an asynchronous or mapped-file provider creates a distinct hot path worth
measuring.

## Tradeoffs and next steps

Owned reads are portable and auditable but copy the complete selected file. Full-epoch charging is
safe but pessimistic. Per-part pins add one shared lifetime object per newly selected part and one
shared pointer per publication descriptor.

The next storage execution increment can now accept several `SnapshotPartImage` values from one
epoch, apply conservative part/granule pruning, and define deterministic part order. Mutable heads
still need a canonical physical materialization/backing contract before a complete aggregate scan
can merge them without violating accounting or row semantics.

## Likely review questions

**Why can an old snapshot load after a newer Manifest is selected?** Stable snapshots are the
contract. Its per-part pin prevents reclamation, and its descriptor remains the authority for that
reader.

**Why not watch the predecessor publication pointer?** Older tablet-refresh publications can name
the same part without retaining that exact object. They do share the part's lifetime identity.

**Why repeat the whole snapshot charge per image?** Current query credit is independently RAII-owned
and cannot be shared safely across arbitrary surviving chunks. Conservative duplication preserves
the lower-bound invariant.

**Does the image prove page integrity forever?** It proves the complete bytes at load time and owns
immutable copies. The selective reader still verifies every requested and mandatory system page at
pull time.

**Does this implement an aggregate query snapshot scan?** No. It establishes safe selected-part
ownership for that later composition.
