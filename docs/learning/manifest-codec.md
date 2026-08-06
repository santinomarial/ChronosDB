# Manifest v1 Codec

## Purpose and boundary

The `chronos_manifest` library implements the pure in-memory foundation for Phase 6 durable state.
It turns one canonical full manifest model into the exact bytes frozen by
[Manifest v1](../formats/manifest-v1.md), and safely turns an untrusted byte prefix back into a
borrowed immutable view.

This layer does not publish query state or schedule flushes. Its naming
helpers format and parse exact final and temporary basenames without touching a directory, while
its referenced-part validator accepts already-read CSEG images. The storage owner now provides the
durable-installation primitives: immutable CSEG and manifest-generation installation plus locked
namespace scanning, temporary cleanup, and recovery selection. It also provides the pure boundary
that converts one pinned nonempty sealed head into one exact, install-ready CSEG image, then builds
the checked next canonical manifest generation around that image and its retry outcomes.

## Public interfaces

The public headers are:

- `chronos/manifest/format.hpp`: frozen sizes, limits, field offsets, and magic;
- `chronos/manifest/types.hpp`: the nominal `DatabaseId`, checkpoint, and descriptor values;
- `chronos/manifest/naming.hpp`: canonical final/temporary basename formatting and strict parsing;
- `chronos/manifest/layout.hpp`: allocation-free canonical layout planning;
- `chronos/manifest/codec.hpp`: owned encoding, borrowed decoding, limits, and error classes;
- `chronos/manifest/validation.hpp`: exact catalog binding and add-only generation transitions;
- `chronos/manifest/part_validation.hpp`: installed CSEG image-to-descriptor validation;
- `chronos/manifest/sealed_head_flush.hpp`: deterministic sealed-head conversion and its exact
  descriptor/WAL identity result;
- `chronos/manifest/generation_builder.hpp`: checked pure construction of the next generation for
  one sealed-head part and its retry outcomes;
- `chronos/manifest/checkpoint_builder.hpp`: read-only proof of the longest globally consecutive
  WAL prefix represented by one checked candidate; and
- `chronos/manifest/storage.hpp`: locked, directory-anchored immutable part and generation
  installation, strict namespace scanning, and temporary cleanup.

Referenced-part validation borrows exact file images supplied in descriptor order. It validates
the catalog lineage first, then requires the canonical identity-derived filename and exact length,
decodes and fully validates each CSEG, binds its physical schema, compares every duplicated header
field, requires every system WAL identity to equal the manifest WAL, and recomputes the record
sequence extrema. It performs no filesystem operations and retains no image bytes.

`plan_manifest_v1_layout()` accepts only the three descriptor counts. It checks each registry bound,
performs multiplication/addition with checked arithmetic, and returns all section offsets plus the
exact total length. Callers never supply or choose offsets.

`encode_manifest_v1()` accepts a nonzero generation, identities, reclaim checkpoint, and spans of
tablet, part, and retry descriptors. The spans are borrowed only for the call. Success returns a
move-only `EncodedManifest` that owns exactly one complete generation and no enclosing filesystem
or WAL framing.

`decode_manifest_v1_prefix()` accepts the first complete generation in a larger byte span and
returns its consumed borrowed bytes. `decode_manifest_v1_exact()` additionally rejects a suffix.
Both return `DecodedManifestView`, which borrows the immutable encoded generation while owning the
parsed descriptor vectors. Therefore the source bytes must outlive the view and every copy or span
obtained from it.

`encode_sealed_head_v1()` accepts a pinned sealed snapshot, new `PartId`, and explicit page
compression policy. It preflights the generation's single WAL identity and sequence bounds, sorts
row indices by the schema physical ordering key followed by `(wal_id, record_sequence,
row_ordinal)`, chooses the longest canonical granules within the frozen row/page limits, and
materializes every user and hidden system page. The result owns exact immutable CSEG bytes and
carries the exact `PartDescriptor` and WAL identity required by `install_part()`. The encoder exact
decodes and fully schema/content validates its own output before returning it. It never mutates,
unseals, publishes, or releases the source generation.

`build_manifest_v1_for_sealed_head()` accepts that immutable result, the currently selected decoded
generation, exact retained schema bindings, and one retry descriptor per WAL record represented in
the part. It fully revalidates the CSEG bytes, derives record-sequence row counts from the hidden
pages, rejects missing/extra/disagreeing retry outcomes and boundary overlap, retains all old
tablets/parts/retries, then canonically inserts the new state. It exact-decodes its own bytes and
runs the add-only transition validator before returning. The reclaim checkpoint is copied exactly;
only a later WAL-coverage proof may advance it.

`build_manifest_v1_checkpointed_generation()` accepts the predecessor, that still-uncheckpointed
candidate, exact schema lineages, every referenced CSEG image, and a WAL directory. It first repeats
the add-only transition and full part validation. It then uses the WAL suffix scanner's two-pass
integrity/preflight contract before comparing each claimed command with its protected retry outcome
and exact CSEG system and user cells. First-applied records require ordinals `0..N-1` exactly once;
an exact retry duplicate requires no rows. Per-tablet boundaries may be ahead of a missing record on
another tablet, but the returned global coordinate stops permanently at that first gap. The result
owns a newly encoded candidate with only the proven coordinate changed; no WAL, file, publication,
or input object is mutated.
The function takes no lock: the database owner must invoke it while its serialized WAL owner
prevents append, rotation, repair, or reclamation and while referenced images remain immutable.

## Canonical model validation

Encoding does not sort or repair input. It rejects a model unless:

- tablet descriptors are strictly ordered by durable `TabletId` bytes;
- tablet part ranges are consecutive, nonoverlapping, in bounds, and cover the global part array;
- parts are strictly ordered within their tablet and `PartId` is globally unique;
- duplicate table/tablet fields bind to the owning tablet;
- part lengths, row counts, record-sequence extrema, and event-time extrema are valid;
- each tablet row count is the checked sum of its part rows;
- retries are strictly ordered by `(ClientId, ClientBatchId)`;
- each retry binds to an existing tablet, its table, the manifest WAL identity, a covered record
  sequence, and a nonzero applied row count; and
- the empty and nonempty WAL checkpoint shapes obey the frozen WAL v1 boundaries.

These checks establish schema-independent physical consistency. They do not prove that a CSEG file
with the named identity and length exists or that its header/content matches the descriptor. They
also cannot prove that a nonempty physical WAL coordinate is the end of the named record without
the WAL bytes. Installation and recovery must perform those second-stage checks.

## Encoding flow

The encoder validates the model before allocation, plans the exact layout, allocates one zero-filled
vector, and writes fields individually in little-endian order. It derives `previous_generation`, all
counts, offsets, total length, flags, and reserved bytes; callers cannot inject alternative durable
representations.

The zero-filled allocation is intentional: every reserved byte and the four trailer-padding bytes
are canonical zero without relying on native struct layout. Identifier bytes are copied in their
existing UUID network order. Signed event times are serialized as their exact two's-complement bit
pattern.

Finally the encoder writes:

1. the header CRC32C over bytes `[0, 248)`; then
2. the file CRC32C over every byte before the final four-byte checksum.

The returned allocation is the authoritative object; no pointer into an input span is retained.

## Decode trust ladder

The decoder deliberately increases trust in stages:

1. validate caller limits and require/compare the eight-byte magic;
2. require the entire 256-byte header;
3. validate the header CRC before using counts, offsets, or total length;
4. classify version and required flags, then require zero reserved fields;
5. recompute the canonical layout from bounded counts and compare every stored offset/length;
6. apply configured resource limits before descriptor allocation;
7. require the exact complete prefix and validate trailer padding plus full-file CRC;
8. parse nominal identities and descriptors; and
9. run the same cross-descriptor model validation used by the encoder.

This sequence prevents hostile length fields from causing an unbounded allocation or invalid
subspan before their integrity and format bounds are established. Loads are bytewise; unaligned
input is safe and native structs are never reinterpreted.

## Failure behavior

Decode failures remain distinct:

- `kIncomplete`: a valid short prefix, with the minimum currently known required size;
- `kCorruption`: bad magic/checksum/reserved bytes, zero identities, noncanonical layout, or
  contradictory descriptor state;
- `kUnsupported`: checksum-valid future major/minor or required flag semantics; and
- `kResourceLimit`: valid-format bounds that exceed caller policy, or invalid caller limits.

Before header integrity, incomplete input can request only eight or 256 bytes. After the header CRC
and canonical layout validate, incomplete reports the exact generation length. Installed final
files are never accepted as incomplete by the future filesystem owner; that layer will translate a
short final file into durable corruption.

Encoding returns ordinary `Status` failures. Invalid canonical input is `kInvalidArgument`, while
an unrepresentable combined layout is `kResourceExhausted`. Expected validation failures do not
produce partial encoded output.

## Catalog binding and generation transitions

`validate_manifest_v1_schema_binding()` takes one sorted binding per tablet. It requires the
lineage table identity and exact recovery/part schema identities and versions, then proves every
part schema is an ancestor of the tablet recovery schema. Bindings borrow each `SchemaLineage` only
for the call.

`validate_manifest_v1_transition()` first binds both generations to the retained catalog, then
requires unchanged database/WAL identities, an exact one-generation advance, monotonic logical and
physical reclaim coordinates, retained tablets with nondecreasing durable boundaries, and
ancestor-to-descendant recovery-schema movement. Every predecessor part must occur byte-for-byte
logically unchanged in the successor tablet range, and every protected retry outcome must remain
exactly unchanged. New tablets, parts, and retries are allowed; removal, replacement, pruning, and
schema regression are rejected in Phase 6.

The transition validator alone does not claim that a part file exists or that a checkpoint crosses
only covered WAL commands. The separate referenced-part validator proves supplied CSEG images, and
the checkpoint builder completes the read-only WAL/content proof. Durable manifest installation and
publication remain separate operations.

## Complexity and allocation

Layout planning is `O(1)` and allocation-free. Encoding and decoding are `O(total bytes + parts log
parts)` because CRC32C scans the image and global `PartId` uniqueness uses a sorted temporary copy.
Both own one descriptor-vector allocation per nonempty descriptor category; encoding additionally
owns the exact byte image. Referenced-part validation is `O(total CSEG bytes + rows)` and repeats
bounded system-page decoding after complete CSEG validation to recompute manifest-specific WAL and
record-sequence facts.

Sealed-head conversion uses `O(rows)` row-index/sort workspace plus
`O(columns + granules + pages)` metadata and page owners. Its deterministic stable merge sort is
`O(rows log rows * key columns)`; page planning and materialization are linear in logical input
bytes. It allocates the complete encoded CSEG before returning because durable installation
requires one immutable exact image.

Sealed-head generation building uses `O(rows + retained parts + retained retries)` temporary
storage. It validates and decodes the new CSEG, sorts represented record sequences, reconstructs
tablet-grouped part ranges and identity-sorted retries, and emits one exact full-generation image.
This intentionally favors an auditable transition over avoiding a second bounded CSEG pass.

Checkpoint proof uses `O(total referenced rows + parts + tablets)` index storage. It fully validates
and decodes referenced CSEG metadata, sorts row identities once, scans the required WAL suffix twice
through the existing integrity-first recovery API, and decodes user pages only for groups that must
match a first-applied command. Its worst-case work is linear in WAL/CSEG bytes plus row-index sorting;
the extra pass prevents any replay-side observation before the entire physical suffix and every
application kind have been accepted.

`ManifestStorage::open_existing()` opens the database root and its exact `parts/` and `manifest/`
children without following final-component symlinks, then acquires the already-existing
`manifest/LOCK`. The move-only owner holds all descriptors and the lock for its lifetime and is
single-writer rather than internally synchronized.

`install_part()` rejects invalid input before filesystem mutation. It exclusively creates the
recognized temporary name, writes all bytes, verifies size, reads back and repeats full validation,
syncs and closes the file, renames without replacement, and syncs `parts/`. A failure before rename
leaves only the candidate temporary. A directory-sync failure after rename poisons the live owner
because the final namespace's crash outcome is uncertain; recovery must reconcile it. Successful
file/directory sync and installed-byte counters advance only at their completed boundaries.

`scan_namespace()` classifies a sorted snapshot of both locked directories without following
symlinks. It accepts only regular files with exact final or recognized temporary names, plus the
regular `manifest/LOCK`, and requires final manifest generations to be nonempty and consecutive
from one. Final CSEG parts need not yet be referenced: an interrupted flush may legitimately leave
an immutable orphan, which later recovery must retain until ownership is proven.

`cleanup_temporaries()` first performs that complete scan, then removes only recognized temporary
files and synchronizes each directory whose entries changed. It never promotes a candidate and
never removes a final generation or part. A failed directory sync poisons the live owner because a
subsequent operation cannot safely assume which removals survived a crash. Repeating successful
cleanup is idempotent and performs no unnecessary sync.

`install_manifest()` accepts one already-owned canonical candidate and selects the current
predecessor from the highest consecutive final name. Before creating a temporary, it exact-decodes
that predecessor without fallback, validates the add-only transition and retained catalog binding,
and reopens and fully validates every referenced final CSEG against its descriptor and schema. It
then exclusively creates the recognized generation temporary, writes and exact-readback decodes
the same bytes, compares the readback byte-for-byte, synchronizes and closes the file, renames
without replacement, and synchronizes `manifest/`.

The final directory sync is the generation durability boundary. A pre-rename failure leaves at
most a recognized temporary and keeps the owner usable. A failed directory sync after rename
poisons the live owner because restart must select the durable namespace truth. Installation
metrics distinguish attempted/failed work, referenced-part validations, file and directory syncs,
and generations/bytes that crossed the complete durability boundary.

`load_selected_manifest()` is the read-only recovery-side trust boundary. It selects only the
highest consecutive final name, exact-decodes it without fallback, checks the configured database
and WAL identities, binds the retained catalog, and reopens and validates every referenced final
CSEG. Success returns a move-only owner of the exact manifest bytes and parsed descriptor arrays,
plus sorted unreferenced final-part identities and recognized temporary names from the same locked
scan. Those orphan and temporary entries are observations, not trusted data or cleanup authority;
the call performs no mutation, WAL replay, or publication.

The current one-GiB format maximum is not a recommended operating size. Runtime limits let an
owner enforce a smaller memory budget before allocation. Retry admission and manifest generation
policy will establish practical bounds in later Phase 6 work.

## Evidence and measurement

The codec tests include an empty-generation golden constructed independently from the specification,
populated round trips, every truncation boundary, checksum-valid semantic corruption, configured
limits, deterministic generated models, and every single-bit mutation. Public headers compile as
self-contained translation units. The installed CMake target and headers are compiled and executed
by the external-consumer test.

The libFuzzer entry exercises arbitrary input plus structured mutation/truncation of a populated
canonical generation. ASan/UBSan and TSan builds run the deterministic suite. The microbenchmarks
measure full canonical encoding, full exact decoding, and add-only transition validation at
increasing retained-retry counts and report processed bytes/items plus descriptor scale. A
referenced-part case measures full compressed CSEG validation through manifest-specific WAL and
record-extrema binding. Benchmark
results are evidence only when captured under the repository benchmark contract; no performance
number is claimed by this document.

The sealed-head suite covers active/empty rejection, canonical ordering and hidden identities,
nullable variable and packed Boolean materialization, raw/Zstandard determinism, the 65,536-row
granule boundary, deterministic generated heads, an independently fingerprinted complete fixture,
and direct durable installation without descriptor translation. Flush microbenchmarks measure
1,024-row and 65,536-row conversion for raw and Zstandard policies and report rows and encoded bytes
processed.

The generation-builder suite covers exact retry-to-CSEG row agreement, WAL/schema/boundary and
duplicate-identity rejection, canonical retention/insertion, deterministic generated identities,
an independently computed complete-generation CRC fixture, and a real part-then-manifest durable
installation followed by recovery selection. Its benchmark measures complete compressed-part
validation, hidden-sequence summarization, canonical edit construction, encoding, self-decoding,
and transition validation at 1,024 and 65,536 rows.

The checkpoint-builder suite covers exact user/system row agreement, independently fingerprinted
output, retry-digest disagreement, absent tablet boundaries, incomplete tails, unsupported kinds,
global multi-tablet gaps, exact duplicates with zero extra rows, and deterministic generated values.
It also installs/selects the returned generation through `ManifestStorage` and proves that recovery
from the selected physical coordinate replays no already-covered record.
Its filesystem-backed microbenchmark measures complete referenced-part validation, WAL discovery and
two-pass scanning, command decoding, exact row comparison, and final manifest encoding at 1,024 and
65,536 rows.

## Tradeoffs and extension rules

Owning parsed descriptor vectors costs memory but keeps the public view simple and avoids repeated
unaligned byte decoding. Borrowing the large original image avoids a second full-file copy. A future
measured need could add descriptor iterators, but it must retain the same validation-before-access
guarantee.

Full immutable generations make encoding proportional to all retained state. This is accepted for
Phase 6 correctness. A future version-edit log or compacted manifest scheme needs a separate durable
contract and cannot change Manifest v1 bytes or selection rules.

Likely review questions are:

- Why is the header CRC checked before the file CRC? It safely bounds the location and size needed
  to find the complete checksum.
- Why does decoding still own vectors? The returned byte image is borrowed, but parsed nominal
  values are aligned ordinary C++ objects with safe access and stable spans.
- Why does the encoder reject unsorted input instead of sorting? Sorting would silently change
  descriptor relationships and hide builder bugs; canonical state construction is a separate
  responsibility.
- What remains before Phase 6 is complete? WAL coverage authorization for checkpoint advancement,
  atomic head-to-part publication and retirement, flush scheduling, and integrated crash-matrix
  evidence.
