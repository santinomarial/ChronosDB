# Manifest v1 Codec

## Purpose and boundary

The `chronos_manifest` library implements the pure in-memory foundation for Phase 6 durable state.
It turns one canonical full manifest model into the exact bytes frozen by
[Manifest v1](../formats/manifest-v1.md), and safely turns an untrusted byte prefix back into a
borrowed immutable view.

This layer does not open files, prove WAL coverage, publish query state, or delete WAL segments.
Its naming helpers format and parse exact final and temporary basenames without touching a
directory, while its referenced-part validator accepts already-read CSEG images. Durable
installation and recovery remain separate.

## Public interfaces

The public headers are:

- `chronos/manifest/format.hpp`: frozen sizes, limits, field offsets, and magic;
- `chronos/manifest/types.hpp`: the nominal `DatabaseId`, checkpoint, and descriptor values;
- `chronos/manifest/naming.hpp`: canonical final/temporary basename formatting and strict parsing;
- `chronos/manifest/layout.hpp`: allocation-free canonical layout planning;
- `chronos/manifest/codec.hpp`: owned encoding, borrowed decoding, limits, and error classes;
- `chronos/manifest/validation.hpp`: exact catalog binding and add-only generation transitions; and
- `chronos/manifest/part_validation.hpp`: installed CSEG image-to-descriptor validation.

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

The transition validator does not claim that a part file exists or that a checkpoint crosses only
covered WAL commands. The separate referenced-part validator proves the supplied CSEG images; WAL
coverage still requires WAL bytes and remains installation-layer work.

## Complexity and allocation

Layout planning is `O(1)` and allocation-free. Encoding and decoding are `O(total bytes + parts log
parts)` because CRC32C scans the image and global `PartId` uniqueness uses a sorted temporary copy.
Both own one descriptor-vector allocation per nonempty descriptor category; encoding additionally
owns the exact byte image. Referenced-part validation is `O(total CSEG bytes + rows)` and repeats
bounded system-page decoding after complete CSEG validation to recompute manifest-specific WAL and
record-sequence facts.

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
- What remains before manifests are durable? Exact installed-CSEG content and WAL-coverage
  validation, filesystem installation, atomic publication, crash recovery, and WAL reclamation.
