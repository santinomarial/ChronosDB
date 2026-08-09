# CSEG storage implementation

This document explains the implemented `chronos_cseg` library. The durable sources of truth are the
[CSEG v1](../formats/cseg-v1.md) and [CSEG v2](../formats/cseg-v2.md) specifications; this guide
describes how the code realizes those contracts and how callers should compose its interfaces.

## Purpose and boundary

CSEG is ChronosDB's immutable, sorted columnar-part format. One file belongs to one table, tablet,
and schema version and contains one or more granules. Every granule has one independently stored
page for every user column and for the mandatory `WAL_ID`, `RECORD_SEQUENCE`, `ROW_ORDINAL`, and
`OPERATION` system columns. CSEG v2 retains the physical envelope but replaces that suffix with
eight temporal columns that support WAL or Raft source identity, correction/tombstone operations,
logical identity, and receive/system commit time.

The library is pure in-memory. It plans layouts, encodes and decodes metadata/pages/complete parts,
validates content, serves projected granule reads, and produces inspection reports. It does not
install files, edit manifests, flush heads, checkpoint WAL coverage, reclaim files, or synchronize
readers with storage deletion. Those are Phase 6 responsibilities.

## Public interface map

- `format.hpp` contains the authoritative v1 constants, limits, field offsets, registry codes, and
  static layout checks.
- `types.hpp` defines nominal `PartId` and the typed column, granule, and page descriptors.
- `layout.hpp` performs checked canonical metadata and page placement without allocating.
- `compression.hpp` owns bounded raw/Zstandard compression and decompression policy.
- `metadata_codec.hpp` deterministically encodes the metadata directory and provides bounded,
  borrowed prefix/exact decoding with explicit incomplete, corrupt, unsupported, and resource-limit
  outcomes.
- `plain_page.hpp` validates the schema-independent physical column shape and encodes/decodes the
  canonical validity, offsets, and values concatenation.
- `page_codec.hpp` composes PLAIN pages with raw/Zstandard storage and CRC32C.
- `part_codec.hpp` creates an exact owned file image or borrows a structurally valid part view.
- `validator.hpp` performs version-strict complete schema-independent row validation and optional
  exact catalog binding for v1 append rows and v2 temporal histories.
- `projected_reader.hpp` authenticates metadata and reads selected granules/columns against a
  retained schema lineage. Its borrowed read plan validates and reports exact decoded-buffer work
  before result allocation for either the four-column v1 or eight-column v2 system suffix.
- `chronos/query/cseg_scan.hpp` is the query-layer single-part source that reserves from that plan
  and retains the decoded granule plus immutable encoded-image pin in one chunk backing.
- `inspection.hpp` returns an owned, value-free report after complete structural and
  schema-independent semantic validation.

All public headers are self-contained and installed with the `chronos::cseg` CMake target. The
`chronos-csegdump` executable is the filesystem adapter for the in-memory inspector.

## File and ownership model

The canonical file is a metadata prefix followed by aligned stored pages in granule-major,
stored-column-major order. Metadata contains fixed-size file, column, granule, and page descriptors.
The header and complete metadata prefix have separate CRC32C coverage; each page descriptor records
the CRC32C of its stored bytes. Reserved and alignment bytes must be zero.

`EncodedCsegMetadata`, `EncodedCsegPlainPage`, `EncodedCsegPage`, and `EncodedCsegPart` own exact
immutable byte vectors and remain valid across moves. Decode views borrow their input: their owner
must outlive the view and any raw page view obtained from it. Zstandard decode results own their
uncompressed bytes. Projected batches own decoded column storage. `CsegInspectionReport` copies all
descriptors and retains no source bytes or row values, so it survives destruction of the encoded
part.

No decoder serializes native structs, performs unaligned typed loads, or trusts platform-sized
integers. Durable integers are fixed-width little-endian values; identifiers retain their exact
16-byte representation.

## Validation layers

Validation is deliberately layered because callers have different trust and I/O boundaries:

1. Layout planning checks counts, additions, multiplications, maximum lengths, and canonical
   offsets before allocation.
2. Metadata decoding validates magic/version/registry values, reserved bytes, descriptor bounds,
   cross-field relationships, header CRC32C, and metadata CRC32C before returning borrowed spans.
3. Page decoding validates stored-byte CRC32C before entering the decompressor, enforces output and
   Zstandard-window bounds, then validates the PLAIN physical buffers.
4. Part decoding validates every page and every alignment byte. Prefix decode reports the exact
   next required length; exact decode rejects any suffix.
5. Complete content validation decodes all correctness-critical pages, validates version-specific
   system rows, recomputes part/granule event-time extrema, and checks strict physical ordering
   across granule boundaries for every logical type, including null and IEEE floating-point edge
   cases. V2 orders the user key followed by its four-field physical source identity.
6. Schema binding requires the exact table/schema identity and version, tablet identity, user-column
   ordinals/IDs/types/nullability, event-time column, ordering key, and system-column definitions.

The projected reader uses a different cost boundary: it authenticates the complete metadata prefix,
then validates requested user pages and every version-mandatory system page for requested granules. It binds
the stored schema through `SchemaLineage`, permits an explicitly requested retained successor, and
synthesizes canonical all-null buffers only for nullable columns appended by that successor.
Unrequested user page bytes are neither read nor claimed to be valid by this API.

`plan_granule` first validates the complete ordinal request with a fixed bounded bitmap and reads
only authenticated descriptors. Its borrowed plan reports source and synthesized counts and exact
decoded canonical bytes split between raw page borrows and compressed/synthesized owned buffers.
The reader and caller-owned ordinal span remain alive, unmoved, and immutable until plan execution.
Execution revalidates the request before allocating, removes the former temporary projection
vectors, and converts allocation failures to `RESOURCE_EXHAUSTED`. The byte plan does not include
container/allocator bookkeeping, provider workspace, file pins, or future query backing objects.

ADR 0026's scan source supplies those next-layer owners for one in-memory part. Its trusted
`CsegPartPin` carries complete byte/snapshot lifetime and a conservative retained charge. Source
credit precedes metadata open; output credit precedes page work; and the returned backing owns the
pin, decoded/synthesized buffers, result containers, selection, and ordinal mapping. Database-wide
snapshot-bound loading now supplies exact publication provenance and reclamation lifetime for one
selected part. Multi-part/head composition and pruning remain separate.

## Failure behavior and limits

Decode APIs separate a valid short prefix from invalid bytes. `kIncomplete` carries the exact next
required size; `kCorruption` identifies malformed or integrity-failing bytes; `kUnsupported`
identifies a well-formed registry/version value unavailable to this implementation; and
`kResourceLimit` reports a caller or format limit. Programming/input-construction errors use common
`InvalidArgument`, `OutOfRange`, or resource statuses as appropriate.

All counts and byte lengths are checked before allocation or subspan formation. Zstandard frames
are rejected before unbounded output allocation and are constrained by both the durable v1 maximums
and caller-provided limits. The inspector checks the filesystem size before allocating its input
buffer and defaults to a 1 GiB operational limit even though the durable format maximum is larger.

None of these CSEG-local APIs make a file durable or safe to reference from a manifest. Manifest v2
now has a separate exact single-image admission boundary that binds CSEG 2/0 bytes, schema, source,
digest, and recomputed temporal extrema. Its complete-generation validator additionally requires
exact descriptor-order image coverage and canonical names. The local Manifest v2 storage path now
durably installs validated candidates with exact readback, file sync, no-replace rename, and
directory sync. They become generation-authorized durable objects after the corresponding v2
Manifest installation boundary. The v2 recovery loader now rereads and validates them before
returning an owning selected-generation result; application recovery and query publication are
still pending.

## Complexity

| Operation | Time | Additional memory |
| --- | --- | --- |
| Layout planning | `O(columns + granules + pages)` | `O(1)` |
| Metadata exact decode | `O(metadata bytes)` | descriptor vectors bounded by declared counts |
| Raw page decode | `O(stored bytes)` for CRC and validation | borrowed page, `O(1)` payload ownership |
| Zstandard page decode | `O(stored + output bytes)` | bounded uncompressed page |
| Complete part decode | `O(metadata + all page bytes)` | descriptor storage plus one decoded page at a time |
| Full semantic validation | `O(rows × stored columns)` | bounded per-page decode and ordering state |
| Projected granule planning | `O(selected columns + system columns)` | fixed 4,096-bit stack bitmap; no heap |
| Projected granule read | `O(metadata + selected/system page bytes)` | owned requested result buffers |
| Single-part physical pull | `O(selected/system page bytes + rows)` | accounted backing, selection, and part pin |
| Inspection | `O(metadata + all page bytes + rows × stored columns)` | owned descriptors plus bounded validation state |

The table is asymptotic. Compression cost depends on the maintained Zstandard provider and data
distribution.

## Compression tradeoff

`PageCompression::kNone` always stores canonical PLAIN bytes. The Zstandard policy uses the accepted
bounded provider configuration but retains compressed output only when it is smaller; otherwise the
descriptor records raw storage. This keeps decoding deterministic and prevents expansion merely to
satisfy a requested compression policy. Page CRC32C always covers the bytes actually stored.

## Testing and measurement

The subsystem has independent metadata and complete-file goldens; checked-boundary and deterministic
property tests; round trips across logical types, nulls, variable-width data, and compression;
truncation, suffix, splice, reserved-byte, registry, checksum, extrema, ordering, system-row,
schema-binding, and decompression-limit corruption cases; decoder fuzzers; external-consumer and
installation tests; and ASan/UBSan/TSan coverage.

Projected planning additionally has exact descriptor/ownership accounting tests, foreign-reader and
hostile-request rejection, deterministic direct-versus-planned execution, a dedicated allocator
test proving zero successful-path plan allocations and classifying every output allocation failure,
and plan-then-read fuzz coverage.

The query scan adds pin-after-source-destruction, pre-decode admission, cross-query, cancellation,
LIMIT composition, multi-granule deterministic properties, exhaustive allocation failure, hostile
scan fuzzing, and raw/Zstandard pull benchmarks.

Microbenchmarks cover metadata, PLAIN payloads, stored pages, complete part composition/decoding,
validation, and projected reads. Interpret them using the repository benchmark contract: retain the
revision, compiler, host, arguments, and complete output; compare like datasets and compression
policies; and do not choose a durable default from a single favorable workload. Important evidence
includes encoded size, encode/decode throughput, selective-read cost, allocations, and raw versus
Zstandard tradeoffs.

## Inspection workflow

`chronos-csegdump [--max-bytes N] [--descriptors] <file>` reads one exact file snapshot, performs
exact part decoding and complete schema-independent semantic validation, and prints deterministic
identities, counts, extrema, storage totals, and optional descriptors. It intentionally prints no
row values and cannot claim catalog schema binding without a schema lineage. Exit status is `0` for
valid input, `3` for an incomplete prefix, `4` for unsupported durable values, `2` for command-line
misuse, and `1` for corruption, limits, or I/O failure. It never modifies the input.

## Likely review and interview questions

- Why are header, metadata, and page CRCs separate? They establish page bounds cheaply, isolate
  corruption, and permit selective page validation without trusting unauthenticated offsets.
- Why does structural part decode not imply semantic validity? Physical buffers can be well-formed
  while system identities, extrema, or global ordering are wrong.
- Why is projected reading not implemented by full part decode first? Full decode would destroy the
  selective-I/O boundary; projected reading authenticates metadata and validates exactly the pages
  whose semantics it returns, plus required row identities.
- Why is the read plan borrowed? Copying projection ordinals would allocate before a query scan can
  reserve for the read; the physical plan already owns stable immutable ordinals.
- Why are decode views borrowed? They avoid redundant copies on immutable input while keeping
  lifetime responsibility explicit; APIs that need independence return owned objects.
- Why is schema binding separate from physical decode? Durable physical types are self-describing,
  but catalog meaning depends on retained schema identity, lineage, and column policy.
- What must happen before a CSEG becomes visible? A future crash-safe installation and manifest edit;
  codec validity alone is not publication or durability.
