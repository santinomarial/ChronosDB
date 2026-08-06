# ADR 0016: CSEG v1 Layout, Integrity, and Compression

- **Status:** accepted
- **Date:** 2026-08-06
- **Owners:** ChronosDB storage-format maintainers

## Context

[ADR 0005](0005-columnar-heads-and-immutable-cseg-parts.md) chooses immutable sorted compressed
columnar parts but deliberately defers their bytes. Phase 5 cannot safely implement a writer,
reader, validator, inspector, fixture, or fuzz target until independent implementations can agree
on exact framing, page interpretation, integrity coverage, resource limits, and rejection behavior.

The existing logical schema, Columnar Batch v1, mutable head, and WAL application command already
freeze user-column identities and representations plus the single-node hidden row identity. CSEG
must carry those meanings without depending on a mutable catalog to establish safe physical
bounds. It must also permit projection without reading or decompressing unrelated columns while
still supporting complete validation before installation.

## Decision

Adopt the normative [CSEG v1 specification](../formats/cseg-v1.md) with these boundaries:

- one nonempty file represents one part, table, tablet, and immutable schema version;
- a fixed checksummed metadata prefix contains the part/schema identities, exact column, granule,
  and page directories, and event-time extrema;
- user columns are followed by four fixed system columns preserving WAL identity, record sequence,
  batch row ordinal, and operation kind;
- granules are canonical longest prefixes of at most 65,536 rows whose individual uncompressed
  column pages fit the 64 MiB bound;
- every granule has one PLAIN page per stored column, ordered granule-major then column-major;
- every stored page has a metadata-protected location, interpretation, size, and stored-byte
  CRC32C; the metadata prefix has both an early header CRC and a complete metadata CRC;
- page compression is either `NONE` or one bounded, dictionary-free Zstandard frame; canonical
  Zstandard writing uses deterministic single-threaded level 3 settings and falls back to `NONE`
  when the frame is not smaller;
- rows sort by the schema physical ordering key followed by the current stable system row identity,
  with exact v1 null, floating, textual, binary, and UUID comparison rules;
- physical decoding, complete semantic validation, page projection, and exact schema binding are
  distinct operations; and
- incomplete input, corruption/invalid input, and checksum-valid unsupported semantics are distinct
  outcomes.

The v1 codec owns all CSEG framing and logical encodings. A maintained Zstandard provider supplies
only the general-purpose compression transform. Its types do not enter ChronosDB public headers,
and it does not define page boundaries, stored codes, checksums, limits, or acceptance semantics.
A production dependency record and reproducible version policy are required with the codec.

Manifests, temporary-file naming, durable installation, flush scheduling, checkpoints, compaction,
delta-part semantics, correction/tombstone operation codes, and reclamation remain outside this
decision.

## Detailed rationale

A single file gives Phase 6 one atomic immutable object to install and identify. A contiguous
metadata prefix allows a reader to validate all locations and interpretations before seeking into
page data. Per-page checksums and compression make projection independent: a scan need not read
unselected columns merely to establish their byte boundaries. A second metadata checksum protects
the complete directory, while the early header checksum prevents hostile counts or offsets from
being trusted to find it.

One page per `(granule, stored column)` matches vectorized projection and avoids introducing a
second buffer-page directory. Reusing Columnar Batch v1 PLAIN buffer rules reduces the number of
durable value representations and makes head/batch/part comparisons direct. Canonical granule
boundaries based on uncompressed sizes remain stable across compressor versions and data ratios.

CRC32C is already a tested common primitive and is sufficient for accidental-corruption detection;
it is not presented as authentication. Zstandard is an established compressor explicitly permitted
by [ADR 0011](0011-dependency-and-build-versus-buy-policy.md) and the project's non-goals. Bounded
output, exact frame consumption, and pre-decompression CRC checks keep provider input within a
ChronosDB-owned safety envelope.

The system sort suffix makes physical output deterministic when user keys tie and preserves the
row-version identity required by snapshots and later compaction. Limiting operation code v1 to the
currently implemented `APPEND_ROWS` semantics avoids inventing correction/tombstone contracts ahead
of their roadmap phase.

## Alternatives considered

- **Parquet as the primary format:** mature and interoperable, but it would not own the accepted
  ChronosDB part lifecycle, hidden version identity, canonical sort, compatibility, and validation
  contract. Parquet remains an interoperability option.
- **One file per column:** simplifies individual column replacement but expands atomic installation,
  naming, missing-piece recovery, and manifest state. Installed parts are immutable, so replacement
  is not a v1 requirement.
- **A footer-only directory:** supports streaming writes but requires locating and trusting a tail
  before page bounds are known. The fixed metadata prefix gives a bounded fail-closed parse and
  selective-read map up front.
- **A whole-file checksum only:** detects broad corruption but forces every projected scan to read
  the complete file. Metadata plus per-page checksums preserves independent validation.
- **Checksumming only compressed page bytes:** leaves offsets, codecs, decoded sizes, and other
  interpretation fields exposed. CSEG v1 includes those fields in the metadata checksum.
- **Custom compression:** conflicts with the explicit non-goal and adds avoidable compatibility and
  security maintenance. General-purpose compression is not a core database subsystem.
- **Compression omitted from v1:** would simplify the first codec but fail the accepted compressed
  part architecture and postpone decompression-limit correctness until after the format shipped.
- **Dictionary and specialized encodings in v1:** may improve selected workloads, but no current
  evidence justifies freezing their semantics. New encoding codes require later accepted evidence.
- **Defer the exact format further:** blocks every Phase 5 implementation and invites fixtures and
  codecs to accidentally become the contract.

## Consequences

- Phase 5 must add and maintain a narrowly wrapped Zstandard production dependency.
- Writers buffer or otherwise know the complete metadata directory before producing the final file;
  they cannot emit an unpatchable one-pass stream to a nonseekable destination.
- Readers can validate and project pages independently, but complete acceptance still requires all
  page values, event-time extrema, and row ordering to be checked.
- PLAIN is the only v1 physical encoding. Future dictionary, delta, bit-pack, or run-length
  encodings need new assigned codes, fixtures, safety limits, and evidence.
- CSEG v1 stores the current single-node WAL-derived version identity. A future replicated commit
  identity requires an accepted compatibility decision rather than reinterpretation.
- Part installation and crash consistency remain unclaimed until Phase 6 supplies the manifest and
  filesystem protocol.

## Affected invariants

This decision directly enforces invariants [3, 6, 7, 8, 10, 11, 13, 14, 16, and
18](../architecture/invariants.md). Immutable identified bytes support stable snapshots and safe
reclamation; the system columns retain commit/version meaning; metadata and page integrity protect
safe interpretation; explicit version and unsupported classifications prevent reinterpretation;
and reusing canonical column buffers preserves complete values across head-to-part conversion.
Compaction equivalence and crash-safe installation remain later executable obligations rather than
claims of this format decision.

## Validation plan

- Independently review raw and Zstandard golden fixtures, exact offsets, zero padding, checksum
  ranges, and provider-independent decoded values.
- Round-trip every logical type, nullable/non-null shape, page/granule boundary, equal user sort key,
  floating edge, empty variable value, and maximum accepted length through writer and full validator.
- Property-generate sorted physical rows and compare decoded values, hidden identities, schema
  binding, extrema, and canonical granule boundaries with an independent reference model.
- Truncate every boundary; splice, reorder, duplicate, and bit-flip header, descriptor, metadata,
  page, and padding regions; require bounded deterministic classification.
- Exercise unknown nonzero versions, flags, types, encodings, compressors, and operation codes
  separately from zero/invalid and checksum failures.
- Test Zstandard content-size, dictionary, checksum, concatenation, trailing-byte, oversized-window,
  output-limit, and malformed-frame cases without allocation beyond configured bounds.
- Run decoder fuzzing, AddressSanitizer, UndefinedBehaviorSanitizer, applicable ThreadSanitizer,
  strict warnings, static analysis, standalone public-header compilation, install/export, and an
  external consumer.
- Benchmark declared raw and compressed datasets for size, encode/full-decode/projected-read
  throughput, allocations, granule widths, and compression tradeoffs under the benchmark contract.

## Migration or rollback considerations

No installed CSEG data exists. Implementation can be rolled back before any Phase 6 manifest names
a part. After installation begins, format 1.0 bytes and meanings are immutable: a reader either
supports them or reports unsupported, and a replacement encoding creates a new part identity.
Changing the header compatibility prefix, descriptor meanings, page payload, system identity, or
sort semantics requires a new accepted format and an explicit converter or rebuild from retained
WAL/parts. Replacing the Zstandard provider is allowed only when old frames remain readable and the
boundary tests pass.

## Unresolved questions

- Dictionary and specialized per-type encodings await Phase 5 measurements and a later format
  allocation; v1 readers reject unknown codes.
- Durable installation names, manifest edits, checkpoints, and temporary-file recovery are owned by
  Phase 6.
- Delta/base classification, zone maps beyond mandatory event-time extrema, sparse fences,
  correction/tombstone storage, and compaction are owned by Phase 7 and Phase 13.
- A commit/version identity suitable for Raft-created parts is owned by the distributed roadmap
  phase and cannot reinterpret the v1 WAL system columns.

## References

- [CSEG v1 specification](../formats/cseg-v1.md)
- [ADR 0005](0005-columnar-heads-and-immutable-cseg-parts.md)
- [ADR 0011](0011-dependency-and-build-versus-buy-policy.md)
- [ADR 0012](0012-correctness-testing-and-performance-evidence.md)
- [ADR 0014](0014-logical-types-schema-identity-and-evolution.md)
- [Columnar Batch v1](../formats/columnar-batch-v1.md)
- [Mutable-head publication](../architecture/mutable-head-publication.md)
- [Roadmap Phase 5](../roadmap.md#phase-5--cseg-v1)
