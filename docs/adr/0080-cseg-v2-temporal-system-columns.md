# ADR 0080: CSEG v2 temporal system columns

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB temporal-semantics and storage maintainers

## Context

CSEG v1 freezes four WAL-derived system columns and assigns only operation `APPEND_ROWS`. Temporal
Mutation Command v1 now durably distinguishes originals, corrections, replacements, and tombstones,
including logical identity, receive time, system commit time, and either WAL or Raft ordering. Using
v1 reserved codes would make old readers accept a layout whose semantics they cannot validate.

## Accepted decision

CSEG v2.0 retains the CSEG outer magic, 256-byte header, descriptor sizes, integrity ranges, page
encodings, compression registry, limits, and canonical alignment. Its major version is `2`, and the
system suffix contains eight non-null columns in exact order:

1. commit source (`UINT8`: WAL 1, Raft 2);
2. source identity (`UUID`);
3. authoritative commit position (`UINT64`);
4. row ordinal (`UINT32`);
5. operation (`UINT8`: original 1, correction 2, replacement 3, tombstone 4);
6. logical identity (`BINARY`);
7. receive time (`TIMESTAMP_NS`); and
8. system commit time (`TIMESTAMP_NS`).

Event time remains the schema-designated user column. The physical source identity is
`(commit_source, source_id, commit_position, row_ordinal)`. Logical identity is distinct from this
physical identity, is nonempty and at most 1,024 bytes, and selects versions for
current/system-time visibility.

## Consequences and alternatives

WAL and Raft created parts use one storage contract without pretending a Raft group is a WAL.
System timestamps remain query boundaries, while commit position is authoritative when timestamps
tie. Tombstones retain complete schema-shaped user rows, matching Temporal Mutation Command v1.

Adding codes to CSEG v1 was rejected because its readers require exactly four system columns and
operation 1. A sidecar file was rejected because atomic part installation and checksummed page
projection would span multiple objects. Encoding temporal metadata as user columns was rejected
because schema evolution could drop or reinterpret correctness-critical fields.

Manifest v1 and existing v1 part validators remain unchanged. Strict v2 entry points now encode and
decode the expanded metadata registry and structurally compose/authenticate complete files,
including every page, checksum, runtime limit, and alignment region, without weakening v1 entry
points. Manifest v2 must explicitly admit CSEG v2 and carry source/checkpoint meaning before v2
files are installed. Until then, codec and semantic-validation support do not claim durable database
publication.

## Affected invariants and validation

Invariants 2–8, 10, 11, 13, 14, and 18 apply. Implemented focused checks freeze the registry,
validate WAL/Raft source and operation domains, bound logical identities, prove checked
metadata/page layout with the expanded suffix, round-trip v2 metadata, reject v1/v2 registry
confusion, bind the exact table schema, deterministically compose complete files, classify every
truncation, and fail closed on stored-page corruption. A canonical raw temporal file now freezes its
exact 2,048-byte length and complete-file `0x3242794c` CRC32C through a tableless test oracle that is
independent of the production checksum implementation. A checksum-valid hostile metadata matrix
also freezes unsupported future formats/registries versus corrupt zero codes, count/layout
contradictions, temporal system-column reshaping, reserved bytes, and page-coordinate overlap.
Additional field-level golden fixtures, expanded page-body corruption matrices, crash tests,
fuzzing, and performance evidence remain subsequent work and Phase 18 validation. Projected
reading, Manifest v2 installation, current/as-of winner resolution, and temporal row/order
validation are implemented with bounded work and exact corrupt-versus-unsupported value
classification.
