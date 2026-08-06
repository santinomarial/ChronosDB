# ChronosDB WAL v1

> **Status: accepted specification; physical WAL lifecycle implemented.** The
> `chronos::wal` library implements segment headers, record headers, complete records, identities,
> positions, size calculations, structural/integrity validation, crash-safe segment installation,
> append, explicit synchronization, rotation, locked discovery, whole-log physical verification,
> explicit final-tail repair, replay-sink preflight/replay, and reopening an existing history.
> Durability-mode acknowledgment coordination and its subprocess crash/recovery harness are
> implemented. The generic 16-byte application-envelope codec and the independent
> `COLUMNAR_APPEND` v1 command codec are implemented; WAL submission and logical application are
> not.
> This document is the normative byte-level definition
> of the ChronosDB single-node WAL v1 physical format. [ADR 0013](../adr/0013-wal-v1-format-and-recovery.md)
> accepts the design, and the [recovery architecture](../architecture/wal-recovery.md) defines how
> these bytes are installed, classified, repaired, and replayed. If another document disagrees with
> a field table or validation rule here, this document controls.

## Scope and normative language

WAL v1 defines the dedicated directory layout, segment names, segment header, physical record
framing, integrity coverage, size limits, ordering identities, and compatibility behavior. It is
sufficient for independent physical encoders, validators, and decoders to produce identical bytes
and classifications.

WAL v1 does not define a table mutation, schema change, checkpoint, CSEG part, manifest edit, or Raft
entry. The one assigned application record type carries a versioned envelope whose kind-specific
body requires its own accepted specification before production use. This separation is not
permission to change an existing payload interpretation: every application kind and body is itself
a durable contract.

The terms **MUST**, **MUST NOT**, **SHOULD**, and **MAY** are normative. All byte offsets are from the
start of the containing object. Ranges are half-open. `MiB` means 1,048,576 bytes.

## Fixed constants

| Name | Value |
| --- | ---: |
| Segment header size | 64 bytes |
| Segment size limit | 67,108,864 bytes (64 MiB), including the segment header |
| Physical record header size | 40 bytes |
| Physical record trailer size | 4 bytes |
| Maximum physical record length | 16,777,216 bytes (16 MiB), including header, padding, and trailer |
| Segment format | major `1`, minor `0` |
| Physical record format | `1` |
| First segment number | `1` |
| First record sequence | `1` |

An encoder MUST reject a value or computed length that cannot be represented in its fixed-width
field, exceeds a limit above, or would overflow intermediate arithmetic. Segment and record
sequences MUST NOT wrap. `UINT64_MAX` is permitted only as a terminal segment/record sequence with
no successor; any operation requiring the next value fails with a surfaced unsupported-capacity
error.

## Primitive encoding and CRC32C

- Every integer is unsigned and encoded little-endian in its stated width.
- Byte arrays are copied in the order shown and are not integer-reversed.
- Implementations MUST encode fields individually. Native structures, padding, enum layout, and
  object representation are never durable bytes.
- CRC fields use CRC32C (Castagnoli): width 32, reflected polynomial `0x82f63b78` (normal polynomial
  `0x1edc6f41`), initial state `0xffffffff`, reflected input/output, and final XOR `0xffffffff`.
  The empty-input checksum is `0x00000000`, and ASCII `123456789` is `0xe3069283`.
- A CRC is a host numeric value while calculated and is serialized as a little-endian `u32`.
- CRC ranges below contain exactly the named stored bytes. A checksum field is not treated as zero
  unless a table explicitly says so; WAL v1 instead places each checksum outside its own coverage.

CRC32C detects accidental corruption. It is not authentication and does not make maliciously
modified data trustworthy.

## WAL directory

The WAL lives in a dedicated `wal/` directory:

```text
wal/
├── LOCK
├── wal-00000000000000000001.cwal
├── wal-00000000000000000002.cwal
└── wal-00000000000000000003.cwal
```

### Final segment names

A final segment name is exactly `wal-<N>.cwal`, where `<N>` is the segment number formatted as
exactly 20 ASCII decimal digits with leading zeroes. `N` is in `1..18446744073709551615`; signs,
spaces, extra digits, and alternate digit characters are invalid. The number in the filename MUST
equal the `segment_number` in its header.

Without an external accepted checkpoint context, an existing WAL history MUST begin at segment 1
and contain every consecutive segment number through the highest final name. A gap, duplicate
numeric identity, or sequence beginning above 1 is corruption. The accepted
[Manifest v1](manifest-v1.md) checkpoint can later authorize a manifest-aware opener to begin at an
exact covered segment/offset or its immediate successor after synchronized prefix removal; this
changes no WAL v1 byte or within-suffix continuity rule and is not yet implemented.

### Temporary segment names

A segment being installed is named exactly
`.wal-<N>.cwal.tmp-<nonce>`, where `<N>` has the same 20-digit encoding and `<nonce>` is 32 lowercase
hexadecimal digits. Temporary names are never log members and are never promoted by recovery.
After acquiring the writer lock, recovery MAY remove recognized temporary files and synchronize the
directory. A temporary file must not be used to fill a missing final segment.

### Other entries and file types

`LOCK` MUST be a regular file and is not part of the durable log format. Its bytes, if any, are
diagnostic only and MUST NOT influence recovery. Final and temporary segment entries MUST be regular
files, not symlinks, directories, devices, or sockets. To prevent silently ignoring misplaced WAL
state, a recovery-capable opener MUST reject every unrecognized directory entry other than `.` and
`..`.

Creation and opening are distinct operations. Creation requires a directory with no final segment
and installs segment 1. Opening an existing database with no final segment is a missing-log failure;
it MUST NOT silently create a new history.

The implemented new-history creator applies a stricter, fail-closed creation policy: before identity
generation and again under the writer lock, the directory must contain no entry other than a regular
`LOCK`. It does not delete recognized temporary files because cleanup belongs to the recovery-capable
opener. A valid final segment, recognized orphan temporary, malformed reserved `wal-`/`.wal-` name,
unrelated entry, symlink, or nonregular entry rejects creation without being removed or overwritten.

The containing database-creation protocol MUST durably install the `wal/` directory itself before
initial segment creation: create it beneath the already opened database root, synchronize the
database-root directory entry, and then perform the segment installation protocol. WAL v1 specifies
entries inside `wal/`; a caller that supplies a directory whose own name is not durable has not
satisfied the `LOCAL_SYNC` platform preconditions.

## Segment header

Every segment begins with this 64-byte header:

| Offset | Size | Field | Required v1 value and meaning |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | Bytes `43 48 52 4e 57 41 4c 00` (ASCII `CHRNWAL` followed by NUL). |
| 8 | 2 | `format_major` | `1`. |
| 10 | 2 | `format_minor` | `0`. |
| 12 | 4 | `header_length` | `64`. |
| 16 | 16 | `wal_id` | Opaque 128-bit identity shared by every segment in one WAL history; all-zero is invalid. |
| 32 | 8 | `segment_number` | Final filename number; starts at 1 and increases by exactly one. |
| 40 | 8 | `first_record_sequence` | Sequence the first record in this segment must have, or the next sequence if the active segment is empty. |
| 48 | 8 | `segment_size_limit` | `67108864`. Readers MUST reject another value in format 1.0. |
| 56 | 4 | `segment_flags` | `0`. Any nonzero value is an unsupported required feature. |
| 60 | 4 | `header_crc32c` | CRC32C of stored bytes `[0, 60)`. |

The creator chooses a nonzero `wal_id`; this specification requires uniqueness for independently
created histories but does not prescribe a random-number API. The identity is compared byte for
byte and is never interpreted using host UUID layout.

The first segment has `segment_number = 1` and `first_record_sequence = 1`. Each later segment's
`first_record_sequence` MUST equal one plus the final record sequence in the preceding segment. A
non-final segment MUST contain at least one record. Only the highest-numbered segment may be empty.

Header validation order is:

1. establish that the file contains at least 64 bytes without reading beyond its size;
2. compare `magic`;
3. verify `header_crc32c`;
4. decode and validate versions, lengths, flags, identities, and filename/sequence relationships;
5. establish that the file size is no greater than `segment_size_limit`.

A bad magic or CRC is corruption. A checksum-valid unsupported version or nonzero feature flag is
`UNSUPPORTED`, not corruption. No record bytes may be interpreted until the segment header passes.

For every nonzero segment format using the `CHRNWAL\0` magic, the 64-byte outer header, field
locations through `header_crc32c`, and CRC coverage `[0, 60)` are the compatibility prefix. A future
format may assign new values and meanings only where its accepted specification permits, but it
must retain this prefix so an older scanner can validate the header and report `UNSUPPORTED`
deterministically. A format that cannot retain the prefix requires a different container magic and
an explicit history-transition procedure; it cannot appear inside a WAL v1 directory.

## Physical record framing

Records begin at offset 64 and are contiguous. Each record's `total_length` is a multiple of 8, so
every subsequent record begins at an 8-byte file offset. Decoders must still use byte-wise or
alignment-safe loads.

### Record header

Every physical record begins with this 40-byte header:

| Offset | Size | Field | Required v1 value and meaning |
| ---: | ---: | --- | --- |
| 0 | 4 | `total_length` | Complete record bytes: header, payload, zero padding, and trailer. |
| 4 | 4 | `total_length_complement` | `total_length XOR 0xffffffff`. |
| 8 | 4 | `record_magic` | Bytes `52 45 43 31` (ASCII `REC1`). |
| 12 | 2 | `record_format` | `1`. |
| 14 | 2 | `record_type` | Physical type; `0` is invalid, `1` is `APPLICATION_ENTRY`, and `2..65535` are unassigned. |
| 16 | 4 | `record_flags` | `0`. Any nonzero value is an unsupported required feature. |
| 20 | 4 | `header_length` | `40`. |
| 24 | 8 | `record_sequence` | WAL-wide sequence; starts at 1 and increases by exactly one across segment boundaries. |
| 32 | 4 | `payload_length` | Number of payload bytes immediately following the header; excludes padding and trailer. |
| 36 | 4 | `header_crc32c` | CRC32C of stored header bytes `[0, 36)`. |

The payload starts at offset 40. It is followed by `padding_length` zero bytes and then a four-byte
`record_crc32c` trailer. The padding length and total length are derived, never chosen:

```text
padding_length = (4 - (payload_length mod 8)) mod 8
total_length   = 40 + payload_length + padding_length + 4
```

Thus `padding_length` is in `0..7`, the minimum record length is 48, and `total_length mod 8` is
zero. Every padding byte MUST be zero. `record_crc32c` is CRC32C of all stored bytes from the start
of the record through the final padding byte: `[record_start, record_start + total_length - 4)`.
That range includes the stored `header_crc32c` and excludes only the trailer itself.

`total_length` MUST be between 48 and 16,777,216 inclusive and MUST fit wholly before the segment's
64 MiB logical limit. The decoder MUST use checked arithmetic to recompute the formula and require
exact equality with `total_length`; it must not allocate based only on an unverified length. The
largest payload permitted by this formula is 16,777,172 bytes.

### Assigned application envelope

For `record_type = 1`, the physical payload begins with this 16-byte application envelope:

| Payload offset | Size | Field | Meaning |
| ---: | ---: | --- | --- |
| 0 | 4 | `application_format` | Format of the kind-specific body. Value `0` is invalid. |
| 4 | 4 | `application_kind` | Kind registry owned by an accepted higher-layer specification. Value `0` is invalid. |
| 8 | 8 | `application_flags` | Required features for that application format/kind; meaning is owned by its specification. |
| 16 | variable | `application_body` | Exact kind-specific bytes. |

An application entry therefore has `payload_length >= 16`. WAL implementations preserve the
payload exactly and do not infer table semantics from it. The accepted higher-layer
[columnar ingestion specification](../architecture/columnar-ingestion.md#columnar-append-command-v1)
allocates application format `1`, kind `2` (`COLUMNAR_APPEND`), with required flags `0`; its command
header, batch body, limits, and compatibility rules are authoritative there. Other nonzero
format/kind combinations remain unassigned until an accepted higher-layer specification defines
them.

For a complete type-1 record, `payload_length < 16`, `application_format = 0`, or
`application_kind = 0` is corruption. A checksum-valid nonzero but unsupported application format,
kind, or required flag is `UNSUPPORTED`. A body that violates the accepted specification for a
supported format/kind is corruption with an `INVALID_APPLICATION_RECORD` diagnostic; CRC validity
does not make an invalid operation replayable.

### Unknown physical types

A physical scanner MUST be able to frame and checksum any nonzero `record_type` using the generic
record layout. A checksum-valid unassigned type is structurally valid and is reported as unknown; it
is not corruption. Normal recovery MUST preflight record-type and application-kind support for the
entire WAL and fail `UNSUPPORTED` before applying any record. It MUST NOT skip an unknown record,
because doing so would create an unproved state transition and break sequence semantics.

A conforming writer MUST NOT emit an unassigned physical type. Allocation of a type in `2..65535`
requires an accepted specification and an update to this registry. Likewise, a production type-1
entry cannot be emitted until its nonzero application format/kind is accepted.

Within segment format 1.0, the 40-byte outer header, length/complement fields, header-CRC position,
padding formula, and full-record trailer are the common envelope even for a future non-1
`record_format`. A format that changes that outer envelope requires a new segment format rather than
an ambiguous record inside a v1 segment. This lets a v1 scanner classify a checksum-valid unknown
record format as unsupported without searching for a new boundary.

## Record validation algorithm

Starting at a known record boundary in a verified segment:

1. Compute `remaining_file_bytes = file_size - offset` using subtraction after proving
   `offset <= file_size`.
2. If zero bytes remain, the segment ends cleanly.
3. If fewer than 40 bytes remain, apply the incomplete-tail rule below; do not inspect absent fields.
4. Read exactly 40 bytes. Verify `record_magic`, `header_crc32c`, length/complement equality, common
   header length, zero global flags, nonzero record format/type, and exact expected sequence. A
   nonzero record format need not be semantically supported yet because its outer framing is fixed
   below.
5. Recompute padding and `total_length` with checked arithmetic; enforce record and segment limits.
6. If `total_length > remaining_file_bytes`, apply the incomplete-tail rule below.
7. Without advancing persistent replay state, read the declared payload, padding, and trailer;
   require zero padding and verify `record_crc32c`.
8. For a complete type-1 record, structurally validate the 16-byte application envelope. Support for
   its nonzero format, kind, and flags is checked during the whole-log semantic preflight, not by
   silently skipping it.
9. Advance by exactly `total_length`. If the sequence is below `UINT64_MAX`, increment the expected
   value with checked arithmetic. If it equals `UINT64_MAX`, require this to be the final complete
   record in the highest segment and mark the history sequence-exhausted; any following byte or
   segment is corruption.

A full record with bad magic, length relationship, sequence, padding, header CRC, or record CRC is
corruption even if it is the final record in the highest segment.

## Clean end, incomplete final tail, and corruption

An **incomplete final tail** exists only when all of these are true:

1. every earlier segment and every earlier record in the WAL has fully verified;
2. the containing segment is the highest-numbered final segment;
3. the candidate begins exactly at the end of the verified record prefix; and
4. either 1–39 bytes remain, or a complete and valid 40-byte record header declares a valid record
   whose `total_length` extends beyond the actual file size.

Zero remaining bytes is a clean end. Any corresponding truncation in a non-final segment is
corruption. A complete header that fails validation, a complete declared record with a checksum
mismatch, nonzero padding, trailing bytes after a corrupt header, or any discontinuity is
corruption—not a repairable tail.

For tail classification, “valid record header” means the common outer header, zero global flags,
checked size formula, and sequence are valid. A nonzero but unknown `record_format` or `record_type`
does not prevent physical incomplete-tail classification because segment format 1.0 fixes their
outer framing. Unknown semantics are rejected during preflight if a complete record survives.

The 1–39 byte case is necessarily an evidence-based classification: under the WAL v1 append-only,
single-writer, no-preallocation model, bytes at that exact location can only be distinguished as an
incomplete attempted header or out-of-contract corruption. WAL v1 classifies them as incomplete
only at the final verified boundary and fails closed everywhere else.

Recovery verifies the entire physical WAL before replay. It may truncate an incomplete final tail
only through the explicit synchronized repair procedure in the
[recovery architecture](../architecture/wal-recovery.md). Scanners and read-only tools never repair.

## Segment append and rotation rules

- A record MUST be completely encoded and checksummed before its first byte is submitted to the WAL
  file write path.
- Records occupy exactly the contiguous range beginning at the writer's known end offset. A short
  write may be retried only for the unwritten suffix at the exact next offset. A permanent error
  after any prefix poisons the writer; no later record may be appended until recovery classifies and
  explicitly repairs the tail.
- The file grows only by record writes. WAL v1 MUST NOT use `fallocate`, `posix_fallocate`, growth by
  `ftruncate`, sparse reservations, or any other preallocation.
- A writer may configure an earlier runtime rotation target greater than 64 bytes and no greater
  than 64 MiB, provided the target can hold the 64-byte header plus one maximum configured record.
  The target is an operational policy and is not serialized: every v1 header still stores the
  frozen 64 MiB `segment_size_limit`. If the complete next record would end beyond the runtime
  target, the writer rotates before writing any of it. A record never crosses a segment boundary.
  If the active segment number is `UINT64_MAX`, rotation is impossible and the append fails without
  writing.
- A closed segment may end below 64 MiB. WAL v1 writes no footer and pads no unused segment capacity.
- Once a newer final segment is active, every prior segment is immutable. The final active segment
  is append-only except for explicit recovery-tail truncation.

The [recovery architecture](../architecture/wal-recovery.md) specifies the synchronized temporary
file, rename, directory-sync, prior-segment sync, acknowledgment, and repair state transitions.

## Compatibility and evolution

- The bytes and meanings assigned by format 1.0 are immutable. A field may not be reinterpreted by
  changing documentation or implementation.
- A checksum-valid unknown segment major/minor, record format, or required flag is `UNSUPPORTED`.
  It is not accepted optimistically and is not labeled corruption.
- Segment or record format value zero is invalid and is corruption. Nonzero unknown formats are
  structurally recognizable under the common outer headers and are unsupported when complete.
- All segments in one WAL v1 directory MUST use segment format 1.0 and one `wal_id`. Mixed segment
  formats are prohibited. A future physical format requires an accepted specification and explicit
  migration/new-history procedure; it cannot appear as the next segment in a v1 directory.
- New physical record types may reuse v1 framing. Old readers can validate their boundaries and
  checksums but normal recovery fails before replay unless support is installed.
- Existing physical type semantics never change. An incompatible meaning requires a new type or a
  new explicitly versioned application format/kind.
- Unknown application formats, kinds, or required flags are handled like unknown physical types:
  structurally retained, reported as unsupported, and never skipped during normal recovery.
- CRC32C, byte order, magic values, size limits, sequence rules, and checksum coverage cannot be
  negotiated within WAL v1.
- Golden fixtures for the empty first segment, boundary-size records, multi-segment continuity, each
  assigned application envelope, and corrupt/truncated variants are required with implementation.

WAL compression, encryption, direct I/O, memory-mapped writing, `io_uring`, and checkpoint records
are not WAL v1 features. Checkpoint-driven segment removal is specified externally by
[Manifest v1](manifest-v1.md) and
[ADR 0017](../adr/0017-manifest-generations-installation-and-checkpoints.md); its implementation
remains pending and does not change WAL physical framing.
