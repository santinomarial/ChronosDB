# Metadata Application Snapshot v1

> **Status: canonical codec, lock-protected local installation, install-before-Raft compaction, and
> exact snapshot-plus-suffix recovery implemented.**

All integers are unsigned little-endian. The object is caller-bounded and has a 1 GiB format
maximum. Its magic is `CHRMASN\0`, major version 1. Minor version 0 retains metadata and schema
entries; minor version 1 additively admits exact Tablet Group Binding v1 entries. Encoders emit the
lowest minor required by their entry set.

## Header

The fixed header is 128 bytes:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `CHRMASN\0` |
| 8 | 2 | Major `1` |
| 10 | 2 | Minor `0` or `1` |
| 12 | 4 | Header size `128` |
| 16 | 8 | Complete byte size including trailer |
| 24 | 4 | Application-entry count |
| 28 | 4 | Voter count |
| 32 | 16 | Metadata Raft group UUID |
| 48 | 8 | Last included Raft index |
| 56 | 8 | Last included Raft term |
| 64 | 8 | Metadata application-snapshot generation |
| 72 | 32 | Application entry-set SHA-256 identity |
| 104 | 8 | Membership configuration index |
| 112 | 8 | Entry-area offset |
| 120 | 4 | Header CRC32C with this field zero |
| 124 | 4 | Required zero |

The header is followed by `voter_count` sorted, unique, nonzero `UINT64` node IDs. The entry-area
offset equals `128 + voter_count * 8`. Snapshot generation is carried in Raft's existing
`manifest_generation` field but is scoped to this metadata application format; it is not a storage
Manifest generation.

The application identity is SHA-256 over the eight-byte domain `CHRMASN\x01`, followed for each
stored entry by its little-endian `UINT64` index, little-endian `UINT64` term, one-byte type,
little-endian `UINT64` payload size, and exact payload. Empty application entry sets hash only the
domain. Metadata snapshot generation equals the included index in v1.

## Application entries

Each entry begins at an 8-byte boundary with this 32-byte header:

| Relative offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Original logical Raft index |
| 8 | 8 | Original Raft term |
| 16 | 1 | Permanent Raft application type (`2` metadata command, `3` schema definition, or minor-1 `4` tablet-group binding) |
| 17 | 3 | Required zero |
| 20 | 4 | Payload size |
| 24 | 4 | Payload CRC32C |
| 28 | 4 | Required zero |

The exact nonempty payload follows, then required-zero padding to the next 8-byte boundary. Indexes
are nonzero, strictly increasing, and no later than the included index. Terms are nonzero,
nondecreasing, and no later than the included term. An entry at the included index has the included
term. Internal Raft entries are omitted and therefore appear as index gaps.

Minor 0 accepts only types 2 and 3 and its bytes are unchanged. Minor 1 accepts types 2, 3, and 4;
a type-4 entry mislabeled as minor 0 is corruption. Readers that do not implement minor 1 reject it
as unsupported. The structural codec still retains exact nested bytes without reinterpreting them.

The final four bytes are CRC32C over every preceding byte. No trailing bytes are allowed. The
structural codec does not reinterpret nested Metadata Command v1 or Schema Definition v1 bytes;
durable installation and recovery must exact-decode them before accepting application state.

Local durable files use `metadata-snapshot-<20-digit-index>.rmas` in a directory exclusively owned
by one metadata group. Installation exact-validates before and after write, file-syncs, renames
without replacement, and directory-syncs before reporting success.

Compiler-independent golden fixtures freeze both emitted layouts under
`tests/fixtures/raft/metadata-application-snapshot-minor-{0,1}.hex`. They were constructed directly
from this specification with little-endian packing, `hashlib` SHA-256, and a standalone bitwise
CRC32C implementation; neither the ChronosDB codec nor checksum implementation generated the
expected bytes. Tests require exact production encode equality and exact decode reconstruction.
Dedicated test-only allocator sweeps fail every observed encoder allocation and every voter,
entry-container, and payload allocation owned by both minor-version decoders. Each failure must
return `RESOURCE_EXHAUSTED`; the first non-failing retry must reproduce the canonical bytes or exact
decoded snapshot, respectively.

The structure-aware ASan/UBSan libFuzzer target feeds the decoder raw caller-bounded bytes and both
generated canonical minors, then exercises exact round trips, lower caller limits, single-byte
mutations with independently refreshed entry/header/file CRC32C layers, and truncation. Successful
hostile decodes must stabilize after semantic re-encoding; the bounded CI smoke is not a sustained
fuzz campaign.

The declared 65,536-entry limit is exercised exactly with nine voters and valid nested Metadata
Command v1 payloads. The maximum catalog must round-trip byte-stably, while entry 65,537 and a caller
limit of 65,535 must return `RESOURCE_EXHAUSTED`. Local-only benchmarks measure encode and owned
decode at 1,024, 16,384, and 65,536 entries and report the entry count, payload bytes per entry, and
complete snapshot bytes; their output is evidence only when retained with the revision, compiler,
host, command, and full result.
