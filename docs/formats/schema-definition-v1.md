# Schema Definition v1

> **Status: codec and committed metadata-group application are implemented.**

Schema Definition v1 is the application payload for logical Raft entry type `3` in the dedicated
metadata group. It persists one complete immutable table-schema generation and its SQL catalog
name. It supplements, and does not reinterpret, Metadata Command v1 kind 2. All integers are
little-endian and UUIDs use their canonical 16 bytes.

## Envelope

The exact byte string is bounded to 16 MiB by default. It has a 48-byte header, one payload, and a
4-byte trailer.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `43 48 52 4e 53 43 48 00` (`CHRNSCH\0`) |
| 8 | 2 | Major `1` |
| 10 | 2 | Minor `0` |
| 12 | 4 | Header size `48` |
| 16 | 4 | Total definition size |
| 20 | 4 | Payload size |
| 24 | 4 | CRC32C of payload |
| 28 | 8 | Required zero |
| 36 | 4 | CRC32C of header with this field zero |
| 40 | 8 | Required zero |

The trailer is CRC32C over the header, including its populated header checksum, plus the payload.
A decoder verifies the fixed header checksum before trusting lengths, then validates exact size
relationships, reserved bytes, payload and trailer checksums, runtime bounds, and complete payload
exhaustion.

## Fixed schema payload

The payload begins with 92 fixed bytes.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 16 | Table UUID |
| 16 | 16 | Schema UUID |
| 32 | 8 | Nonzero schema version |
| 40 | 1 | Parent-present flag, `0` or `1` |
| 41 | 1 | Quoted catalog-name flag, `0` or `1` |
| 42 | 2 | Required zero |
| 44 | 4 | Catalog-name byte length |
| 48 | 4 | Column count |
| 52 | 4 | Physical-ordering-key count |
| 56 | 4 | Partition-column count |
| 60 | 4 | Shard-key count |
| 64 | 4 | Deduplication-key count |
| 68 | 8 | Required zero |
| 76 | 16 | Parent schema UUID, or zero UUID iff absent |

The fixed bytes are followed by the exact UTF-8 catalog name. Names are nonempty and contain no
NUL. An unquoted name is canonical lowercase `[a-z_][a-z0-9_]*`; a quoted name preserves exact
case and UTF-8 bytes.

## Columns and roles

Each column, in schema ordinal order, has a 32-byte fixed prefix followed by its exact nonempty
UTF-8 name.

| Relative offset | Size | Field |
| ---: | ---: | --- |
| 0 | 16 | Nonzero column UUID |
| 16 | 2 | Frozen logical-type code |
| 18 | 2 | Logical-type parameter 0 |
| 20 | 2 | Logical-type parameter 1 |
| 22 | 1 | Nullable flag, `0` or `1` |
| 23 | 1 | Required zero |
| 24 | 4 | Column-name byte length |
| 28 | 4 | Required zero |

Columns are followed by the event-time column UUID and then the physical-ordering, partition,
shard, and deduplication role UUID arrays in that order. Every identity and schema relationship is
revalidated through the public logical-schema constructor. Unknown logical-type codes and unknown
format versions report unsupported; damaged checksums or invalid schema semantics report
corruption. Limits are checked before retaining variable-sized data.

## Application and recovery

Schema definitions apply only at consecutive committed metadata-group indexes. The first definition
for a table must be version 1; every later definition must pass the frozen v1 successor rules
against the active predecessor. A schema UUID cannot be redefined, and a catalog name cannot belong
to two table identities. Schema evolution cannot change the catalog name; rename requires a future
dedicated metadata command. The active table definition advances only after all catalog maps can be
updated coherently.

Recovery starts from empty state and replays both Metadata Command v1 entries and Schema Definition
v1 entries from the retained committed log. Metadata snapshots remain required before compacting
that authoritative prefix.
