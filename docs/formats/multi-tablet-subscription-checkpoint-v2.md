# Multi-tablet Subscription Checkpoint v2

> **Status: implemented source-tagged checkpoint and generation format.** New durable coordinator
> generations use v2. Checkpoint v1 generations remain readable as WAL-only state.

Every integer is fixed-width little-endian. UUIDs use their canonical 16 bytes. No native C++
object representation is serialized. The complete checkpoint and its generation envelope each end
in an independent CRC32C over every preceding byte.

## Checkpoint header

The header is exactly 128 bytes.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | ASCII magic `CHSUBCP2` |
| 8 | 2 | major version, `2` |
| 10 | 2 | minor version: `0` normal, `1` terminal schema state |
| 12 | 4 | header size, `128` |
| 16 | 8 | exact total checkpoint size including trailer |
| 24 | 4 | source count |
| 28 | 4 | retained-change count |
| 32 | 16 | database UUID |
| 48 | 16 | table UUID |
| 64 | 32 | plan fingerprint |
| 96 | 16 | schema UUID |
| 112 | 8 | schema version |
| 120 | 1 | schema-state flags: minor 0 requires `0`; minor 1 requires `1` |
| 121 | 7 | required zero |

A terminal schema-invalidated checkpoint retains no changes and has `expired_through == latest` for
every source.

## Source vector

Exactly `source_count` 56-byte entries follow, strictly increasing by tablet UUID.

| Relative offset | Size | Field |
| ---: | ---: | --- |
| 0 | 16 | tablet UUID |
| 16 | 1 | source kind: `1` WAL, `2` Raft |
| 17 | 7 | required zero |
| 24 | 16 | WAL ID for kind `1`; Raft group UUID for kind `2` |
| 40 | 8 | latest committed WAL record sequence or Raft log index |
| 48 | 8 | expired-through sequence/index |

Tablet and selected source identities are nonzero. The expiry frontier cannot exceed latest. The
unused identity namespace does not exist in the bytes and cannot be inferred from equal UUID bytes.

## Retained admission-order changes

Exactly `retained_change_count` records follow in the coordinator's authoritative admission order.
Each begins with an 88-byte envelope followed by result-key bytes and payload bytes.

| Relative offset | Size | Field |
| ---: | ---: | --- |
| 0 | 16 | tablet UUID |
| 16 | 1 | source kind: `1` WAL, `2` Raft |
| 17 | 7 | required zero |
| 24 | 16 | source-specific WAL ID or Raft group UUID |
| 40 | 8 | WAL record sequence or Raft log index |
| 48 | 16 | schema UUID |
| 64 | 8 | schema version |
| 72 | 1 | operation: `1` UPSERT, `2` DELETE |
| 73 | 7 | required zero |
| 80 | 4 | result-key byte length |
| 84 | 4 | payload byte length |
| 88 | variable | result key then payload |

Result keys are nonempty. DELETE payloads are empty. For each source independently, retained
positions are consecutive from `expired_through + 1` through `latest`. Cross-source order is the
stored admission order and is never reconstructed by sorting unrelated positions.

## Checkpoint trailer and validation

The final four bytes are CRC32C of `[0, total_size - 4)`. The decoder bounds outer bytes and counts,
selects a supported version, validates fixed framing and the complete checksum before allocating
decoded state, then checks required-zero bytes, source kinds, identities, canonical source order,
length/operation rules, schema binding, exact per-source suffix continuity, and trailing bytes.
Unknown versions and required fields fail as unsupported; malformed or checksum-invalid bytes fail
as corruption.

## Generation Envelope v2

The durable envelope has magic `CHSUBCG2`, major/minor `2.0`, a 64-byte header, exact total size,
nonzero 64-bit checkpoint generation, exact nested Checkpoint v2 size, and 24 required-zero bytes.
The nested checkpoint follows and a final four-byte CRC32C covers the complete envelope prefix.

The filesystem namespace and installation ordering remain those accepted by ADR 0100: immutable
contiguous `generation-%020u.subc` files, a lock-owning directory, exclusive temporary creation,
complete decode readback, file synchronization, no-replace rename, and directory synchronization.
Compatibility selection accepts Envelope/Checkpoint v1 or v2 for each generation. New files use
v2; an exact retry of an installed v1 generation is compared against its byte-identical v1
encoding and does not rewrite it.
