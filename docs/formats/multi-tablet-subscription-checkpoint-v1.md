# Multi-tablet Subscription Checkpoint v1

## Status and byte order

This document freezes major version 1. Minor 0 is the initial layout and minor 1 adds terminal plan-
schema state in a previously reserved header byte. Every integer is fixed-width little-endian. UUID
and WAL identities use their canonical 16 bytes. No native C++ object representation is serialized.
The complete file is bounded by the decoder configuration and ends in a CRC32C over every preceding
byte. Compatible checkpoints continue to encode byte-identical minor-0 files; only a terminal
schema-invalidated checkpoint emits minor 1.

## Header

The header is exactly 128 bytes.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | ASCII magic `CHSUBCP1` |
| 8 | 2 | major version, `1` |
| 10 | 2 | minor version, `0` |
| 12 | 4 | header size, `128` |
| 16 | 8 | exact total file size including trailer |
| 24 | 4 | source count |
| 28 | 4 | retained-change count |
| 32 | 16 | database UUID |
| 48 | 16 | table UUID |
| 64 | 32 | plan fingerprint |
| 96 | 16 | schema UUID |
| 112 | 8 | schema version |
| 120 | 1 | schema-state flags: minor 0 requires `0`; minor 1 requires `1` (plan schema invalidated) |
| 121 | 7 | reserved zero |

A plan-schema-invalidated checkpoint is terminal for its plan. It contains no retained changes and
every source has `expired_through == latest`; old-plan registration and resume must fail as
incompatible after recovery. Unknown minor versions or schema-state flag values fail as unsupported
or corrupt, respectively.

## Canonical source vector

Exactly `source_count` 48-byte entries follow the header, strictly increasing by tablet UUID.

| Relative offset | Size | Field |
| ---: | ---: | --- |
| 0 | 16 | tablet UUID |
| 16 | 16 | WAL ID |
| 32 | 8 | latest committed record sequence |
| 40 | 8 | expired-through record sequence |

The expiry frontier cannot exceed latest. It means all earlier positions for that source are no
longer recoverable from this checkpoint.

## Retained admission-order changes

Exactly `retained_change_count` records follow in the coordinator's authoritative admission order.
Each begins with an 80-byte envelope followed immediately by result-key bytes and payload bytes.

| Relative offset | Size | Field |
| ---: | ---: | --- |
| 0 | 16 | tablet UUID |
| 16 | 16 | WAL ID |
| 32 | 8 | record sequence |
| 40 | 16 | schema UUID |
| 56 | 8 | schema version |
| 64 | 1 | operation: `1` UPSERT, `2` DELETE |
| 65 | 7 | reserved zero |
| 72 | 4 | result-key byte length |
| 76 | 4 | payload byte length |
| 80 | variable | result-key then payload |

Result keys are nonempty. DELETE payloads are empty. For each source independently, retained
sequences are consecutive from `expired_through + 1` through `latest`; their cross-source order is
the recorded delivery admission order and must not be reconstructed by sorting source positions.

## Trailer and validation order

The final 4 bytes are CRC32C of `[0, total_size - 4)`. A decoder validates outer size, magic,
version, fixed header fields, configured counts, and CRC before allocating decoded state. It then
checks reserved bytes, identities, canonical source order, operation/length rules, exact per-source
suffix continuity, schema binding, and absence of trailing data. Unknown versions fail as
unsupported; malformed or checksum-invalid bytes fail as corruption.

## Durable generation envelope

Filesystem generations wrap one complete checkpoint in a second v1.0 envelope so renaming older
bytes to a newer filename cannot change selected state. The envelope has magic `CHSUBCG1`, the same
major/minor fields, a 64-byte header, exact total size, nonzero 64-bit checkpoint generation, exact
nested byte size, and 24 reserved zero bytes. The nested Checkpoint v1 bytes follow, then a second
4-byte CRC32C over the complete envelope prefix. Both checksums and the generation/name binding are
validated on load.

## Filesystem generation namespace

A lock-owning storage directory contains an advisory `LOCK` and immutable generation files named
`generation-%020u.subc`. Generations begin at one and remain contiguous. Installation uses the
corresponding `.tmp` name, exact readback validation, file synchronization, no-replace rename, and
directory synchronization. Reopen removes only canonical temporaries. The latest generation is
recoverable only after its filename, embedded generation, owner identity, canonical source set,
checksums, and nested semantics all validate.
