# Multiplexed Raft Persistent-State Record v1

> **Status: codec and single-owner segmented append/sync/recovery are implemented.**
> This format does not alter WAL v1, CSEG v1, or Manifest v1.

The record is a node-level physical envelope for one logical Raft group checkpoint. Groups may be
interleaved and batched in one future segmented append stream. Physical sequence never replaces the
group's logical term/index. All integers are little-endian and native structs are never dumped.

## Header (64 bytes)

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `43 48 52 4e 4d 52 4c 00` (`CHRNMRL\0`) |
| 8 | 2 | Major `1` |
| 10 | 2 | Minor `0` |
| 12 | 4 | Header size `64` |
| 16 | 4 | Total record size including 4-byte trailer |
| 20 | 4 | Persistent-state payload size |
| 24 | 8 | Node-global physical sequence |
| 32 | 16 | Logical group UUID |
| 48 | 4 | CRC32C of payload |
| 52 | 4 | CRC32C of header with this field zero |
| 56 | 8 | Required-zero bytes |

The trailer is CRC32C over header plus payload. The maximum complete record is 16 MiB.

## Persistent-state payload

The fixed 96-byte prefix contains current term, voted-for node (`0` means none), commit index,
applied index, snapshot last index/term, manifest generation, 32-byte immutable-part-set checksum,
log-entry count, and four required-zero bytes. Each entry then contains logical index (8), term (8),
type (1), seven required-zero bytes, payload length (4), four required-zero bytes, and payload.

The decoder validates header integrity before trusting lengths, then exact size relationships,
payload and full-record integrity, required-zero bytes, bounded entry count, and exact exhaustion.
`RaftNode::create` performs the semantic validation: contiguous logical indexes, bounded entries,
valid term/vote/snapshot state, and `applied <= commit <= last`.

## Segment v1 envelope

Files are named `raft-NNNNNNNNNNNNNNNNNNNN.rlog`, using a nonzero, 20-digit, contiguous decimal
segment number. `LOCK` is the only other durable entry. Installation may temporarily use the same
stem with `.tmp`; recovery removes only recognized regular temporary files while holding `LOCK`.

Each segment starts with this 64-byte little-endian header:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `43 48 52 4e 52 53 47 00` (`CHRNRSG\0`) |
| 8 | 2 | Major `1` |
| 10 | 2 | Minor `0` |
| 12 | 4 | Header size `64` |
| 16 | 8 | Physical segment number |
| 24 | 8 | First physical record sequence in the segment |
| 32 | 4 | CRC32C of the complete header with this field zero |
| 36 | 28 | Required-zero bytes |

The remainder is an exact concatenation of complete record-v1 values. Segment numbers and physical
sequences are contiguous from one. A segment is at most 1 GiB; the runtime target may rotate sooner.
The maximum physical record remains 16 MiB.

## Installation and durability

The owner writes a new header to an exclusive temporary file, synchronizes the complete file,
renames without replacement, and synchronizes the directory before appending any record. Rotation
first data-synchronizes and closes the predecessor. A successful append means only that all record
bytes completed the write path. A successful explicit synchronization advances the locally durable
physical sequence through the then-complete active-file prefix.

Recovery validates the namespace, every segment header, and every record in physical order before
returning latest per-group state. It may explicitly truncate only a structurally incomplete suffix
in the highest segment. Complete checksum failure, gaps, unknown entries, and non-regular entries
fail closed.

## Remaining limitation

`MultiRaftRuntime` returns this full logical state with a persist-before-send contract, and
`RaftPersistentLog` can append and synchronize it. No current coordinator batches runtime
transitions through the owner, reclaims checkpoint-covered segments, or proves majority durability.
`QUORUM_SYNC` therefore remains unavailable.
