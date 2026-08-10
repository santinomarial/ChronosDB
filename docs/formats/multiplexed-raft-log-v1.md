# Multiplexed Raft Persistent-State Record v1

> **Status: minor 1 codec and single-owner segmented append/sync/recovery are implemented.**
> This format does not alter WAL v1, CSEG v1, or Manifest v1.

The record is a node-level physical envelope for one logical Raft group checkpoint. Groups may be
interleaved and batched in one future segmented append stream. Physical sequence never replaces the
group's logical term/index. All integers are little-endian and native structs are never dumped.

## Header (64 bytes)

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `43 48 52 4e 4d 52 4c 00` (`CHRNMRL\0`) |
| 8 | 2 | Major `1` |
| 10 | 2 | Minor `1` (`0` remains readable) |
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

The common first 88 bytes contain current term, voted-for node (`0` means none), commit index,
applied index, snapshot last index/term, manifest generation, and the 32-byte immutable-part-set
checksum. Minor 0 then stores log-entry count and four required-zero bytes, producing its historical
96-byte fixed prefix.

Minor 1 instead appends snapshot configuration index (8), snapshot voter count (4), four
required-zero bytes, and ascending unique nonzero voter IDs (8 each), followed by log-entry count
and four required-zero bytes. Its fixed portion is 112 bytes before voter IDs. An empty snapshot has
configuration index zero and no voters. A nonempty snapshot checkpoint supplies the stable
membership base for suffix validation.

Each log entry contains logical index (8), term (8), type (1), seven required-zero bytes, payload
length (4), four required-zero bytes, and payload.

Logical entry type `253` is the Raft leader progress no-op. Its payload must be empty. Types `254`
and `255` contain [Raft Membership Command v1](raft-membership-command-v1.md) joint and final
commands. These three types are reserved for Raft internals; application proposal interfaces reject
them. Readers must reject a type-253 entry with a nonempty payload, including when it arrives in an
AppendEntries suffix.

The decoder validates header integrity before trusting lengths, then exact size relationships,
payload and full-record integrity, required-zero bytes, bounded entry count, and exact exhaustion.
`RaftNode::create` performs the semantic validation: contiguous logical indexes, bounded entries,
valid term/vote/snapshot state, canonical bounded checkpoint voters, and
`applied <= commit <= last`.

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

`MultiRaftRuntime` returns this full logical state with a persist-before-send contract.
`DurableMultiRaftRuntime` can execute a bounded caller-provided operation batch, append all resulting
persistent states through `RaftPersistentLog`, cover them with one local sync, and then release the
outbound messages. On a stable or joint-consensus leader, its checked quorum-sync receipt composes
the required Raft quorum commit with those durable follower-response and leader-commit boundaries. No current
asynchronous worker reclaims checkpoint-covered segments, and native protocol/client acknowledgment
does not yet expose the receipt as a requested durability mode.
