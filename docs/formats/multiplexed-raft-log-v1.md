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
zero last term, Manifest generation, checksum bytes, and configuration index, with no voters. A
term-zero persistent state has no recorded vote. A nonempty snapshot checkpoint supplies the stable
membership base for suffix validation. Local compaction derives that base at the exact
last-included index. A prefix ending in joint state is not representable and must be rejected; later
membership entries may remain in the retained suffix only when replay from the stable checkpoint is
valid.

Each log entry contains logical index (8), term (8), type (1), seven required-zero bytes, payload
length (4), four required-zero bytes, and payload.

For minor 1, the exact persistent-state payload size is
`112 + 8 * snapshot_voter_count + sum(32 + entry_payload_size)`. The deterministic core applies
this aggregate bound to recovered state and every transition that can replace a snapshot or retain,
append, or replace log entries. Capacity failure is reported before term, vote, log, commit, apply,
snapshot, role, or pending-install state can change. The default payload budget is the 16 MiB record
limit less the 64-byte record header and 4-byte trailer.
When a legacy nonempty snapshot has no encoded voter checkpoint, semantic recovery first copies the
bootstrap voters into the canonical snapshot and then evaluates this minor-1 size formula. Budget
validation therefore includes every voter that a subsequent full-state record must encode.

Logical entry type `253` is the Raft leader progress no-op. Its payload must be empty. Types `254`
and `255` contain [Raft Membership Command v1](raft-membership-command-v1.md) joint and final
commands. These three types are reserved for Raft internals; application proposal interfaces reject
them. Readers must reject a type-253 entry with a nonempty payload, including when it arrives in an
AppendEntries suffix.

The decoder validates header integrity before trusting lengths, then exact size relationships,
payload and full-record integrity, required-zero bytes, bounded entry count, and exact exhaustion.
`RaftNode::create` performs the semantic validation: contiguous logical indexes, bounded entries,
snapshot and retained-log terms no greater than the current term, valid vote/snapshot state,
logical and snapshot indexes below the reserved `UINT64_MAX`, canonical term-zero and empty-snapshot
state, bounded checkpoint voters, canonical post-legacy-backfill payload size, and
`applied <= commit <= last`.

## Segment v1 envelope

Files are named `raft-NNNNNNNNNNNNNNNNNNNN.rlog`, using a nonzero, 20-digit, contiguous decimal
segment number. `LOCK` is the only other durable entry. Installation may temporarily use the same
stem with `.tmp`; recovery removes only recognized regular temporary files while holding `LOCK`.
If initial creation stops before segment 1 is renamed, a later `create_new` may restart only when the
locked namespace contains `LOCK` and the exact regular segment-1 temporary. It removes that
temporary, synchronizes the directory, and performs the complete installation again. Any other
entry remains an invalid nonempty creation namespace.

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

The remainder is an exact concatenation of complete record-v1 values. Before the first reclamation,
segment numbers and physical sequences are contiguous from one. After node-wide checkpoint
reclamation, they are contiguous from the base and first sequence named by a valid
[Raft Recovery Anchor v1](raft-recovery-anchor-v1.md). A segment is at most 1 GiB; the runtime target
may rotate sooner. The maximum physical record remains 16 MiB.
Physical sequence `UINT64_MAX` may identify the terminal record, but no later runtime transition or
checkpoint is admitted because another contiguous record identity cannot be assigned. Transition
rejection occurs before the group core can mutate in-memory state.
`DurableMultiRaftRuntime` further tightens each group's persistent-state payload budget to the
configured segment target less the 64-byte segment header, 64-byte record header, and 4-byte record
trailer. Consequently, every state admitted by its core can be encoded as one full-state record in
an otherwise empty segment; a smaller target cannot cause a post-mutation encoder or append failure.

## Installation and durability

The owner writes a new header to an exclusive temporary file, synchronizes the complete file,
renames without replacement, and synchronizes the directory before appending any record. Rotation
first data-synchronizes and closes the predecessor. A reported pre-rename failure may leave a
recognized temporary for recovery to remove. A reported failure after an ambiguous successful
rename may leave a valid empty successor for recovery to adopt; no record sequence is inferred from
that header alone. A successful append means only that all record bytes completed the write path. A
successful explicit synchronization advances the locally durable physical sequence through the
then-complete active-file prefix.

Recovery validates the namespace, authoritative anchor when present, every retained segment header,
and every retained record in physical order before returning latest per-group state. It may
explicitly truncate only a structurally incomplete suffix in the highest segment. Complete
checksum failure, retained gaps, unknown entries, and non-regular entries fail closed.
An I/O failure while opening or inspecting the directory, lock, anchor, retained segments, or final
active handle; reading the anchor, segment headers, or records; enumerating or cleaning the
namespace; repairing an incomplete final tail; or performing the final synchronization gates aborts
recovery and releases the exclusive owner. Truncate and synchronization results may be ambiguous,
so a failed repair may already have removed the incomplete suffix. A later open always restarts
validation from the filesystem state and safely accepts either the original incomplete suffix or
the already-truncated complete prefix; it never resumes partially constructed in-memory recovery
state. After full validation, recognized temporaries, segments below the authoritative base, and
non-authoritative anchors are removed in that order before one cleanup directory sync. Each removal
is independently retryable: an ambiguous unlink or sync error may leave any cleanup prefix absent,
but it cannot change the selected anchor or retained logical state, and later recovery resumes from
the newly observed namespace.

A deterministic 31-point subprocess matrix sends `SIGKILL` after successful production-path
operations spanning initial installation, rotation, checkpoint and anchor publication, and both
cleanup epochs. It requires exact repeated recovery and next-sequence continuation. This is
process-restart evidence over the host page cache; it does not qualify the ordering under power
loss. See the [Raft persistent-log crash matrix](../testing/raft-persistent-log-crash-harness.md).

The retained-byte corruption campaign builds a two-group checkpoint whose two 277-byte segments and
64-byte anchor contain 618 authority bytes. It flips one bit at every byte and truncates every file
at every strict prefix, then removes each authority file in turn. Strict recovery and
repair-authorized recovery must classify all 1,239 images as corruption and leave their bytes and
namespace unchanged. This exhausts single low-bit mutations and truncations for that canonical
image; it is not a proof against multiple coordinated checksum-preserving modifications.

The deterministic recovery-layout matrix uses canonical 213-byte records and segment targets
`277`, `278`, `489`, `490`, `491`, `702`, `703`, and `1024`. These values exercise exact and
one-byte-adjacent capacity boundaries for one, two, three, and four records after the segment
header. Crossing each target with 1, 3, and 8 groups verifies 24 exact layouts through interleaved
latest-state reconstruction, next-sequence append, fresh-base checkpoint reclamation, and a later
tail. The oracle performs the documented header-plus-record arithmetic independently of the writer.

## Remaining limitation

`MultiRaftRuntime` returns this full logical state with a persist-before-send contract.
`DurableMultiRaftRuntime` can execute a bounded caller-provided operation batch, append all resulting
persistent states through `RaftPersistentLog`, cover them with one local sync, and then release the
outbound messages. On a stable or joint-consensus leader, its checked quorum-sync receipt composes
the required Raft quorum commit with those durable follower-response and leader-commit boundaries.
The bounded asynchronous durable owner schedules caller-triggered all-group checkpoint reclamation,
but no autonomous reclamation policy chooses when to invoke it. Native protocol/client
acknowledgment does not yet expose the receipt as a requested durability mode.
