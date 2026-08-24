# Raft Recovery Anchor v1

> **Status: implemented.** This file establishes the retained base of one segmented Multiplexed
> Raft log after a complete node-wide persistence checkpoint.

Files are named `raft-base-NNNNNNNNNNNNNNNNNNNN.rbase`, where the 20-digit number is the nonzero
base segment number. Installation may use the same stem with `.rbase.tmp`. All integers are
little-endian.

## Fixed 64-byte layout

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `43 48 52 4e 52 42 41 00` (`CHRNRBA\0`) |
| 8 | 2 | Major `1` |
| 10 | 2 | Minor `0` |
| 12 | 4 | Header size `64` |
| 16 | 8 | First retained physical segment number |
| 24 | 8 | First checkpoint physical record sequence |
| 32 | 8 | Last checkpoint physical record sequence |
| 40 | 8 | Checkpoint logical-group count |
| 48 | 4 | CRC32C of the complete file with this field zero |
| 52 | 12 | Required-zero bytes |

The filename number must equal the encoded base segment. The checkpoint range is nonempty and its
inclusive sequence count must equal the group count.

## Authority and recovery

Without an anchor, recovery starts at segment 1 and physical sequence 1. With anchors, the highest
named valid anchor is authoritative. Recovery begins at its exact base segment and first sequence,
requires contiguous retained segments and records, and requires the checkpoint sequence range to
contain exactly one record for each distinct logical group. A missing base, incomplete checkpoint,
duplicate group, checksum error, or unsupported version fails closed. Lower segments and lower
anchors are obsolete cleanup residue and may be removed only after the authoritative retained
history validates.

## Installation and reclamation order

The single log owner rotates to a fresh segment, writes one current full-state record for every
resident group using consecutive node-global sequences, and synchronizes the complete checkpoint.
It then writes and synchronizes a temporary anchor, renames it without replacement, and synchronizes
the directory. Only after that durable authority exists may it remove whole older segments and
synchronize the directory. A crash before anchor publication retains the old history; a crash after
publication can recover solely from the complete checkpoint even if old-segment deletion was
partial. Individual segment headers and records retain their existing CRC coverage.

At process restart, a reported failure before the rename leaves the old base authoritative and any
recognized anchor temporary is removed while holding `LOCK`. A reported failure after an ambiguous
successful rename can leave the complete anchor authoritative; recovery validates its entire
checkpoint before removing old history. Either path recovers the same latest group states and next
physical sequence. This arbitration does not by itself qualify the ordering under power loss.

Once the newest anchor is visible, a reported error from an ambiguous successful old-segment or
old-anchor unlink, or from the directory synchronization following either cleanup epoch, does not
change recovery authority. On process restart the newest anchor still selects the exact checkpoint;
obsolete artifacts that remain visible are cleanup residue and are removed only after validation.

The subprocess crash matrix exercises every successful anchor write, file-sync, rename,
directory-sync, and close boundary, followed by each obsolete-segment and obsolete-anchor unlink
and directory synchronization. Every image must select the exact base described above, recover it
identically twice, and continue at the next physical sequence. The evidence and its power-loss
limits are documented in the
[Raft persistent-log crash matrix](../testing/raft-persistent-log-crash-harness.md).

A companion deterministic corruption campaign flips one bit at every byte and truncates at every
prefix of the authoritative anchor and both retained checkpoint segments, then removes each file in
turn. Both strict and repair-authorized recovery reject every image without falling back, repairing,
or changing the damaged namespace. Restoring the original bytes recovers the exact two-group
checkpoint.

Linux packaged-daemon qualification creates an authoritative one-group anchor through the public
checkpoint-and-reclaim path and proves a clean reopen before damage. Flipping one covered base-field
byte then requires the exact anchor-checksum failure before socket admission and leaves both the
damaged 64-byte anchor and retained checkpoint segment unchanged.

A portable `SingleNodeDatabase` case removes the authoritative anchor only after the public
checkpoint path has reclaimed segment 1 and a clean database reopen has accepted the new base. The
next two startups report `Raft recovery base segment is absent`, release ownership, do not infer an
unanchored segment-2 history, and preserve that retained segment byte-for-byte.

Unknown files, nonregular entries, a damaged authoritative anchor, or damage in retained history
remain corruption. Recovery does not fall back to an older anchor after the newest authority is
damaged.
