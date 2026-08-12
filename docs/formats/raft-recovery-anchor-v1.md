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

Unknown files, nonregular entries, a damaged authoritative anchor, or damage in retained history
remain corruption. Recovery does not fall back to an older anchor after the newest authority is
damaged.
