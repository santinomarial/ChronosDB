# Raft Membership Command v1

> **Status: codec, joint-consensus quorum rules, durable recovery, and internal application no-op
> handling are implemented.**

Raft Membership Command v1 is the logical payload for Raft-internal entry types `254` (joint) and
`255` (final). It is independent of application commands and WAL v1. All integers are unsigned
little-endian. A command is an exact byte string with a 32-byte header, bounded voter arrays, and a
4-byte checksum trailer.

## Envelope

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `43 48 52 4e 4d 42 43 00` (`CHRNMBC\0`) |
| 8 | 2 | Major version `1` |
| 10 | 2 | Minor version `0` |
| 12 | 1 | Kind: `1` joint, `2` final |
| 13 | 3 | Required zero |
| 16 | 4 | Total command size, including trailer |
| 20 | 8 | Joint-entry Raft log index; zero for a joint command, nonzero for final |
| 28 | 2 | Old-voter count; nonzero for joint, zero for final |
| 30 | 2 | New-voter count; always nonzero |
| 32 | variable | Ascending old voter IDs, then ascending new voter IDs, each `u64` |
| final 4 | 4 | CRC32C over every preceding byte |

Both voter arrays are nonempty where required, ascending, unique, nonzero, and bounded by the
runtime voter limit. Because both counts are `u16`, that configured limit is in `1..65535`; node
construction rejects a larger value before any transition can depend on an unencodable membership
command. The union of the old and new arrays must also fit that limit. Encoders sort the arrays;
decoders reject noncanonical bytes, invalid field relationships, trailing data, checksum damage,
and unknown versions or kinds.

## State transition

A joint command must name the currently committed voter set as `old_voters`. Once the entry is
present in a node's accepted log, elections and commit advancement require a majority of both the
old and new voter sets. The active transport target set is their union. Only one membership change
may be present at a time.

The leader may append a final command only after the named joint entry commits. The final command
must repeat exactly the joint command's new voter set and joint index. It also commits under both
majorities. Once committed, the new voter set becomes the sole configuration; a removed leader
sends the commit update to the new voters and steps down before returning control.

The configured voter array supplied at group creation is the bootstrap configuration. Recovery
replays membership commands from the retained Raft log to reconstruct the active configuration.
Until Raft snapshots preserve a membership checkpoint, compacting away those commands is not
supported. Tablet and metadata state machines treat committed membership entries as ordered
Raft-internal no-ops while durably advancing their applied indexes.

Entry type `253` is a distinct empty-payload leader progress no-op, not a membership command.
Application state machines apply it with the same ordered internal no-op behavior, but it does not
change the active or checkpointed configuration.
