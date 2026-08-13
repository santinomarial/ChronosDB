# Distributed Grouped FLOAT64 Aggregate Exchange v1

> **Status:** accepted with implemented canonical owned encoding and exact borrowed decoding.

This fixed-width frame carries one nullable FLOAT64 group key and one mergeable Float64 aggregate
state from a tablet worker. It is a distinct protocol from the ungrouped 128-byte exchange and does
not change or extend those frozen bytes. All integers are unsigned little-endian; UUIDs use
canonical UUID order; floating values preserve IEEE-754 binary64 bits except for the group-key
canonicalization below.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `CHDXGRP1` |
| 8 | 2 | Major version `1` |
| 10 | 2 | Minor version `0` |
| 12 | 4 | Exact frame length `136` |
| 16 | 16 | Query UUID |
| 32 | 16 | Tablet UUID |
| 48 | 8 | Per-tablet message sequence, nonzero |
| 56 | 8 | Canonical nullable FLOAT64 group-key bits |
| 64 | 8 | Aggregate count |
| 72 | 8 | Sum, IEEE-754 binary64 bits |
| 80 | 8 | Minimum, IEEE-754 binary64 bits |
| 88 | 8 | Maximum, IEEE-754 binary64 bits |
| 96 | 8 | Mean, IEEE-754 binary64 bits |
| 104 | 8 | M2, IEEE-754 binary64 bits |
| 112 | 4 | Flags |
| 116 | 16 | Zero reserved bytes |
| 132 | 4 | CRC32C of bytes `[0, 132)` |

Flag bit 0 is `terminal`, bit 1 is `group key present`, bit 2 is `minimum present`, and bit 3 is
`maximum present`. All other bits are zero. An absent group key represents SQL NULL and requires
positive-zero key bits. A present key canonicalizes both signed zeros to positive zero and every
NaN sign/payload to quiet-NaN bits `0x7ff8000000000000`; other bits remain exact. This matches the
existing grouped SQL equality contract, under which signed zeros share a group and all NaNs share a
group.

The extrema flags must agree. Absent extrema use positive-zero fields. An empty aggregate state has
count zero, no extrema, and positive-zero sum, mean, and M2. A nonempty state has both extrema.

Exact decoding requires 136 bytes and checks magic and CRC32C before interpreting the version,
length, identities, flags, key, aggregate state, or reserved bytes. Checksum-valid unknown versions
are unsupported. Noncanonical key/aggregate encodings and all other semantic damage are corruption.
Invalid values supplied to the encoder are invalid arguments.

CRC32C is accidental-corruption coverage, not authentication. The existing authenticated cluster
transport remains a separate trust boundary. This first grouping-state format does not define
multi-key tuples, non-FLOAT64 keys, grouped fragment plans, coordinator merge/order semantics,
top-N, or LIMIT.

## Stream ownership

The grouped reader retains one 136-byte frame, reports the exact prefix consumed from each caller
view, and leaves a coalesced successor with the caller. It emits only a complete exact-decoded frame
and fails sticky after corruption. The reader is neither copyable nor movable.

The move-only grouped write cursor owns one canonical frame and exposes only its unwritten suffix.
An over-advance fails before changing progress. Moving forces the source complete so it cannot
retransmit the same bytes. These primitives define byte ownership only, not a socket or retry
protocol.

## Empty-stream terminal

A tablet with no groups cannot emit an empty partial under a NULL key because NULL is a real group.
It instead emits the distinct 64-byte Grouped Exchange Terminal v1 frame:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `CHDXGRT1` |
| 8 | 2 | Major version `1` |
| 10 | 2 | Minor version `0` |
| 12 | 4 | Exact frame length `64` |
| 16 | 16 | Query UUID |
| 32 | 16 | Tablet UUID |
| 48 | 8 | Per-tablet message sequence, nonzero |
| 56 | 4 | Zero reserved bytes |
| 60 | 4 | CRC32C of bytes `[0, 60)` |

The frame has no key or partial; successful exact decoding is the terminal event. An empty stream
uses sequence 1. Distinct magic prevents reinterpretation as a NULL group. Nonempty streams may
still mark their last grouped partial terminal. Contiguous sequencing and duplicate arbitration
are enforced by the bounded single-key grouped coordinator.

Its fixed reader retains one 64-byte frame, reports the exact consumed prefix, leaves coalesced
successor bytes with the caller, and fails sticky after exact-decode damage. Its move-only write
cursor owns the canonical frame, exposes only the unwritten suffix, rejects over-advance without
progress, and leaves the moved-from cursor complete. These classes own bytes only; a future
grouped-stream carrier must still choose the frame type before dispatching to either fixed reader.

## Coordinator semantics

Each planned tablet starts at sequence 1. Exact retries compare the canonical key and every
aggregate bit; conflicts and gaps do not mutate retained state. A terminal-only frame is legal only
at sequence 1 of an empty stream. A nonempty stream closes through the terminal flag on its last
grouped partial. Results remain unavailable until every tablet closes, then equal canonical keys
merge across tablets. The coordinator's NULL-first canonical-token iteration is deterministic but
does not define SQL result ordering.
