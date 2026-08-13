# Raft Observation Transport v1

> **Status:** accepted with implemented exact request/response codecs and authenticated receiver.

This cluster protocol acquires one ordered local Raft-group observation from an exact node. It is
separate from Raft Transport v1: observation requests do not participate in consensus and cannot
mutate a group. All integers are little-endian. CRC32C detects accidental damage; mutual TLS and
principal-to-node authorization establish peer identity.

## Request

The request is exactly 84 bytes: an 80-byte header followed by a 4-byte frame CRC32C.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `CHROBSQ1` |
| 8 | 2 | Major version `1` |
| 10 | 2 | Minor version `0` |
| 12 | 4 | Header length `80` |
| 16 | 8 | Total length `84` |
| 24 | 8 | Nonzero source node |
| 32 | 8 | Nonzero distinct target node |
| 40 | 16 | Nonnil Raft group UUID |
| 56 | 8 | Nonzero caller correlation ID |
| 64 | 12 | Zero reserved bytes |
| 76 | 4 | CRC32C of bytes `[0, 76)` |
| 80 | 4 | CRC32C of bytes `[0, 80)` |

## Response header

Every response starts with a 96-byte header and ends with a 4-byte CRC32C over all preceding frame
bytes. A successful response carries one observation payload; a failed response carries none.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `CHROBSR1` |
| 8 | 2 | Major version `1` |
| 10 | 2 | Minor version `0` |
| 12 | 4 | Header length `96` |
| 16 | 8 | Exact total frame length |
| 24 | 8 | Request target as response source |
| 32 | 8 | Request source as response target |
| 40 | 16 | Exact request group UUID |
| 56 | 8 | Exact request correlation ID |
| 64 | 1 | Frozen status code `0` through `12` as enumerated below |
| 65 | 1 | Canonical observation-present Boolean |
| 66 | 10 | Zero reserved bytes |
| 76 | 8 | Exact payload length |
| 84 | 4 | CRC32C of the payload, including the empty payload |
| 88 | 4 | Zero reserved bytes |
| 92 | 4 | CRC32C of bytes `[0, 92)` |
| 96 | variable | Optional observation payload |
| final 4 | 4 | CRC32C of every preceding frame byte |

Status zero requires presence and a nonempty canonical observation. Every nonzero status forbids
both. Status values are: OK, CANCELLED, INVALID_ARGUMENT, OUT_OF_RANGE, NOT_FOUND, ALREADY_EXISTS,
CORRUPTION, IO_ERROR, RESOURCE_EXHAUSTED, UNAVAILABLE, NOT_SUPPORTED, UNAUTHENTICATED, and INTERNAL.

## Observation payload

The payload starts with a fixed 72-byte header, then four node-ID arrays in the listed order.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Observed node; must equal response source |
| 8 | 8 | Nonzero current term |
| 16 | 8 | Leader node, or zero when absent |
| 24 | 8 | Last log index |
| 32 | 8 | Commit index |
| 40 | 8 | Applied index |
| 48 | 4 | Current-voter count |
| 52 | 4 | Committed-voter count |
| 56 | 4 | Joint-old-voter count |
| 60 | 4 | Joint-new-voter count |
| 64 | 1 | Role: follower `1`, candidate `2`, leader `3` |
| 65 | 1 | Canonical leader-present Boolean |
| 66 | 1 | Canonical joint-membership-active Boolean |
| 67 | 1 | Canonical joint-membership-can-finalize Boolean |
| 68 | 1 | Canonical final-membership-pending Boolean |
| 69 | 3 | Zero reserved bytes |
| 72 | variable | Current, committed, joint-old, then joint-new voter IDs |

Every voter set is ascending, unique, nonzero, and independently bounded; current and committed
sets are nonempty. Applied is no later than commit, which is no later than the last log index. A
leader names itself, a candidate names no leader, and a follower cannot name itself. Stable state
has no joint vectors or flags. Active joint state has both joint vectors, and cannot be both ready
to finalize and final-pending.

## Trust, compatibility, and ownership

The receiver rejects missing transport authentication before decoding. After decoding it authorizes
the principal for the claimed source, exact-matches the configured local target, and only then asks
an embedding-owned service for one observation through the node's ordered durable owner. A service
failure becomes one correlated failure response. A malformed or uncorrelated service success is not
encoded.

Header integrity is checked before declared response lengths or payload counts are trusted. Unknown
checksum-valid versions return `NOT_SUPPORTED`; damage and noncanonical decoded bytes return
`CORRUPTION`; configured voter bounds return `RESOURCE_EXHAUSTED`. Minor-version compatibility is
exact. Implemented request and response stream readers validate the fixed header before retaining
the remaining exact bounded frame, consume at most one frame per call, and leave coalesced suffixes
to their caller. The move-only write cursor owns one fully validated request or response and exposes
only its unwritten suffix. TLS socket ownership, retry, fan-out, and timeouts still require a
separate maintained carrier contract.
