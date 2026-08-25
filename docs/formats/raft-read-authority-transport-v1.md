# Raft Read Authority Transport v1

> **Status:** accepted with implemented codecs, bounded stream ownership, authenticated receiver,
> maintained mutual-TLS sessions, finite TCP retry, and all-group attempt fan-out.

This cluster protocol asks one exact node to issue one linearizable read barrier for one Raft group
and returns that barrier with the leader observation that proves its meaning. It is distinct from
Raft Transport v1 and Raft Observation Transport v1: the request can initiate quorum work but does
not add a consensus message type or alter durable Raft bytes. All integers are little-endian.
CRC32C detects accidental damage; the carrier must provide authenticated peer identity.

## Request

The request is exactly 84 bytes: an 80-byte header followed by a 4-byte frame CRC32C.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `CHRRAUQ1` |
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

## Response

Every response starts with a 128-byte header and ends with a 4-byte frame CRC32C. A success carries
one barrier and one nested canonical Raft Observation Response v1 success frame. A failure carries
neither.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `CHRRAUR1` |
| 8 | 2 | Major version `1` |
| 10 | 2 | Minor version `0` |
| 12 | 4 | Header length `128` |
| 16 | 8 | Exact total frame length |
| 24 | 8 | Request target as response source |
| 32 | 8 | Request source as response target |
| 40 | 16 | Exact request group UUID |
| 56 | 8 | Exact request correlation ID |
| 64 | 1 | Frozen status code `0` through `12` |
| 65 | 1 | Canonical authority-present Boolean |
| 66 | 6 | Zero reserved bytes |
| 72 | 8 | Barrier term, or zero on failure |
| 80 | 8 | Barrier context, or zero on failure |
| 88 | 8 | Barrier read index, or zero on failure |
| 96 | 8 | Exact nested-payload length |
| 104 | 4 | CRC32C of the payload, including empty payload |
| 108 | 16 | Zero reserved bytes |
| 124 | 4 | CRC32C of bytes `[0, 124)` |
| 128 | variable | Optional canonical observation response frame |
| final 4 | 4 | CRC32C of every preceding frame byte |

Status zero requires a present authority, three nonzero barrier fields, and a nested successful
observation response. Every nonzero status forbids all of them. Status numbers have the same frozen
mapping as Raft Observation Transport v1: OK, CANCELLED, INVALID_ARGUMENT, OUT_OF_RANGE, NOT_FOUND,
ALREADY_EXISTS, CORRUPTION, IO_ERROR, RESOURCE_EXHAUSTED, UNAVAILABLE, NOT_SUPPORTED,
UNAUTHENTICATED, and INTERNAL.

The nested observation response repeats the exact source, target, group, and correlation. Its
observation must identify the response source as the current leader in the exact barrier term. Its
commit index covers the barrier read index. Membership must be stable: current and committed voters
are identical, ascending, unique, nonzero, contain the leader, and carry no joint-state vectors or
flags. These repetitions are intentional independent correlation checks, not extensibility slots.

## Trust, compatibility, and ownership

The receiver rejects missing transport authentication before parsing attacker-controlled bytes. It
then authorizes the authenticated principal for the claimed source and exact-matches the configured
target before invoking the borrowed service. The service owns barrier scheduling and must return the
barrier with the exact ordered observation that validated it. Exceptions become correlated failure
responses; malformed service successes fail locally and are never encoded.

The decoder validates both checksums, fixed lengths, bounded nested payload length, reserved zeros,
canonical presence/status relationships, the nested observation codec, and authority semantics.
Unknown checksum-valid versions return `NOT_SUPPORTED`; damaged or noncanonical bytes return
`CORRUPTION`; invalid local construction returns `INVALID_ARGUMENT`. Minor-version compatibility is
exact. Implemented readers validate each fixed header before retaining the exact bounded frame,
consume at most one frame per call, and expose any coalesced suffix through exact prefix accounting.
The move-only cursor owns short-write progress. Maintained mutual-TLS client/server sessions
authenticate both node claims before request dispatch and impose exact handshake/exchange deadlines.
Maintained TCP endpoints now own exact nonblocking connect completion, a separate connect deadline,
bounded listener admission, finite accepts per poll, metrics, and TLS-before-descriptor shutdown. A
finite retry owner rotates a bounded immutable address snapshot under one capped attempt/backoff
budget without changing request authority. An all-or-nothing batch owner concurrently drives one
finite acquisition per canonical group, cancels every sibling on failure, and publishes only the
complete authority vector. Daemon listener and Native query integration remain subsequent consumers.
The production service adapter now issues only the requested configured group through the existing
durable replicated barrier owner on a non-poll thread. Daemon listener and Native query integration
remain subsequent consumers.
The implemented shared query-control listener authenticates the client certificate before reading
the eight-byte application magic and selects this protocol only for exact `CHRRAUQ1`; mutable
`CHDMREQ1` requests retain their separate reader and all other magic is rejected without dispatch.
The packaged multi-peer daemon now owns that shared listener on its committed private data endpoint
and serves one requested local group through the replicated read-barrier adapter. Native outbound
authority acquisition remains a subsequent consumer; this is not yet split-leader query support.
