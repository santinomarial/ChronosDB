# Distributed Vector Grouped Aggregate Query Transport v2

> **Status:** accepted and implemented exact response codec, partial-I/O contract, authenticated
> receiver, finite sender/retry owner, mutual-TLS session carrier, outbound nonblocking TCP owner,
> bounded inbound TCP server, production real-CSEG inbound composition, and all-tablet outbound
> scheduling. Requests reuse the exact Fragment-v2 `CHDVREQ2` carrier. Native grouped SQL result
> finalization remains a separate follow-on boundary.

All integers are unsigned little-endian. Reserved bytes are zero. CRC32C detects accidental damage
and is not authentication. The nested grouped payload retains its own independent checksums.

## Request

The request is exactly [Distributed Vector Query Transport
v2](distributed-vector-query-transport-v2.md) `CHDVREQ2`. A grouped sufficient-state endpoint must
additionally admit only a `GROUPED_AGGREGATE` Fragment-v2 plan and bind its exact key and aggregate
authority from current local schema/snapshot proof. Sharing the request preserves one canonical
general-vector dispatch; endpoint capability and the distinct response magic prevent row,
ungrouped-state, and grouped-state response confusion.

## Response

| Offset | Size | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | `CHDVGRP2` |
| 8 | 2 | major | `2` |
| 10 | 2 | minor | `0` |
| 12 | 4 | header length | `112` |
| 16 | 8 | frame length | Exact header + payload + trailer |
| 24 | 8 | source node | Nonzero |
| 32 | 8 | target node | Nonzero and different from source |
| 40 | 16 | query UUID | Nonzero |
| 56 | 16 | tablet UUID | Nonzero |
| 72 | 1 | status | Fixed v1 status-code mapping `0..12` |
| 73 | 1 | payload kind | `0` absent, `1` grouped aggregate exchange v1 |
| 74 | 1 | flags | Bit 0 means leader hint present |
| 75 | 1 | reserved | Zero |
| 76 | 4 | payload length | Exact nested length |
| 80 | 4 | payload CRC32C | Zero iff absent |
| 84 | 4 | reserved | Zero |
| 88 | 8 | leader node | Nonzero iff hint present |
| 96 | 8 | placement epoch | Nonzero iff hint present |
| 104 | 4 | reserved | Zero |
| 108 | 4 | header CRC32C | CRC32C of bytes `[0,108)` |
| 112 | variable | payload | One exact [grouped aggregate exchange v1](distributed-vector-grouped-aggregate-exchange-v1.md) on success |
| final - 4 | 4 | frame CRC32C | CRC32C of every preceding byte |

`OK` requires kind 1. Every failure status requires kind 0, zero payload length, and zero payload
CRC. Nested query/tablet identity must exact-match the outer response. The complete response is
`116..67,108,980` bytes.

## Authority, resource, and I/O ownership

Every encoder, exact decoder, reader, and write-cursor constructor requires the complete ordered
group-key and aggregate-definition vectors. Decoder and reader additionally require the query
resource context that owns decoded variable key bytes and nested variable extrema. Both authority
vectors and every nested limit are validated even for failure responses, so an error-only path
cannot omit or weaken the admitted schema contract.

The reader validates magic, fixed header CRC, versions, reserved fields, identity, status/payload
shape, hard payload bounds, caller nested-frame bound, and caller outer-frame bound before exact
allocation. It consumes at most one frame, leaves a coalesced successor caller-owned, and retains
sticky frame failure. Exact decode then validates the complete outer CRC, payload CRC, nested frame,
authority, and correlation. The move-only cursor exposes only its unwritten suffix and leaves a
moved-from cursor complete.

Unknown versions are `NOT_SUPPORTED`; damage, type confusion, and wire contradiction are
`CORRUPTION`; invalid local authority/limits are `INVALID_ARGUMENT`; lower deployment bounds and
allocation failure are `RESOURCE_EXHAUSTED`. The codec and partial-I/O cursors own no socket, peer
authentication, retry policy, clock, coordinator, or process lifecycle.

## Authenticated receiver

The receiver authenticates the peer and authorizes the claimed source node before binding or
execution. It then verifies the exact local target and grouped plan mode, asks the embedding to bind
fresh proof-derived grouped authority, validates that authority against the Fragment-v2 plan and
result schema, and only then executes the worker. Bound authority and executed authority must match
field for field.

Successful worker output must contain either one canonical empty terminal or a complete contiguous
group stream whose count, ordinal, sequence, terminal flag, query, and tablet agree exactly. The
receiver exact-decodes every nested frame under a request-local query-memory ceiling and constructs
the complete response vector under independent frame and byte ceilings before publishing anything.
An execution failure becomes one correlated payload-free response. `UNAVAILABLE` may carry a fresh
advisory leader hint; route, authentication, binding, decoding, and worker-contract failures remain
local errors. No success prefix crosses the receiver boundary.

## Finite sender

The sender owns one immutable canonical `CHDVREQ2` request, complete grouped authority, query
resource context, and finite exponential retry budget. Each attempt copies the identical request
and retains the admitted target; leader hints are advisory and never rewrite request authority.
Transport failures and retryable response statuses schedule only whole-request retries.

A successful response vector is accepted only while one attempt is pending. Every response must
have exact route/query/tablet correlation and form either one empty terminal or one complete
contiguous group stream. The sender canonically encodes and exact-decodes each outer response under
its owned authority and query memory, then re-encodes the nested grouped frame. Only the complete
vector is published. Malformed, partial, over-count, over-byte, or allocation-failed vectors leave
the sender waiting with no retained prefix. Terminal success or failure rejects later attempts.

Receiver and sender are single-thread-affine policy owners. The mutual-TLS carrier owns one already
connected nonblocking session, authenticates both verified certificate fingerprints before
application I/O, and requires client-side authorization of the exact attempt target before request
write. It retains complete grouped authority and query resources while reading and exposes only one
complete empty-or-contiguous response vector. Its server independently revalidates the receiver's
bound authority and complete response vector before constructing any write cursor.

The outbound TCP owner retains the immutable attempt, complete grouped authority, and query
resources through a deadline-bound nonblocking connection. It validates all authority and limits
before opening a descriptor, exact-binds the authentication address to the endpoint, proves
`SO_ERROR` completion, transfers ownership into TLS only afterward, and destroys TLS before its
descriptor on failure.

The bounded TCP server owns the listener, TLS context, fixed poll storage, finite acceptance, stable
descriptor/carrier records, saturating metrics, and ordered shutdown. Every admitted session still
enters the same authenticated grouped receiver and publishes no partial stream.

The production inbound owner heap-stabilizes the proof-revalidating real-CSEG worker, receiver, and
server in dependency order. Binding and execution reacquire independent coherent Manifest, schema,
placement, group, and barrier authority; a changed proof cannot reuse the binding result.

The all-tablet scheduler owns a finite prevalidated route snapshot, one sender and at most one TCP
client per plan-ordered tablet, retry-address rotation, earliest-deadline polling, whole-query
cancellation, metrics, and the portable grouped execution. It shares the pinned execution's query
resource authority with every sender and client. Complete sender results enter the coordinator once;
physical rows remain unavailable until every tablet and the global merge close. Leader hints remain
advisory and never alter route or snapshot authority.

The transport and scheduler still own no process lifecycle, Native protocol result packaging,
computed pre-group plan splitting, or shuffle routing.
