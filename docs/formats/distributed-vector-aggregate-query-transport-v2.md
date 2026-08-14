# Distributed Vector Aggregate Query Transport v2

> **Status:** accepted and implemented exact response codec/partial-I/O contract. Requests reuse
> the exact Fragment-v2 `CHDVREQ2` carrier. An authenticated receiver owns definition binding,
> worker handoff, and complete bounded response publication. A finite sender owns definition-bound
> retry and result memory. Connected mutual-TLS carriers own definition-bound one-attempt I/O, and a
> deadline-bound client owns outbound TCP acquisition. A bounded TCP server owns listener admission
> and per-connection TLS lifetimes. A production inbound service owns worker, receiver, and server
> lifetimes while supplying fresh definition authority and proof-revalidated real-CSEG execution.
> Leader-linearizable, already-proved bounded-stale, and complete remote-follower outbound
> construction are packaged; broader daemon ownership remains separate.

All integers are unsigned little-endian. Reserved bytes are zero. CRC32C detects accidental damage
and is not authentication. The nested payload retains its own independent checksums.

## Request

The request is exactly [Distributed Vector Query Transport v2](distributed-vector-query-transport-v2.md)
`CHDVREQ2`. An aggregate endpoint additionally admits only an `UNGROUPED_AGGREGATE` Fragment-v2
plan. Sharing the request preserves one canonical general-vector dispatch; endpoint capability and
the distinct response magic prevent row and aggregate response confusion.

## Response

| Offset | Size | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | `CHDVARP2` |
| 8 | 2 | major | `2` |
| 10 | 2 | minor | `0` |
| 12 | 4 | header length | `112` |
| 16 | 8 | frame length | Exact header + payload + trailer |
| 24 | 8 | source node | Nonzero |
| 32 | 8 | target node | Nonzero and different from source |
| 40 | 16 | query UUID | Nonzero |
| 56 | 16 | tablet UUID | Nonzero |
| 72 | 1 | status | Fixed v1 status-code mapping `0..12` |
| 73 | 1 | payload kind | `0` absent, `1` Vector Aggregate Exchange v1 |
| 74 | 1 | flags | Bit 0 means leader hint present |
| 75 | 1 | reserved | Zero |
| 76 | 4 | payload length | Exact nested length |
| 80 | 4 | payload CRC32C | Zero iff absent |
| 84 | 4 | reserved | Zero |
| 88 | 8 | leader node | Nonzero iff hint present |
| 96 | 8 | placement epoch | Nonzero iff hint present |
| 104 | 4 | reserved | Zero |
| 108 | 4 | header CRC32C | CRC32C of bytes `[0,108)` |
| 112 | variable | payload | One exact [Vector Aggregate Exchange v1](distributed-vector-aggregate-exchange-v1.md) on success |
| final - 4 | 4 | frame CRC32C | CRC32C of every preceding byte |

`OK` requires kind 1. Every failure status requires kind 0, zero payload length, and zero payload
CRC. Nested query/tablet identity must exact-match the outer response. The complete response is
`116..1,048,908` bytes.

## Definition, resource, and I/O ownership

Every encoder, exact decoder, reader, and write-cursor constructor requires the complete ordered
Fragment-v2 aggregate definition vector. A decoder and reader additionally require the query
resource context that owns any variable-length extremum retained by the nested state. Definitions
and decode limits are validated even for failure responses, so the authority contract cannot be
omitted on an error-only path.

The reader validates the checksummed fixed header, hard bounds, deployment payload bound, and
caller outer-frame bound before exact allocation. It consumes at most one frame, leaves a
coalesced successor caller-owned, and retains sticky frame failure. The move-only cursor exposes
only its unwritten suffix and leaves a moved-from cursor complete. Unknown versions are
unsupported; damage and wire contradiction are corruption; invalid local contracts are invalid
arguments; lower deployment bounds are resource exhaustion.

## Authenticated receiver

The receiver rejects missing peer authentication before decode, authorizes the claimed source
node, exact-matches the local target, and admits only `UNGROUPED_AGGREGATE`. It first asks the
embedding worker service to bind definitions from current local authority, checks operation, input
ordinal, output type/nullability, and width against the admitted Fragment v2, and only then invokes
execution. Definition binding failure publishes no response because no definition authority exists
for even a failure carrier.

Execution returns its independently proof-derived definitions beside exactly one state per
aggregate. The receiver exact-matches both definition vectors, query/tablet identity, ordinal,
sequence, and terminal position, then encodes and retains the complete response vector. A worker
failure after definition binding becomes one correlated failure response; `UNAVAILABLE` may carry
an authenticated metadata-derived leader hint. Frame-count and total exact encoded-byte ceilings
turn an otherwise valid oversized result into one `RESOURCE_EXHAUSTED` response. Any contract,
encoding, or allocation failure exposes no success prefix. TLS and socket ownership remain outside
this synchronous borrowed-service boundary.

`receive_bound` returns the freshly authority-derived ordered definition vector beside the complete
encoded response vector. The ordinary `receive` API delegates to it and discards only the returned
definition copy. Bound publication lets a later owner independently decode response states without
guessing input authority from final output descriptors.

## Finite sender

One `DistributedVectorAggregateQuerySenderV2` owns the immutable canonical request bytes, complete
ordered definition vector, query resource context, finite attempt/backoff policy, and optional
complete result. Creation requires an ungrouped Fragment-v2 dispatch whose aggregate intents and
result columns exactly match the definitions. The configured frame count must admit the complete
definition width, while nested decode limits, outer encoded bytes, and attempts remain independently
bounded.

An accepted success has exactly one response per definition in ordinal order. Reverse route,
query/tablet identity, sequence, terminal position, payload definition, and absence of a success
hint are exact. The sender canonically re-encodes and decodes every response under its own query
resources before publishing any state, so directly constructed values cannot bypass the carrier
and variable extrema remain query-accounted. Failed validation or allocation destroys the
temporary reconstructed prefix and leaves the attempt pending.

Only one correlated failure frame is valid. `UNAVAILABLE`, `RESOURCE_EXHAUSTED`, and `IO_ERROR`
schedule a whole new attempt under positive capped exponential backoff until the finite attempt
budget ends. A leader hint is advisory and never changes the request target. The sender owns no
socket, TLS state, clock, multi-tablet coordinator, or durable authority.

## Mutual-TLS attempt

The client transfers one exact definition vector and query resource context into its aggregate
response reader before handshake progress. Both peers authenticate certificate fingerprints before
application I/O, and the client additionally authorizes the exact request target. A success stream
has the fixed definition width; one correlated failure frame is terminal. Decoded prefixes remain
private until complete, and any later failure clears them and their query reservations.

The server requests the receiver's bound response form, revalidates those definitions against the
decoded Fragment-v2 request, and exact-decodes the complete response vector before constructing any
write cursors. Fixed 16-KiB TLS scratch, exact header-first frame ownership, frame count, total outer
bytes, nested frame, aggregate-count, state-frame, and variable-extremum limits remain independent.
Handshake and exchange deadlines are positive and sticky. TLS carriers own no connector, listener,
retry, coordinator, or process lifecycle.

## Outbound TCP attempt

`DistributedVectorAggregateQueryTcpClientV2` validates the exact request target, ordered
definitions, nested limits, endpoint/authentication address equality, and positive connect deadline
before opening a socket. It owns the nonblocking descriptor, attempt, definitions, and query
resources until `SO_ERROR` confirms connection success; only then does it create TLS and transfer
the complete authority bundle to the mutual-TLS client. Connect or carrier failure is sticky and
closes the descriptor after destroying TLS state. Retry, endpoint rotation, and coordination remain
outside this one-attempt owner.

## Inbound TCP admission

`DistributedVectorAggregateQueryTcpServerV2` owns one IPv4 listener, server TLS context, a
capacity-reserved stable connection table, and fixed poll storage. Every poll drives at most one TLS
operation per admitted connection and at most a configured finite number of accepts. Descriptors
accepted while full are immediately rejected. Per-connection allocation preserves TLS-before-socket
destruction; shutdown clears carriers before closing the listener. Saturating metrics expose
accepted, rejected, accept-error, completed, failed, and active sessions. Authentication and the
receiver remain borrowed; worker construction and process lifecycle remain outside the server.

## Production inbound composition

`ReplicatedDistributedVectorAggregateQueryTcpServerV2` heap-pins the production real-CSEG aggregate
worker, constructs the authenticated receiver against that stable address, and then constructs the
bounded TCP server against the stable receiver address. Reverse destruction removes every TCP/TLS
session before the receiver and worker. Receiver publication limits are copied from the carrier
limits. Polling, metrics, endpoint access, and idempotent shutdown delegate without changing wire or
execution semantics; storage, coherent authority, authentication, authorization, and optional
leader-hint providers remain borrowed.

## Portable multi-tablet execution

`DistributedVectorAggregateQueryExecutionV2` accepts only a compatible Fragment-v2 snapshot with
nonempty cross-tablet-proved definitions and an ungrouped plan. It retains the Manifest pin, one
query-wide resource context, one finite sender per plan-ordered tablet, and one definition-bound
aggregate coordinator. Complete sender vectors enter the coordinator exactly once; terminal sender
failure enters exactly once, and no partial global result is exposed. Successful finish returns the
original global plan attached to the merged definitions, schema, and finalized scalar values. The
owner has no socket, thread, callback, or clock.

## Multi-tablet TCP scheduling and Native result publication

`DistributedVectorAggregateQueryTcpExecutionV2` validates all node routes, nested carrier bounds,
definition-width coverage, three transport deadlines, and final Native Protocol output bounds
before opening a socket. It owns the portable execution and at most one definition-bound client per
tablet. Finite retries rotate only the immutable target's prevalidated IPv4 addresses. Every due
attempt receives the exact pinned definitions and shared query resource authority. Failure or
cancellation destroys all live attempts and publishes nothing. After every sender closes
successfully, the global scalar result is finalized exactly once into one retained canonical Native
Protocol v1 `QUERY_RESULT` payload.

## Production outbound composition

`create_replicated_distributed_vector_aggregate_query_v2` is the synchronous
leader-linearizable construction boundary. It acquires one correlated group authority vector,
requires the same committed catalog to cover the metadata-group barrier, binds the exact Manifest
snapshot and caller-owned result schema into one compatible v2 owner, resolves only that owner's
immutable targets, and transfers the result through portable aggregate execution into the TCP
scheduler. The call opens no socket; connection attempts begin only when the returned poll owner is
driven. Catalog, read-barrier, and projection views are borrowed only during construction.
Authentication, authorization, and node TLS policy are borrowed for the returned owner's lifetime.
`create_replicated_follower_distributed_vector_aggregate_query_v2` applies the same post-binding
route, execution, scheduling, and finalization path to a canonical same-term leader/follower
authority vector. It separately verifies metadata-group barrier coverage and never routes follower
policy through the leader barrier binder. Remote acquisition of that authority remains a distinct
bounded authenticated lifecycle.

`ReplicatedFollowerDistributedVectorAggregateQueryV2` owns that remote acquisition lifecycle. It
retains the original vector plan, caller result schema, and Manifest pin while placement-backed
leader/follower observation pairs execute. Only the complete canonical authority batch transfers
into bounded-stale binding and the existing TCP owner. Phase-aware metrics, failure, cancellation,
and final result access never expose a partial authority or aggregate result. The executable
loopback contract drives both mutual-TLS phases through `COMPLETE`, decodes the retained Native
Protocol result, and verifies that the committed follower route is unchanged across the phase
boundary. A production-composition loopback uses that same lifecycle with the owning real-CSEG
inbound service and decodes the expected count and sum from installed temporal-part bytes. A
two-tablet variant proves that one completed follower stream remains private until the second
production follower completes and global finalization can publish exactly once. Its failure variant
returns one correlated nonretryable response after the other follower succeeds and proves the
composite closes every attempt without exposing the retained state prefix. A live-session shutdown
variant reaches the same fail-closed publication gate through terminal transport I/O rather than an
application response. A placement-drift variant changes fresh execution authority after definitions
bind and proves the correlated `UNAVAILABLE` response also remains whole-query terminal.
Construction allocation injection additionally proves that every failure before the owner becomes
observable is classified as `RESOURCE_EXHAUSTED`, releases the Manifest pin, and leaves no active
authority pair. This is lifecycle evidence only and changes no bytes in this format.
The transition sweep extends that classification across real mutual-TLS observation acquisition,
follower binding, resource/sender/coordinator creation, route packaging, and aggregate TCP scheduler
installation. Failure remains whole-query terminal with no retained execution or result; the first
successful transition has not yet opened a query attempt. These checks also change no wire bytes.
The next lifecycle-only sweep begins at that execution boundary and classifies every allocation in
the first attempt-start poll. Local allocation failure is terminal rather than a retryable transport
outcome, owns no active client, and releases the pinned snapshot when the failed owner is destroyed;
the first successful poll owns exactly one cancellable attempt. No frame or status encoding changes.
