# Distributed Aggregate Exchange

## Purpose and public interfaces

`encode_exchange_message` converts one validated `ExchangeMessage` into an owned 128-byte worker/
coordinator frame. `decode_exchange_message_exact` validates one borrowed exact frame and returns
value-owned state. `BoundedExchange::push` and `DistributedAggregateCoordinator::accept` enforce
the same state invariants before retaining a message.

`ExchangeFrameReader` composes exact decoding with fragmented/coalesced stream input. Each call
reports how much of the caller's view belongs to one frame. `ExchangeFrameWriteCursor` owns one
encoded frame and exposes the remaining suffix after each checked short-write acknowledgement.
`DistributedAggregateCoordinator` retains a finite per-tablet retry history, enforces contiguous
sequence order, and merges only accepted messages into terminal tablet state.
`encode_distributed_aggregate_fragment` and exact decoding provide the corresponding request-side
bytes for one snapshot-bound projected aggregate scan.
The dispatch envelope adds the distinct Raft group identity that scopes every admission index;
workers never execute the bare inner fragment.
`bind_distributed_aggregate_fragment` constructs that envelope only after one Manifest v2 snapshot,
committed placement, schema, group, and proof admission exact-match.
`bind_compatible_distributed_aggregate_snapshot` applies that boundary to every planned tablet in
plan order and returns a move-only owner that pins the one Manifest v2 epoch behind all dispatches.
`bind_metadata_backed_distributed_aggregate_snapshot` first resolves every active schema,
placement, and immutable tablet-to-group identity from one committed metadata publication. It
derives admissions from stable plan-ordered Raft observations and policy-specific proofs, then
enters the same compatible Manifest binder.
`execute_distributed_aggregate_fragment` repeats local authority checks, resolves temporal winners
from validated generation-pinned parts, filters event time, and emits one terminal partial state.
Distributed Query Transport v1 wraps the dispatch and terminal exchange in correlated cluster
request/response frames. `DistributedQueryReceiver` authenticates and authorizes the source before
an embedding-owned worker service can execute the dispatch.

## Data, ownership, and invariants

The frame names both query and tablet and includes a nonzero per-tablet sequence. Its aggregate is
the mergeable Welford state: count, sum, extrema, mean, and M2. Presence flags are explicit because
zero is a legitimate extremum. Empty state has exactly one representation: absent extrema and
positive-zero numeric state. This also rejects negative-zero alternatives that would decode to the
same arithmetic value but produce different bytes.

`EncodedExchangeMessage` owns a `std::array<std::byte, 128>`. Its byte view remains valid only while
that owner lives. Exact decoding does not retain the input view. The current in-memory exchange is
mutex-protected MPMC state; the codec itself has no shared state and needs no synchronization.
The reader is a noncopyable, nonmovable connection-owned state machine. The write cursor is
move-only, and moving it makes the source complete so only the destination can continue output.
The coordinator is single-owner and unsynchronized. Its history owns message values until the
coordinator is destroyed, making exact retry decisions independent of carrier-buffer lifetime.
The encoded fragment owns one bounded vector. Decoding borrows input only for the call, checks both
integrity boundaries before projection allocation, and returns owned ordinals.
The dispatch decoder validates its outer integrity and group first, then delegates exact inner
decoding. Tablet identity is never substituted for group identity.
The binder borrows its authority only for the call and copies a coherent immutable request; it
never retains spans or references and performs no storage or metadata mutation.

## Failure behavior and complexity

Length, magic, and CRC are rejected before payload interpretation. Unknown checksum-valid versions
return `NOT_SUPPORTED`; damaged or contradictory bytes return `CORRUPTION`; invalid encoder input
returns `INVALID_ARGUMENT`. Transport authentication is separate from CRC integrity.

Encoding and decoding are `O(1)` because the frame is fixed at 128 bytes, use constant storage, and
perform no successful-path heap allocation. Across arbitrary fragments, the reader is `O(total
bytes)` and retains exactly one frame; cursor advancement is `O(1)`. The bounded exchange still
charges its in-memory `ExchangeMessage` representation, not the wire length.
Coordinator sequence lookup is `O(1)` within one tablet, retained memory is `O(accepted messages)`
under a 65,536-message hard ceiling, and final merge is `O(planned tablets)`.
Fragment encoding/decoding is `O(projected columns)` with a 4,096-column and 16,604-byte hard cap.
Binding is `O(replicas + tablets + projected columns)` and allocates only the owned projection.
Compatible batch binding adds `O(fragments log fragments + total projected columns)` validation and
retains one shared Manifest generation plus the bounded plan-ordered dispatch vector. Execution
creation adds an ordered `O(fragments)` index with `O(log fragments)` event lookup; the owner is
single-threaded and requires external method serialization.
Worker authority checks precede I/O. Current execution inherits the bounded temporal resolver's
decode and winner-selection costs, then scans visible logical rows once for filtering/aggregation.
Transport requests retain at most 16,772 bytes and responses at most 244 bytes. Authentication,
outer and nested decoding, route validation, and worker execution occur in that order.
Request/response stream readers retain those fixed maxima, integrity-check the complete header
before trusting its declared length, consume no coalesced successor bytes, and fail sticky. The
move-only write cursor exposes only the unwritten suffix. The deterministic sender permits one
outstanding attempt, exact-correlates the reverse route/query/tablet, and caps retry count and
exponential backoff.
`DistributedQueryExecution` retains the compatible snapshot and one sender per tablet. It delivers
each successful terminal exchange to the coordinator once, ignores nonterminal backoff for merge
purposes, and reports only terminal sender failure. Coordinator `finish` therefore remains the sole
complete-result boundary. Its preferred constructor derives the coordinator's owned admissions
directly from the already-validated dispatches instead of accepting a second caller-assembled
authority vector.
`DistributedQueryTlsClient` owns one sender attempt's maintained TLS readiness, authenticates the
server certificate principal for the immutable target before writing, applies exact handshake and
exchange deadlines, and retains one fixed-bound canonical response for sender correlation.
`DistributedQueryTlsServer` symmetrically authenticates the client certificate before reading one
fixed-bound request, invokes the authenticated receiver once, and owns the sole response through
all TLS short writes under the same deadline model.
`DistributedQueryTcpServer` owns the dedicated listener, long-lived TLS context, fixed-capacity poll
storage, bounded stable connection records, deadline driving, metrics, and carrier-before-descriptor
shutdown order for real multi-connection serving.
`DistributedQueryTcpClient` validates one attempt before connect, owns its connect deadline and
descriptor, creates TLS only after `SO_ERROR` success, delegates exchange readiness, and retains the
exact response while preserving carrier-before-descriptor teardown.
`DistributedQueryTcpExecution` owns that execution and one optional client per plan-ordered tablet.
It prevalidates immutable target routes, starts ready or deadline-due attempts, drives every active
client from one fixed poll table, reports each transport outcome once, closes peers on terminal
failure, and publishes only the all-tablet coordinator result. The pinned Manifest epoch therefore
outlives every attempt and retry. Its optional whole-query monotonic deadline and explicit
cancellation both close every active client, retain no partial result, and remain sticky.
`resolve_distributed_query_node_routes` constructs those immutable routes for only the selected
serving nodes from one committed node-metadata snapshot. Node-specific TLS contexts remain explicit.
Strict numeric endpoints bypass resolution; lowercase DNS endpoints acquire one fresh bounded,
ordered, unique IPv4 candidate set before the poll owner starts. Finite sender retries rotate those
candidates without changing the target node, proof, retry budget, or TLS certificate identity.
After a retryable terminal failure, explicit finite rebinding accepts only an independently proved
execution for the same plan-ordered logical query and a nonregressing Manifest generation. It
discards every old partial and pin together before new attempts begin; leader hints remain advisory.
On `UNAVAILABLE`, the authenticated receiver may query a committed metadata provider for the exact
tablet/group and publish its advisory leader/placement pair through the canonical response. Provider
failure emits no hint or response, and the scheduler exposes a correlated hint only for a caller's
fresh authority lookup.

## Tradeoffs and deferred work

A fixed ungrouped-aggregate frame gives partial-I/O carriers an unambiguous payload without
prematurely defining a general physical-fragment language. The cost is a specialized first exchange
type. Grouping state, physical plans, ordering/top-N, cancellation delivery, and general duplicate
sequencing require their own bounded contracts. A leader hint never
mutates an existing proof-bound dispatch: following it requires explicit coordinator rebinding.
The replicated read-barrier owner now returns exact correlated leader observations for
leader-linearizable proof construction. The group-backed binder joins that group-sorted authority
to plan-ordered tablets through committed immutable tablet-to-group bindings and ignores unrelated
groups. The packaged replicated-query constructor first requires the committed catalog to cover the
exact metadata-group barrier, then carries the authority through binding, committed route
resolution, execution creation, and the TCP lifecycle owner without exposing intermediate
correlation vectors. Bounded-stale binding now derives the commit frontier from a same-group,
same-term leader/follower observation pair rather than a caller scalar, and a distinct packaged
constructor carries that pair through the metadata barrier, Manifest binding, follower route, and
TCP owner. Raft Observation Transport v1 now canonically binds one authenticated source/target,
group, and correlation identity to either a complete ordered local Raft observation or an exact
failure, and its receiver rejects trust and route failures before invoking the embedding's durable
observation service. Its bounded readers now validate fixed headers before retaining exact frames,
and its move-only cursor preserves short-write ownership. An outbound maintained mTLS attempt now
authenticates and authorizes one exact target before writing and exposes only a correlated response.
A nonblocking TCP composite owns connection establishment and closes the TLS session before its
borrowed descriptor on failure. An accepted-socket mTLS session authenticates before reading and
dispatches exactly one observation request. A dedicated server adds bounded listener admission,
stable connection ownership, metrics, and ordered shutdown. A single-node acquisition owner rotates
an ordered address snapshot under one finite retry/backoff budget while preserving request and node
authority. A two-target owner fans out the selected leader and follower, cancels a survivor on
failure, and publishes only a complete same-term stable-membership authority pair. A blocking
pre-poll resolver joins canonical selected nodes to committed numeric/DNS endpoint metadata and
exact TLS contexts under hard answer bounds. Automatic catalog-wide multi-pair acquisition, remote
worker-interrupt delivery, pooled multiplexing, asynchronous worker completion, live DNS churn
qualification, and broader multi-node fault handling remain embedding work.

## Verification and review questions

The golden-layout test reads every field independently and freezes one whole-frame CRC. Corruption
tests rewrite CRC where needed so reserved, version, and canonical-state validation are exercised
beyond the checksum gate. Carrier tests enumerate every two-part split, coalesced frames, sticky
failure, short writes, invalid advances, and move ownership. Sanitizer and installed-consumer checks
cover runtime safety and public header/link visibility.

Coordinator tests reject gaps and conflicts without mutation, accept bit-exact retries, close on
terminal state, preserve the first failure, ignore post-terminal worker loss, and exhaust finite
history explicitly.

Fragment tests freeze its complete identity/proof/predicate/projection layout and both CRCs, then
exercise corruption, canonical rejection, unknown versions, and lower deployment limits.

**Why require positive zero for empty state?** Arithmetic equality is too weak for canonical bytes:
positive and negative zero compare equal but have different IEEE-754 representations.

**Why preserve Welford state instead of rows?** Workers reduce data before transfer, while merging
count/mean/M2 retains population variance semantics without replaying individual values.

**Why is sequence not enforced by the codec?** The codec proves that a sequence exists and is
nonzero. Ordering, deduplication, and retry windows depend on connection/coordinator lifecycle and
belong to the carrier protocol.

**Why retain full messages instead of only the last sequence?** A delayed retry can name any
already-accepted sequence. Retaining bounded full values distinguishes a true retry from a
conflicting reuse without trusting hashes or accepting collision risk.
