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
`encode_grouped_float64_exchange_message` and exact decoding add a distinct fixed frame for one
nullable FLOAT64 group key. Signed zeros and every NaN payload use the same canonical tokens as the
local grouped operator; ungrouped v1 bytes remain unchanged. Its fixed reader and move-only cursor
apply the same bounded fragmented-read/coalesced-suffix/short-write ownership as ungrouped v1. A
distinct 64-byte terminal frame closes an empty grouped tablet without fabricating a NULL-key group.
Its separate fixed reader and move-only cursor provide the same bounded fragmented-read,
coalesced-successor, sticky-failure, and checked short-write ownership without assuming how a
carrier discriminates grouped partial and terminal magic.
`DistributedGroupedFloat64Coordinator` then owns one bounded exact retry history per planned
tablet, accepts a terminal-only frame only for an empty sequence-one stream, and merges canonical
keys only after every tablet closes. Its canonical-token iteration is deterministic for tests and
retries but is not a SQL ordering guarantee.
`encode_distributed_grouped_float64_fragment` adds request-side grouping intent as a distinct
checksummed envelope around one exact aggregate fragment. It names only the projected key input;
the nested frame remains the sole owner of snapshot, route, proof, projection, and event-filter
bytes. `bind_distributed_grouped_float64_fragment` first delegates every authority check to the
existing aggregate binder, then proves the key ordinal is FLOAT64 under that same schema and returns
owned group-plus-intent values. The result remains nonexecutable until a canonical grouped dispatch
binds those values for worker-side revalidation.
`encode_distributed_grouped_float64_fragment_dispatch` now supplies that distinct group-scoped
outer format. Its magic cannot be decoded as ungrouped Dispatch v1, and its integrity checks finish
before nested grouped-intent decoding. `bind_distributed_grouped_float64_fragment_dispatch`
delegates the complete grouped authority check and moves its exact owned group plus intent directly
into that dispatch, avoiding a second caller-assembled authority join.
`execute_distributed_grouped_float64_fragment` reuses the ungrouped worker's extracted local
authority validator, resolves generation-pinned temporal winners, then groups only selected visible
rows. It emits terminal partial messages in canonical key-token order or the distinct empty terminal
value; that reproducibility order is not SQL ordering.
Distributed Grouped FLOAT64 Query Transport v1 uses distinct request/response magics to carry the
canonical grouped dispatch and explicitly identify one correlated partial, terminal-only value, or
failure. Its exact codecs own bounded values. Fixed-storage readers retain only one bounded frame,
leave coalesced successors caller-owned, and fail sticky; the move-only cursor owns checked short
writes. The authenticated receiver authorizes before invoking its borrowed worker, validates the
entire bounded contiguous result, and returns response frames only after all encode successfully.
The production grouped service acquires one owning request-local Manifest/schema/placement/group/
barrier context and invokes the same real-CSEG worker without rewriting authority. Sender/network
ownership remains separate. A move-only packaged receiver keeps the service and authenticated
receiver at stable addresses and exposes only the complete response-frame vector. The grouped
mutual-TLS client/server pair then owns one already-connected nonblocking socket, authenticates both
peers before protocol bytes, preserves the encoded response order, and exposes client response
values only after terminal closure. Its outbound TCP composite owns nonblocking connection
completion, exact route binding, a separate connect deadline, and carrier-before-descriptor
teardown. A dedicated bounded grouped TCP server owns listener/TLS lifetime, finite admission,
stable connection records, metrics, and deterministic shutdown. A production owner keeps that
server, the authenticated receiver, and request-local real-CSEG worker in reverse-safe dependency
order; scheduling remains separate.
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
an embedding-owned worker service can execute the dispatch. `ReplicatedDistributedQueryWorker`
provides the production service bridge: its provider acquires one request-local owning Manifest,
schema-lineage, placement, group, and local-barrier context before the existing worker opens any
part. `ReplicatedDistributedQueryTcpServer` owns that worker, the authenticated receiver, and the
bounded mTLS server in reverse-safe destruction order.

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

Encoding and decoding are `O(1)` because each frame is fixed, use constant storage, and perform no
successful-path heap allocation. Across arbitrary fragments, each fixed reader is `O(total bytes)`
and retains exactly one frame of its declared type; cursor advancement is `O(1)`. The bounded
exchange still charges its in-memory `ExchangeMessage` representation, not the wire length.
Coordinator sequence lookup is `O(1)` within one tablet, retained memory is `O(accepted messages)`
under a 65,536-message hard ceiling, and final merge is `O(planned tablets)`.
The grouped coordinator uses ordered tablet and key maps: admission and group merge are
`O(log groups)`, retained history is bounded by the same message ceilings, and `finish` owns a fresh
`O(groups)` merged result or fails without publishing a partial vector.
Fragment encoding/decoding is `O(projected columns)` with a 4,096-column and 16,604-byte hard cap.
The grouped-intent envelope adds constant header/trailer work and one linear CRC over the nested
frame, retaining the nested 4,096-column bound and raising the outer hard cap to 16,648 bytes.
The group-scoped grouped dispatch adds another constant header/trailer and linear CRC pass, with a
16,732-byte hard cap.
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
`DistributedGroupedQueryTlsClient` retains a bounded value-owned response vector but makes it
unavailable until a terminal or failure frame closes the stream; any later protocol or transport
failure clears the prefix. `DistributedGroupedQueryTlsServer` invokes the authenticated receiver
once and writes its already-complete bounded response vector in exact order. Both perform at most
one TLS operation per readiness call and apply sticky handshake/exchange deadlines.
`DistributedGroupedQueryTcpClient` validates the attempt and route before opening a socket, creates
TLS only after authoritative connect completion, and delegates the terminal response publication
boundary without adding retry policy.
`DistributedGroupedQueryTcpServer` reserves its connection and poll tables at startup, admits only a
finite number per poll, keeps each carrier/descriptor pair at a stable allocation, and destroys the
carrier before its borrowed descriptor on completion, failure, and shutdown.
`ReplicatedDistributedGroupedQueryTcpServer` then establishes stable worker/receiver/server
addresses and reverse dependency destruction for the complete production inbound real-CSEG stack.
`DistributedGroupedQuerySender` independently constructs immutable attempts, validates the complete
terminal response vector before publication, and retries only whole attempts under a finite capped
backoff. Advisory hints never mutate its proof-bound target.
`CompatibleDistributedGroupedFloat64Snapshot` delegates complete multi-tablet authority to the
aggregate batch binder, proves the shared key input under every exact schema, derives every grouped
dispatch in plan order, and retains the one pinned Manifest epoch.
`DistributedGroupedQueryExecution` accepts only that proof-carrying owner, constructs one finite
sender per bound tablet, and delivers a sender's already-validated terminal payload vector to the
grouped coordinator once. Backoff never mutates coordinator state; terminal failure does. Its
`finish` method therefore cannot publish before every tablet closes successfully.
`DistributedGroupedQueryTcpExecution` retains that owner and one optional grouped TCP client per
tablet. It validates complete immutable routes before I/O, starts ready and due attempts in bound
order, rotates finite addresses by attempt number, and reports each terminal transport outcome
once. Failure, deadline, and cancellation synchronously release every client; only all-tablet
success publishes the grouped vector.
The packaged replicated grouped constructor first executes the existing leader-linearizable
metadata/Manifest aggregate bind. A specialization overload consumes that exact compatible owner,
matches every nested fragment to the committed active schema, proves the projected FLOAT64 key,
and transfers its Manifest pin into grouped execution. Routes are resolved from the same catalog
before the grouped scheduler is returned.
The bounded-stale grouped entry point instead enters through the correlated follower aggregate
binder. It keeps the leader-derived commit frontier and selected follower application proof intact,
then uses the same compatible-owner specialization and scheduler construction; it never routes a
follower plan through leader-barrier semantics.
`ReplicatedFollowerDistributedGroupedFloat64Query` owns the preceding remote phase as well. It
retains the plan and Manifest pin while the placement-backed batch acquires every authenticated
leader/follower pair, then transfers the complete canonical vector directly into bounded-stale
grouped construction. Cancellation and metrics remain phase-aware without exposing observations.
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
type. A separate first grouped frame now carries one nullable FLOAT64 key with bounded
coordination and authenticated multi-response TLS ownership, but multi-key and non-FLOAT64
grouping, general physical plans, automatic authority reacquisition, arbitrary-expression/general
row ordering, cancellation
delivery, and durable recovery require their own bounded contracts. A leader hint never mutates an existing
proof-bound dispatch: following it requires explicit coordinator rebinding.
For the supported FLOAT64 key surface, result options apply numeric/NaN order, explicit null
placement, and LIMIT only after every tablet terminates and equal keys merge globally. ORDER BY key
plus LIMIT therefore provides top-N without unsafe per-tablet truncation.
The same final pass can order on globally merged COUNT, SUM, extrema, mean, or population variance.
Equal aggregate values use ascending canonical group-key order as the deterministic tie-breaker.
A distinct vector envelope reuses canonical Columnar Batch v1 for general all-type row results. The
outer header contributes query/tablet/sequence/terminal correlation and a second integrity boundary;
it does not reinterpret or duplicate the nested physical column format. Its nonmovable stream
reader retains only the fixed header until integrity-covered outer and nested byte limits pass,
then owns exactly one frame and leaves coalesced successors caller-owned. Its move-only cursor owns
the exact short-write suffix and leaves a moved-from owner inert.
A separate vector-plan intent describes row output, ungrouped or multi-key grouped aggregation,
final order keys, and LIMIT using only bounded projected-input/final-output indices. The later
authority binder must supply exact types and resource policy. ORDER plus LIMIT remains global
intent and cannot be applied independently per tablet without a merge-preservation proof.
The vector fragment adds query/database/table/tablet/schema/group identity, one exact snapshot and
read proof, unique destination projection, and optional event bounds around that plan. Its early
header CRC protects all allocation-driving fields; the outer and nested complete CRCs remain
independent. Decoding alone is not runtime authority: construction and worker revalidation still
have to join the frame to committed metadata, Manifest, and local Raft state.
Its nonmovable stream reader retains only that fixed header until the derived byte shape and caller
frame/projection limits pass, then allocates exactly one frame and enforces caller plan-shape limits
before publication. It reports the consumed prefix without retaining a coalesced successor. Failure
after retained input is sticky. Its move-only cursor owns the encoded variable frame and makes the
moved-from source complete.
The first vector binder now performs the coordinator half of that join for one tablet. It shares
the aggregate path's read-admission rules, exact-matches committed placement and Manifest-v2
Raft/source/schema authority, maps plan indices through the unique projection, and invokes the
local aggregate type oracle before returning owned bytes. A later worker must still independently
reprove its local side.
The compatible vector snapshot repeats that binder in exact plan order under one acquire-pinned
Manifest-v2 database generation. It owns both the pin and dispatch vector, rejects duplicate or
reordered tablets, and bounds aggregate projection ordinals without claiming one comparable Raft
position across groups.
The schema-bound v2 specialization owns one result schema beside that compatible v1 authority set.
It proves the descriptor shape against every tablet's exact committed projection before
publication, then retains the schema once instead of once per tablet. The Fragment-v2 reader waits
for a checksummed fixed header before exact frame allocation; its cursor owns all short-write
progress.
The metadata-backed vector entry point shares the aggregate path's canonical catalog and stable
observation resolver. It derives plan-ordered admissions plus active schema, committed placement,
and immutable group authority, then creates temporary borrowed binding views and publishes only the
compatible vector owner. Metadata and observation lifetimes therefore end at the call boundary.
For leader-linearizable reads, the group-backed vector entry point shares the aggregate path's
canonical group-authority resolver. It uses committed tablet-to-group bindings to select exact
barrier/observation pairs, ignores unrelated groups, and removes the caller-side plan-order join
before entering the metadata-backed vector boundary.
The follower group-backed vector entry point similarly shares the aggregate pair validator and
group resolver. It derives the lag frontier only from the correlated leader observation, retains
the follower's exact applied position and serving identity, and then delegates every remaining
bounded-stale authority check to metadata-backed vector binding.
The first vector transport slice adds a distinct node-routed request envelope around one exact
dispatch. Header integrity protects its allocation-driving length, while payload and complete CRCs
retain independent corruption boundaries. It deliberately defines no response or security semantics
and does not resolve how arbitrary plan outputs acquire Columnar Batch schema identities.
The matching response exact-correlates reverse route, query, tablet, one vector exchange or failure,
and an optional advisory leader hint. It validates header and payload lengths before nested decode
but deliberately leaves multi-frame terminal sequencing to a separate coordinator contract.
Its header-first readers retain fixed request/response headers until integrity-protected hard and
caller frame bounds pass, then own exactly one declared frame and preserve coalesced suffixes. The
shared move-only cursor validates one complete frame before owning checked short-write progress.
The vector coordinator retains canonical messages under per-tablet/global count and total-batch-byte
ceilings, arbitrates exact retries by every encoded byte, and releases complete streams in plan
tablet order only after terminal closure. It deliberately does not infer result column identity from
the nested table-schema-shaped batches.
General vector output now has a separate owned result-schema value modeled on native query result
descriptors. It preserves names verbatim and proves type/nullability against the vector intent and
projected physical inputs. Result Exchange v2 pairs that fragment-bound schema with the unchanged
native schema-light result-cell payload and exact-matches every repeated descriptor. V1 Columnar
Batch bytes remain unchanged and table-shaped.
Fragment v2 now carries that schema beside the unchanged exact v1 authority dispatch. Its binder
derives projected input shapes from the same committed schema used by v1 and rejects descriptor
shape mismatch before publishing the owning wrapper.
Distributed Vector Query Transport v2 carries that complete Fragment-v2 value on a distinct
node-routed request and returns one Result-Exchange-v2 value or an explicit correlated failure. The
response API always requires the admitted result schema, so a transport caller cannot silently
skip descriptor validation. Header-first readers allocate only after fixed integrity and length
checks; the carrier deliberately does not claim peer authentication, socket/TLS lifecycle, retry,
coordination, or execution ownership.
The v2 receiver now consumes a transport-authenticated peer result before decode, authorizes the
claimed source and local target, and then calls one borrowed worker service. It validates and
schema-binds the complete terminal response stream before returning any encoded frame. Separate
frame-count and exact encoded-byte ceilings prevent large native batches from turning a nominally
bounded stream into multi-gigabyte retained output. At that receiver boundary, TLS/TCP progress and
the production worker implementation remained separate owners.
The v2 mutual-TLS carriers now take the authenticated handoff through one complete nonblocking
exchange. The client transfers the admitted schema from its exact request into the response reader
before any response arrives, and it withholds decoded prefixes until terminal closure. The server
authenticates before reading, invokes the receiver once, and schema-validates the complete returned
vector before writing typed cursors. Fixed 16-KiB scratch plus header-first exact-frame ownership
avoids a maximum-size request and response array in every connection.
The v2 outbound TCP composite retains one exact attempt through a separately deadline-bound
nonblocking connect, proves `SO_ERROR` completion, and creates TLS only afterward. It requires the
authenticated peer address to match the endpoint and declares ownership so TLS is destroyed before
its borrowed descriptor. Retry, address rotation, and listener admission remain above or beside
this one-attempt boundary.
The matching v2 inbound TCP owner reserves a finite connection table and poll set at startup,
admits only bounded work per poll, and holds each socket/carrier pair behind a stable handle. It
drives TLS deadlines even without readiness, rejects excess accepted descriptors immediately,
exposes saturating lifecycle metrics, and clears every carrier before the listener on shutdown.
The v2 sender is the policy boundary above one-attempt TCP. It canonically reproduces the immutable
Fragment-v2 request, revalidates every complete response against the owned result schema, and
publishes no payload until exact sequence/terminal/count/byte checks all pass. Retryable outcomes
schedule only finite whole attempts under capped backoff; authenticated leader hints remain
advisory and cannot rewrite the target.
The v2 result coordinator independently re-encodes every admitted in-memory message against one
owned schema, then sequences and deduplicates it per planned tablet. Count and exact exchange-frame
bytes bound retention. Finish first proves every tablet terminal, then transfers the schema and
plan-ordered message vector together so downstream consumers cannot detach values from descriptor
authority.
The first production vector-v2 execution path handles row fragments only. It freshly acquires and
retains local Manifest/schema/placement/group/barrier authority, repeats the proof gates at the
worker, resolves logical winners from real temporal CSEGs, then materializes the admitted row shape
through bounded vector chunks. Its service adapter converts those chunks to native result payloads
and publishes only one complete bounded terminal stream. Tablet-local workers do not apply final
ordering or limit, and aggregate modes fail closed until an all-type merge-state protocol exists.
The production vector-v2 inbound owner places that worker, the authenticated receiver, and the
bounded TCP/mTLS server behind one heap-stable move-only handle. Dependency-order declaration makes
destruction reverse-safe, while moving the public handle cannot invalidate the internal borrowed
addresses. Receiver response bounds are derived from the carrier bounds, so retained publication
cannot cross an inconsistent layer limit.
The portable vector-v2 execution owner accepts only the compatible schema-bearing snapshot. It pins
that Manifest epoch, constructs one immutable sender per plan-ordered dispatch, and delivers each
complete terminal stream to the bounded coordinator once. Completion transfers the global plan,
admitted result schema, and plan-ordered messages together; retry backoff exposes nothing, and
terminal sender or coordinator failure poisons the whole result.
The pinned vector-v2 TCP scheduler validates complete immutable routes before acquisition, then
owns one client slot per tablet and one preallocated readiness table. Sender attempt numbers rotate
only the target node's finite address list; hints never change authority. Caller waits are capped by
the earliest retry and whole-query deadline, while terminal failure or explicit cancellation closes
every active client before returning and never exposes a result prefix.
The vector-v2 row finalizer consumes only that all-tablet result. It validates stream closure and
native schemas before allocating bounded decoded state, stably compares canonical bytes for every
current scalar type, applies final ORDER BY followed by LIMIT, and repacks rows under native payload
and total-output ceilings. Equal keys retain plan-tablet/message/row order, and an empty result still
carries one schema-bearing native batch.
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
exact TLS contexts under hard answer bounds. A canonical batch owner starts every selected group
pair before blocking, polls all descriptors under the earliest deadline, and publishes only one
complete group-sorted authority vector. Automatic placement-backed pair selection, remote
pair selection now prefers an eligible coordinator follower and otherwise the lowest nonleader
replica, resolves every unique target once, and assigns bounded correlations before I/O. Packaged
service ownership now pins the plan/Manifest through acquisition, binds the complete authority
through the metadata barrier, and transfers directly into TCP query execution. A focused
one-process loopback now returns a real installed-CSEG response through the production inbound
service. Moved or multi-process real-CSEG responses, remote worker-interrupt delivery, pooled
multiplexing, asynchronous worker completion, live DNS churn qualification, and broader multi-node
fault handling remain work.

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
