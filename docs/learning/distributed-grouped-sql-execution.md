# Distributed Grouped SQL Execution

## Purpose and public interfaces

Distributed grouped SQL composes two existing correctness boundaries: authority-proved all-tablet
row exchange and the local bounded physical GROUP BY engine. It deliberately does not serialize a
`PhysicalPipelinePlan` or invent a second grouped evaluator.

`lower_bound_sql_select_to_distributed_vector_grouped()` returns:

- `input_rows`, an unlimited identity projection of the complete source schema for fragment
  binding;
- `coordinator_pipeline`, the ordinary owned physical plan for the complete bound SELECT; and
- `result_schema`, the exact SQL output names, types, and nullability.

`finalize_distributed_vector_physical_rows_v2()` consumes the complete schema-bound execution
result, synchronously borrows the immutable physical plan, owns the output schema, and returns the
same all-or-none Native batch product used by distributed row finalization.

`lower_bound_sql_select_to_distributed_vector_grouped_aggregate()` is the distinct scalable-path
entry point. It owns the unique source dependency projection, exact event-time range, and raw
key/aggregate schema consumed by the grouped sufficient-state scheduler. Direct keys and aggregate
inputs keep the original compact plan. If any input is computed, the lowerer additionally owns an
ordered, versioned pre-group `VectorExpression` program and makes grouped-plan input ordinals name
that program's outputs. For non-identity SELECT lists it owns checked final expressions, the client
schema, and projected global ordering and limit. Non-event-time predicates and hidden order
expressions still return `NOT_SUPPORTED` so the caller can select the row-backed plan explicitly.

## Execution sequence

```text
bound grouped SELECT
        |
        +--> unlimited full-source row intent --> authority-bound tablet workers
        |                                           |
        |                                  schema-bound Native rows
        |                                           |
        +--> local PhysicalPipelinePlan  <--- all streams close and validate
                                                    |
                                      query-accounted batch source
                                                    |
                          WHERE -> prepare -> GROUP BY -> project
                                      -> global sort -> LIMIT
                                                    |
                                     owned Native result batches
```

Workers retain exact snapshot, placement, group, barrier, schema, and projection checks. They do not
apply GROUP BY, ORDER BY, or LIMIT. The coordinator therefore sees every selected row exactly once
before global aggregation.

The sufficient-state path instead validates every pre-group input leaf against the exact worker
schema, evaluates the owned outputs after event-time filtering, and groups those output vectors.
Fragments with no program retain v1 bytes. Computed fragments nest the checksummed program in
Mutable Fragment v2, and the complete logical identity prevents tablets or authority rebindings
from changing it.

## Native-row to vector conversion

The finalizer first validates every stream and exact-decodes each nonempty Native batch only for
preflight. It computes canonical buffer bytes column by column: nullable validity bitmaps, Boolean
bitmaps, fixed-width row storage, variable offsets and payload, and the identity selection vector.
The configured batch working limit is checked before pipeline execution.

The source decodes again lazily and materializes one batch per pull. It reserves exact query credit
before allocating canonical buffers. Fixed and UUID bytes are copied unchanged, Boolean bytes are
packed into the physical bitmap, NULL fixed slots remain canonical zero storage, and variable bytes
receive checked 32-bit offsets. `OwnedPhysicalColumn`, `VectorChunk`, and the physical source-shape
operator independently revalidate the result. Downstream demand releases each source chunk after
its rows enter grouped state.

## Group semantics and ordering

All behavior comes from the existing physical pipeline. Group keys may be fixed or variable,
nullable, computed, and multi-column. Hash matches still use exact canonical key equality. COUNT,
SUM, AVG, MIN, MAX, and both variances retain their established NULL, widening, NaN, overflow, and
variable-extremum rules. Final expressions address group keys and aggregate results through checked
programs.

ORDER BY runs after every tablet row has entered global group state. The physical lowerer appends
group-key tie breakers, then removes helpers before LIMIT and Native encoding. Without ORDER BY,
current plan/tablet/message order makes first-seen emission deterministic, but SQL clients must
treat the result as unordered.

## Ownership, failures, and complexity

The completed exchange result owns encoded batch bytes throughout synchronous conversion. Each
materialized chunk owns canonical columns plus query credit. Group keys, aggregate states,
variable extrema, sort state, and output chunks retain the existing move-only query reservations.
Encoded output batches are separately bounded and are published only after end-of-stream.

Malformed correlation, sequence, closure, descriptor, canonical-cell, or physical shape is rejected
before output. Allocation and all size/cardinality limits return resource exhaustion. Any physical
runtime error destroys the pipeline and no partial Native response escapes. Empty input creates no
groups and one zero-row schema-bearing output payload.

For `R` rows, `K` keys, `A` aggregate calls, and `G` groups, grouped work is expected
`O(R * (K + A))`; final ordering adds `O(G log G * order keys)`. Exchange volume is the complete
projected source row set. Retained memory is bounded by one input batch plus query-accounted group,
aggregate, sort, and output state.

## Tradeoffs and interview questions

**Why run the local plan at the coordinator?** It immediately preserves the proved multi-key and
all-type SQL semantics without freezing implementation objects into a protocol. It is the clearest
correctness baseline for a later sufficient-state shuffle.

**Why send every source column?** Bound physical programs use exact source ordinals, and WHERE,
computed keys, or aggregate arguments may reference any source leaf. Projection remapping is a
separate optimization that needs measured exchange evidence and a retained mapping contract.

**Why decode twice?** Preflight proves all stream, schema, row, and working bounds before expensive
group state can publish output. Lazy decoding then keeps peak canonical input ownership to one
batch. The second linear pass is an explicit recoverability and memory-bound tradeoff.

**What remains for scalable distributed grouping?** The versioned multi-key, all-type grouped-state
frame, shared query-accounted group table, plan-ordered coordinator, proof-revalidated real-CSEG
worker, authenticated transport, all-tablet scheduler, and atomic Native finalizer are implemented.
The direct SQL lowerer now produces their exact projection/key/aggregate/result contract without a
second binding oracle. The replicated SQL constructor derives every committed table fragment from
one catalog, Manifest epoch, and single acquired authority vector; it rejects missing/extra
Manifest tablets and transfers the complete owned contract into the scheduler. Computed final
expressions now run over the globally merged raw key/aggregate vector through the shared checked
projection, sort, and limit stages. Computed group keys and aggregate inputs now lower into the
owned program, cross authenticated transport, and execute before tablet-local grouping. Canonical
bounded source-side partition splitting, complete node-bound destination authority,
and an exact checksummed per-message remote carrier now exist. An atomic complete-stream owner also
authorizes the already authenticated source principal, locks one remote edge, proves terminal
closure, and withholds every decoded group until full success. A fixed checksummed reverse-route
receipt binds successful extraction to the exact edge and accepted frame/byte extent. A connected
mutual-TLS owner now authenticates both certificate fingerprints before application I/O, authorizes
the destination before the source writes, and completes only after that receipt. A finite policy
owner reconstructs byte-identical attempts for the unchanged edge under capped exponential backoff
and recognizes success only after that receipt. TCP listener ownership now reserves result capacity
before admission and retains acknowledged streams in a fixed FIFO. Duplicate-admission ownership,
partition reduction, packaged selection, and broader fault/
measurement evidence remain open. The row-backed path remains the differential oracle for that work.

### Canonical source-side partition boundary

`DistributedVectorGroupedAggregatePartitioner` exact-decodes one complete tablet-local grouped
stream before exposing output. It applies the same versioned canonical hash used by the local
grouped table, so nullable typed keys, signed zero, and NaN cannot acquire a second routing
identity. `hash-v1 % fixed_partition_count` selects a destination; collisions remain harmless
because the eventual reducer must still use exact key equality.

Each source produces one complete stream for every partition, including an explicit empty terminal.
This makes reducer closure a finite all-source terminal condition instead of a timeout. Group order
within a partition remains source-local first-seen order and is re-ordinalized canonically. Hard
input, destination-group, per-stream-byte, and total-output-byte bounds classify skew or empty-edge
amplification before a partial vector can escape. Allocation failure is likewise atomic and the
immutable owner can retry the same caller-owned input.

The partition ID is owned beside the unchanged grouped frame; the complete shuffle authority binds
hash version, partition count, every ordered source tablet/node, every partition/destination node,
and exact key/state shape. It permits local edges but requires them to bypass a self-network route.
The distinct `CHDVGSF1` outer carrier now binds one nested message to an exact remote edge, verifies
header, payload, and complete-frame integrity, and recomputes hash routing after canonical decode.
Its header-first reader rejects route and allocation-length drift before retaining the full frame,
while a move-only cursor owns short writes. A complete-stream sender now exact-decodes and privately
constructs every same-edge frame before exposing bytes. Its receiver consumes a previously
authenticated principal, authorizes the claimed source node once, locks all subsequent frames to
that edge, and destroys any incomplete or invalid prefix. The distinct `CHDVGAK1` receipt reverses
the route and binds exact successful extraction plus accepted count/bytes. A nonblocking connected
mutual-TLS client/server pair authenticates before application bytes, applies exact source and
destination node authorization, enforces handshake/exchange deadlines, and retains no failed
prefix. A separate finite policy owner retains the immutable edge and canonical nested bytes,
reconstructs byte-identical whole-stream attempts, applies capped exponential backoff, and records
success only after that TLS session validates the exact receipt. One outbound TCP owner validates
the route before opening a descriptor, enforces a separate connect deadline, proves `SO_ERROR`, and
transfers the attempt into TLS with carrier-before-descriptor teardown. It does not yet own bounded
listener admission or idempotent duplicate admission. The complementary listener now reserves one
preallocated result slot per admitted TLS session, rejects capacity pressure before application
acceptance, and retains receipt-acknowledged complete streams in allocation-free FIFO order. The
listener hands retained results to an authority-bound local partition reducer. That reducer
revalidates complete extent, source, destination, terminal sequence, and canonical hash route even
though transport already authenticated the peer. It reserves finite per-source and total outer
bytes, feeds the existing query-accounted coordinator, and records the exact accepted prefix so a
caller-retained stream can resume after allocation failure. A byte-identical whole-stream retry is
a no-op; a conflicting retry fails closed. Output remains unavailable until every authority source
terminal arrives and then merges in authority source order. The reducer is intentionally in-memory:
it does not claim process-crash recovery or durable deduplication. The current packaged path still
sends complete tablet streams to one coordinator rather than scheduling every partition edge.

Packaged authority selection no longer needs a caller-authored destination vector. The complete
proof-bound mutable fragment vector supplies source tablets and their exact serving nodes. Source
order remains plan order for deterministic merge, while the sorted unique serving-node set assigns
one contiguous partition per participating node. This mapping is deterministic and placement-aware
but deliberately not load-aware; it does not consult route order, DNS, or live utilization.

One atomic source-plan owner now joins that authority to canonical partitioning. It privately
constructs every edge for one complete tablet stream. A self-route exact-decodes into the same
complete-stream value accepted by the local reducer; a remote route becomes a finite byte-identical
retry owner. Both paths recheck the frozen authority, empty partitions remain explicit, and the
whole source fails before publication if any edge or total outer-byte bound fails. Extracting local
and remote vectors transfers ownership without allocating. This is fan-out preparation, not yet
the event-loop scheduler that drives all remote edges and destination reducers.

Remote fan-out now has one bounded poll owner. It exact-matches every retry to the same authority,
preflights complete node/IPv4/TLS routes, rotates addresses by attempt number, and permits one
active TCP/mTLS client per edge. Success is all-edge and receipt-gated. Caller wait is capped by the
whole execution, retry, connect, handshake, and exchange deadlines; the listener now applies the
same cap to server sessions. Cancellation and terminal failure tear down every client. Destination
stream draining and partition-result gathering remain outside this owner.

Each destination node now has one execution owner for every authority partition assigned to it.
Self-routes enter the matching reducer directly; acknowledged listener results first move into a
single pending slot and remain there across retryable reducer allocation failure. Reducers finish
only after all authority sources close. Remote ingress stays live after reducer readiness so an
exact retransmission caused by receipt loss is still acknowledged, even after output was consumed;
the query coordinator may seal transport only after all senders prove receipt. Query-wide result
gathering and joint source/destination lifecycle remain outside this owner.

Once every sender receipt is proven and destination transport is sealed, one exclusive gatherer
takes all destination owners. It rejects missing, extra, duplicate, unsealed, or already consumed
owners and drains their disjoint reducer outputs in partition-ID order. Because hash partitions
cannot share a key, this boundary concatenates rather than re-aggregates. A separate bounded query
resource context is retained for later global projection, sort, limit, and Native encoding.

That global boundary is now proof-bound too. The original mutable fragments re-derive and
exact-compare shuffle authority while supplying the immutable plan, result schema, and input width.
Gathered chunks are canonical-copied from destination resource contexts into the global query
context before the established checked projection, ORDER BY, LIMIT, and atomic Native encoder run.

One heap-stable post-worker lifecycle now owns that whole chain. It exact-validates source and
destination coverage, starts every reducer/listener before transport, delivers local edges,
receipt-schedules all remote edges, keeps ingress live until closure proof, seals, gathers, and
atomically finalizes Native output. Cancellation tears down both sides. At this boundary
independent-process partition-result return remains separate work.

The mutable grouped worker scheduler can now publish complete canonical tablet streams instead of
draining them through its direct coordinator. A second stable owner derives shuffle authority from
the same fragments, schedules local or authenticated remote workers, performs that one-shot source
handoff, and then delegates to the full shuffle lifecycle. Source publication and direct grouped
finalization are mutually exclusive, so the embedding cannot accidentally expose both answers.
At this layer, Native SQL selection and independent-process partition-result return remain separate.

Replicated Native SQL now has an explicit optional selector for that composition. A borrowed
deployment provider creates fresh destination/listener/route configuration for each proof-bound
attempt, while the service retains final projection, protocol output bounds, cancellation,
deadline, and whole-query rebinding authority. With no provider, the direct grouped coordinator
remains the default. Selection never silently falls back after worker or shuffle work begins.
Independent-process result return is still separate.

The first independent-process return boundary is now explicit. `CHDVGRR1` carries one reduced
partition chunk or canonical empty terminal from the authority destination node to an explicit
coordinator. Query, partition, hash version, nodes, sequence, raw grouped schema, Native
descriptors, payload, and complete frame are checked before publication. This is the versioned
result product; authenticated complete-stream sessions and lifecycle composition still remain.

The result stream layer now constructs every partition frame before write publication and assigns
contiguous sequence plus one terminal. Its receiver binds the authenticated peer principal to the
authority destination, locks one partition, withholds all batches until terminal closure, and
discards the whole prefix on drift, gaps, suffixes, or exhaustion. A fixed checksummed reverse-route
result receipt binds the same authority, raw schema, and exact accepted stream extent so a
connected sender need not mistake completed socket writes for coordinator acceptance. The
connected result carrier now drives
both through nonblocking mutual TLS: the reducer authorizes the coordinator before writing, the
coordinator authenticates the reducer before application reads, and one-shot result transfer is
unavailable until the receipt is fully written. TCP ownership, retry, and process lifecycle
composition remain separate. A finite retry owner now retains one immutable partition and reconstructs
byte-identical whole-stream attempts after capped backoff; only a validated receipt ends it in
success. A deadline-bound TCP client proves nonblocking connect completion before transferring that
attempt into mutual TLS, while a bounded server caps accepted descriptors, active sessions, and
per-poll work. The all-remote collector preallocates one slot per authority partition, suppresses
exact retransmissions, rejects conflicting duplicates, and publishes only complete canonical
partition order. A reducer-side scheduler now joins immutable retry to the TCP client: it
prevalidates exact authority/schema identity, unique local partitions, and coordinator routes,
rotates finite addresses only between whole attempts, and bounds polling by retry, carrier, and
query deadlines. Only receipt-proven partitions complete; cancellation or one exhausted partition
closes every active attempt.

Collected Native batches now enter the existing global grouped SQL pipeline through a move-only
materializer. It reconstructs each stream extent before execution, decodes one nonempty batch at a
time, reserves its complete physical footprint from a distinct query resource context, and emits
canonical accounted chunks. Finalization additionally requires the exact raw-schema object owned
by the fragment-derived plan authority, then reuses the established checked projection, global
`ORDER BY`, `LIMIT`, and atomic Native encoder. Focused coverage proves two remote reducer
partitions are globally ordered and limited only after complete collection.

The coordinator-side result lifecycle now composes the bounded listener, idempotent collector,
accounted materializer, deadline/cancellation policy, and atomic finalizer. Its critical ownership
rule is that a stream acknowledged over the network first moves into a pending slot; allocation
failure cannot destroy that slot. Collector admission and materializer construction move caller
bytes only after all fallible work succeeds, so local exhaustion is retryable without asking a
receipt-proven reducer to resend. Exact duplicates remain no-ops. Once all authority partitions are
present, the listener closes and the proof-bound global pipeline publishes one take-once Native
result. This owner is process-memory recovery, not durable query recovery: a coordinator crash
still requires a new query attempt. Reducer scheduling remains process-local to each reducer, and
packaged distinct-process qualification is the next deployment gate.

A standalone Unix qualification now crosses that boundary without pretending to be daemon
packaging. One coordinator executable and two reducer executables independently reconstruct the
same fragment proof, then exchange partition results through real TCP and mutual TLS. One reducer
rotates away from a refused address before receipt success; the coordinator still exposes only the
globally ordered and limited row. A required reducer killed before publication leaves the
coordinator with incomplete coverage, so its deadline cancels without output and a completely new
process set must retry the query. This proves address-space independence and the intended
process-loss boundary. Production `chronosd` role configuration, route discovery, and shared
service polling remain separate.

Reducer-job setup no longer needs to invent an in-memory authority transfer. The versioned
`CHDVGSA1` product carries the complete plan-order source set, canonical destinations, typed keys,
typed aggregate inputs, query identity, and hash version with header and whole-frame integrity.
Decoding applies hard and deployment bounds before rebuilding the same validated immutable
authority. It deliberately excludes result schema, routes, deadlines, and credentials so the proof
remains reusable.

The `CHDVGJC1` reducer-job request now binds that proof to its exact raw grouped result schema,
explicit coordinator and target reducer identities, numeric coordinator result endpoint, and
relative execution timeout. PREPARE owns and revalidates both nested values; SEAL is a canonical
identity-only action for an already admitted job. The timeout begins at successful admission, so
the wire never serializes a process-local steady-clock epoch. Authentication remains an enclosing
carrier responsibility; correlation, finite admission, progress, cancellation, and cleanup are
owned above the codec.

The fixed `CHDVGJR1` response echoes action, query, coordinator, target, and stable status. Only a
successful PREPARE may carry the reducer's live shuffle-listener endpoint; failed admission and all
SEAL responses have one canonical endpoint-free representation. The standalone control session
proves mutual authentication, authorizes the server principal for the target reducer, and requires
exact response correlation before source routing begins.

The request stream reader retains the fixed checksummed header inline and allocates exactly the
declared request only after hard and deployment bounds pass. The fixed response reader remains
allocation-free. Both expose exact consumed bytes so a one-exchange carrier can reject coalesced
suffixes, while move-only cursors own every unwritten request or response byte.

The bounded reducer-job service now owns the process-local lifecycle behind those messages. It
places each decoded PREPARE on stable storage before starting destination reducers, publishes only
a successfully opened shuffle listener, exact-compares duplicate configuration, and accepts local
worker streams alongside authenticated remote streams. SEAL returns `UNAVAILABLE` until all
authority sources close. It then shuts ingress, drains and Native-encodes every locally assigned
partition, and constructs the complete result retry scheduler before acknowledging success.
Receipt-proven result completion, cancellation, and the relative job deadline are polled by the
same thread. Terminal jobs remain idempotently addressable only until that deadline, after which
cleanup releases bounded admission capacity. The committed query-control endpoint now dispatches
the job magic after mutual authentication and polls installed jobs alongside its bounded
connections. The replicated service package owns that optional reducer service before the
listener, requires one shared peer authority, and joins every authorized coordinator node to its
exact borrowed TLS client context before admission. `chronosd` installs those stable routes from
the same canonical peer set used by distributed queries.

Coordinator-side control now has two explicit levels. A single-route acquisition freezes canonical
request bytes, exact-decodes a fresh owned PREPARE, INSTALL_ROUTES, SEAL, or CANCEL for each attempt,
rotates only authorized addresses, and keeps the whole operation under one deadline. Only SEAL may treat a correlated
`UNAVAILABLE` as transient readiness. The reducer-set coordinator starts every PREPARE before a
bounded wait and withholds every remote shuffle route until all destination nodes accept. It
requires an endpoint only when a destination has a nonlocal source. After explicit source closure,
it seals the complete reducer set and enters the existing lossless result coordinator only after
all SEAL responses succeed. Result bytes remain unavailable until every partition is receipt-proven
and the global Native finalizer succeeds.

The packaged continuation inserts one route-install phase between PREPARE and worker scheduling.
Job Control v2 broadcasts the same sorted destination listener map to every reducer; each reducer
exact-checks its own advertised endpoint and resolves the remaining nodes through configured TLS
authority. The production grouped worker is decorated at the source node, so a successful worker
result publishes its complete canonical state stream to a matching job before worker success can
escape. Exact submission retry is idempotent. Local edges enter reducers directly and remote edges
use the finite receipt-gated scheduler. A missing matching job remains a no-op for the direct path.

One whole-query owner now drives PREPARE, route installation, workers, source closure, SEAL, result
collection, and final Native ownership in that order. The Native provider receives routes resolved
from the same committed query snapshot. Remote reducers retain finite authenticated control
acquisitions. A coordinator-owned reducer instead uses the stable packaged in-process job service,
which accepts only an exact local coordinator/target identity. Its partition result exact-validates
authority, schema, canonical Native batches, and equivalent encoded extent before entering the same
all-partition collector as remote results. The wire protocols continue to reject network self
routes. The reducer service mutex serializes the Native query thread's local control/source/result
calls with query-control polling; no contained owner progresses concurrently. Failure or explicit
client cancellation now closes
workers and enters Job Control v3 cancellation for every reducer. The service installs a bounded
expiring tombstone when CANCEL wins the cross-connection race with PREPARE, so a delayed PREPARE
cannot recreate the job after acknowledged cleanup. Unreachable reducers retain their relative
deadline fallback. After route installation, Job Control v4 activates one authenticated relative
coordinator lease at every reducer before workers start. The whole-query owner renews all leases
during worker, SEAL, and result phases and caps its other waits by the next maintenance deadline.
If coordinator progress disappears, each reducer cancels its job after its last acknowledged lease
duration without requiring synchronized clocks. Pre-activation loss retains the original PREPARE
deadline. Standalone multi-process gates now run the production coordinator and shared-endpoint
reducer owners in separate children. They kill the coordinator after acknowledged PREPARE, after
acknowledged route installation but before lease activation, and after activation plus renewal.
The first two cases discard suspended control connections and prove the original execution-deadline
fallback; the third proves relative lease expiry. Two-reducer gates also stop peers at different
PREPARE, route, and activation prefixes before coordinator loss. Each admitted reducer selects its
own execution-deadline or lease cleanup, a reducer that accepted no control retains no abandoned
job, and both retained services then admit, renew, and cancel a fresh query identity together.
The shared production endpoint now also times out authenticated request prefixes before protocol
magic, within the fixed header, and immediately after the validated header without dispatching a
job, then accepts a fresh complete acquisition. The production client withholds a correlated result
when its peer stalls after a valid response prefix. A distinct opt-in Linux process gate now adds
kernel-owned directional black holes after both reducers activate and renew: dropping packets to
one reducer or dropping its response direction forces that reducer's lease expiry, authenticated
cancellation of the reachable peer, and coordinator failure. Removing
the exact port-scoped rule lets both retained reducers activate, renew, and cancel a fresh identity.
The same privileged target now freezes a real TCP/mTLS Job Control session immediately after
authentication and drops its request direction: both sessions time out, the reducer dispatch count
stays zero, and the retained listener accepts a healed lifecycle. A second case stops after PREPARE
admission but before response writing, drops the response direction, and proves the client cannot
publish success. An exact retry after healing succeeds against the retained job as one duplicate
PREPARE, followed by authenticated cancellation and original-execution-deadline reclamation.
These are whole-application-frame black holes; controlled partial encrypted TLS-record interruption
remains distinct. A real-loopback abortive-close gate now sends authenticated
application-frame prefixes at three PREPARE boundaries and one exactly correlated response
boundary, then closes the owning socket with `SO_LINGER(0)` after destroying its TLS borrower. The
production server dispatches no partial request, the production client publishes no partial
response, both fail on the immediate transport event, and the same listener/service completes a
fresh PREPARE, authenticated CANCEL, and execution-deadline reclamation. Controlled partial
encrypted TLS records remain distinct. Another exact gate stops the production server in
`WritingResponse`, after complete PREPARE admission and response encoding but before its first write,
then makes the coordinator reset visible. The server now observes readable peer failure before
writing, fails the abandoned session, retains exactly one job, accepts the same PREPARE as one
duplicate through the same listener, and completes authenticated cancellation plus original-deadline
reclamation. A raw two-leg loopback proxy now preserves the production mutual-TLS endpoints while
capturing their exact encrypted request and response records. At each direction it forwards only two
header bytes, the complete five-byte header plus one ciphertext byte, or every record byte except the
last, then resets the forwarding leg. No request prefix dispatches, no response prefix publishes a
result, exact response retries retain one job, and the same backend listener completes authenticated
cleanup. Handshake-record cuts, multi-record frames, resets racing buffered suffixes, durable job
recovery, delay, duplication, reordering, probabilistic loss, and larger-set campaigns remain open.

### Portable sufficient-state execution boundary

`DistributedVectorGroupedAggregateQueryExecutionV2` now joins the compatible snapshot to the
all-tablet grouped-state coordinator without choosing a carrier. It pins the Manifest epoch and
exact grouped key/aggregate authority, owns a separately bounded decode resource context, and
accepts only complete canonical frame batches identified by one planned tablet. Every frame is
exact-decoded against the pinned authority before coordinator admission.

The complete-batch rule is important: if decode or coordinator admission fails after an earlier
frame was retained, the owner records a sticky worker failure. A missing terminal does the same.
That local failure remains authoritative even if a malformed retry arrives after a prior terminal,
so no retained prefix can be promoted accidentally. Exact byte-identical complete retries remain
idempotent. `finish()` withholds output until all tablet streams close and preserves the
coordinator's retryable resource-exhaustion behavior; only a successful finish enables pull-based
group rows.

This boundary is intentionally transport-free. A scheduler must still construct worker requests,
authenticate responses, own deadlines/retries/cancellation, and feed only complete response
vectors. Keeping that policy outside the owner makes the all-or-none merge and Manifest-pin
lifetime directly testable before a socket implementation exists.

The first carrier step now reuses the exact Fragment-v2 request but gives grouped states their own
`CHDVGRP2` response envelope. One success nests one canonical grouped-state frame and binds it to
exact source/target, query/tablet, status, and optional leader-hint fields. Every decode still
requires the complete key and aggregate authority plus query memory, including failure-only frames.
Header-first reading proves fixed integrity and all allocation-driving lengths before retaining an
exact frame; outer and nested checksums then gate typed decode.

The authenticated receiver now authorizes the claimed source before it binds fresh local grouped
authority. Binding and execution are deliberately separate: the receiver validates the admitted
plan/result shape first, then rejects any worker whose executed authority drifted. It exact-decodes
the entire empty-or-contiguous worker stream under request-local memory and independent count/byte
limits before publishing any response. Worker failures become one correlated payload-free frame;
only unavailable leadership may add an advisory hint.

The matching sender owns one immutable request and a finite whole-attempt retry policy. A response
vector must be completely route-correlated and terminal before the sender canonically reconstructs
its nested frames under owned authority and query memory. Any partial, malformed, or over-limit
vector leaves it waiting without a retained prefix. The enclosing scheduler now supplies TCP
attempt ownership, deadlines, cancellation, multi-address routing, and all-tablet scheduling.

One connected-session owner now carries that immutable attempt through mutual TLS. Both certificate
fingerprints are authenticated before application bytes, and the client authorizes the exact target
node before writing. Complete grouped authority and query resources stay with the client reader;
data-dependent success closes only at the declared group count, while one distinct empty terminal
closes without inventing a group. Any later TLS, integrity, sequence, byte, or deadline failure
clears the private prefix. The server likewise validates the receiver's complete authority-bound
vector before exposing its first response byte. TCP acquisition/listening and all-tablet scheduling
remain outside this connected-session owner but are composed by the enclosing client, server, and
scheduler owners.

The completed outbound scheduler performs global merge, order, limit, and Native encoding before
completion. Replicated service constructors place a proof-preserving preparation boundary in front
of it: acquire metadata barrier coverage, bind one leader-linearizable or correlated bounded-stale
Manifest authority, resolve routes from the same catalog, create the portable execution, and
transfer all finite policies into the scheduler. The resulting owner retains the Manifest pin
through terminal output.

For direct grouped SQL, `create_replicated_distributed_vector_grouped_aggregate_sql_query_v2()`
also constructs the plan rather than trusting one supplied by the embedding. It checks the lowered
table/projection/predicate against configuration, selects the complete committed table placement
set, joins each tablet to its immutable group, exact Manifest recovery schema/source/position, and
the same acquired leader authority, then requires equal catalog/Manifest table cardinality. Only
that complete derived vector can enter compatible binding and route resolution.

The packaged Native service currently binds mutable `TabletState` fragments. A distinct mutable
grouped worker now reacquires one coherent snapshot/schema/placement/group/barrier context,
revalidates the exact fragment publication, runs the shared grouping pipeline, and emits the same
canonical sufficient-state frames as the Manifest/CSEG worker. Its request-local production
adapter implements only the distinct mutable grouped service interface. The authenticated receiver
and finite sender pair exact `CHDMREQ1` applied-head requests with authority-agnostic `CHDVGRP2`
state responses, rebind fresh authority, and publish only a complete canonical stream. They cannot
enter the Fragment-v2 endpoint because that request names a Manifest generation. A connected
mutual-TLS owner now authenticates both peers before application bytes, authorizes the immutable
target before request write, and retains no partial response. A deadline-bound outbound TCP owner
validates authority before connect, proves `SO_ERROR`, and transfers the exact attempt/resources
into TLS. The complementary server bounds listener admission and per-poll accepts, pins every
descriptor/carrier pair, progresses finite deadlines, exposes saturated lifecycle metrics, and
shuts sessions down before its listener. A heap-stable production owner now composes the request-
local TabletState worker, receiver, and server in reverse-safe dependency order; binding and
execution still acquire independent current authority. A distinct portable outbound owner retains
one exact mutable fragment/sender per tablet, shared decode accounting, complete grouped authority,
and the all-tablet coordinator without fabricating a Manifest generation. A bounded TCP scheduler
now prevalidates complete immutable routes and nested wire limits, rotates only within each target's
finite address list, arbitrates retry and query deadlines, cancels all active clients on failure,
and enables merged physical output only after every mutable tablet closes. Native finalization and
scheduler publication now reuse the same authority-revalidating, query-accounted projection,
sort/limit, and all-or-nothing encoder as the Manifest-pinned path. Replicated Native preparation
selects this mutable lifecycle after successful direct grouped lowering. A local serving node uses
the same worker and canonical state validation without constructing a forbidden TCP self-route;
remote fragments use committed routes. Only `NOT_SUPPORTED` from direct lowering selects the
row-backed grouped differential oracle. The packaged shared query-control endpoint exact-decodes the
common mutable request before selecting the row or grouped receiver by plan mode. Grouped replies
retain their distinct envelope and independent frame, byte, nested decode-memory, and completion
checks; remote authority, rows, and grouped sufficient state therefore share the one committed node
endpoint without inferred ports.

## Process qualification boundary

The Linux packaged gate executes this row-backed grouped path in a three-daemon, two-Raft-group
topology with metadata initially led by node 1 and the tablet group by node 2. It checks a computed
nullable STRING key, a Boolean key, COUNT, global ordering, LIMIT, direct no-redirect coordination
across local and authenticated remote authority, abrupt tablet-leader loss, whole-attempt rebinding,
higher-term exact ingest retry, orderly survivor shutdown, persistent initial votes, and retained-root
recovery. Exact per-group local election timeouts make that split deterministic.

This evidence qualifies the controlled packaged split-leader topology, not arbitrary cluster
schedules or a globally atomic cross-group instant. A replicated Native gateway fixture additionally
qualifies the packaged mutable worker, reducer job, route installation, result return, and final SQL
path through real mTLS. The standalone result-return process gate qualifies
distinct reducer/coordinator address spaces, finite route retry, missing-reducer cancellation, and
a fresh whole-query retry. Multi-process real-CSEG scans, network partitions, and skew/scale
measurement remain separate gates.
