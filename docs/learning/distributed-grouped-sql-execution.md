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
entry point for direct pre-group SQL. It owns the unique source projection, remapped direct group
keys and aggregate inputs, exact event-time range, and the raw key/aggregate schema consumed by the
grouped sufficient-state scheduler. For non-identity SELECT lists it also owns checked final
expressions, the client schema, and projected global ordering and limit. It deliberately returns
`NOT_SUPPORTED` for computed group keys, computed aggregate inputs, non-event-time predicates, and
hidden order expressions so the caller can select the row-backed plan explicitly.

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
projection, sort, and limit stages. Computed pre-group expressions still need an owned worker-plan
split, and partitioned shuffle routing plus broader fault/measurement evidence remain open. The
row-backed path remains the differential oracle for that work.

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
and the all-tablet coordinator without fabricating a Manifest generation. TCP polling and Native
finalization remain before Native can choose this path. Until then, row-backed grouped execution
remains the packaged differential oracle.

## Process qualification boundary

The Linux packaged gate executes this row-backed grouped path in a three-daemon, two-Raft-group
topology before and after abrupt common-leader loss. It checks a computed nullable STRING key, a
Boolean key, COUNT, global ordering, LIMIT, safe follower redirect, replacement election, exact
ingest retry, orderly survivor shutdown, and retained-root recovery. Distinct fixed local election
timeouts make both groups choose the same leader for this deterministic qualification.

That evidence does not prove arbitrary split-leader process coordination because it deliberately
elects one common leader. The Native coordinator now combines local and authenticated remote
per-group authority in focused in-process coverage, but Linux multi-daemon split-leader
qualification remains separate. Multi-process real-CSEG scans and a multi-key/all-type
sufficient-state shuffle also remain separate gates.
