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
frame now exists, and the local grouped hash/equality/state table now exposes stable borrowed groups
to its synchronous encoder without a second grouping oracle. That same query-accounted table now
coalesces decoded equal-key states and finalizes rows; every logical type, signed zero, and NaN share
the physical-row identity rules. Workers still need pre-group physical plan execution and stream
construction. Coordinators still need canonical all-tablet attempt/order/closure arbitration,
partition authority, bounded shuffle/skew policy, authenticated transport, and fault evidence. The
row-backed path remains the differential oracle for that work.

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
