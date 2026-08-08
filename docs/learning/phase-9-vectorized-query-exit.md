# Phase 9 Vectorized Query Exit

## Accepted boundary

Phase 9 is complete for the SQL surface that `lower_bound_sql_select` and
`lower_bound_sql_asof_select` accept. That boundary includes checked scalar preparation, WHERE,
LATEST BY, global and grouped aggregation, left-deep ASOF, exact ORDER BY, hidden identity removal,
LIMIT, complete append-only tablet snapshot sources, bounded serial/parallel source composition,
and in-memory or checksummed external sort.

Completion does not broaden SQL. Unsupported expressions, missing generated logical identity,
correction/delete winner resolution, tablet morsel splitting, mapped/asynchronous storage,
adaptive plans, joins other than accepted ASOF, and distributed fragments still fail explicitly or
remain later-phase work. In particular, correction/delete resolution cannot be implemented until
its operation encoding and authoritative visibility rule are accepted.

## End-to-end differential model

`PhysicalPlanDifferentialPropertyTest` is the cross-layer exit oracle. A fixed seed creates 192
plans from eight supported compositions while changing all source values, duplicate event times,
NULL strings, logical groups, WAL commit positions, row ordinals, and input batch widths from one
row through the entire input. Every case parses and binds once, then runs:

1. the Phase 8 scalar engine against one immutable `ScalarTableSnapshot`; and
2. the lowered Phase 9 vector pipeline against equivalent canonical chunks.

The matrix includes base projection/filter/LIMIT, alias and non-projected ORDER BY, global and
grouped aggregates, variable-width extrema, LATEST BY, ASOF LEFT, post-ASOF grouping, NULL
placement, deterministic ties, and hidden-column removal. It compares every result type and scalar
storage value in exact row order. A separate overflow case requires both engines to fail and proves
that vector cancellation releases every queued batch reservation.

This suite complements, rather than replaces, operator-specific independent models. Snapshot tests
cover durable/sealed/active publication composition and old-epoch pinning; row-version, LATEST,
ORDER BY, and ASOF tests cover system-time winner identities; spill tests use an independent sorted
model and corruption injection; scheduler tests control publication interleavings.

## Ownership, cancellation, and limits

All physical inputs and outputs remain owned by one `QueryResourceContext`. Blocking operators
reserve finite state before retention, external sort has explicit disk/run/record quotas, and
parallel publication has finite workers/tasks/queue bytes. A runtime error requests cancellation;
operator destruction joins workers, removes temporary runs, releases snapshot pins, and returns
query credit. Exhaustive allocation sweeps cover every owned allocation introduced by each
increment, including optimized snapshot construction.

Authoritative optimizer bounds select an implementation but never bypass runtime limits. The
selected strategy owns its exact checked pipeline, so a decision cannot be replayed against another
stage graph.

## Reproducible measurement

`chronos_query_benchmarks` contains operator, lowering, scheduler, spill, optimizer, snapshot, and
end-to-end plan modes. `execute_bound_grouped_vector_plan` holds 4,096 input rows and group
cardinality constant while varying batch width across 1, 16, 256, and the accepted 2,048-row chunk
limit. It reports CPU and wall time through Google Benchmark plus input rows, batch width, maximum
query-credit peak, and
measured p50/p95/p99 execution latency. `execute_grouped_scalar_reference` profiles the same bound
SQL and rows through the oracle. Spill modes report actual bytes read/written; scheduler modes vary
one to four tasks/workers; snapshot modes include plan/load/compose and execution.

Reproduce the focused exit profile with:

```sh
cmake -S . -B build/benchmark -DCHRONOS_BUILD_BENCHMARKS=ON
cmake --build build/benchmark --target chronos_query_benchmarks
./build/benchmark/chronos_query_benchmarks \
  --benchmark_filter='execute_bound_grouped_vector_plan|execute_grouped_scalar_reference' \
  --benchmark_repetitions=7 --benchmark_report_aggregates_only=true
```

Results are host/build observations, not portable performance guarantees. Optimizations require a
new profile plus identical differential, failure, sanitizer, and ownership evidence.

## Review questions

**Why is a fixed seed still randomized evidence?** The generator explores changing data, plans,
ties, NULLs, and batch boundaries reproducibly. Failures can be replayed exactly, while fuzzers
continue non-deterministic hostile exploration.

**Why can Phase 9 exit with deferred correction/delete resolution?** The accepted current storage
boundary is append-only. Inventing an operation encoding or winner rule would weaken correctness;
the roadmap explicitly assigns that contract before its implementation.

**Does parallel merge establish SQL order?** No. It is selectable only under an explicit complete-
pipeline order-independence proof. SQL order always comes from complete physical sort keys.

**Can optimizer statistics authorize excess memory or disk?** No. They are finite selection upper
bounds. Operators independently recheck chunks, rows, records, retained bytes, and spill bytes.
