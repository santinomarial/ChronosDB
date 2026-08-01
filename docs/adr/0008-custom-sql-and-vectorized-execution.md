# ADR 0008: Custom SQL and Vectorized Execution

- **Status:** accepted
- **Date:** 2026-08-01
- **Owners:** ChronosDB query-engine maintainers

## Context

ChronosDB's query semantics include event-time windows, ASOF relationships, current versus system-time versions, snapshot-to-live continuation, columnar heads, CSEG pruning, and later tablet-local execution. Delegating planning or execution to an embedded database would make those semantics depend on an engine whose storage, visibility, and live-state contracts ChronosDB does not control.

At the same time, implementing optimized operators without a simpler oracle would make semantic bugs difficult to distinguish from vectorization, scheduling, or pruning bugs.

## Accepted decision

ChronosDB owns its SQL tokenizer, parser, binder, logical planner, physical planner, optimizer, and execution engine. The initial SQL subset is deliberately bounded by documented workload needs and fails clearly for unsupported syntax or semantics.

A scalar reference executor is implemented before the vectorized executor. It prioritizes direct, auditable semantics over speed and serves as a differential correctness oracle.

Vectorized execution operates on column-oriented chunks with explicit validity and selection vectors. Operators avoid per-row allocation on data paths. Projection and predicate pushdown occur before decoding unnecessary columns or pages where practical, but pruning metadata can never be required for truth.

Parallel scans divide work into bounded morsels scheduled under memory and cancellation limits; they do not create one thread per part. Later distributed execution pushes scans and eligible partial aggregations to tablets while preserving the same bound plan, types, snapshots, and result semantics.

JIT compilation is deferred until the vector engine is stable and reproducible profiles show an end-to-end benefit. DuckDB or PostgreSQL may be used as differential references for the intersection of supported semantics, never as the hidden executor or authoritative oracle for ChronosDB-specific temporal/live behavior.

## Detailed rationale

Owning the stack lets binding use stable catalog identities, planning reason about CSEG/head visibility, and execution share expression semantics between historical and live operators. Vector chunks amortize interpretation and function overhead and fit column projection, but only evidence will determine widths, layouts, and operator specializations.

The scalar path makes expected semantics executable early. Comparing scalar and vector results across randomized plans catches batch-boundary, selection, null, decimal, and scheduling errors that unit examples miss.

## Alternatives considered

- **Volcano tuple-at-a-time execution only:** is useful as the scalar reference but leaves substantial call, branch, and row-materialization overhead on analytical scans; it is not the final performance path.
- **Embed DuckDB:** would accelerate SQL breadth, but ChronosDB would surrender planner/executor ownership and force custom storage, system-time, and live semantics through an external engine boundary.
- **JIT-first execution:** adds compiler/runtime complexity before stable operator semantics or evidence that compilation latency and generated code improve representative workloads.
- **A non-SQL language as the only interface:** could express streams directly but would abandon familiar relational tooling and require users to learn a novel language for historical analysis.
- **Full SQL standard from the start:** increases grammar and semantic surface before core storage/query correctness is established.

## Consequences

- SQL breadth arrives gradually and is documented by supported semantic contracts.
- ChronosDB bears long-term ownership of parsing, typing, optimization, diagnostics, and execution.
- Every vectorized operator needs scalar differential coverage or another explicit oracle.
- Pushdown, parallelism, and future distributed fragments must retain snapshot and system-time semantics.
- External engines remain test dependencies only and cannot define unsupported ChronosDB behavior.

## Affected invariants

This decision supports invariants [6, 7, 11, 13, 17, and 18](../architecture/invariants.md): stable snapshots, compaction-equivalent results, lifetime-safe cancellation, temporal semantics, gap-free handoff, and optimization without semantic weakening.

## Validation plan

- Fuzz tokenization/parsing and property-test parse/format or AST invariants where defined.
- Generate small typed databases and compare scalar results with a formal/reference model.
- Differentially execute randomized physical plans through scalar and vector engines, varying chunk/morsel boundaries and selection density.
- Compare the supported conventional SQL intersection with DuckDB or PostgreSQL while separately testing ChronosDB-specific semantics.
- Inject cancellation, memory exhaustion, spill, and scheduler interleavings; verify pins and resources are released.
- Benchmark with profiles that record decoding, operator, scheduling, memory, and I/O costs before accepting optimizations or JIT work.

## Deferred decisions

Grammar, type coercion, decimal precision, null/NaN ordering, exact window and ASOF syntax, plan representation, chunk width, selection encoding, optimizer rules, join algorithms, scheduler, spill format, memory admission, distributed exchange protocol, and JIT technology remain deferred.

## Migration or reversal implications

SQL semantics become user contracts once released and require versioned compatibility or explicit migration. Internal plan and chunk representations may evolve before they become external formats. Replacing the custom engine with an embedded executor would reverse the project's core scope and requires a superseding ADR.

## References

- [Architecture query engine](../architecture/overview.md)
- [Representative workload SQL](../product/workloads.md)
- [Roadmap phases 8 and 9](../roadmap.md)
