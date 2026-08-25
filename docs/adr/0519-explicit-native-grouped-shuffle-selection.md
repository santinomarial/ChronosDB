# ADR 0519: Explicit Native grouped-shuffle selection

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB service, distributed-query, and deployment maintainers
- **Extends:** [ADR 0518](0518-worker-to-shuffle-grouped-execution.md)

## Context

The worker-to-shuffle owner was executable but the replicated Native service still always selected
the original direct grouped coordinator. Shuffle destination listeners, client routes, TLS
contexts, reducer resources, and limits are per-query deployment decisions; placing move-only
instances in the service's reusable static configuration would either consume them once or retain
stale query authority.

## Decision

Add an optional borrowed Native grouped-shuffle provider. For each eligible direct-input grouped
query attempt, the service supplies the complete proof-bound mutable fragments, key and aggregate
definitions, and the shared execution deadline. The provider returns fresh destination/shuffle
execution configuration plus authority limits. Borrowed security and resource dependencies returned
by it must outlive the synchronous query call.

When the provider is configured, the service:

1. reuses the existing proof acquisition, SQL lowering, fragment preparation, worker routes, local
   worker selection, cancellation, deadline, and authority-rebinding policy;
2. overwrites final projection and Native output bounds from the lowered SQL and protocol service
   limits, and propagates the shared deadline into remote shuffle transport;
3. runs the worker-to-shuffle lifecycle and publishes only its one-shot Native result; and
4. recreates both deployment plan and lifecycle after an allowed whole-query authority rebind.

Without a provider, the direct grouped Native path remains the default. A provider failure is a
query failure; the service does not silently fall back after selecting the shuffle path because
that could repeat worker effects under a different execution policy.

The provider call is synchronous and serialized with query execution. It must not retain the input
spans or block beyond the supplied deadline. This adds no inter-thread algorithm or memory-ordering
requirement.

## Consequences

Replicated Native SQL can now explicitly select the complete path from proof-bound mutable workers
through partitioned shuffle and atomic protocol output. Deployment owns listener and route policy,
while SQL/protocol authority remains service-owned. A local two-tablet differential test selects
the provider path and produces the exact same Native bytes as the established direct path.

This does not yet put destination reducers in independent database processes. The provider may
currently compose destinations in the coordinator process; authenticated cross-process result
return remains required before that deployment is complete.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): service lowering, fragments, provider plan, worker
  execution, shuffle authority, and final Native result remain one exact query product.
- [Invariant 11](../architecture/invariants.md): provider and returned borrowed dependencies have an
  explicit synchronous lifetime.
- [Invariant 15](../architecture/invariants.md): service output bounds override deployment values;
  all nested worker/shuffle bounds remain finite.
- [Invariant 18](../architecture/invariants.md): explicit selection publishes no direct-path or
  shuffle-path prefix before complete Native encoding.

## Validation plan

The service integration test runs real replicated local grouped workers for two tablets, invokes a
fresh local shuffle plan, finalizes through Native, and compares row count, payload bytes, and exact
response bytes with the direct distributed result. It also verifies the provider is invoked only
for the eligible grouped query. Composite one-shot result transfer, cancellation, allocation
injection, header self-containment, full regression suites, ASan/UBSan, formatting, changed-source
static analysis, and final diff review remain required.

The warning-as-error build and all 302 cluster, 55 cluster allocation-failure, and 109 service tests
pass; the unchanged 427 query and 63 query allocation-failure suites passed at the immediately prior
worker-composition gate. The focused composite and replicated Native differential tests pass under
ASan/UBSan with leak detection disabled for the macOS runtime. Changed C++ files pass LLVM 18
formatting. Clang-tidy reports no new diagnostic on changed lines before the known LLVM 18/macOS 26
libc++ builtin incompatibility; whole-file service analysis also repeats pre-existing findings
outside these hunks. The repository-wide formatter retains only the unchanged grouped TLS v2
header self-containment violation.

## Migration or rollback considerations

The provider pointer defaults to null, preserving every existing service deployment. Rollback
removes the optional selection seam and service branch while leaving the worker-to-shuffle owner
available to other embeddings. No durable or network format changes.

## Unresolved questions

- Carry completed destination partitions back from independent processes.
- Define daemon configuration and admission policy for production shuffle listeners.
- Carry computed pre-group expressions in an owned, versioned worker program.
- Add process-loss, skew, scale-out, and multi-process differential qualification.

## References

- [Worker-to-shuffle grouped execution](0518-worker-to-shuffle-grouped-execution.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
