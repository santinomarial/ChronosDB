# ADR 0346: Explicit grouped whole-query rebinding

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, cluster, and networking maintainers
- **Extends:** [ADR 0180](0180-explicit-whole-query-authority-rebinding.md),
  [ADR 0342](0342-pinned-grouped-query-tcp-scheduling.md)

## Context

The grouped TCP scheduler retained one proof-bound Manifest epoch and could receive an advisory
leader hint after a retryable failure, but it could not install fresh caller-proved authority.
Retargeting only the failed tablet would mix the replacement authority with grouped partials already
accepted from the old epoch. Recreating the owner externally would not structurally enforce logical
query identity, generation monotonicity, the original deadline, or a finite replacement budget.

## Decision

`DistributedGroupedQueryTcpExecution::rebind` replaces the complete failed grouped execution only
after terminal `UNAVAILABLE`, `RESOURCE_EXHAUSTED`, or `IO_ERROR`. The caller must construct a new
`DistributedGroupedQueryExecution` from fresh metadata, read admission, placement, schema, Manifest,
and route authority. Advisory hints remain nonauthoritative.

Compatibility exact-compares every plan-ordered grouped dispatch: query, database, table, tablet,
Raft group, destination schema, read policy, projection, aggregate input, event-time predicate, and
group-key input index. Tablet order and database ownership cannot change, and the replacement
Manifest generation cannot regress. Authority fields validated by the replacement binder may
change.

The replacement discards all prior coordinator partials, senders, sockets, and the old Manifest pin.
It preserves the original absolute deadline and configured rebinding limit, rejects rebinding while
running or after success/cancellation/nonretryable failure, and permits at most 1,024 configured
rebindings. Attempt, retry, transport, and rebinding metrics remain cumulative.

## Consequences and validation

Validation is `O(fragments * projection width)` and performs no I/O or durable mutation. Each
accepted replacement restarts every tablet, intentionally preferring one coherent snapshot over
retaining old work. The scheduler remains single-threaded, so no inter-thread memory-ordering
argument applies. No durable or network format changes.

The focused real-mTLS test accepts one old grouped partial with value 100, then receives a retryable
failure from the peer. It rejects a replacement with a different query identity, accepts a compatible
replacement, and returns count two and sum six rather than 106 while cumulative metrics prove four
completed transports and one whole-query rebind. Header self-containment and the installed consumer
cover the public boundary.

Automatic metadata refresh, general vector fragments, multi-key/non-FLOAT64 grouping, real
multi-process failover, and broad fault/measurement evidence remain incomplete. No Phase 16 exit
gate is claimed.

Invariants 5, 6, 11, 15, and 18 apply.

## References

- [Explicit whole-query authority rebinding](0180-explicit-whole-query-authority-rebinding.md)
- [Pinned grouped-query TCP scheduling](0342-pinned-grouped-query-tcp-scheduling.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
- [Architecture invariants](../architecture/invariants.md)
