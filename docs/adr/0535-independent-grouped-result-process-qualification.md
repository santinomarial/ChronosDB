# ADR 0535: Independent grouped result process qualification

- **Status:** accepted
- **Date:** 2026-08-26
- **Owners:** ChronosDB distributed-query, networking, and integration-test maintainers
- **Extends:** [ADR 0534](0534-atomic-grouped-result-coordinator-lifecycle.md)

## Context

Focused tests exercised reducer scheduling and coordinator finalization in one address space. They
could not prove that independently reconstructed proof objects produce compatible wire authority,
that process-local ownership assumptions survive `exec`, or that one missing reducer process never
allows a partial global answer.

## Decision

Add a Unix process-qualification child with three explicit roles: coordinator, reducer, and stalled
reducer. Each coordinator or reducer invocation independently reconstructs the same two-fragment
mutable query proof and therefore owns distinct authority and schema objects. The coordinator
starts the bounded result lifecycle on a real loopback TCP listener. Each reducer constructs its own
immutable partition retry and drives the deadline-bound TCP/mutual-TLS scheduler until the exact
receipt succeeds.

Add a parent GoogleTest owner that launches each role with `fork` followed by `exec`, captures
stdout and stderr through close-on-exec pipes, applies finite line and process-exit waits, and kills
remaining children in reverse-safe destruction. The success case requires one reducer to rotate
from a refused address, both reducer processes to report receipt-proven completion, and the
coordinator process to publish only the globally ordered and limited `west, 2` result.

The loss case starts one real reducer and one authority-valid stalled reducer, kills the stalled
process abruptly, and requires the coordinator deadline to expire without any result. It then runs
a completely fresh coordinator and reducer process set and requires success. This matches the
in-memory receipt contract: reducer retry repairs transport loss within a live query, but a process
loss that prevents complete partition coverage requires a whole new query attempt.

This is a standalone component qualification executable, not yet a `chronosd` endpoint. It proves
the process boundary before adding daemon configuration, shared service polling, or production
route discovery.

## Consequences

The reduced-partition protocol, TLS identities, retry rotation, collector closure, and global SQL
finalization now have executable evidence across three independent processes. Missing partition
coverage remains unavailable through deadline cancellation, and no partial row is emitted.

The test child intentionally contains deterministic proof and row fixtures. It is not a second
production daemon and must not become an alternate deployment surface.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): independently reconstructed query, partition,
  schema, and node authority must correlate across the wire before finalization.
- [Invariant 11](../architecture/invariants.md): every process, pipe, socket, TLS carrier, retained
  stream, and final result has one explicit owner and finite teardown.
- [Invariant 15](../architecture/invariants.md): child startup, line reads, process exit, carrier
  work, retries, and the whole coordinator query have finite deadlines and capacity.
- [Invariant 18](../architecture/invariants.md): process separation and address retry preserve the
  same complete-set and atomic-result semantics as focused execution.

## Validation plan

Build and run the Unix process test under the configured address and undefined-behavior sanitizers.
Require deterministic attempt/retry counts, mutual-TLS success, exact final values, abrupt stalled
reducer loss, coordinator cancellation, and a successful fresh attempt. Run the cluster and
allocation-failure suites to protect the component owners, then run formatting, static-analysis,
and diff gates.

## Migration or rollback considerations

No production, durable, or wire behavior changes. Rollback removes only qualification targets and
evidence; it must not be used to claim that independent-process support is absent from the
underlying result components.

## Unresolved questions

- Package coordinator result listening and reducer result scheduling into the committed daemon
  query-control lifecycle with production identities and route discovery.
- Qualify abrupt coordinator loss, packet loss/duplication, skew, and process replacement in the
  packaged topology.

## References

- [Coordinator lifecycle decision](0534-atomic-grouped-result-coordinator-lifecycle.md)
- [Result scheduler decision](0533-deadline-bound-grouped-result-retry-scheduling.md)
- [Three-daemon query qualification](0460-three-daemon-mutable-sql-failover-qualification.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
