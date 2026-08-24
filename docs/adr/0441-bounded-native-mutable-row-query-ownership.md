# ADR 0441: Bounded Native mutable row query ownership

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB cluster, query, networking, and Native-protocol maintainers
- **Extends:** [ADR 0435](0435-bounded-mutable-vector-query-tcp-scheduling.md),
  [ADR 0379](0379-bounded-global-vector-row-finalization-v2.md)

## Context

The mutable TCP scheduler retained a complete schema-bound all-tablet result behind a const
observer, while global row finalization deliberately consumed that result. An embedding therefore
could not transfer ownership without copying every encoded worker batch or adding privileged access.
It also had to coordinate scheduler terminal state, finalization failure, cancellation, fresh
authority rebinding, and publication of the final Native payloads.

## Decision

`DistributedMutableVectorQueryTcpExecution::take_result` transfers its completed value exactly once
and leaves the scheduler terminal. A second or premature transfer returns `UNAVAILABLE`.

`DistributedMutableVectorRowsQueryTcpExecution` is a move-only, single-threaded request owner. Its
creation consumes one proof-bound fragment vector and TCP route configuration, constructs the
portable multi-tablet owner, and installs it in the finite scheduler. Each poll delegates bounded
socket progress. On all-tablet success it takes the intermediate result exactly once, synchronously
runs bounded global row finalization, and retains the final schema plus Native Protocol v1
`QUERY_RESULT` payloads. Completion is visible only after finalization succeeds. Scheduler,
transport, decoding, sorting, limit, encoding, or allocation failure publishes no result.

Cancellation destroys live clients through the scheduler. A retryable scheduler failure may be
rebound only through the existing logical-identity check with freshly proof-bound fragments and
routes; a finalization failure is not rebindable because the consumed intermediate value cannot be
replayed. Authentication, authorization, and TLS contexts remain explicitly borrowed and must
outlive the request owner.

The internal composite constructor may allocate diagnostic state and is intentionally not
`noexcept`; creation catches allocation and length failures at the outer boundary.

## Consequences

There is now one value owner from immutable fragment admission through final Native row payloads.
The owner adds constant orchestration state to the scheduler and finalizer bounds. Polling remains
`O(tablets)` plus readiness work; successful terminal processing retains the finalizer's
`O(cells + rows log rows * order keys)` bound. One caller thread serializes every method, so no
inter-thread memory-ordering argument applies. No durable or network format changes.

Native request-envelope and reactor integration, authority reacquisition policy, and multi-process
daemon composition remain separate boundaries.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): retries and rebindings cannot rewrite proof-bound
  authority in place.
- [Invariant 6](../architecture/invariants.md): only one complete all-tablet, globally finalized
  result can become visible.
- [Invariant 11](../architecture/invariants.md): intermediate and final bytes have one move-only
  owner; external security contexts are explicit borrows.
- [Invariant 15](../architecture/invariants.md): scheduler and finalizer retain independent finite
  limits for clients, frames, rows, messages, memory, batches, and bytes.
- [Invariant 18](../architecture/invariants.md): any terminal failure suppresses partial Native
  payload publication.

## Validation

A real loopback mutual-TLS test drives the composite owner through a worker, consumes the scheduler
result, and verifies one schema-bearing zero-row Native payload plus terminal metrics. A second run
forces finalization rejection and verifies failed state with no result. Existing split-leader,
address-rotation, cancellation, deadline, and rebind tests remain on the scheduler. The scheduler's
exact-once transfer is tested directly. An allocation sweep covers outer owner construction in
addition to scheduler construction and finalization allocations. Header self-containment and an
installed consumer protect the public API.

## Migration and rollback

The APIs are additive and not yet wired to a Native request envelope. Rollback removes the
composite owner and transfer method without changing fragment, carrier, exchange, or Native bytes.

## References

- [Proof-bound mutable vector query execution](0434-proof-bound-mutable-vector-query-execution.md)
- [Bounded mutable vector query TCP scheduling](0435-bounded-mutable-vector-query-tcp-scheduling.md)
- [Bounded global vector row finalization v2](0379-bounded-global-vector-row-finalization-v2.md)
- [Native Protocol v1](../protocol/native-v1.md)
