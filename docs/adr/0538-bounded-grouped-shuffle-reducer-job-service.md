# ADR 0538: Bounded grouped shuffle reducer-job service

- **Status:** accepted
- **Date:** 2026-08-26
- **Owners:** ChronosDB distributed-query, networking, and service maintainers
- **Extends:** [ADR 0537](0537-grouped-shuffle-reducer-job-control-envelope.md)

## Context

Portable PREPARE/SEAL messages and correlated responses still left the embedding to preserve
authority lifetimes, start reducer listeners before acknowledgment, compare duplicate prepares,
decide when SEAL is safe, encode local reducer output, schedule result retries, and clean up failed
or expired jobs. Implementing those transitions ad hoc in `chronosd` would recreate the same partial-
publication hazards already removed from the shuffle and result components.

## Decision

Add one move-only, single-thread-affine reducer-job service with a fixed maximum retained-job count.
It receives only already decoded control requests plus an already authenticated peer result. It
authorizes the claimed coordinator principal, requires the exact local target node, and owns each
accepted PREPARE on stable heap storage before constructing components that borrow its authority and
raw schema.

PREPARE starts the complete destination reducer execution and any required mTLS shuffle listener
before returning success. The response publishes that live listener endpoint; an all-local source
set has no network listener and therefore returns canonical endpoint absence. An exact duplicate
PREPARE returns the same endpoint. Reuse of a query ID with any different source, destination, key,
aggregate, schema, coordinator route, target, or timeout returns `ALREADY_EXISTS` without replacing
the live job.

The service exposes an explicit local-stream handoff for a co-located worker and polls remote
shuffle ingress under the existing bounded destination owner. SEAL succeeds only after every
authority source has closed every local partition. Before acknowledging SEAL, the service closes
ingress, drains each local partition, encodes nonempty Native batches against the exact raw schema,
constructs one immutable result retry per partition, and starts the deadline-bound result TCP
scheduler. Any failure before that point returns a failure and makes the whole job terminal; no
partial result attempt has started.

Polling progresses destination ingress or result return. Only receipt-proven completion marks a job
complete. Explicit cancellation and the relative PREPARE deadline close both owners. Terminal jobs
remain available for idempotent duplicate control until their deadline, then bounded cleanup
removes them and releases capacity. All configuration, TLS contexts, authenticators, and node
authorization policy are borrowed and outlive the service.

## Consequences

Reducer admission now has one process-local production owner from PREPARE through authenticated
shuffle input and receipt-proven result return. A successful PREPARE never advertises an unopened
listener, and successful SEAL never precedes complete result-scheduler construction.

The service deliberately does not parse sockets or perform the initial TLS handshake. A bounded
header-first mutual-TLS control carrier and integration into the shared daemon query-control
listener remain separate. Completed jobs are in-memory and not durable; coordinator or reducer
process loss still requires a fresh whole-query attempt.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): authority, raw schema, local reducer node,
  coordinator route, ingress, and returned partitions remain one exact owned job.
- [Invariant 11](../architecture/invariants.md): stable job storage outlives every destination and
  result sender that borrows its proof; cancellation follows reverse-safe ownership order.
- [Invariant 15](../architecture/invariants.md): jobs, query memory, retained streams, accepts,
  reducer admissions, result batches, retries, sockets, poll work, and time are finite.
- [Invariant 18](../architecture/invariants.md): SEAL cannot publish before complete source closure,
  and returned partitions use only authority-assigned reducer output.

## Validation plan

Use real loopback TCP and mutual TLS in both directions. Idempotently prepare one reducer job,
reject a conflicting duplicate, deliver one source locally and one through the advertised listener,
seal only after closure, return one reduced partition to a coordinator result server, validate the
exact merged Native count, and require receipt-proven completion. Inject construction and PREPARE
ownership allocation failure. Run full cluster/allocation suites, sanitizers, formatting, static
analysis, and diff gates.

## Migration or rollback considerations

No durable or existing wire bytes change. Rollback removes the unadvertised service owner; a daemon
must reject reducer-job control rather than acknowledge PREPARE without equivalent stable ownership
and listener publication ordering.

## Unresolved questions

- Add bounded request/response readers, cursors, and mutual-TLS control sessions.
- Dispatch the protocol from the shared query-control endpoint and own the service in `chronosd`.
- Qualify multi-daemon source execution, abrupt coordinator/reducer loss, skew, and whole-query
  replacement with production route discovery.

## References

- [Job-control envelope](0537-grouped-shuffle-reducer-job-control-envelope.md)
- [Destination execution](0514-lossless-grouped-shuffle-destination-execution.md)
- [Result retry scheduling](0533-deadline-bound-grouped-result-retry-scheduling.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
