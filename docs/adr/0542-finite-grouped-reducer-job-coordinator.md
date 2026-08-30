# ADR 0542: Finite grouped reducer-job coordinator

- **Status:** accepted
- **Date:** 2026-08-30
- **Owners:** ChronosDB distributed-query, cluster transport, and service maintainers
- **Extends:** [ADR 0534](0534-atomic-grouped-result-coordinator-lifecycle.md),
  [ADR 0540](0540-mutually-authenticated-grouped-reducer-job-control-session.md), and
  [ADR 0541](0541-shared-grouped-reducer-job-control-endpoint.md)

## Context

The packaged reducer service and shared authenticated endpoint can accept one correlated PREPARE or
SEAL, while the coordinator result owner can collect and finalize every partition. An embedding
still had to retry control connections, preserve the move-only authority across attempts, decide
when partial PREPARE success could become visible, coordinate SEAL readiness, and join the result
listener to the same query deadline. Publishing the first successful reducer endpoint or accepting
one result early would expose a partial distributed query.

## Decision

Add a finite single-route control acquisition and one move-only, single-thread-affine reducer-set
coordinator. The acquisition validates the complete node, endpoint, TLS, request, timeout, and retry
configuration before connecting. It canonically encodes one immutable PREPARE or SEAL and
exact-decodes a fresh owned request for each attempt because PREPARE contains a move-only shuffle
authority. Attempts rotate only within the route's canonical address vector; query, coordinator,
target, proof, schema, action, and TLS identity cannot change. Transport unavailability, I/O error,
and resource exhaustion retry under capped positive backoff and an optional outer deadline. Only a
correlated SEAL `UNAVAILABLE` response may be retried as reducer-readiness polling. Other
application responses are terminal. TLS state is destroyed before its socket descriptor.

Coordinator construction requires the finalization proof to borrow the exact supplied shuffle
authority. The sorted unique control-route node set must equal the authority destination-node set,
and result-listener identity, authentication, authorization, and deadline must match the control
owner. It starts the bounded result listener and retains one PREPARE acquisition per reducer. One
poll starts and advances every PREPARE before performing a bounded outer wait. No source route is
published until every reducer returns success. A reducer with at least one source on another node
must return a valid shuffle endpoint; an all-local reducer may omit the unnecessary listener and is
excluded from the remote route vector.

After worker source streams have been delivered, the caller invokes one explicit `seal()`
transition. The coordinator creates every SEAL acquisition before exposing progress and retries
only correlated readiness responses. It enters result collection only after every reducer accepts
SEAL, then delegates receipt-preserving partition collection and atomic Native finalization to the
existing result coordinator. Native output is take-once and unavailable before complete global
finalization. Control and result metrics are saturating; PREPARE totals remain committed when SEAL
acquisitions replace the first phase.

Failure, cancellation, or the absolute query deadline cancels every coordinator-owned control
attempt, closes the result listener, clears unpublished routes, and suppresses output. The current
wire protocol has no CANCEL action, so an already admitted reducer that cannot be sealed is released
by its bounded relative execution deadline; this decision does not claim immediate remote
cancellation or durable query recovery.

## Consequences

Coordinator setup, reducer readiness, and independent-process result return now have one finite
all-or-nothing control boundary. Address retry cannot mutate proof authority, partial PREPARE
success cannot leak a route, and receipt-proven partition results cannot leak a partial Native
answer. All-local execution avoids opening an unused shuffle listener.

[ADR 0543](0543-packaged-grouped-shuffle-job-lifecycle.md) now constructs source plans on their
authenticated worker nodes and installs the coordinator in packaged Native SQL for gateway
topologies. Immediate remote cancellation still requires a separately versioned wire action. A
coordinator crash still retries the whole query with fresh in-memory jobs.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): authority, routes, reducer control, returned
  endpoints, result schema, and final output remain one correlated query product.
- [Invariant 11](../architecture/invariants.md): move-only attempts, TLS/socket teardown, borrowed
  proof/security objects, and result-listener lifetime have one explicit owner.
- [Invariant 13](../architecture/invariants.md): no reducer route or result becomes observable from
  an incomplete destination set.
- [Invariant 15](../architecture/invariants.md): reducers, endpoints, attempts, backoff, connection,
  exchange, result resources, per-poll work, and total time are independently finite.
- [Invariant 18](../architecture/invariants.md): only authenticated correlated control success and
  complete receipt-proven partition coverage authorize Native publication.

## Validation plan

Use the real shared query-control TCP listener, reducer service, result sender, result coordinator,
and mutual TLS. Prove PREPARE, local stream delivery, SEAL, partition return, global finalization,
and take-once Native output. Hold a second reducer connection before TLS while another reducer
accepts PREPARE; require terminal failure with zero published routes and zero output. Prove route
coverage and exact-proof rejection, refused-address rotation, finite SEAL-readiness retries,
construction allocation classification, header self-containment, full cluster and allocation
suites, ASan/UBSan, formatting, static analysis, and final diff review.

The warning-as-error build and all 346 cluster tests plus 76 cluster allocation-failure tests pass.
Five focused lifecycle/retry tests and all five job-control allocation tests pass under ASan/UBSan
with macOS leak detection disabled for the documented runtime limitation. Repository formatting,
workflow-action pins, and whitespace checks pass. LLVM 18 static analysis reaches only the known
macOS 26 libc++ unsupported-builtin errors after all ChronosDB-source findings were corrected.

## Migration or rollback considerations

No durable or network bytes change. Rollback removes the coordinator and finite control acquisition
while retaining the single-attempt TCP client, shared endpoint, reducer service, and independent
result coordinator. Any replacement embedding must preserve whole-set PREPARE/SEAL publication and
the immutable-attempt retry contract.

## Unresolved questions

- Add an explicitly versioned authenticated remote CANCEL action if deadline cleanup is
  insufficient operationally.
- Qualify the complete lifecycle across packaged independent daemons under process and network
  faults.

## References

- [Grouped reducer-job control format](../formats/distributed-vector-grouped-aggregate-shuffle-job-control-v1.md)
- [Packaged grouped shuffle job lifecycle](0543-packaged-grouped-shuffle-job-lifecycle.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
