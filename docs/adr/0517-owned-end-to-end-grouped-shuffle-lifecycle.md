# ADR 0517: Owned end-to-end grouped shuffle lifecycle

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB distributed-query, network, and Native protocol maintainers
- **Extends:** [ADR 0512](0512-atomic-grouped-shuffle-source-fanout.md),
  [ADR 0513](0513-bounded-grouped-shuffle-remote-edge-scheduling.md),
  [ADR 0514](0514-lossless-grouped-shuffle-destination-execution.md), and
  [ADR 0516](0516-proof-bound-global-grouped-shuffle-finalization.md)

## Context

Every grouped-shuffle phase existed behind a separate move-only interface, but callers still had
to preserve their borrowing order and manually decide when remote receipts authorized destination
sealing and finalization. An omitted source or destination, early listener shutdown, or authority
object replacement could make individually valid components describe an incomplete query.

## Decision

Add one heap-stable, single-thread-affine post-worker lifecycle owner. Construction consumes the
complete shuffle authority, proof-bearing mutable fragments, exactly one canonical source stream
per authority tablet, and exactly one destination configuration per authority node. It then:

1. derives and retains the exact finalization authority from the owned shuffle authority;
2. validates complete, duplicate-free source and destination coverage before publication;
3. starts every destination reducer/listener before source transport;
4. atomically partitions every source, delivers self-routes in process, and combines all remote
   edges under the existing finite TCP/mTLS receipt scheduler;
5. keeps destination ingress live until every remote receipt succeeds and every reducer is ready;
6. seals all destinations, transfers them exclusively into the authority-ordered gatherer, and
   runs checked projection, global order, limit, and Native encoding; and
7. exposes no result until the complete Native result is owned.

Cancellation tears down remote clients and destination servers. Retryable destination polling
allocation failure leaves the owner running; a failure after destinations transfer into final
gathering is terminal because consumed input cannot be replayed safely. Source planning, reducer,
result, and Native limits remain caller-configured and finite.

All calls are serialized by one caller thread. The owner publishes no shared state, so no atomic
memory ordering is introduced. TLS contexts, authenticators, authorizers, and route policy are
borrowed and must outlive the owner; authority and all execution objects are owned in dependency
order inside its stable implementation.

## Consequences

The partitioned sufficient-state path now has one executable boundary from complete worker streams
through local/remote shuffle and atomic Native output. Two serving nodes can exchange opposite
edges over authenticated loopback TLS within this owner and finalize one complete result.

[ADR 0518](0518-worker-to-shuffle-grouped-execution.md) subsequently composes mutable worker
execution with this owner. Native SQL selection remains separate. Destination results also remain
coordinator-process objects; running destination reducers in independent processes requires a
distinct authenticated result-return protocol.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): fragments, sources, destinations, shuffle
  authority, finalization authority, and result are one exact owned product.
- [Invariant 11](../architecture/invariants.md): heap-stable ownership preserves every borrowed
  authority and security dependency for the complete lifecycle.
- [Invariant 13](../architecture/invariants.md): every tablet contributes one complete stream to
  every partition before global output.
- [Invariant 15](../architecture/invariants.md): source fan-out, remote edges, reducer admission,
  result working memory, and Native output retain explicit bounds.
- [Invariant 18](../architecture/invariants.md): sealing and result publication occur only after
  authenticated receipts and complete reducer closure.

## Validation plan

Focused tests cover local fan-out through Native decoding, incomplete coverage rejection,
cancellation, and two-node bidirectional remote edges with mutual-TLS fingerprints and exact
receipts. Allocation injection covers every construction and synchronous finalization allocation,
including the implementation diagnostic-state constructor. Header self-containment,
warning-as-error ASan/UBSan suites, formatting, changed-source static analysis, and final diff
review are required.

The warning-as-error normal build, all 299 cluster tests, all 54 cluster allocation-failure tests,
all 427 query tests, and all 63 query allocation-failure tests pass. The three focused lifecycle
tests and construction/finalization allocation sweep pass under ASan/UBSan with leak detection
disabled for the macOS runtime. Changed C++ files pass LLVM 18 formatting. Changed-source
clang-tidy reaches only the known LLVM 18/macOS 26 libc++ builtin incompatibility after project
findings were corrected. Repository-wide formatting and final diff scope are checked separately.
The repository-wide formatting check reports only the unchanged
`distributed_vector_grouped_aggregate_query_tls_v2_header_self_contained.cpp` violation.

## Migration or rollback considerations

No durable or wire bytes change. Rollback removes only the lifecycle owner and returns composition
to callers; all component protocols and execution types remain independently usable.

## Unresolved questions

- Select the worker-to-shuffle composition from replicated Native SQL execution.
- Carry completed destination partition results back from independent processes.
- Add process-loss, result-return retry/deduplication, skew, and scale-out measurements.
- Carry computed pre-group expressions in an owned, versioned worker program.

## References

- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
