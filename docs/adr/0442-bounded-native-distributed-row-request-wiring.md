# ADR 0442: Bounded Native distributed row request wiring

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB service, cluster, query, and Native-protocol maintainers
- **Extends:** [ADR 0441](0441-bounded-native-mutable-row-query-ownership.md),
  [ADR 0440](0440-correlated-sql-mutable-query-preparation.md)

## Context

The replicated Native service already parsed and bound SQL after a read gate, while the distributed
row owner accepted a fully correlated fragment-and-route package and retained terminal Native
payloads. No request boundary joined those owners. Embeddings therefore could not execute one
Native `QUERY_REQUEST` across split tablet leaders without reconstructing internal authority or
copying finalized result bytes.

## Decision

`NativeProtocolService` gains an optional borrowed
`NativeDistributedMutableVectorRowsQueryConfig`. When installed, a replicated `SELECT`:

1. acquires and retains current-term group read authorities and the matching replicated snapshot;
2. binds the SQL against that snapshot and lowers the supported direct-column row subset;
3. generates a non-nil query identity and asks the snapshot to correlate all canonical tablets,
   resident publications, leader barriers, placements, group bindings, and TLS routes;
4. constructs the bounded mutable-row TCP owner, drives it until its finite deadline, transfers its
   final result exactly once, and moves the encoded batches into the original request route; and
5. publishes only a complete `QUERY_RESULT*`, `QUERY_END` sequence or one terminal error.

The configured source node is a coordinator transport identity and must differ from every routed
worker. The config, authenticator, authorizer, TLS-context span, and referenced TLS client contexts
are borrowed and must outlive the service. Service result bounds clamp finalization bounds. Invalid
top-level identity and timing configuration fails before read acquisition; nested execution and
route limits are validated by their owning layers. Unsupported distributed SQL fails closed instead
of falling back to a local scan, and preliminary single-group redirects are disabled for this path.

The current Native service call is synchronous. It polls in bounded intervals but does not consume
a later `CANCEL` request while the call is active. The whole execution deadline is finite. Authority
rebinding is disabled because this boundary has no policy for reacquiring and recorrelating every
group after a retryable failure.

## Consequences

One request now preserves the Native connection, principal, request, and negotiated-protocol route
from ingress to terminal output while the distributed owner preserves proof-bound data-plane
authority. No partial worker or finalized payload is exposed. Response assembly moves encoded
batches and adds `O(batches)` envelope storage; query execution retains the existing bounded
network, row, ordering, and encoding costs. The service remains thread-affine, so no new
inter-thread memory-ordering argument applies. No durable or network format changes.

Production database-to-worker context provision, a co-located local-fragment execution boundary,
reactor-aware cancellation, fresh all-group rebinding, daemon configuration, and multi-process
split-leader query qualification remain separate work.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): the request retains correlated read authority and
  does not rewrite it after a transport failure.
- [Invariant 6](../architecture/invariants.md): only one complete globally finalized result becomes
  visible.
- [Invariant 11](../architecture/invariants.md): request/result bytes have move-only owners and
external security policy is an explicit lifetime borrow.
- [Invariant 15](../architecture/invariants.md): SQL, route, carrier, execution, finalization, and
Native response bounds all remain finite.
- [Invariant 18](../architecture/invariants.md): terminal failure produces no partial result stream.

## Validation

A replicated two-tablet integration test starts the production mutable worker TCP server behind
mutual TLS, executes a real projected/filtered/ordered/limited Native SQL request through the new
service constructor, and verifies the decoded schema and row plus terminal framing. Header
self-containment and installed-consumer compilation protect the public API. Adjacent owner tests
cover exact-once transfer, split leaders, deadlines, cancellation, route rotation, and allocation
failure. The milestone gate runs the full service and cluster suites, deterministic allocation
failure coverage, ASan/UBSan builds and suites, installed-consumer compilation, and formatting.

## Migration and rollback

The constructors and configuration are additive. Existing replicated services retain local query
behavior and redirect selection when no distributed config is installed. Rollback removes the
optional branch without changing SQL, fragment, worker, carrier, or Native bytes.

## References

- [Correlated SQL mutable query preparation](0440-correlated-sql-mutable-query-preparation.md)
- [Bounded Native mutable row query ownership](0441-bounded-native-mutable-row-query-ownership.md)
- [Native Protocol v1](../protocol/native-v1.md)
