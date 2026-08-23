# ADR 0414: Exact native QUORUM_SYNC redirect replay

- **Status:** accepted
- **Date:** 2026-08-23
- **Owners:** ChronosDB networking, client, ingest, and replicated-service maintainers
- **Extends:** [ADR 0067](0067-bounded-native-client-session.md),
  [ADR 0413](0413-bounded-native-leader-redirect-routing.md)

## Context

The native redirect router could select a bounded authenticated destination, but a caller still had
to retain the original append, create a fresh Protocol 2 session, negotiate both required features,
assign a new connection-local request identity, and correlate the final receipt. Reusing the old
session after a terminal redirect or rebuilding the append from decoded views could violate request
lifecycle and retry identity.

## Decision

`NativeQuorumIngestRetry` is a move-only, single-threaded, transport-independent owner for one exact
encoded columnar append. Creation validates that a Protocol 2 QUORUM_SYNC envelope fits the supplied
buffer bound, constructs the exact-group redirect router, and starts a fresh one-request
`NativeClientSession` with QUORUM_SYNC and leader redirect both required.

The caller writes the exposed handshake bytes to the current route and feeds response fragments
back to the owner. Only after the server selects Protocol 2 and both features does the owner queue
the retained append as QUORUM_SYNC. A terminal redirect first prepares a complete replacement
session; only then may the router accept and publish the new authority. The old connection-local
request identity is discarded, the replacement session starts again at request ID one, and the
append bytes remain exact. The caller receives an explicit reconnect event and must replace its
transport before writing the new handshake.

Completion requires a canonical QUORUM_SYNC receipt whose group matches the bound group, whose
leader is the selected destination, and whose term does not regress the accepted redirect. Missing
features, protocol/lifecycle damage, stale or conflicting redirects, wrong receipt authority, and
server errors become sticky failures and expose no result. Only a complete validated receipt is
retained and returned.

## Consequences

Portable clients now have one finite replay state machine from initial handshake through any
accepted redirects to one exact receipt. At most one client session, one retained append, one route
map, and one receipt are owned. Each redirect pays the router's `O(log routes)` lookup plus bounded
session/buffer construction. One caller thread serializes every method, so no inter-thread
memory-ordering argument applies. No durable or protocol bytes change.

The owner deliberately has no descriptor, TCP connect policy, TLS handshake driver, readiness
poller, deadline, transport retry, deployment parser, or process integration. ADR 0415 subsequently
composes it with one deadline-bound nonblocking TCP/mutual-TLS carrier while retaining the rule that
a generic transport error is not permission to replay. Event-loop, deployment-parser, and process
integration remain separate.

## Affected invariants

- [Invariant 4](../architecture/invariants.md): success is only the existing QUORUM_SYNC applied or
  matching-retry receipt; no durability downgrade is possible.
- [Invariant 5](../architecture/invariants.md): redirect authority and the final receipt cannot be
  mixed across group, leader, or regressing term.
- [Invariant 6](../architecture/invariants.md): one exact append survives every fresh session.
- [Invariant 11](../architecture/invariants.md): append, buffers, sessions, and receipt are owned;
  route TLS contexts remain explicitly borrowed.
- [Invariant 14](../architecture/invariants.md): Protocol 2 bytes and request identities remain
  unchanged and connection-local.
- [Invariant 18](../architecture/invariants.md): replacement preparation precedes route publication,
  and every failure is sticky.

## Validation

Focused tests decode both emitted requests and prove byte-identical append replay through a fresh
session and request ID, exact route change, feature negotiation, final receipt publication, and
terminal empty output. They also prove stale redirect failure leaves the original route/attempt
count unchanged and that missing redirect negotiation or wrong receipt leadership fails sticky.
Header self-containment, installed consumption, formatting, warnings-as-errors, clang-tidy,
ASan/UBSan, and the full serialized suite are required before completion.

## Migration and rollback

Manual native clients may replace hand-built reconnect state with this owner while retaining their
carrier. Rollback removes the portable replay composition but changes no server, durable, or wire
state.

## References

- [Bounded native client session](0067-bounded-native-client-session.md)
- [Bounded native leader-redirect routing](0413-bounded-native-leader-redirect-routing.md)
- [Deadline-bound native QUORUM_SYNC TCP client](0415-deadline-bound-native-quorum-ingest-tcp-client.md)
- [Native Protocol v2](../protocol/native-v2.md)
- [Native client session](../learning/native-client-session.md)
