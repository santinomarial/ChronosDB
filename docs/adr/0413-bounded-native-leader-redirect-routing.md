# ADR 0413: Bounded native leader-redirect routing

- **Status:** accepted
- **Date:** 2026-08-23
- **Owners:** ChronosDB networking, client, security, and replicated-service maintainers
- **Extends:** [ADR 0295](0295-negotiated-native-leader-redirect.md),
  [ADR 0296](0296-authoritative-replicated-ingest-leader-redirect.md)

## Context

Protocol 2 can terminally return a committed-placement and ordered-Raft leader observation, but the
client boundary still had no way to join that stable node ID to an authenticated native endpoint or
to prevent stale, contradictory, or cyclic observations from driving unbounded retry. Treating the
Raft peer address as a native endpoint would cross trust domains, while accepting each redirect in
isolation could regress placement or term authority.

## Decision

`NativeLeaderRedirectRouter` is a move-only, single-threaded policy owner for one finite request.
Creation owns a canonical node-sorted vector of unique IPv4 native endpoints, each paired with a
borrowed immutable TLS client context. It exact-binds one nonnil Raft group, the initial destination,
a minimum committed placement epoch, and finite route and redirect bounds. The initial node must be
present in that explicit map; no Raft transport endpoint is inferred.

An accepted redirect must be semantically complete, name the exact group, meet the configured and
previous placement floors, and never regress the observed Raft term. Two different leaders in the
same term, a redirect back to the current destination, an unknown node, or an exhausted retry budget
fails without changing router state. A successful decision returns the exact endpoint/TLS route,
the complete observed authority, and its one-based retry number, then atomically advances the
current node and authority floor.

The redirect remains an observation rather than a lease. This owner opens no socket, retries no
bytes, and grants no request authority; a later carrier must repeat the normal Protocol 2 handshake,
authentication, request correlation, and consistency proof at the selected destination.

## Consequences

Endpoint selection and retry policy are now deterministic, bounded, and independent of consensus
transport configuration. Creation is `O(routes log routes)` because the canonical map also rejects
duplicate endpoints; each redirect uses `O(log routes)` node lookup and constant retained state.
TLS contexts are borrowed and must outlive every carrier attempt using a returned route. One caller
thread serializes mutation, so no inter-thread memory-ordering argument applies. No durable or
network bytes change.

The native TCP/TLS retry carrier, deployment-text native endpoint configuration, reconnect
deadlines, request-body retention, process composition, and multi-group SELECT routing remain
separate work.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): routing does not turn an advisory observation into
  read or write authority.
- [Invariant 6](../architecture/invariants.md): group, placement, term, leader, endpoint, and TLS
  identity stay correlated through one owner.
- [Invariant 11](../architecture/invariants.md): route storage is owned and TLS-context borrowing is
  explicit.
- [Invariant 14](../architecture/invariants.md): no existing native or Raft bytes change.
- [Invariant 18](../architecture/invariants.md): stale, conflicting, unknown, and exhausted routing
  decisions fail closed.

## Validation

Focused tests prove canonical construction, two monotonic leader changes, exact endpoint/TLS
selection, finite exhaustion, and failure-atomic rejection of wrong-group, stale-term,
stale-placement, same-term split-leader, self, unknown-node, duplicate-endpoint, missing-TLS, and
over-limit inputs. A self-contained public-header translation unit and installed consumer retain the
API boundary. Formatting, warnings-as-errors, static analysis, sanitizer, and full serialized-suite
checks remain required before completion.

## Migration and rollback

Native clients may insert this owner between decoded terminal redirects and a future reconnect
carrier. Existing manual clients remain wire compatible. Rollback removes bounded policy support
but changes no server, durable, or protocol state.

## References

- [Negotiated native leader redirect](0295-negotiated-native-leader-redirect.md)
- [Authoritative replicated-ingest leader redirect](0296-authoritative-replicated-ingest-leader-redirect.md)
- [Native Protocol v2](../protocol/native-v2.md)
- [Native server operations](../operations/native-server.md)
