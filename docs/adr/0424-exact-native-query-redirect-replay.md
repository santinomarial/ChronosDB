# ADR 0424: Exact native finite-query redirect replay

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB networking, client, query, and distributed-systems maintainers
- **Extends:** [ADR 0067](0067-bounded-native-client-session.md),
  [ADR 0413](0413-bounded-native-leader-redirect-routing.md)

## Context

Protocol 2 permits a leader redirect for a finite query before any result batch, but the existing
client composition only replays one exact QUORUM_SYNC append. A query caller would otherwise have
to retain SQL, replace the terminally redirected session, assign a fresh connection-local request
identity, validate every result batch, and avoid exposing a partial stream. Publishing rows before
`QUERY_END` would make a later protocol or limit failure indistinguishable from success to a caller.

## Decision

`NativeQueryRetry` is a move-only, single-threaded, transport-independent owner for one exact SQL
string and one exact Raft group route. Creation validates the SQL against the configured protocol
payload bound, validates finite nonzero aggregate result limits, constructs the redirect router,
and starts a fresh one-request Protocol 2 client session requiring leader redirect negotiation.

After the handshake, the owner queues the retained SQL. An accepted redirect can occur only before
any result batch. The owner prepares a replacement session before the router publishes new
authority, discards the old connection-local request identity, starts the new session at request ID
one, and emits an explicit reconnect event. A carrier may reconnect only for that event; ambiguous
transport failure remains terminal transport policy.

Every `QUERY_RESULT` is decoded against the negotiated payload ceiling and configured row, column,
and column-name limits. The owner also checks cumulative row, batch, and payload-byte ceilings before
copying the encoded batch into owned storage. No result is visible through `result()` until the
client session accepts the terminal `QUERY_END`. Any protocol, allocation, server, redirect, or
limit failure is sticky and erases all retained result batches.

## Consequences

The portable layer can now replay one exact finite query through authoritative redirect observations
without duplicating or leaking partial rows. Memory is bounded by the configured route/session
limits, exact SQL size, and aggregate result limits. Work is linear in accepted result bytes plus
the router's logarithmic route selection. One caller thread serializes every method, so no
inter-thread memory-ordering argument applies. No wire or durable format changes.

This owner does not decide that a multi-group query has one redirectable authority. The packaged
replicated query service must emit a redirect only for a query plan whose required authority is
exactly the configured group; multi-group routing and remote mutable-tablet fragments remain
separate. It also has no socket, TLS handshake, deadline, poll, deployment parser, or CLI policy.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): redirect authority is exact-group and monotonic.
- [Invariant 6](../architecture/invariants.md): the exact SQL bytes survive every fresh session.
- [Invariant 11](../architecture/invariants.md): SQL, sessions, and encoded result batches are owned;
  route TLS contexts remain explicitly borrowed.
- [Invariant 14](../architecture/invariants.md): Protocol 2 framing and request identities remain
  unchanged and connection-local.
- [Invariant 18](../architecture/invariants.md): replacement preparation precedes route publication,
  and terminal state never publishes partial results.

## Validation

Focused tests decode both emitted requests and prove byte-exact SQL replay through a fresh session
and request ID, authoritative route change, result invisibility before `QUERY_END`, exact complete
batch retention, sticky stale-redirect failure, required feature negotiation, and aggregate limit
failure without result publication. Header self-containment, installed consumption, formatting,
warnings-as-errors, static analysis, sanitizers, and the serialized suite remain required gates.

## Migration and rollback

This is an additive client owner with no protocol or durable-state change. Rollback removes the
composition while existing Protocol 2 clients and servers remain compatible.

## References

- [Native Protocol v2](../protocol/native-v2.md)
- [Negotiated native leader redirect](0295-negotiated-native-leader-redirect.md)
- [Exact native QUORUM_SYNC redirect replay](0414-exact-native-quorum-ingest-redirect-replay.md)
- [Native client session](../learning/native-client-session.md)
