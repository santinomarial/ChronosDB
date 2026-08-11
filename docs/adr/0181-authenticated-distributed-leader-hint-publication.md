# ADR 0181: Authenticated distributed leader-hint publication

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB cluster, metadata, and networking maintainers
- **Extends:** [ADR 0168](0168-authenticated-distributed-query-transport.md), [ADR 0180](0180-explicit-whole-query-authority-rebinding.md)

## Context

Distributed Query Transport v1 already encoded an advisory leader node and placement epoch, and the
sender retained a correlated hint. The production TLS/TCP server path never supplied one, however,
because the worker service owns query execution rather than the committed metadata view. A server
must not read an untrusted request's tablet/group identity before authentication and authorization,
nor may it present a cached or guessed leader as authority.

## Decision

`DistributedQueryLeaderHintProvider` is an embedding-owned, synchronized view of committed metadata.
It resolves an optional leader/placement hint for one exact tablet and Raft group. A receiver may
borrow it through `DistributedQueryReceiverConfig`; the provider must outlive the receiver.

The receiver consults this provider only after mutual-TLS authentication has produced an authorized
principal, the canonical request has decoded, the claimed source is authorized, the target is the
local node, and the worker has returned `UNAVAILABLE`. An explicitly supplied hint continues to take
precedence for callers that already hold an ordered metadata observation. No lookup occurs for a
successful result or another status.

Provider failure aborts response construction, causing the carrier to fail closed rather than
publishing absent or stale metadata as though lookup had succeeded. A returned hint still passes
the response codec's nonzero node/epoch validation and is covered by both response CRCs and the
authenticated TLS session. The sender exact-correlates route, query, and tablet before retaining it;
the TCP execution exposes that optional hint after failure.

A hint remains advisory. It can direct a caller's next authoritative metadata lookup, but cannot
retarget an attempt or satisfy the fresh admissions, barriers, placement, and compatible snapshot
required by whole-query rebinding.

## Consequences and validation

The metadata lookup occurs at most once for an authenticated `UNAVAILABLE` worker result. Its
synchronization and lookup complexity belong to the embedding; the receiver retains only a borrowed
pointer and one optional value. No durable or wire format changes because the accepted v1 hint
fields are used exactly as specified.

Direct receiver tests prove unauthenticated and misrouted requests cannot reach the provider, an
exact tablet/group lookup populates the correlated failure response, and provider failure is
returned without a response. A real multi-tablet mutual-TLS test carries leader `13` and placement
epoch `14` from the server-side provider through TCP to the failed scheduler, which exposes the
same value before explicit whole-query rebinding.

Invariants 5, 6, 10, 14, and 18 apply.

## Alternatives considered

- **Let the worker invent a hint:** rejected because query execution failure is not a committed
  metadata observation.
- **Resolve before authentication:** rejected because untrusted peers could drive metadata lookups
  and probe tablet/group identities.
- **Treat the hint as rebinding proof:** rejected because it omits term, barrier, schema, durable
  generation, and the full compatible tablet set.
- **Omit hints whenever lookup fails:** rejected because silently hiding metadata-service failure
  makes routing diagnosis and failure ownership ambiguous.

## Migration and rollback

This activates optional fields already frozen in Distributed Query Transport v1. Servers may omit
the provider and continue returning correlated failures without hints. Production embeddings that
configure it must supply a committed metadata view with synchronization and lifetime covering the
receiver. Removing it preserves protocol compatibility but loses the routing advisory.

## References

- [Authenticated distributed query transport](0168-authenticated-distributed-query-transport.md)
- [Explicit whole-query authority rebinding](0180-explicit-whole-query-authority-rebinding.md)
- [Distributed Query Transport v1](../formats/distributed-query-transport-v1.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
- [Architecture invariants](../architecture/invariants.md)
