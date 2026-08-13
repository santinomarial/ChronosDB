# ADR 0305: Bounded DNS and multi-address distributed query routing

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB cluster, network, and query maintainers
- **Extends:** [ADR 0175](0175-nonblocking-ipv4-tcp-descriptor-ownership.md),
  [ADR 0178](0178-pinned-multi-tablet-tcp-query-scheduling.md),
  [ADR 0298](0298-committed-distributed-query-route-resolution.md)

## Context

Committed query route resolution accepted only one canonical IPv4 endpoint. The durable metadata
field is deliberately generic, TLS already supports a DNS certificate identity, and the one-attempt
TCP owner deliberately leaves address selection above it. A coordinator still could not use a DNS
endpoint or retain more than one address for a node, so one failed address exhausted a logical
sender attempt without trying another answer.

DNS must not run inside the nonblocking execution poll loop. Results also need finite ownership and
must remain subordinate to the committed node ID and proof-bound dispatch target: an address answer
is reachability information, not authority to change the serving node.

## Decision

`resolve_ipv4_endpoints` accepts either the existing canonical nonzero IPv4 endpoint or one strict
lowercase DNS name plus canonical nonzero decimal port. DNS labels and total name length are
bounded. It performs one fresh system `getaddrinfo` lookup restricted to IPv4 TCP, retains answer
order, removes exact duplicates, requires the requested port and a nonzero address, and rejects an
answer set above the caller's positive hard limit. Numeric endpoints bypass DNS.

The lookup is a blocking pre-execution operation. Callers must complete it before starting or
re-entering the single-threaded query poll owner. ChronosDB adds no hidden cache: whole-query
rebinding reruns committed route resolution and obtains a fresh returned candidate snapshot. DNS
TTL, system resolver configuration and caching, and lookup latency remain deployment inputs; the
query's connect, TLS, exchange, retry, and whole-query deadlines begin only after resolution
succeeds.

`DistributedQueryNodeRoute` retains an ordered nonempty candidate vector under one committed node
ID and one explicit node-specific `TlsClientContext`. Scheduler creation rejects zero, duplicate,
or over-limit candidates before opening a socket. Attempt `n` uses candidate
`(n - 1) modulo candidate_count`, so retries rotate deterministically while the existing sender
remains the sole finite attempt/backoff authority. Every attempt passes its actual IPv4 address to
peer authentication, while the independent TLS context continues to enforce the configured DNS or
IP certificate identity and principal-to-target-node authorization.

## Consequences

DNS and multi-address routes now compose with committed metadata, packaged leader-linearizable and
bounded-stale constructors, and the existing all-tablet success boundary. A failed candidate cannot
silently change a dispatch target, reset a retry budget, retain a partial from another authority, or
weaken certificate verification.

Resolution is `O(system lookup + answers + selected nodes)` and retains at most the configured
addresses per selected node. Scheduler selection is constant time per attempt. The system resolver
may block before execution; asynchronous resolver integration, DNS caching/TTL policy, IPv6, and
live DNS-failure qualification remain later operational work. No durable or network bytes change.

## Alternatives considered

- **Resolve inside `TcpSocket::begin_connect`:** rejected because a blocking lookup would violate
  the nonblocking descriptor owner's contract and hide candidate ownership.
- **Treat every address as a different route/node:** rejected because DNS answers do not alter
  committed node identity or proof authority.
- **Race every answer:** rejected because it multiplies descriptors per tablet and needs a separate
  bounded Happy Eyeballs/cancellation contract.
- **Reset retry count for each address:** rejected because address count must not expand the
  sender's accepted finite retry budget.
- **Cache indefinitely:** rejected because stale reachability would survive explicit whole-query
  rebinding without an accepted TTL/refresh contract.

## Failure modes and operations

Noncanonical names and ports reject before lookup. DNS absence and resolver failures are
`UNAVAILABLE`; allocation and answer-bound exhaustion are `RESOURCE_EXHAUSTED`. The metadata route
layer maps a selected generic endpoint outside the supported IPv4/DNS grammar to `UNAVAILABLE`
rather than declaring the generic durable metadata corrupt. Invalid caller-assembled candidate
vectors reject before any attempt opens.

Operators should distinguish pre-execution DNS failures from connect/TLS/exchange attempt failures.
The latter consume the existing bounded sender attempts and rotate candidates; explicit whole-query
rebind is still required to acquire changed committed node or proof authority.

## Validation

Focused network tests cover numeric bypass, real `localhost` IPv4 resolution, port preservation,
uniqueness, name/label/port rejection, and invalid limits. Route tests cover committed DNS metadata,
bounded address ownership, TLS-node correlation, unsupported generic forms, and existing catalog
failure modes. A real mutual-TLS loopback test gives one node a refused first address and a live
second address, proving finite retry rotation, exact target authentication, complete two-tablet
aggregation, and expected attempt/failure metrics.

Invariants 5, 6, 11, 14, 15, and 18 apply.

## Migration and rollback

Existing numeric route metadata resolves to a one-element candidate vector with unchanged connect
behavior. Public callers that assemble routes directly must wrap each former endpoint in a vector.
Rollback is wire- and durable-format compatible but cannot execute DNS or multi-address routes.

## References

- [Committed distributed query route resolution](0298-committed-distributed-query-route-resolution.md)
- [Deadline-bound distributed-query TCP client](0177-deadline-bound-distributed-query-tcp-client.md)
- [Pinned multi-tablet TCP query scheduling](0178-pinned-multi-tablet-tcp-query-scheduling.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
- [Phase 16 roadmap](../roadmap.md#phase-16--distributed-query-execution-and-rebalancing)
