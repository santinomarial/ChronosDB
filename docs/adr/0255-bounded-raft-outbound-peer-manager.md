# ADR 0255: Bounded Raft Outbound Peer Manager

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB Raft transport and networking maintainers

## Context

Per-peer connection, TLS, queue, and reconnect owners still required a catalog to reserve one route
per configured peer, expose exact descriptors to a readiness loop, install successful carriers, and
return terminal carriers to reconnect custody without an unbounded fresh-result queue.

## Decision

`RaftTransportPeerManager` preallocates a fixed immutable route catalog, one reconnect owner per
peer, and a pool with capacity for every configured route. Creation rejects foreign local-node
ownership, duplicate peers, invalid connector/security configuration, and pool undercapacity before
opening descriptors.

`drive` starts deadline-due connectors and installs completed descriptor/carrier pairs into their
reserved pool slots. `interests` returns exact peer, descriptor, and read/write readiness for every
connecting or connected route. `on_ready` advances the exact connector or carrier; terminal carrier
failure is drained from the pool and handed back to the same peer's reconnect owner. Recoverable
connect/carrier failure therefore changes route state without poisoning unrelated peers.

Fresh durable results are borrowed and routed only through the pool's whole-result preflight. A
missing/disconnected/full destination returns a status without consuming the result. The manager
does not duplicate those transitions into a side queue: the timer, inbound session, or other bounded
completion owner must retain/backpressure them until routing succeeds.

## Consequences and validation

Outbound Raft TCP/TLS now has one bounded production ownership chain from immutable route through
connect, authenticated carrier, exact-peer pool, failure drain, capped backoff, and replacement.
The manager remains single-event-loop-affine and does not call `poll` itself, choose DNS addresses,
or own upstream durable completions.

Focused two-peer tests prove descriptor interests, exact route installation, all-destination
durable-result routing, carrier timeout recycling, disconnected-result rejection without admission,
exact backoff restart, and duplicate/foreign route rejection. Poll-loop composition with inbound
sessions, timers, and durable completion queues remains the next integration boundary.

## References

- [ADR 0251](0251-bounded-raft-peer-carrier-pool.md)
- [ADR 0253](0253-ownership-safe-raft-tcp-connect-attempt.md)
- [ADR 0254](0254-capped-raft-peer-reconnect-policy.md)

