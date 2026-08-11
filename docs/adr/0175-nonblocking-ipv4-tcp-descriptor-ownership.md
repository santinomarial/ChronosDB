# ADR 0175: Nonblocking IPv4 TCP descriptor ownership

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** network and cluster-transport subsystems
- **Extends:** [ADR 0009](0009-network-reactor-strategy.md), [ADR 0064](0064-bounded-linux-epoll-reactor.md)

## Context

The distributed-query TLS carriers accepted already-connected borrowed descriptors, but production
embeddings still had to create, configure, connect, accept, identify, move, and close raw sockets.
Ad hoc ownership risks descriptor leaks, blocking calls, inheritance across `exec`, inconsistent
latency policy, closing a descriptor before its TLS session, and treating connect initiation as
successful completion.

## Decision

`TcpSocket` is a move-only RAII owner for one IPv4 TCP descriptor. `begin_connect` validates a
nonzero remote endpoint, creates the descriptor, applies nonblocking and close-on-exec flags plus
`TCP_NODELAY`, and reports whether connection establishment completed immediately or remains in
progress. After writable/error readiness, `finish_connect` reads `SO_ERROR`; it is idempotent after
success and never treats `EINPROGRESS`/`EALREADY` as completion. Local and peer endpoints are
available only after connection completion.

`TcpListener` is a separate move-only RAII owner. `bind` validates a finite positive backlog,
creates a nonblocking close-on-exec listener, optionally enables address reuse, binds an explicit
IPv4 endpoint, listens, and records the kernel-selected port. `accept_one` performs at most one
accept, returns an empty optional on would-block, validates the IPv4 peer, and configures the
accepted descriptor as nonblocking, close-on-exec, and `TCP_NODELAY` before exposing it.

Closing is explicit and idempotent, while destruction closes any descriptor still owned. Moving
transfers the sole close obligation and leaves the source invalid. A `TlsSocket` may borrow
`TcpSocket::descriptor`, but must be destroyed before the TCP owner is closed or overwritten.
Readiness polling, connect deadlines, DNS/address resolution, admission limits, and TLS/protocol
state remain higher-level responsibilities.

The public API exposes no `sockaddr`, platform event type, or OpenSSL type. The implementation is
POSIX IPv4 and is built on the currently supported macOS development and Linux production targets.

## Consequences and validation

Descriptor memory is constant and no read/write buffer is retained. Creating a socket performs
constant system calls plus one PIMPL allocation. Connect and accept never block; one accept call
cannot drain an unbounded backlog. Error statuses retain the failing operation and system error.

Real loopback tests poll a port-zero listener and nonblocking connector to completion, validate both
directions of local/peer identity, verify nonblocking and close-on-exec flags plus nonzero
`TCP_NODELAY`, transfer bytes, prove would-block admission, move the sole close obligation, observe
`EBADF` after explicit close, reject invalid endpoints/backlogs, and prove a refused connect closes
and cannot become a false success after `SO_ERROR` is consumed. The loopback test requires local-bind
permission when run in a sandbox.

Invariants 10, 14, 15, and 18 apply.

## Alternatives considered

- **Expose raw descriptor factories:** rejected because the close obligation and TLS lifetime would
  remain implicit.
- **Use blocking connect with a timeout:** rejected because one route can stall an event-loop owner.
- **Drain all pending accepts in one call:** rejected because a busy listener must remain subject to
  the caller's per-poll admission budget.
- **Resolve DNS inside the socket owner:** deferred because resolver concurrency, caching, and
  identity selection require a separate bounded contract.
- **Apply `TCP_NODELAY` only opportunistically:** rejected because the established network latency
  contract requires deterministic admission failure if the option cannot be set.

## Migration and rollback

This adds no durable or wire format. Existing reactors keep their current internal socket
ownership. Cluster embeddings can replace raw socket setup with these owners, retain the owner
beside each TLS carrier, and destroy the carrier before the socket. Removing the wrappers returns
descriptor correctness to each embedding without changing TLS or distributed-query protocols.

## References

- [Network reactor strategy](0009-network-reactor-strategy.md)
- [Bounded Linux epoll ownership](0064-bounded-linux-epoll-reactor.md)
- [Maintained mutual-TLS client socket](0172-maintained-mutual-tls-client-socket.md)
- [Bounded outbound distributed-query TLS carrier](0173-bounded-outbound-distributed-query-tls-carrier.md)
- [Bounded inbound distributed-query TLS carrier](0174-bounded-inbound-distributed-query-tls-carrier.md)
- [Architecture invariants](../architecture/invariants.md)
