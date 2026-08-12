# ADR 0264: Client-Initiated Raft TLS Handshake

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB Raft transport and security maintainers

## Context

The outbound Raft TLS carrier initially requested read readiness. A TLS client must first produce a
ClientHello, so a real readiness-driven poll loop could wait forever for server bytes. Focused tests
that manually supplied both read and write readiness masked this liveness defect.

## Decision

A newly created outbound Raft TLS carrier requests write readiness. Its first handshake call then
uses OpenSSL's returned `WANT_READ` or `WANT_WRITE` state for every subsequent interest transition.
Authentication and queued frame delivery remain blocked until the maintained mutual-TLS handshake
completes and the peer principal is authorized for the exact node.

## Consequences and validation

Portable poll owners now initiate real client handshakes without fabricated readability. A focused
assertion freezes the initial interest, and the unified two-node transport integration exercises the
actual readiness path. Fragmentation, handshake failure, and reconnect matrices remain Phase 18
work.

## References

- [ADR 0172](0172-maintained-mutual-tls-client-socket.md)
- [ADR 0248](0248-persistent-outbound-raft-mtls-carrier.md)
