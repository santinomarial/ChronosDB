# ADR 0256: Bounded Inbound Raft TCP Server

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB Raft transport and networking maintainers

## Context

Persistent inbound Raft TLS sessions still lacked listener admission, descriptor lifetime, finite
connection capacity, readiness polling, and a lossless embedding handoff for post-sync results.

## Decision

`RaftTransportTcpServer` owns one nonblocking listener, maintained mutual-TLS context, pre-reserved
connection table, and poll storage. It admits finitely many sockets per poll, rejects overload,
derives the authentication address from the accepted peer endpoint, and declares each TCP socket
before its borrowing TLS carrier so teardown destroys TLS first.

Every session retains at most one frame, durable admission, or completed result. Result-ready
sessions remain admitted with no read interest until `take_completed` transfers the exact group,
source, and post-sync transition; only then does that persistent session resume reading. Failed
sessions are removed independently. Shutdown destroys sessions before closing the listener.

## Consequences and validation

Inbound TCP/TLS admission is bounded and cannot drop an accepted durable result merely to free a
connection slot. The caller still routes taken results through the bounded outbound manager and
must backpressure pickup when routing cannot accept them. Focused real-loopback coverage proves
connect, mutual TLS, source authentication, durable vote admission, post-sync result retention, and
persistent-session ownership. Overload/churn and combined runtime polling remain hardening work.

## References

- [ADR 0246](0246-authenticated-raft-transport-receiver.md)
- [ADR 0247](0247-persistent-inbound-raft-mtls-carrier.md)
- [ADR 0255](0255-bounded-raft-outbound-peer-manager.md)

