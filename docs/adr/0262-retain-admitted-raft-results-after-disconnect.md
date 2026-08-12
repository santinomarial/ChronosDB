# ADR 0262: Retain Admitted Raft Results After Disconnect

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB Raft transport and recoverability maintainers

## Context

An inbound peer may disconnect after its complete authenticated message has entered the asynchronous
durable runtime but before that operation completes. Destroying the carrier on `POLLHUP` would drop
the sole owning completion even though local persistent state may already have changed.

## Decision

Each inbound TCP connection records terminal transport closure separately from carrier progress.
Connections still handshaking or reading an incomplete frame are removed immediately. Connections
awaiting a durable result or retaining a result-ready transition stay owned, expose no further
descriptor interest, and continue completion driving. After the embedding takes that exact result,
the closed connection is removed instead of returning to frame reading.

This rule applies to both the standalone poll wrapper and external readiness API. A readable terminal
event is processed first so a complete frame may cross admission; closure is recorded afterward.
No response-delivery guarantee is inferred—the disconnected peer may retry the duplicate-safe Raft
RPC—but local transition, application, snapshot, and outbound ownership cannot disappear.

## Consequences and validation

Disconnect no longer cancels irreversible durable work or leaks a result. Closed admitted sessions
may retain one descriptor until their bounded asynchronous operation completes, matching the
existing one-operation-per-session bound. A real mutual-TLS regression test closes the transport at
result readiness, proves the session remains active until pickup, and then proves immediate removal.
Disconnect timing matrices and process-crash coverage remain Phase 18 work.

## References

- [ADR 0247](0247-persistent-inbound-raft-mtls-carrier.md)
- [ADR 0256](0256-bounded-inbound-raft-tcp-server.md)
- [ADR 0260](0260-embedding-owned-inbound-raft-readiness.md)
