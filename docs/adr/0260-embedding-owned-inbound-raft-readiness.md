# ADR 0260: Embedding-Owned Inbound Raft Readiness

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB Raft transport and networking maintainers

## Context

The bounded inbound Raft TCP server could drive its own portable `poll`, but a node runtime needs to
wait on inbound sessions, outbound peers, durable completions, and deadlines in one descriptor set.
Exposing vector indexes would be unsafe because connection removal compacts the owning table.

## Decision

Each accepted session receives one nonzero, never-reused 64-bit connection ID. The server exposes
its borrowed listener descriptor and a bounded snapshot of connection ID, descriptor, and TLS
read/write interest. One embedding event loop may call bounded accept, deliver readiness by stable
ID, report terminal transport closure, and drive deadline or durable-completion progress without
readiness. The original standalone `poll_once` remains a wrapper over the same owned state.

Connection IDs stop admission before wraparound. Missing or stale IDs fail explicitly. Ordinary
connection protocol/timeout failure removes only that connection and increments metrics; listener
accept failures remain visible to the caller. Result-ready sessions retain their ID and descriptor
with no read interest until the exact durable result is taken.

## Consequences and validation

An outer node runtime can build one stable per-iteration poll table without borrowing connection
objects or relying on compacting indexes. Socket and TLS lifetime order, admission bounds, and result
backpressure are unchanged. A focused real mutual-TLS test uses only the external accept/readiness/
drive API through durable response completion, then proves explicit terminal removal. Poll churn,
ID exhaustion, and high-connection fault testing remain Phase 18 work.

## References

- [ADR 0247](0247-persistent-inbound-raft-mtls-carrier.md)
- [ADR 0256](0256-bounded-inbound-raft-tcp-server.md)
- [ADR 0259](0259-exact-raft-runtime-deadline-introspection.md)
