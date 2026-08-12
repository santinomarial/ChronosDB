# ADR 0259: Exact Raft Runtime Deadline Introspection

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB Raft runtime and transport maintainers

## Context

A unified event loop must not block beyond an election, heartbeat, reconnect, TCP-connect, TLS
handshake, frame-read, or frame-write deadline. The individual owners already enforced their
monotonic deadlines when driven, but their composition layer could not determine the earliest safe
poll timeout.

## Decision

Every timer and Raft transport owner exposes allocation-free `next_deadline()` introspection. Leaf
owners return a deadline only while time can advance their current state. Aggregate timer, peer-pool,
peer-manager, and inbound-server owners return the minimum exact child deadline. In-flight durable
operations and result-ready/backpressured sessions expose no synthetic deadline because the durable
completion descriptor or explicit pickup is their progress source.

Deadline access is read-only, single-thread-affine state inspection. It neither advances time nor
changes admission, queue, generation, or retry ownership. An embedding clamps its caller-supplied
maximum wait to the earliest returned steady-clock point and then drives all owners at the observed
post-poll time.

## Consequences and validation

One poll loop can derive a correct finite wait without arbitrary tick polling. Exact focused tests
cover election/heartbeat rearming, TCP connect, reconnect backoff, TLS handshake/idle transitions,
and the aggregate peer-manager/inbound-server views. Clock-change simulation and long deadline
distribution measurements remain Phase 18 work.

## References

- [ADR 0248](0248-persistent-outbound-raft-mtls-carrier.md)
- [ADR 0249](0249-generation-tagged-raft-runtime-timers.md)
- [ADR 0254](0254-capped-raft-peer-reconnect-policy.md)
- [ADR 0256](0256-bounded-inbound-raft-tcp-server.md)
- [ADR 0258](0258-portable-durable-raft-completion-wakeup.md)
