# ADR 0276: Bounded replicated ingest coordinator

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB service, network, ingestion, and Raft maintainers

## Context and decision

One replicated ingest operation is nonblocking, but a service worker needs finite ownership for many
reactor requests, exact cancellation, deadlines, fair polling, and response correlation.

`ReplicatedIngestCoordinator` is a thread-affine bounded vector of move-only operations. Admission
requires an authenticated route identity, exact frame/payload consistency, captured Protocol 2.0
context with the QUORUM_SYNC feature, canonical QUORUM_SYNC envelope bytes, a configured group, and
an exact leader term. Duplicate connection/request identities and capacity overflow are rejected
before coordinator ownership grows.

Round-robin `poll` performs no wait and inspects each live operation at most once per call. It
returns at most one owning response so the caller can retain it under SPSC response backpressure.
Success carries the frozen receipt acknowledgement; operation failure or deadline expiry carries a
bounded correlated ERROR. Explicit or disconnect-generated cancellation erases only the exact
connection/request owner and emits no response because the reactor already retired or lost it.

An admitted Raft proposal cannot be undone by cancellation, timeout, or destruction. Its eventual
application remains authoritative and retry-safe even when response ownership disappears.

## Consequences and validation

Node-wide pending count, high water, admissions, completions, cancellations, timeouts, and
rejections are observable. Focused tests cover finite overload, exact cancellation, successful
correlated acknowledgement, deadline error, metrics, and refusal without negotiated authority.

Placement/schema authorization, leader routing, integration into the packaged daemon loop,
completion-driven wakeup, multi-node delayed commit, queue saturation, disconnect races, TSan, and
load/latency measurement remain subsequent work. No durable or network bytes change.

## References

- [ADR 0274](0274-nonblocking-replicated-ingest-operation.md)
- [ADR 0275](0275-negotiated-network-task-context.md)
