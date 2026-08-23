# ADR 0276: Bounded replicated ingest coordinator

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB service, network, ingestion, and Raft maintainers
- **Extended by:** [ADR 0280](0280-authoritative-replicated-ingest-routing.md)

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

One optional observer can be borrowed for a synchronous `poll(observer)` call. Successful writes
report correlated route-validated, proposal-admitted, application-proved, and response-ready stages
in order on the coordinator thread. Errors and redirects emit no write stage. The callback is
non-throwing, is not retained, and must not reenter or mutate the coordinator; blocking it blocks
that poll call.

## Consequences and validation

Node-wide pending count, high water, admissions, completions, cancellations, timeouts, and
rejections are observable. Focused tests cover finite overload, exact cancellation, successful
correlated acknowledgement, deadline error, metrics, refusal without negotiated authority, and
the exact success-stage order without duplicate callbacks.

A real-process matrix kills the packaged database at each observed write stage. Route validation
is pre-proposal and must reopen to the prior publication; proposal admission permits either prior
or committed state; application proof and response readiness require the committed mutation and
retry identity. Every outcome is resolved by an exact protocol retry and repeated reopen.

Authoritative tablet/group placement and local leader routing are added by ADR 0280, and committed
active-schema authorization by ADR 0281. Remote leader redirection, packaged daemon integration,
completion-driven wakeup, multi-node delayed commit, queue saturation, disconnect races, TSan, and
load/latency measurement remain subsequent work. No durable or network bytes change.

## References

- [ADR 0274](0274-nonblocking-replicated-ingest-operation.md)
- [ADR 0275](0275-negotiated-network-task-context.md)
