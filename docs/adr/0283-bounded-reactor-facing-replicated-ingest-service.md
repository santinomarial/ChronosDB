# ADR 0283: Bounded reactor-facing replicated-ingest service

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB service, networking, ingestion, and Raft maintainers

## Context

The replicated-ingest coordinator accepts owning `NetworkTask` values and produces at most one
response per poll, but a native reactor hands requests across an SPSC queue and requires one exact
response owner to survive response-ring saturation. The packaged process also needs a defined drain
boundary rather than destroying admitted Raft operations when shutdown begins.

## Decision

`ReplicatedIngestService` is a thread-affine, nonblocking adapter over one coordinator and one
distinct request/response queue pair. Each poll first retries one retained response, then consumes
at most one request, and then advances the coordinator by at most one response. A successful poll
reports whether it enqueued a response so the embedding worker can wake a blocked reactor without
polling shared queue internals.

Ingest tasks move directly into coordinator admission. Admission failures become bounded,
correlated native errors using the captured negotiated payload limit. Explicit and disconnect-
generated cancellation detach the exact connection/request owner and emit no response because the
reactor has already retired that request. Unrelated message types fail explicitly.

When the response ring is full, the service retains exactly one owning response and performs no
other work until it publishes those exact bytes. `begin_shutdown` closes new ingest admission but
continues consuming cancellation, rejects already-queued new work, and polls previously admitted
operations through success, failure, or their existing finite deadlines. `drained` requires an
empty input queue, no retained response, and no coordinator-owned request.

## Consequences and validation

The adapter preserves the existing SPSC ownership edge and adds no durable or network bytes. It
does not own a thread, reactor, runtime, election, or remote transport. The embedding must stop
reactor dispatch eventually, call `poll_once` until drained, deliver the external response queue,
and join the sole response producer before destroying queues or the runtime.

Focused tests cover a real committed/applied QUORUM_SYNC request, exact acknowledgement retention
through a full response ring, release notification, exact response-less cancellation, and shutdown
rejection/drain. Queue saturation races, disconnect churn, completion-driven wakeup scheduling,
multi-node delayed commit, TSan, and process-signal tests remain deferred.

## Affected invariants

Invariants 1, 4, 8, 9, 11, 14, 15, and 18 apply.

## References

- [ADR 0063](0063-bounded-reactor-shard-spsc-routing.md)
- [ADR 0275](0275-negotiated-network-task-context.md)
- [ADR 0276](0276-bounded-replicated-ingest-coordinator.md)
- [ADR 0282](0282-owning-replicated-ingest-runtime.md)
