# ADR 0292: Packaged replicated native SELECT

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB daemon, service, query, networking, metadata, and Raft maintainers

## Context

The replicated database can now pin a stable local-applied catalog/tablet vector, but the packaged
daemon's replicated queue consumer forwarded every task to the asynchronous ingest adapter. Adding
a second request-ring consumer would violate the SPSC ownership contract. Bypassing the bounded
native result encoder would also give replicated reads different limits and wire behavior.

## Decision

`NativeProtocolService` accepts either a single-node database or a replicated database. The
replicated form implements current-state, single-source SELECT by acquiring one
`ReplicatedQuerySnapshot` per request, binding against its owning catalog, lowering through the
existing physical engine, and instantiating the table-wide tablet-state pipeline. It uses the same
result row, batch, payload, memory, and protocol encoding limits as the single-node path.

Replicated CREATE TABLE, SQL INSERT, ASOF JOIN, and `FOR SYSTEM_TIME` remain explicit errors. Native
canonical QUORUM_SYNC ingest continues through `ReplicatedIngestCoordinator`; the synchronous
protocol service is not a second write path. Reads retain the ADR 0291 local-applied contract and do
not claim a quorum barrier or cross-group linearizability.

`ReplicatedIngestService` remains the sole daemon request-ring consumer. It optionally owns a
borrowed native query dispatcher, recognizes QUERY_REQUEST, and retains the returned finite response
sequence in order. At most one frame additionally occupies the existing response-backpressure slot.
No later request or coordinator poll overtakes the sequence. Shutdown rejects newly consumed query
or ingest work, publishes retained frames, and still drains admitted asynchronous writes.

## Consequences and validation

`chronosd --replicated-groups` now serves bounded native SELECT for a table only when every committed
tablet placement is resident. Partial tables fail as one protocol execution error rather than
returning local rows. The worker and queue topology do not change, and no durable or network bytes
change.

Focused service integration applies a QUORUM_SYNC batch, reopens the replicated database, routes a
native count through the queue adapter while the response ring is full, verifies ordered
QUERY_RESULT/QUERY_END delivery, and checks explicit DDL rejection. The Linux process regression
now queries the applied rows before restart and the same recovered rows after an idempotent retry.
This macOS verification compiles that Linux-only source but cannot run its epoll subprocess case.
Allocation fault injection, long result sequences, disconnect/cancel timing, stronger read barriers,
remote fragments, and real three-process evidence remain in the hardening ledger.

## Affected invariants

Invariants 4–6, 11, 14, 15, and 18 apply.

## References

- [ADR 0222](0222-bounded-native-vector-query-results.md)
- [ADR 0283](0283-bounded-reactor-facing-replicated-ingest-service.md)
- [ADR 0291](0291-stable-local-applied-replicated-query-snapshot.md)
- [Native server operations](../operations/native-server.md)
