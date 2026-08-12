# ADR 0220: Native Protocol Ingest Service Adapter

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB service, ingest, WAL, and native protocol maintainers

## Context

Protocol v1 had canonical ingest request and acknowledgement codecs, bounded reactor queues, and
request lifecycle validation. The recoverable single-node owner exposed real tablet, retry, and WAL
authorities. No checked boundary connected them, so the packaged daemon could only reject every
data-plane request.

The protocol request owns encoded bytes only until dispatch completes, while tablet preparation and
WAL admission require an immutable batch owner. A matching retry also differs materially from a new
append: it must not invent a physical WAL coordinate or imply that a second durability operation
occurred.

## Decision

`NativeProtocolService` is a thread-affine synchronous adapter over `SingleNodeDatabase`. Its ingest
path:

1. validates the Protocol v1 ingest envelope under a finite payload limit;
2. exactly decodes the embedded canonical Columnar Append v1 command;
3. resolves the durable table lineage, requires the active schema, and routes only to a local
   tablet;
4. schema-validates and copies canonical column buffers into immutable ownership;
5. executes through the database-wide retry directory and WAL coordinator; and
6. returns one codec-validated acknowledgement or terminal protocol error with the original
   connection, principal, and request identity.

New applies report the actual requested and effective WAL durability plus record start coordinate.
Matching retries report the durability requested by that retry and encode all position fields as
zero, as required by Protocol v1. Malformed wire and command bytes are client-invalid requests;
internal corruption remains an internal failure.

The adapter does not own sockets, queue waiting, cancellation races, or worker threads. Those remain
at the reactor/daemon worker boundary, which can retain an encoded response while its bounded SPSC
queue is full.

## Consequences

Native requests can now exercise the real single-node durable ingest path without duplicating
format parsing or batch ownership logic. `chronos_service` publicly depends on `chronos_network`
because its public adapter interface consumes and returns `NetworkTask`.

This change deliberately does not connect the adapter to `chronosd`, implement query/DDL dispatch,
authorize principals, enforce event-time policy, or make synchronous execution cancellable. Those
are subsequent service tasks. The active-schema-only rule also rejects ancestor-schema retries;
schema-evolution retry admission requires a separately specified policy.

## Validation

Focused tests cover LOCAL_SYNC application, real nonzero WAL coordinates, an ASYNC matching retry
with no second row publication and no fabricated coordinate, routing-envelope retention, malformed
ingest conversion to a Protocol v1 error, and the existing database create/recovery/DDL suite.

## References

- [ADR 0061](0061-native-protocol-handshake-and-request-lifecycle.md)
- [ADR 0218](0218-recoverable-single-node-database-owner.md)
- [Native Protocol v1](../protocol/native-v1.md)
