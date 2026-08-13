# ADR 0376: Owned schema-bound vector query v2 TCP service

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB service, query, cluster, and networking maintainers
- **Extends:** [ADR 0372](0372-bounded-schema-bound-vector-query-v2-tcp-server.md),
  [ADR 0375](0375-proof-revalidated-schema-bound-vector-row-worker-v2.md)

## Context

The schema-bound vector receiver, bounded TCP/mTLS server, and proof-revalidating real-CSEG row
worker were implemented separately. Each lower layer borrows the address of the next dependency.
Constructing and moving them independently could therefore leave a receiver pointing at a moved or
destroyed worker, or a server pointing at a moved or destroyed receiver. Production startup needed
one owner that established their complete lifetime and destruction order.

## Decision

`ReplicatedDistributedVectorQueryTcpServerV2` is the move-only, single-threaded owner of the inbound
vector-v2 row stack. Startup validates and creates `ReplicatedDistributedVectorQueryWorkerV2`,
places it in one heap-stable implementation, constructs `DistributedVectorQueryReceiverV2` with
the stable worker address, then starts `DistributedVectorQueryTcpServerV2` with the stable receiver
address.

The implementation declares worker, optional receiver, and optional server in dependency order.
Reverse destruction therefore removes all accepted TCP/TLS connections and the server before the
receiver, and the receiver before the worker. Moving the public owner transfers only its
implementation pointer and cannot invalidate either internal address.

Configuration accepts the worker's borrowed Manifest storage and coherent authority provider,
listener and TLS credentials, connection authenticator, node authorizer, optional committed
leader-hint provider, carrier count/byte/deadline limits, and finite TCP admission limits. Receiver
response limits are copied from the carrier limits so an accepted response vector can always be
bounded consistently across both layers. External worker and receiver pointers are deliberately
not configurable.

Polling, metrics, endpoint access, and idempotent shutdown delegate to the existing bounded server.
The owner adds no durable or network bytes. It retains the row worker's deliberate rejection of
aggregate modes and leaves final ordering and limit for a later global execution owner.

## Consequences and validation

One production object now composes authentication-before-request-bytes, source and target
authorization, exact Fragment-v2 decoding, fresh request-local authority acquisition, proof
revalidation, real temporal CSEG winner resolution, schema-bound native response encoding, bounded
complete publication, finite listener admission, metrics, and ordered shutdown. Borrowed storage,
authority, authentication, authorization, and optional hint dependencies must outlive the owner.

The focused real-Manifest/CSEG service test constructs and then moves the public owner, completes
mutual TLS on a kernel-selected loopback port, and exact-decodes the two-row terminal native result.
It proves fresh vector authority acquisition, both certificate fingerprints, one completed
connection, invalid packaged configuration rejection, and deterministic shutdown. Header
self-containment and installed-consumer coverage protect the public boundary.

ADR 0377 subsequently supplies the pinned portable sender/coordinator owner. This task does not
provide outbound multi-tablet TCP scheduling, global ordering/limit, all-type aggregate merge state,
cancellation across tablet attempts, or process integration. No Phase 16 exit gate is claimed.

Invariants 5, 6, 10, 11, 14, 15, and 18 apply.

## References

- [Bounded schema-bound vector query v2 TCP server](0372-bounded-schema-bound-vector-query-v2-tcp-server.md)
- [Proof-revalidated schema-bound vector row worker v2](0375-proof-revalidated-schema-bound-vector-row-worker-v2.md)
- [Distributed Vector Query Transport v2](../formats/distributed-vector-query-transport-v2.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
