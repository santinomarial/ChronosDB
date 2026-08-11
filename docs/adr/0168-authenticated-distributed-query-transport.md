# ADR 0168: Authenticated distributed query transport

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB query, cluster, and networking maintainers
- **Extends:** [ADR 0167](0167-proof-revalidated-distributed-aggregate-worker.md)

## Context

The coordinator can construct authority-bound dispatches and a worker can reprove them locally, but
passing an in-process value does not define cross-node framing, correlation, authentication, or a
safe rejection boundary. Reusing client SQL messages would mix client and cluster trust domains.

## Decision

Distributed Query Transport v1 defines separate canonical request and response frames. A request
binds exact source and target nodes to one checksummed group-scoped dispatch. A response binds the
reverse route, query, and tablet to either one terminal sequence-1 exchange or one failure status;
an optional leader/placement hint is advisory only. Fixed headers are integrity-checked before
variable lengths drive payload interpretation, total retained bytes are format-bounded, and nested
payloads retain their own integrity and semantic validation.

`DistributedQueryReceiver` requires a transport-authenticated principal before decoding, authorizes
that principal for the claimed source, verifies the local target, and only then calls an
embedding-owned `DistributedQueryWorkerService`. Worker failures become correlated response status
codes. Worker results must exactly match query and tablet identity and terminal sequencing before
encoding. Thrown worker exceptions are contained at this boundary and become bounded failure
responses rather than escaping into the carrier.

Cluster-node authorization is deliberately distinct from native client authentication. CRC32C is
damage detection, mutual TLS establishes the peer principal, and the worker's local Raft/placement/
snapshot checks establish execution authority.

## Consequences and validation

Request memory is capped at 16,772 bytes and responses at 244 bytes. Codec work is linear in frame
size and returns value-owned results. Direct authentication, malformed-frame, authorization, and
wrong-target failures do not invoke the worker. There is no silent consistency downgrade and a
malformed or uncorrelated worker result cannot be emitted as success.

Tests cover exact round trips and lengths, outer and nested corruption, checksum-valid unknown
versions, success/failure correlation, authentication/source authorization, wrong-target routing,
worker failure mapping, and rejection of an uncorrelated worker result. Installed-consumer and
sanitizer checks cover the public ABI and runtime parser boundary.

Partial stream I/O, socket ownership, connection deadlines, retry/backoff, cancellation, and
coordinator scheduling remain follow-up carrier work.

ADRs 0169, 0173, and 0174 subsequently supply bounded stream/retry ownership and symmetric
single-attempt mutual-TLS readiness/deadline carriers over embedding-owned connected descriptors.

Invariants 6, 10, 11, 14, 15, and 18 apply.

## Migration and rollback

This is the first version of this cluster protocol. Rollback removes its listener/dispatch route;
nodes must not reinterpret these frames as client protocol or bare exchange frames. Mixed versions
reject checksum-valid unknown protocol versions explicitly.

## References

- [Distributed Query Transport v1](../formats/distributed-query-transport-v1.md)
- [Distributed Aggregate Fragment Dispatch v1](../formats/distributed-aggregate-fragment-dispatch-v1.md)
- [Distributed Aggregate Exchange v1](../formats/distributed-aggregate-exchange-v1.md)
- [Bounded inbound distributed-query TLS carrier](0174-bounded-inbound-distributed-query-tls-carrier.md)
