# ADR 0369: Authenticated schema-bound vector query receiver v2

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, cluster, and networking maintainers
- **Extends:** [ADR 0168](0168-authenticated-distributed-query-transport.md),
  [ADR 0181](0181-authenticated-distributed-leader-hint-publication.md),
  [ADR 0368](0368-schema-bound-distributed-vector-query-transport-v2.md)

## Context

Distributed Vector Query Transport v2 freezes schema-bound request and response bytes, but a codec
does not establish peer identity or define the safe handoff to execution. General vector responses
can also approach 16 MiB per frame, so the grouped receiver's frame-count bound alone would permit
roughly 16 GiB at its default 1,024-frame limit.

## Decision

`DistributedVectorQueryReceiverV2` rejects a missing authentication result before request decode,
authorizes the authenticated principal for the claimed source node, exact-matches the local target,
and then invokes one embedding-owned `DistributedVectorQueryWorkerServiceV2`. The service must
outlive the receiver and returns one complete value-owned Result-Exchange-v2 message stream.

Before publication, the receiver requires a nonempty stream, exact query/tablet identity, one-based
contiguous sequence, and terminal only on the final message. Every response is encoded with the
Fragment-v2 result schema, so native result descriptors must exact-match the admitted physical
shape. All frames are retained in one local vector and returned together; any later correlation,
schema, encoding, or allocation failure exposes no prefix.

The receiver applies two independent response bounds: a positive frame limit no greater than the
65,536-message coordinator hard limit, and a total encoded response-byte limit between the
116-byte failure frame and a 1-GiB hard ceiling. The default byte limit is 64 MiB. A valid worker
stream over either configured limit becomes one correlated `RESOURCE_EXHAUSTED` response. An empty
or malformed worker stream is a contract error and returns no response vector.

Worker failures become one correlated status response. `UNAVAILABLE` may consult the existing
committed-metadata leader-hint provider for the exact tablet and Raft group. Provider failure aborts
publication. Worker allocation exceptions become `RESOURCE_EXHAUSTED`; all other exceptions become
`INTERNAL`.

The receiver is synchronous and single-owner. It borrows the authorizer, worker, and optional hint
provider; it does not own their synchronization or lifetime. It consumes an authentication result
but does not own TLS, descriptors, connection closure, writes, retries, coordination, or execution
implementation.

## Alternatives considered

- **Frame-count bound only:** rejected because large native result batches make the retained-byte
  bound operationally unsafe.
- **Publish each encoded response immediately:** rejected because a later invalid sequence or
  schema mismatch could expose partial query success.
- **Trust result descriptors supplied by the worker:** rejected because Fragment v2 is the admitted
  schema authority and every response must validate against it.
- **Merge peer authentication with worker authority:** rejected because certificate identity,
  claimed-node authorization, and fresh Raft/snapshot execution authority are distinct gates.

## Consequences

Unauthenticated, unauthorized, corrupt, and misrouted requests cannot invoke the worker. Successful
published response storage is bounded by both frame count and exact encoded bytes. Work is linear in
the complete request and result stream. No network or durable bytes change.

ADRs 0370–0372 subsequently supply mutual-TLS progress and TCP acquisition/listener ownership;
ADRs 0375–0376 supply the production row worker and stable inbound service composition. Retry
arbitration and schema-bound coordination are implemented separately. Aggregate state, outbound
multi-tablet scheduling, global result semantics, and process integration remain incomplete.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): request bytes remain bounded and exact-decoded before
  variable state reaches execution; response publication has explicit count and byte ceilings.
- [Invariant 10](../architecture/invariants.md): every native result descriptor is checked against
  the Fragment-v2 admitted schema.
- [Invariant 11](../architecture/invariants.md): the complete authority-bound dispatch reaches one
  worker invocation without weakening its snapshot identity.
- [Invariant 14](../architecture/invariants.md): peer route, query/tablet correlation, execution
  authority, and result schema remain explicit across the handoff.
- [Invariant 15](../architecture/invariants.md): authentication, source authorization, local target,
  and worker-local authority are separate fail-closed gates.
- [Invariant 18](../architecture/invariants.md): borrowed lifetimes and single-owner publication are
  explicit.

## Validation plan

Focused tests prove authentication-before-decode, source authorization, local-target rejection,
two-frame terminal success, terminal-only empty success, unavailable leader-hint lookup, schema and
sequence rejection without publication, empty-stream rejection, exception containment, and both
response bounds. Allocation injection classifies every owned decode/encode/publication allocation.
Header self-containment, installed consumption, ASan/UBSan, relevant static analysis, formatting,
and the full serialized suite are required before completion.

## Migration or rollback considerations

No bytes change. TLS/session owners may adopt this receiver explicitly after capability selection.
Rollback removes the receiver and leaves the v2 exact carrier available without an execution
handoff; callers must not bypass authentication by calling the worker directly.

## References

- [Distributed Vector Query Transport v2](../formats/distributed-vector-query-transport-v2.md)
- [Authenticated distributed query transport](0168-authenticated-distributed-query-transport.md)
- [Authenticated distributed leader-hint publication](0181-authenticated-distributed-leader-hint-publication.md)
