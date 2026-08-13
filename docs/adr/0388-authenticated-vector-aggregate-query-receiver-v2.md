# ADR 0388: Authenticated vector aggregate query receiver v2

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, cluster, and networking maintainers
- **Extends:** [ADR 0369](0369-authenticated-schema-bound-vector-query-receiver-v2.md),
  [ADR 0387](0387-definition-bound-vector-aggregate-query-response-v2.md)

## Context

The aggregate response carrier freezes definition-bound state bytes but does not authenticate the
request, select current local definition authority, or define an all-or-nothing worker handoff. A
Fragment-v2 result schema cannot reconstruct COUNT/AVG input types, so a receiver cannot safely
invent definitions from request output descriptors. It also cannot encode a success stream from
only a frame-count bound because each aggregate state can approach one MiB.

## Decision

`DistributedVectorAggregateQueryReceiverV2` rejects absent authentication before request decode,
authorizes the authenticated principal for the claimed source node, exact-matches the local target,
and rejects every plan mode except `UNGROUPED_AGGREGATE` before definition binding or execution.

The embedding-owned worker service has two explicit calls. `bind_definitions` derives the complete
ordered definition vector from current local authority. The receiver validates its width,
operations, input presence/ordinals, and output type/nullability against the admitted plan and
result schema. Only then does `execute` acquire and reprove worker authority. Execution returns its
independently derived definition vector beside the complete state vector; the receiver exact-matches
the two vectors so an authority change cannot silently alter state interpretation. A definition
binding failure returns locally without publishing a carrier because no valid definition authority
exists for the mandatory response API.

A successful result contains exactly one message per definition with exact query/tablet identity,
ordinal `i`, sequence `i + 1`, and terminal only on the last message. The receiver encodes and
retains every response before returning any. It applies a frame ceiling no greater than 4,096 and a
total exact encoded-byte ceiling between the 116-byte failure size and a one-GiB hard maximum; the
default is 64 MiB. An otherwise valid result over either bound becomes one correlated
`RESOURCE_EXHAUSTED` response.

Worker errors after definition binding become one correlated status response. `UNAVAILABLE` may
consult the committed leader-hint provider. Worker allocation exceptions become
`RESOURCE_EXHAUSTED`; other exceptions become `INTERNAL`. Malformed worker values and later
encoding/allocation failures expose no prefix. The receiver synchronously borrows the authorizer,
worker, and optional hint provider; TLS, descriptors, retries, and execution implementation remain
separate owners.

## Alternatives considered

- **Infer definitions from result columns:** rejected because COUNT and AVG outputs do not identify
  their input logical type.
- **Trust only execution-returned definitions:** rejected because a malicious or defective worker
  could select the authority used to validate its own states.
- **Bind definitions once and skip execution comparison:** rejected because authority can change
  between binding and execution.
- **Publish frames while validating later states:** rejected because a later mismatch would expose
  partial aggregate success.

## Consequences

Unauthenticated, unauthorized, misrouted, and wrong-mode requests cannot reach binding or execution.
Definition authority exists before a response can be encoded, and execution must independently
agree with it. Retained response storage is bounded by exact bytes and count. Binding and execution
may acquire local authority twice; immutable schema identity makes stable definitions comparable,
while a change fails closed.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): request and complete response retention remain
  finitely bounded.
- [Invariant 10](../architecture/invariants.md): definitions are derived from local authority and
  checked against admitted plan/result shape.
- [Invariant 11](../architecture/invariants.md): the complete authority-bound dispatch reaches the
  proof-revalidating worker without weakening snapshot identity.
- [Invariant 14](../architecture/invariants.md): authenticated route, query/tablet, definition,
  sequence, and terminal identity remain explicit.
- [Invariant 15](../architecture/invariants.md): authentication, source authorization, local route,
  definition binding, and execution authority are distinct fail-closed gates.
- [Invariant 18](../architecture/invariants.md): borrowed service lifetimes and all-or-nothing
  response ownership are explicit.

## Validation plan

Prove authentication-before-decode, source authorization, local-target and plan-mode rejection,
definition checks before execution, two-state terminal success, unavailable leader hints, worker
exception containment, frame and exact-byte bounds, and contract mismatch without publication.
Inject every owned binding/execution/publication allocation failure. Run the public header,
formatting, static analysis, ASan/UBSan, installed consumer, and full serialized suite gates.

## Migration or rollback considerations

This adds no bytes. A TLS/session owner adopts this receiver only on the aggregate capability
boundary. Rollback removes the handoff and leaves the exact carrier available; callers must not
bypass authentication by invoking the worker service directly.

## References

- [Distributed Vector Aggregate Query Transport v2](../formats/distributed-vector-aggregate-query-transport-v2.md)
- [Definition-bound vector aggregate query response v2](0387-definition-bound-vector-aggregate-query-response-v2.md)
- [Authenticated schema-bound vector query receiver v2](0369-authenticated-schema-bound-vector-query-receiver-v2.md)
