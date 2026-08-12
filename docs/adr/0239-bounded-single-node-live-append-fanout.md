# ADR 0239: Bounded Single-Node Live Append Fan-Out

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB service, live-query, and ingest maintainers

## Context

The single-node database reports each newly applied append through a non-rejecting observer, and
the committed-batch evaluator can derive one deterministic result for a row-preserving subscription
plan. Neither component decides which durable plan coordinators should observe a table mutation.
That routing boundary must remain finite and must not turn a failure after commit into a false write
failure or a resumable subscription gap.

## Decision

`SingleNodeLiveAppendFanout` owns a bounded, fixed vector of borrowed bindings. Each binding names
one immutable prepared plan, one durable multi-tablet coordinator, one query resource context, and
finite evaluator limits. Creation rejects null owners, duplicate plan fingerprints, mismatched
plan/coordinator identity, excessive binding counts, and physical plans whose stateful stages cannot
be evaluated independently per append.

On the single database-owner thread, one observed append is routed only to coordinators with the
same table and exact tablet/WAL source. A matching schema is evaluated and published. A different
schema is published through the coordinator's existing incompatible-schema transition. Evaluation
or publication failure invokes `mark_continuity_lost` at the exact applied source position. If even
that containment transition fails, the affected binding is permanently disabled. The callback is
`noexcept`, and metrics distinguish observation, evaluation, publication, invalidation, continuity
loss, and containment failure.

Bindings and all borrowed owners must outlive the fan-out. The fan-out and its metrics are
thread-affine; no atomics or internal locks imply cross-thread authority. Dynamic registration,
retirement, and daemon connection routing belong to the service-runtime composition rather than
this post-apply primitive.

## Consequences

Every supported applied append either becomes an exact logical result, terminates the old plan on
schema change, or explicitly destroys old replay continuity. The already-committed write result is
never rewritten. Admission is deliberately conservative: aggregate, grouped, sorted, LATEST, and
LIMIT plans must use retained incremental state before they can enter this fan-out.

The fixed vector makes routing linear in configured plan count. A different index requires profile
evidence and must preserve deterministic owner-thread ordering. Startup replay is not observed;
coordinator recovery and source-log suffix replay remain separate responsibilities.

## Validation

Focused service tests prove a filtered projection reaches an active subscriber, oversized result
publication advances the source while overflowing the subscriber, unsupported plans and duplicate
bindings fail admission, and the public header is self-contained. The complete service and live
unit-test executables remain green.

## References

- [ADR 0236](0236-committed-append-subscription-result-changes.md)
- [ADR 0237](0237-single-node-applied-append-observation.md)
- [ADR 0238](0238-fail-closed-subscription-continuity-loss.md)
- [Committed append subscription evaluation](../learning/committed-append-subscription-evaluation.md)
