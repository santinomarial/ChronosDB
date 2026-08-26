# ADR 0526: Correlated grouped shuffle result success acknowledgment

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB distributed-query and networking maintainers
- **Extends:** [ADR 0525](0525-authenticated-complete-grouped-shuffle-result-stream.md)

## Context

A reducer finishing its local TLS writes does not prove that the coordinator authenticated and
retained the terminal-closed result. EOF also cannot distinguish acceptance from truncation. A
future finite retry owner therefore needs one exact positive application receipt.

The first result-frame implementation fingerprinted the complete canonical schema frame including
its own trailing CRC. That computes a CRC residue instead of a content fingerprint, so an empty
terminal had no schema-drift defense beyond the ineffective value.

## Decision

Adopt fixed `CHDVGRK1` 1.0 bytes as specified by the
[result acknowledgment format](../formats/distributed-vector-grouped-aggregate-shuffle-result-ack-v1.md).
The receipt reverses the result route and binds query, reducer source, coordinator target,
partition, partition count, hash version, canonical raw result-schema fingerprint, accepted frame
count, and accepted encoded byte count. Header and complete-frame CRC32C cover every field.

Correct the `CHDVGRR1` schema fingerprint at the same time to hash canonical schema bytes excluding
their trailing CRC. The layout and version do not change; this makes the already-declared schema
identity field enforce its intended semantics before a connected session can use it.

The coordinator may construct the receipt only after one-shot extraction of a complete authorized
result stream. The reducer accepts success only when the receipt extent equals its immutable
sender. Failure closes the attempt. The fixed reader and move-only write cursor are bounded and
single-thread-affine. TLS, TCP, retry, deduplication, and lifecycle composition remain separate.

## Consequences

Result return now has a canonical application success boundary suitable for mutual-TLS composition.
A receipt lost after acceptance can cause a byte-identical retry; the later collector must make
duplicate installation idempotent.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): success names one immutable query partition and
  exact raw result schema.
- [Invariant 10](../architecture/invariants.md): header and frame CRC32C cover all interpreted
  receipt fields.
- [Invariant 14](../architecture/invariants.md): explicit 1.0 compatibility rejects unknown
  versions.
- [Invariant 15](../architecture/invariants.md): receipt size and accepted stream extent are
  bounded.
- [Invariant 18](../architecture/invariants.md): transport write completion cannot substitute for
  coordinator acceptance.

## Validation plan

Round-trip the reverse route and schema fingerprint at every fragmented read split and through
short writes. Reject invalid extents, damage, authority drift, schema drift, and checksum-valid
unknown versions. Sweep encoding and cursor allocations. Run cluster, allocation-failure,
sanitizer, formatting, static-analysis, and diff gates.

## Migration or rollback considerations

No existing durable or network bytes change. Rollback removes only this unused additive receipt;
result-return success must remain unavailable until an equivalent application acknowledgment
exists.

## Unresolved questions

- Compose the receipt with an authenticated mutual-TLS result stream.
- Add result-return TCP ownership and finite exact retry.
- Install complete remote partitions idempotently into global gathering.

## References

- [Result acknowledgment format](../formats/distributed-vector-grouped-aggregate-shuffle-result-ack-v1.md)
- [Result stream decision](0525-authenticated-complete-grouped-shuffle-result-stream.md)
