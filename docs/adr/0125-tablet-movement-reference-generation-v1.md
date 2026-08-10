# ADR 0125: Tablet movement reference generation v1

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB distributed-systems and storage maintainers
- **Extends:** [ADR 0124](0124-tablet-movement-external-prefix-reference-v1.md)
- **Extended by:** [ADR 0126](0126-mixed-tablet-movement-checkpoint-generations.md)

## Context

External-prefix reference bytes need the same immutable, contiguous recovery ordering as existing
self-contained movement checkpoints. The accepted `CHRMOVG\0` v1.0 envelope explicitly contains a
Tablet Movement Checkpoint v1 value. Broadening its frozen meaning to another nested format would
let an old version parse framing it does not semantically understand.

## Decision

Tablet Movement Reference Generation v1 is a distinct envelope with magic `CHRMVRG\0`, major 1,
minor 0. Its 64-byte header binds a nonzero generation, exact nested reference size, nested CRC32C,
and header CRC32C. The nested value is exactly one Tablet Movement External-Prefix Reference v1,
and a four-byte trailer CRC32C covers the header and nested bytes.

The complete envelope fits the same configured reference limit, never above 1 MiB. Unknown
versions, zero generation, reserved bytes, size disagreement, checksum failure, trailing bytes, or
an invalid nested reference fail closed. The envelope alone does not define filenames, mixed-format
selection, or chunk recovery authority.

## Rationale and alternatives

Distinct magic preserves explicit compatibility: `CHRMOVG` remains self-contained, while
`CHRMVRG` always requires external chunk composition. Reusing the old envelope with a nested-kind
flag was rejected because no reserved field can acquire meaning without changing its exact v1.0
contract. Filename-only type dispatch was rejected because renaming could change interpretation.

## Consequences and validation

The generation storage owner inspects exact envelope magic and applies format-specific decode while
retaining one filename generation coordinate. Rollback readers reject reference generations rather
than misinterpreting them.

Invariants 2, 8, 10, 14, and 18 apply. Focused tests exact-round-trip generation and nested
reference identity and reject zero generation and nested-byte damage. ADR 0126 implements mixed-
format durable storage, generation continuity, and rename binding. Composed chunk recovery, crash
points, allocation failure, and fuzzing remain follow-up work.

## Migration and rollback

This is an additive envelope. No `CHRMOVG` bytes change. Once a reference generation becomes the
latest durable recovery point, software without `CHRMVRG` support cannot safely resume it and must
fail closed or use an explicit offline migration.

## References

- [Tablet Movement External-Prefix Reference v1 format](../formats/tablet-movement-checkpoint-reference-v1.md)
- [ADR 0118](0118-durable-tablet-movement-checkpoint-generations.md)
