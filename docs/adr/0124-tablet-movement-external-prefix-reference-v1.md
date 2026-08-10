# ADR 0124: Tablet movement external-prefix reference v1

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB distributed-systems and storage maintainers
- **Extends:** [ADR 0117](0117-tablet-movement-checkpoint-v1.md) and
  [ADR 0123](0123-durable-tablet-movement-snapshot-chunks.md)
- **Extended by:** [ADR 0125](0125-tablet-movement-reference-generation-v1.md) and
  [ADR 0127](0127-composed-tablet-movement-checkpoint-recovery.md)

## Context

Tablet Movement Checkpoint v1.0 embeds the complete received prefix, so installing a checkpoint
after each chunk rewrites all prior bytes. The durable chunk owner removes that need, but its files
cannot be trusted from received length alone. A compact checkpoint must bind the exact external
session and retain enough epoch history to find it after placement changes.

The live movement `placement_epoch` advances once when the target is promoted and again when the
source is removed. It therefore stops identifying the original chunk session in later phases.
Changing the accepted v1.0 checkpoint layout or silently reinterpreting its minor version would
violate the frozen durable-format contract.

## Decision

Tablet Movement External-Prefix Reference v1 is a separate canonical value with magic
`CHRMOVR\0`. It retains the movement record fields and adds the original nonzero snapshot-session
placement epoch, but carries no received snapshot bytes. Its 64-byte header protects framing with a
header CRC32C, the payload has a CRC32C, and the trailer covers the complete preceding value.

The format is valid only after a snapshot session begins. Transferring, catching-up, and ready
records must have current placement epoch equal to the session epoch. Target-promoted records must
be exactly one epoch later; complete records exactly two epochs later. Overflow fails closed.
Membership, phase, snapshot boundary, received length, ordering, and configured limits use the same
structural validator as in-memory movement recovery.

Decoding a reference establishes no claim that external bytes exist or match the snapshot CRC. A
recovery owner must derive the exact chunk session, open that session-bound chunk directory, require
its durable received length to equal the record, load the exact prefix, and call full
`TabletMovement::recover` validation. Completed phases thereby require the whole-content CRC before
state adoption.

Tablet Movement Checkpoint v1.0 remains the self-contained encoding and continues to cover the
pre-snapshot adding-target phase. This ADR does not alter its bytes, version policy, or generation
envelope. ADR 0125 defines a distinct generation envelope for external-prefix references.

## Rationale and alternatives

A distinct magic makes compatibility explicit and lets old v1 readers fail closed rather than
misinterpret a new payload. Keeping the original session epoch avoids guessing it from a current
placement that legitimately changed. Repeating the full chunk-session identity was unnecessary
because tablet, source, target, and snapshot boundary are already present in the movement record.

A pathname or directory UUID was rejected because durable identity must come from checksummed
semantic coordinates, not deployment layout. Treating structural validation as byte validation was
rejected because it would authorize missing or substituted external data.

## Consequences and validation

The reference size depends only on replica count, not transferred snapshot size. Existing v1
fixtures remain valid. A generation owner may store both encodings only with explicit envelope-
magic dispatch and must compose reference recovery with the exact durable chunk session before
returning a movement.

Invariants 2, 4, 8, 10, 11, 14, and 18 apply. Focused tests cover partial progress, exact
round-trip, session derivation, recovery with separately supplied bytes, promoted/complete epoch
relations, no-session rejection, corruption, invalid limits, and the distinction between structural
and full byte validation. Durable mixed-format generation storage, old/new recovery,
chunk/checkpoint crash ordering, allocation failure, fuzzing, and large-transfer testing remain
follow-up work.

## Migration and rollback

This format is additive. Rollback retains v1 self-contained checkpoint support but cannot resume a
reference-only generation. Future storage integration must define selection so older readers never
select bytes they can misinterpret.

## References

- [Tablet Movement External-Prefix Reference v1 format](../formats/tablet-movement-checkpoint-reference-v1.md)
- [Tablet Movement Checkpoint v1 format](../formats/tablet-movement-checkpoint-v1.md)
- [Tablet Movement Snapshot Chunk v1 format](../formats/tablet-movement-snapshot-chunk-v1.md)
