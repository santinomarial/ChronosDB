# ADR 0127: Composed tablet movement checkpoint recovery

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB distributed-systems and storage maintainers
- **Extends:** [ADR 0123](0123-durable-tablet-movement-snapshot-chunks.md),
  [ADR 0124](0124-tablet-movement-external-prefix-reference-v1.md), and
  [ADR 0126](0126-mixed-tablet-movement-checkpoint-generations.md)
- **Extended by:** [ADR 0128](0128-tablet-movement-rtas-handoff.md)

## Context

The chunk owner and checkpoint owner have separate durable installation boundaries. A crash may
therefore leave more immutable chunks than the latest checkpoint references. The inverse state—a
durable checkpoint that references bytes absent from its exact chunk session—cannot arise from a
correct installation order and must fail closed. Recovery needs one authority rule that handles
both states without rolling progress forward from chunks alone or falling back to an older
self-contained checkpoint.

## Decision

Production reference installation first derives and exact-matches the reference's chunk session,
requires the claimed prefix to be durable and end at an installed chunk boundary, loads and
revalidates exactly that prefix, and calls `TabletMovement::recover`. Only after full semantic and
whole-content checksum validation succeeds may it install the reference generation. The lower-level
reference-generation installer remains a storage primitive, not composed recovery authority.

Recovery dispatches the authoritative loaded generation. Self-contained generations recover from
their embedded prefix and need no chunk owner. Reference generations require the exact session-bound
chunk owner; omitting it fails as unsupported. If the chunk prefix is shorter than a durable
reference claims, or the claimed length falls inside rather than at the end of an installed chunk,
recovery reports corruption.

Chunks beyond the referenced length are valid. They represent the crash window after a later chunk
became durable but before its checkpoint did. Recovery loads only through the exact checkpointed
boundary and ignores that suffix, so chunks alone never advance movement state. Caught-up and later
phases still validate the whole snapshot CRC through `TabletMovement::recover`.

The checkpoint and chunk owners remain independently locked. A caller serializes use of each live
instance. Because installed chunks are immutable and append only, a concurrent suffix append cannot
invalidate a prefix already validated for checkpoint installation; deletion and reclamation remain
outside this decision.

## Rationale and alternatives

The checkpoint generation is the progress authority because it is the only durable object that
binds movement phase, membership, epoch, and received length. Treating the longest chunk prefix as
progress was rejected because a crash could expose bytes the state machine never checkpointed.
Rejecting chunks-ahead was rejected because it would make the intended chunk-before-checkpoint
write ordering unrecoverable. Accepting a checkpoint ahead of chunks was rejected because it would
fabricate missing durable state.

## Consequences and validation

Recovery is deterministic across the two-object crash window: checkpoint-behind is accepted and
bounded by the checkpoint; checkpoint-ahead fails as corruption. Prefix access is linear in the
number and bytes of included chunks and revalidates each included durable record. A requested
interior boundary fails closed.

Invariants 1, 2, 4, 8, 10, 11, 14, and 18 apply. Real-filesystem tests cover chunks ahead across
reopen, missing prefixes before install, durable references ahead of chunks, wrong sessions,
interior boundaries, completed-content checksum failure, and self-contained recovery without a
chunk owner. Syscall fault injection, process-kill matrices, power-loss qualification, reclamation,
and final RTAS/Manifest/CSEG installation remain deferred.

## Migration and rollback

No durable bytes change. Reference-aware software must use composed recovery. Software that only
understands self-contained checkpoints continues to fail closed when a reference is latest. A
rollback must not delete suffix chunks or select an older checkpoint in place.

## References

- [Tablet Movement External-Prefix Reference v1 format](../formats/tablet-movement-checkpoint-reference-v1.md)
- [Tablet Movement Snapshot Chunk v1 format](../formats/tablet-movement-snapshot-chunk-v1.md)
- [Tablet movement checkpoint learning guide](../learning/tablet-movement-checkpoint.md)
