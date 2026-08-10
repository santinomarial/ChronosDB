# ADR 0126: Mixed tablet movement checkpoint generations

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB distributed-systems and storage maintainers
- **Extends:** [ADR 0118](0118-durable-tablet-movement-checkpoint-generations.md) and
  [ADR 0125](0125-tablet-movement-reference-generation-v1.md)

## Context

A movement begins before snapshot metadata exists, so its first durable generation uses the
self-contained `CHRMOVG` envelope. After transfer begins, compact progress uses `CHRMVRG`. Recovery
needs one monotonic sequence across that transition without trusting filenames to select nested
semantics or weakening the existing immutable installation protocol.

## Decision

`TabletMovementCheckpointStorage` retains its tablet-owned locked directory, canonical
`generation-<20-digit-generation>.movc` names, contiguous-from-one rule, and temporary/write/
readback/file-sync/no-replace-rename/directory-sync protocol. Each final file is now dispatched by
its exact first eight magic bytes: `CHRMOVG\0` invokes self-contained generation v1 decode and
`CHRMVRG\0` invokes external-reference generation v1 decode. Any other magic is unsupported; a
recognized envelope that fails exact decode is corruption or its precise decoder error.

Both alternatives must embed the filename generation and configured tablet. They share one next-
generation admission rule, byte-identical retry contract, conflicting-byte rejection, temporary
namespace, and directory durability boundary. Checkpoint and reference codec configurations must
use identical movement limits so changing envelope type cannot change accepted snapshot or replica
semantics.

`load_any_generation` and `load_latest_any` return the exact variant. The original typed load APIs
remain available for self-contained callers but return `NOT_SUPPORTED` when the selected generation
requires external-prefix recovery; they never silently discard the reference or fall back to an
older self-contained generation.

This owner establishes durable ordering and format identity only. Returning a reference variant is
not movement recovery authority until its exact chunk session and received length are composed and
fully validated.

## Rationale and alternatives

One sequence avoids a second latest pointer or cross-directory ordering transaction. Magic-based
dispatch prevents rename from changing interpretation. Preserving the old typed API makes the
compatibility failure explicit while general callers migrate to variant handling.

Selecting the newest decodable legacy generation was rejected because it would roll back movement
progress after a newer authoritative reference. Using a filename suffix per format was rejected
because suffix and embedded bytes could disagree. Using different movement limits per alternative
was rejected because generation transition could otherwise change validity.

## Consequences and validation

The generation sequence may transition formats without gaps and uses the same established POSIX
durability boundary. Older software fails closed on a latest reference rather than resuming stale
state. Reference-aware software must use the general load API.

Invariants 1, 2, 4, 8, 10, 11, 14, and 18 apply. Real-filesystem tests cover old-to-new generation
transition, general and typed selection, exact reference retry, same-generation conflict, reopen,
and copied reference bytes renamed to another generation. Composed chunk recovery, crash/fault
injection, process-kill points, power-loss qualification, reclamation, allocation failure, and
mixed-version executable testing remain deferred.

## Migration and rollback

Existing all-`CHRMOVG` directories remain valid. Installing the first `CHRMVRG` generation is the
upgrade boundary. Rollback software must fail closed if that generation is latest; an offline
migration may install a new explicitly validated self-contained generation but must never delete or
skip the newer coordinate in place.

## References

- [Tablet Movement Checkpoint v1 format](../formats/tablet-movement-checkpoint-v1.md)
- [Tablet Movement External-Prefix Reference v1 format](../formats/tablet-movement-checkpoint-reference-v1.md)
- [POSIX I/O learning guide](../learning/posix-io.md)
