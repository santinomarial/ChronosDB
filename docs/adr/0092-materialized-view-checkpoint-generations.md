# ADR 0092: Materialized-view checkpoint generations

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB live-query and durability maintainers
- **Extends:** [ADR 0090](0090-materialized-view-checkpoint-v1.md) and
  [ADR 0091](0091-durable-materialized-view-checkpoint-storage.md)

## Context

The first storage grammar keyed immutable files only by applied WAL sequence. Watermarks can advance,
finalize windows, and increment output revisions without another source record. Two valid durable
states can therefore share one WAL position, making same-sequence immutable installation conflict
and preventing a clean shutdown from preserving the later watermark state.

## Accepted decision

Bound Materialized View Checkpoint minor 1 adds a nonzero monotonic checkpoint generation at header
offset 148 and retains four reserved zero bytes at 156. Minor 0 remains decodable and requires all
twelve bytes from 148 to be zero. The nested logical state remains byte-for-byte format 1.0.
Encoding uses bound minor 0 only for an explicit legacy generation zero; every production owner
uses minor 1.

Generation files are named `generation-<20-digit-generation>.mvcg`. The locked storage owner exact-
matches the minor-1 generation to its filename and selects the highest valid generation whenever
one exists. For compatibility it continues to parse legacy `checkpoint-<20-digit-WAL-sequence>.mvcp`
minor-0 files and selects their highest sequence only when no generated file exists. Recognized
temporaries for both grammars receive the same cleanup and sync protocol.

Once any generated file exists, a new legacy file or a previously absent lower generation is
rejected as backward durable movement. Exact same-byte retries of an already installed older file
remain idempotent and cannot change latest selection.

Checkpoint generation orders durable view states only; it is not a source commit position, event
time, watermark, or external delivery sequence. The nested state retains the authoritative source
WAL boundary used for suffix replay.

## Consequences and alternatives

Multiple checkpoints may safely preserve watermark-only progress at one WAL boundary. Opening a
legacy directory permits the next application owner to install generation 1 without rewriting or
reinterpreting the old file.

Using watermark as the filename coordinate was rejected because watermark values are signed and may
advance without changing other state, while future state changes may not be watermark-driven. Using
a content hash alone was rejected because it does not define recovery order. Replacing one sequence
file in place was rejected because it violates immutable installation and crash clarity.

## Affected invariants and validation

Invariants 1, 4, 8, 10–15, and 17 apply. Focused codec tests round-trip minor-1 generation identity.
Filesystem tests install legacy checkpoints, then two generated checkpoints at the same source
sequence with the latter carrying watermark finalization, select generation 2 before and after
reopen, and preserve legacy decoding. Wraparound, mixed-version binaries, crash points, and obsolete-
generation reclamation remain in the Phase 18 ledger.
