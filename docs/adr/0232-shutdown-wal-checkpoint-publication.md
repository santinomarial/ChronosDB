# ADR 0232: Shutdown WAL Checkpoint Publication

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB single-node, WAL, Manifest, and recovery maintainers

## Context

Live sealed-head flush advanced per-tablet durable boundaries but deliberately preserved the global
Manifest reclaim checkpoint. The existing checkpoint proof requires a stable WAL directory and
acquires its process lock, so running it while the live commit coordinator owns the writer would
violate WAL ownership and race append/rotation.

## Decision

Orderly single-node shutdown now performs:

1. drain all ready sealed heads;
2. stop and close the WAL commit coordinator;
3. acquire one aggregate storage snapshot;
4. construct an exact metadata-only next Manifest candidate retaining every tablet, part, retry,
   and the old checkpoint;
5. reload and validate every selected CSEG image;
6. run `build_manifest_v1_checkpointed_generation()` against the now-locked stable WAL directory;
7. if the coordinate advanced, install and aggregate-publish that exact next generation; and
8. destroy flush/Manifest ownership and finish Raft/root shutdown.

No generation is installed when the proof finds no longer prefix. A durable checkpoint-only
generation uses an empty sealed-head replacement set because its part and head identities are
unchanged. Startup enables the existing conservative covered-segment reclamation path; it may
remove only closed segments entirely covered by that selected coordinate and never the active
highest segment.

## Consequences

The global checkpoint advances only under quiescent WAL ownership and exact CSEG/WAL/retry proof.
Recovery begins after that coordinate and replays only the uncovered suffix. Segment deletion is
restart cleanup, so a crash after checkpoint publication can retain extra safe WAL bytes without
losing authority.

Abrupt process death before orderly shutdown may leave a conservative older checkpoint even though
parts are durable. Background online checkpointing would require a new stable-WAL inspection
capability or explicit writer handoff; it is not inferred here.

## Validation

The focused service flush case installs generation 2 for a four-row part, shuts down, then reopens
generation 3 with reclaim sequence 2 and a two-row record-3 suffix. Complete native `count(*)`
remains six before and after restart. Existing checkpoint-builder and startup-recovery suites retain
their exact row/content and reclamation validation.

## References

- [ADR 0017](0017-manifest-generations-installation-and-checkpoints.md)
- [ADR 0230](0230-live-single-node-sealed-head-flush.md)
- [Manifest installation and checkpointing](../architecture/manifest-installation-and-checkpointing.md)
