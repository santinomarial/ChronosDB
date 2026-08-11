# ADR 0155: Durable physical-part receipt reclamation

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB cluster, storage, and distributed-systems maintainers
- **Extends:** [ADR 0148](0148-durable-physical-part-chunk-receipt.md) and
  [ADR 0154](0154-physical-ownership-gated-tablet-movement-readiness.md)

## Context

Physical CSEG receipt chunks are redundant after their exact final part is installed, query-visible
destination ownership is published, and movement readiness is durable. Deleting chunks without a
terminal durable state permits late retries to recreate the transfer. Deleting in arbitrary order
can also leave a gap that the strict prefix recovery logic must reject after a crash.

## Decision

`reclaim_tablet_physical_part_receipt()` first revalidates one exact ready movement report against
the immutable destination publication. Session table, tablet, group, placement epoch, source,
target, source Manifest generation, part ID, file length, SHA-256, RTAS boundary, and canonical
part-set checksum must all agree. Reclamation is rejected before storage mutation otherwise.

The receipt owner then exact-finalizes the complete object and installs Reclamation Marker v1 by
exclusive temporary creation, exact readback, file synchronization, no-replace rename, and
directory synchronization. Only afterward are chunk finals removed from highest offset to lowest,
with a directory synchronization after every unlink. Thus every crash leaves a valid contiguous
prefix plus the durable marker. Reopen validates the marker against its configured session,
continues deletion, and permanently rejects install, load, and finalize operations for late retries.

An exact already-reclaimed call is successful and reports zero removed chunks. Marker damage or a
different configured session is corruption. A directory-sync failure after marker rename or chunk
unlink poisons the live owner because the crash-durable namespace is uncertain.

## Consequences and validation

Per-chunk directory synchronization is intentionally conservative and may amplify cleanup I/O.
Receipt chunks are bounded and off the query path; changing this policy requires an equally explicit
torn-cleanup recovery protocol and evidence.

Real-filesystem tests cover rejection of incomplete cleanup, terminal marker installation, late
retry rejection, reopen, exact retry, marker corruption, and resumption from a marker plus partial
prefix. The composed movement test proves wrong-phase authority cannot delete and that published
ownership permits deletion and an idempotent readiness/reclamation retry.

Invariants 1–5, 8, 10, 11, 14, and 18 apply.

## Migration and rollback

Existing transfer directories without `RECLAIMED` remain active receipts. A directory containing a
valid marker is terminal and must not be rolled back into an active transfer. Older binaries that do
not recognize the marker must not open a reclaimed receipt namespace.

## References

- [Tablet Physical Part Reclamation Marker v1](../formats/tablet-physical-part-reclamation-marker-v1.md)
- [Tablet Physical Part Chunk v1](../formats/tablet-physical-part-chunk-v1.md)
- [Manifest v2](../formats/manifest-v2.md)
