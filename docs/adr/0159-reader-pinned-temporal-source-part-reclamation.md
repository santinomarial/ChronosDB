# ADR 0159: Reader-pinned temporal source-part reclamation

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB manifest, storage, and distributed-systems maintainers
- **Extends:** [ADR 0158](0158-reader-pinned-raft-tablet-source-retirement-publication.md)

## Context

An authorized publication yields exact removed descriptors and weak reader pins, but unlinking is a
separate irreversible filesystem mutation. Reclaiming from an orphan scan alone cannot distinguish
an authorized source retirement from incomplete installation or corruption. Trusting only a part
identifier or length could also discard a damaged file without surfacing that durable corruption.

## Decision

`ManifestStorage::reclaim_retired_temporal_parts()` accepts only the move-only publication proof and
an owning selected Manifest v2 generation. It returns pending before filesystem inspection while any
published generation that names a candidate remains alive. Once every weak pin has expired, those
owners cannot be reacquired.

Storage then rescans the locked namespace, requires the supplied generation to remain the durable
maximum, rereads its exact Manifest bytes, and rejects any candidate still referenced by it. Before
the first unlink, every present candidate is reread at its exact published length and its full-file
SHA-256 must match the publication descriptor. Only after the entire candidate set passes does
storage unlink files and synchronize the parts directory. A failure after any unlink poisons the
owner until restart; a failure before the first unlink leaves it usable. Already-absent files make
retries idempotent and are reported explicitly.

The reclaimer shares the existing part-reclamation metrics because both v1 compaction and v2 source
retirement have the same attempt, pending, failure, byte, file, and directory-sync semantics.

## Consequences and validation

Reader lifetime, durable selection, exact authorization, and physical-byte integrity are all
checked at distinct boundaries. The successor never references a removed file, so a crash after an
unlink cannot make selected recovery depend on that file. Directory sync is the acknowledgment
boundary for durable deletion.

Focused tests prove pending behavior, release-triggered deletion, synchronized accounting,
idempotent retry after absence, and rejection of reintroduced damaged bytes before unlink.

Invariants 1–6, 8, 10, 11, 14, and 18 apply.

## Migration and rollback

No durable format change. A removed file can be restored only from an independently verified copy;
older Manifest generations remain non-selected and cannot become rollback authority. Restart-time
discovery and old-generation namespace reclamation remain separate work.

## References

- [Manifest v2](../formats/manifest-v2.md)
- [Manifest installation and checkpointing](../architecture/manifest-installation-and-checkpointing.md)
- [Tablet reconfiguration](../learning/tablet-reconfiguration.md)
