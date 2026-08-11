# ADR 0190: Reader-pinned tiered local CSEG reclamation

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB manifest, tiering, query, and recovery maintainers
- **Extends:** [ADR 0185](0185-atomic-tiered-storage-publication.md), [ADR 0189](0189-tier-aware-pair-recovery.md)

## Context

A cold route and successful upload do not alone authorize deletion of a local CSEG. The aggregate
pair marker must be durable, every reader that can still require local bytes must drain, the remote
image must remain exact, and the local file must not have changed. Manifest v2 intentionally keeps
the logical descriptor after tiering, so the existing source-retirement reclaimer correctly rejects
this case and cannot be reused by weakening its invariant.

## Decision

`TieredDatabaseStoragePublisher` records one weak entry for every release-published aggregate epoch,
installing that entry before the atomic store. Expired entries are pruned during later publication;
every live epoch remains represented. The single writer can authorize a strictly sorted set of
currently Manifest-referenced Raft parts only when the supplied pair record exactly matches the
current Manifest and cold owners by database/store identity, generation, length, and SHA-256, and
every candidate has an exact cold route.

Authorization captures weak pins only for historical epochs that name a candidate but lack their
own exact cold route. A current or historical reader with an exact route can tolerate deletion: an
already opened local file remains readable, while a later missing-local open follows its pinned
remote route. Once an unsafe epoch's weak pin expires it cannot be reacquired.

`TieredLocalPartReclamationCoordinator::reclaim` returns pending without I/O while any unsafe pin is
live. Otherwise it:

1. reloads the highest consecutive pair marker without fallback and requires byte-for-byte record
   equality with the proof;
2. fully downloads and validates every exact remote CSEG, including object metadata, SHA-256,
   format, schema, tablet, and source;
3. creates a private cross-layer Manifest capability unavailable to ordinary callers;
4. makes `ManifestStorage` reread the exact pair-selected Manifest and require every candidate
   descriptor to remain exactly referenced;
5. prevalidates every present local file's exact length and SHA-256 before the first unlink;
6. unlinks the complete set and synchronizes the parts directory.

Absent local files make retry idempotent. A failure after an unlink poisons `ManifestStorage`; a
failure before mutation leaves it usable. A changed pair marker invalidates the old proof.

The first authorization scope is Raft-owned temporal parts. WAL-owned Manifest startup still uses a
local-only history restoration path, so deleting those files is rejected until that startup path is
tier-aware. Tier-enabled deployments must route every part-opening Raft query through the aggregate
tiered snapshot; bypassing it is unsupported once local reclamation is enabled.

## Consequences and validation

Publication retains one weak pointer per live aggregate epoch, not per reader. Authorization and
reclamation are single-writer operations. The publication memory-order argument remains: history is
updated before release-publication, readers acquire the epoch, and weak expiration proves no new
owner can be acquired from that historical entry. No lock-free progress is claimed.

The integration test holds a pre-cold reader and observes pending with zero remote I/O, releases it,
rejects wrong remote metadata without local mutation, validates and deletes the exact local file,
loads the current query image remotely, and proves idempotent absent-file retry. It also rejects a
reintroduced corrupt local file and a proof whose pair marker is no longer selected. Manifest and
tiering suites plus installed-consumer compilation cover the cross-layer capability.

Invariants 1–3, 6, 8, 10, 11, 14, and 18 apply.

## Alternatives considered

- **Delete immediately after upload:** rejected because neither query authority nor crash recovery
  is committed at that point.
- **Wait for every current reader:** rejected as unnecessarily conservative when a reader already
  owns an exact cold route; only readers lacking a route require the local file.
- **Expose an arbitrary unlink API from ManifestStorage:** rejected because logical Manifest
  references alone cannot authorize physical removal.
- **Trust remote HEAD only:** rejected because full CSEG/schema/source validation is required before
  discarding the local durability source.

## Migration and rollback

Local files remain untouched unless the coordinator is explicitly invoked with a proof for the
currently selected pair. After synchronized deletion, restart must use tiered pair recovery. Restore
of the exact bytes to their canonical local names is required before rollback to standalone
Manifest recovery or local-only operation.

## References

- [Tiered CSEG loading](../learning/tiered-cseg-loading.md)
- [Tiered Pair Commit v1](../formats/tiered-pair-commit-v1.md)
- [Architecture invariants](../architecture/invariants.md)
