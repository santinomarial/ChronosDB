# ADR 0185: Atomic Manifest v2 and cold-location publication

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB storage, tiering, query, and recovery maintainers
- **Extends:** [ADR 0152](0152-atomic-temporal-manifest-publication.md), [ADR 0184](0184-durable-cold-location-generations.md)

## Context

Manifest v2 and Cold Location Manifest generations cross independent filesystem durability
boundaries. Loading or publishing their pointers independently lets a reader observe a logical
Manifest with cold keys for a different generation. It also allows an old query's location strings
to disappear while the query still plans or performs object reads.

## Decision

`TieredDatabaseStoragePublisher` release-publishes one immutable shared epoch containing exactly one
owning `TemporalDatabaseStorageSnapshot` and zero or one owning loaded cold generation. Readers
acquire that one shared epoch. They therefore observe the complete old pair or complete new pair,
never a mixed pair, and reference counting retains both owners until the last reader releases its
snapshot.

Creation re-decodes both owners and requires exact database/generation/part-byte binding. Ordinary
publication permits the same Manifest v2 bytes with the same or exact next cold generation, or the
exact next valid Manifest v2 generation with compatible cold authority. Equal generation numbers
must have equal bytes. Cold authority cannot disappear wholesale once published; a successor is
exactly consecutive and may omit an individual route only under the newer logical-Manifest proof in
[ADR 0191](0191-manifest-retirement-bound-cold-route-removal.md). A claimed durable successor that
cannot be validated poisons the publisher so
restart recovery, rather than in-process rollback, resolves durable truth.

The memory-ordering argument is direct: the single writer completely initializes an immutable epoch,
then stores its shared pointer with release ordering. Readers load that same pointer with acquire
ordering before dereferencing any fields. The release/acquire edge makes all epoch initialization
visible, and shared ownership prevents destruction until every acquiring reader is done. No field
inside an epoch is mutated and the primitive does not claim lock-free progress.

This is an in-memory publication boundary. It does not make two directory renames crash-atomic.
[ADR 0186](0186-durable-tiered-pair-commit.md) now supplies the durable pair-commit/recovery
protocol and explicit old/new selection. [ADR 0190](0190-reader-pinned-tiered-local-reclamation.md)
uses the aggregate epoch history to authorize local CSEG reclamation separately.

## Consequences and validation

Every reader pays one atomic shared-pointer acquisition and retains one aggregate epoch rather than
two unrelated owners. Cold-only changes do not require rewriting Manifest v2, but every published
cold generation still binds to the exact current logical generation. A base generation advance must
also supply compatible cold authority whenever the preceding epoch had cold authority.

Focused tests use real durable Manifest and cold storage owners. They prove cold-only and paired
advancement, predecessor lifetime retention through a weak-owner observation, concurrent old/new
reader pairing, and fail-closed rejection of a durable cold/base mismatch. Installed public-target
compilation covers the exported API. TSan and durable two-directory crash-matrix coverage remain
release-qualification obligations.

Invariants 2, 3, 6, 8, 11, 14, and 18 apply.

## Alternatives considered

- **Publish two atomic pointers:** rejected because independently consistent values do not form one
  consistent pair.
- **Look up the latest cold generation per object read:** rejected because one query could change
  routing mid-snapshot.
- **Copy object keys into every query plan:** rejected as the authority/lifetime boundary and because
  it duplicates variable bytes without solving compatible acquisition.
- **Allow cold authority removal:** rejected until a restore/local-presence proof exists.

## Migration and rollback

Creation with no cold owner preserves local-only behavior. Once cold authority is published, normal
successors retain it. Rollback to local-only routing requires a separately authorized restore proof;
it is not represented as an ordinary publication.

## References

- [Durable cold-location manifests](../learning/cold-location-manifest-storage.md)
- [Atomic temporal Manifest publication](0152-atomic-temporal-manifest-publication.md)
- [Architecture invariants](../architecture/invariants.md)
