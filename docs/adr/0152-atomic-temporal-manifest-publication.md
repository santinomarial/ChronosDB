# ADR 0152: Atomic temporal Manifest publication

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB manifest, query, and distributed-systems maintainers
- **Extends:** [ADR 0066](0066-atomic-database-storage-publication.md) and
  [ADR 0151](0151-raft-tablet-destination-manifest-composition.md)

## Context

Manifest v2 installation makes a generation recoverable but does not change the immutable epoch
held by live readers. Publishing descriptor fields independently could expose a mixture of
generations. Continuing after disk advances but runtime publication rejects the successor could
also permit later operations to use a stale live authority.

## Decision

`TemporalDatabaseStoragePublisher` is a move-only, single-writer owner of one
`shared_ptr<const LoadedTemporalManifestGeneration>`. Creation exact-decodes the selected owner and
validates complete retained schema coverage. `snapshot()` performs an acquire load and returns one
copyable owning epoch; all bytes and descriptor spans are accessed through that owner.

`publish_manifest()` accepts only a `LoadedTemporalManifestGeneration` returned after durable
Manifest installation. It exact-decodes current and successor bytes, repeats the complete add-only
v2 transition and schema validation, verifies the loaded owner's generation/database identity,
then performs one release store of the successor shared pointer. Initialization and validation of
the immutable successor happen before that release; an acquiring reader therefore observes either
the complete predecessor or complete successor. Held predecessor pointers keep their bytes and
descriptors alive.

Any validation, decoding, or allocation failure after a nonnull durable successor is supplied
poisons the live publisher with release ordering. Subsequent snapshots acquire that state and fail
unavailable. Restart recovery selects the highest durable generation; the publisher does not roll
back disk or guess which transition should be live. A null request is caller error before a durable
successor claim and does not poison the owner.

## Consequences and validation

Publication is constant-time aside from repeated bounded Manifest decoding and transition proof.
The publisher carries no mutable heads or Raft application state; those require a later aggregate
epoch composition before query service can expose a moved tablet.

Real-filesystem tests install and reload an exact empty successor, hold the predecessor across
publication, and verify old/new snapshot generations. A durable generation that skips the live
epoch is rejected and poisons all later snapshots. The full Manifest suite, installed consumer,
and focused ASan/UBSan runs cover the public ownership boundary.

Invariants 2, 5, 6, 8, 11, 14, and 18 apply.

## Migration and rollback

No durable format changes. On rollback, a process that observes a higher installed generation must
recover it normally before service; it must not restore a stale in-memory epoch or delete the
successor.

## References

- [Manifest v2](../formats/manifest-v2.md)
- [Manifest installation and checkpointing](../architecture/manifest-installation-and-checkpointing.md)
