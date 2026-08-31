# ADR 0564: Authoritative bounded live-flush identity allocation

- **Status:** accepted
- **Date:** 2026-08-31
- **Owners:** ChronosDB single-node, Manifest, CSEG, and common-foundation maintainers

## Context

The live single-node sealed-head owner generated one `PartId`, one part-candidate nonce, and one
Manifest-candidate nonce for every flush. It required the three UUIDs to be nonzero and mutually
distinct, but compared the `PartId` only with the other two newly generated values. A deterministic
or broken source could therefore return an identity already present in the selected Manifest or as
an orphan final. The durable installer would eventually reject the no-replace rename, but only
after encoding, creating, writing, reading back, and synchronizing a temporary part. Repeated
collisions also had no caller-configured owning-domain bound or injectable service-level test seam.

An interrupted installation can leave a recognized temporary. Although that name is not durable
object identity, reusing its `PartId` contradicts the accepted fresh-candidate retry policy and can
collide again if the same nonce is repeated. Manifest candidates have the analogous exact
`(generation, nonce)` temporary-name collision.

## Decision

`SingleNodeDatabase` owns live Manifest-v1 flush identity allocation. Its configuration accepts an
optional borrowed, thread-affine `UuidGenerator`; null selects the OS-backed generator. A nonzero
per-identity candidate limit is validated before database-root creation.

Immediately before invoking `SealedHeadFlushCoordinator`, the owner scans the locked Manifest
namespace. A part candidate is accepted only when it is nonnil and its exact UUID bytes are absent
from every final part and from the `PartId` component of every recognized temporary part. A part
nonce is nonnil, differs from the part identity, and does not reproduce an existing exact candidate
basename. A Manifest nonce is nonnil, differs from both earlier values, and does not reproduce an
existing temporary for the exact next generation. The namespace is single-writer and thread-affine,
so it cannot change between this scan and the coordinator call.

Each identity has the configured finite attempt budget. A source error propagates immediately.
Nil or colliding candidates are skipped; exhaustion returns `RESOURCE_EXHAUSTED` before the
coordinator acquires queue work or creates any part/Manifest candidate. The ready sealed head keeps
its original queue identity and can be retried with later fresh candidates.

The lower storage protocol remains authoritative for exact prevalidation, exclusive creation,
readback, synchronization, and no-replace installation. This allocation preflight does not turn a
same-identity final into success and does not weaken its independent revalidation.

## Consequences

A finite run of source collisions no longer reaches filesystem mutation. Exhaustion is bounded and
observable, while the queued sealed head and selected Manifest remain unchanged. The scan and
temporary-name formatting are linear in the bounded locked namespace and occur only on the cold
flush path.

Native ingest can already have a WAL-durable mutation when synchronous flush allocation fails. It
continues to return an error, and the existing durable retry identity resolves that ambiguity; this
decision changes no acknowledged durability mode.

This is not a global UUID registry. Table/catalog UUIDs and CSEG `PartId` values remain separate
authorities. The decision covers only live single-node Manifest-v1 sealed-head output and its two
operation nonces. WAL IDs, append-only compaction outputs, Manifest-v2/temporal parts, physical
movement, retry identities, distributed-control identities, and future deletion/no-reuse policy
remain separate work.

## Validation

Focused owner coverage injects a script that returns nil, repeats the current allocation values,
and then repeats the selected first part identity before supplying a fresh second part. Both flushes
complete with the expected exact `PartId` values. Retained recognized part and next-generation
Manifest temporaries prove both namespace collision paths select later candidates without deleting
the earlier forensic state. A second script repeats the selected part through the exact attempt
limit: the owner returns `RESOURCE_EXHAUSTED`, the selected generation, final part count, and both
temporary counts remain unchanged, and a later call consumes the same queued work with fresh
identities. A zero attempt limit is rejected before the database root is populated.

## References

- [ADR 0017](0017-manifest-generations-installation-and-checkpoints.md)
- [ADR 0230](0230-live-single-node-sealed-head-flush.md)
- [Manifest v1](../formats/manifest-v1.md)
- [Manifest installation and checkpointing](../architecture/manifest-installation-and-checkpointing.md)
- [Durable sealed-head flush coordination](../learning/sealed-head-flush-coordinator.md)
- [Recoverable single-node database owner](../learning/single-node-database-owner.md)
