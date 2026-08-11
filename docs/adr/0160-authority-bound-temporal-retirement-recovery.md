# ADR 0160: Authority-bound temporal retirement recovery

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB manifest, recovery, Raft, and distributed-systems maintainers
- **Extends:** [ADR 0159](0159-reader-pinned-temporal-source-part-reclamation.md)

## Context

The live publication proof is intentionally non-durable because weak reader ownership has meaning
only within one process lifetime. A crash after source-retirement publication but before part
reclamation must not strand files forever. Conversely, restart must not treat every unreferenced
part as authorized deletion work.

## Decision

`ManifestStorage::recover_temporal_source_retirement()` reconstructs a proof only from durable
history plus the same completed movement and committed final-placement authority required during
installation and publication. It requires the supplied selected owner to remain the exact durable
maximum, rereads that Manifest byte for byte, and scans consecutive generations without trusting
orphan filenames.

For each decodable Manifest v2 predecessor, recovery independently rebuilds the one authorized
source-retirement successor. Exactly one adjacent durable generation must equal those canonical
bytes. Every v2 predecessor filename must agree with its encoded generation. Unsupported older v1
generations may precede the v2 history, but corruption and resource failures are not skipped. The
current selected generation must still omit both the retired tablet and every exact retired part
identity.

The resulting `TemporalRetiredPartSet` has no live weak pins: process restart proves that all reader
owners from the previous process are gone. It is still only input to the ordinary temporal
reclaimer, which repeats selected-Manifest, absence, length, and SHA-256 validation before unlink.

## Consequences and validation

Restart closes the liveness gap without weakening authorization. A wrong source, stale placement,
missing transition, ambiguous duplicate transition, reintroduced tablet, or reintroduced part fails
closed. The scan is linear in retained generation count and keeps at most two history images plus
one decoded view at a time, in addition to the caller-owned selected generation.

Focused tests durably install retirement, release the original storage owner, reopen from disk,
reject mismatched authority, reconstruct the exact unpinned descriptor set, and reclaim it through
the same verified deletion path.

Invariants 1–6, 8, 10, 11, 14, and 18 apply.

## Migration and rollback

No durable format change. Recovery depends on retaining the adjacent predecessor/successor pair;
future Manifest-generation reclamation must not remove that history until it has installed its own
durable retirement completion authority.

## References

- [Manifest v2](../formats/manifest-v2.md)
- [Manifest installation and checkpointing](../architecture/manifest-installation-and-checkpointing.md)
- [Tablet reconfiguration](../learning/tablet-reconfiguration.md)
