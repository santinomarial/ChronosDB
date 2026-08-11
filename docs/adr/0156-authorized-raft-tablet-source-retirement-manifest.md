# ADR 0156: Authorized Raft-tablet source-retirement Manifest

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB manifest, Raft, and distributed-systems maintainers
- **Extends:** [ADR 0132](0132-durable-tablet-reconfiguration-phase-checkpoints.md) and
  [ADR 0155](0155-durable-physical-part-receipt-reclamation.md)

## Context

The add-only Manifest v2 transition correctly forbids ordinary callers from removing a tablet or
its history. Once joint consensus has removed the old replica, committed metadata has published the
same final placement, and the durable movement is complete, the old source nevertheless needs a
precise successor that stops exposing its local tablet. Treating local movement phase alone as
authority could remove the source before consensus or routing state is safe.

## Decision

`build_raft_tablet_source_retirement_manifest()` is the pure composition boundary for this special
transition. It requires a structurally valid completed movement and a committed final placement
whose table, tablet, epoch, canonical replicas, and valid leader hint agree exactly. The named source
must be absent, the target must remain, and the selected Manifest tablet must be Raft-owned by the
named group.

The builder emits exactly generation `N+1`, preserves database identity and WAL reclamation state,
copies every foreign tablet, part, and retry descriptor unchanged, rebuilds canonical part indexes,
and removes only the retired tablet's descriptors and protected retries. It returns the exact
removed part descriptors for the later lifetime-pinned reclamation boundary.

This result is not installation, publication, or file-deletion authority. Those steps must
independently revalidate the completed Raft/metadata proof and keep predecessor readers safe.

## Consequences and validation

The dedicated builder avoids weakening the ordinary add-only validator. It also makes the removed
set explicit rather than rediscovering deletion candidates from unreferenced files.

Focused tests cover canonical two-tablet removal with exact foreign-state preservation and reject
both stale pre-removal placement and a movement that is not complete.

Invariants 1–6, 8, 10, 11, 14, and 18 apply.

## Migration and rollback

No format change. A built candidate has no authority until the later installation protocol makes it
durable. After publication, rollback must not re-expose the removed source without a new committed
placement and Raft membership transition.

## References

- [Manifest v2](../formats/manifest-v2.md)
- [Tablet reconfiguration](../learning/tablet-reconfiguration.md)
- [Manifest installation and checkpointing](../architecture/manifest-installation-and-checkpointing.md)
