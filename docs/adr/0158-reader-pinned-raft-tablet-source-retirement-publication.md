# ADR 0158: Reader-pinned Raft-tablet source-retirement publication

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB manifest, query-storage, Raft, and distributed-systems maintainers
- **Extends:** [ADR 0157](0157-durable-raft-tablet-source-retirement-installation.md)

## Context

A durable source-retirement Manifest is not yet query-visible truth. Publication must atomically
replace one immutable reader epoch without allowing the ordinary add-only publisher to remove a
tablet. It must also identify when retired source parts are no longer reachable by readers. Pinning
only the immediate predecessor is insufficient: an older held generation may name the same part
after it was retained through several add-only publications.

## Decision

`TemporalDatabaseStoragePublisher::publish_source_retirement_manifest()` is the sole special
publication boundary. It rereads the live predecessor and supplied durable successor, independently
rebuilds the authorized candidate from the completed movement and committed placement, requires
byte-exact equality, validates the successor's exact schema bindings, and release-publishes the
already-durable owner in one atomic pointer swap. The ordinary publication method continues to
reject tablet removal and fails closed when handed such a durable successor.

The single-writer publisher keeps non-owning weak references to every still-live generation it has
published, pruning expired entries before each publication. An authorized retirement returns a
move-only `TemporalRetiredPartSet` containing the exact removed descriptors and weak pins for every
live published generation that still names any one of them. The proof owns no generation and cannot
extend reader lifetime. Reclamation remains forbidden while any pin is live and is a separate
storage mutation.

Readers use acquire loads and the writer uses a release store. A reader that acquires the old epoch
before the swap shares a control block represented by the retirement proof; readers after the swap
can acquire only the successor, which does not name the removed parts. The publisher is explicitly
single-writer, so its weak-generation registry needs no additional synchronization.

## Consequences and validation

Publication and proof issuance cannot diverge: all validating and allocating work completes before
the atomic swap, and no fallible operation follows it. Expired weak owners cannot be reacquired.
Tracking all live published generations prevents premature reclamation across an arbitrary number
of part-retaining successors without keeping those generations alive.

Focused tests publish a retaining intermediate generation, hold its earlier predecessor, prove that
ordinary publication rejects source removal, publish the authorized successor, and verify the
retirement remains pinned until both immediate and older reader epochs are released.

Invariants 1–6, 8, 10, 11, 14, and 18 apply.

## Migration and rollback

No durable format change. A process restart removes all live reader epochs; recovery reconstructs
the publisher from the selected durable generation. A published removal cannot be rolled back by
reselecting older bytes without a new committed placement and Raft membership transition.

## References

- [Manifest v2](../formats/manifest-v2.md)
- [Manifest installation and checkpointing](../architecture/manifest-installation-and-checkpointing.md)
- [Tablet reconfiguration](../learning/tablet-reconfiguration.md)
