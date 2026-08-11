# ADR 0151: Raft tablet destination Manifest composition

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB manifest, cluster, and distributed-systems maintainers
- **Extends:** [ADR 0146](0146-raft-tablet-physical-snapshot-projection.md) and
  [ADR 0150](0150-verified-physical-part-destination-installation.md)

## Context

A validated one-tablet physical snapshot carries a source Manifest generation. The destination
cannot install that projection as its database Manifest: its own generations, other tablets,
global WAL checkpoint, descriptor ordering, and database identity are independent durable state.
Re-encoding the projection with a destination generation while dropping local state would violate
snapshot and recovery authority.

## Decision

`build_raft_tablet_destination_manifest()` accepts one decoded selected destination generation and
one untrusted source projection bound to full Raft `SnapshotMetadata`. It repeats exact projection
validation, requires the same database identity, and admits only a tablet absent from the selected
destination. Existing-tablet replacement needs a separate equivalence and epoch transition proof.

The builder copies every destination tablet, part, retry, and optional WAL reclaim checkpoint. It
inserts the projected tablet in canonical tablet order, reconstructs every tablet's consecutive
part range in that order, retains each part descriptor exactly, merges retry outcomes in canonical
client/batch order, and encodes destination generation `N + 1`. It exact-decodes the result and
runs the ordinary add-only Manifest v2 transition validator against exact retained schema
bindings before returning owned candidate bytes.

The source generation remains bound in Raft snapshot metadata and transfer sessions; it is not
used as the local generation. Candidate construction does not prove that part files are durable or
authorize visibility. Every projected CSEG must first cross destination installation, and the
candidate must then cross the Manifest directory-sync boundary and one atomic runtime publication.

## Consequences and validation

Destination and source generation histories remain unambiguous. Canonical rebuilding costs memory
linear in the selected descriptors and is bounded by existing Manifest decode/format limits.
Duplicate part or retry identities fail through canonical model validation; missing schema lineage
fails through transition validation.

Tests merge an incoming tablet before an existing destination tablet, prove exact local descriptor
preservation and part-range rewriting, and repeat full transition validation. Existing-tablet and
foreign-database inputs fail. Projection checksum, source, applied-position, and corruption cases
remain covered by the underlying authority tests.

Invariants 2–6, 8, 10, 11, 14, and 18 apply.

## Migration and rollback

No format change. A candidate has no authority until installed. Rollback may discard an in-memory
candidate; an installed successor must be handled as the highest durable generation during
recovery and must not be silently removed.

## References

- [Raft tablet physical snapshot projection](../formats/raft-tablet-physical-snapshot-v1.md)
- [Manifest v2](../formats/manifest-v2.md)
- [Manifest installation and checkpointing](../architecture/manifest-installation-and-checkpointing.md)
