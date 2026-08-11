# ADR 0146: Raft tablet physical snapshot projection

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB storage and distributed-systems maintainers
- **Extends:** [ADR 0082](0082-source-neutral-manifest-v2-layout.md),
  [ADR 0088](0088-owned-raft-tablet-snapshot-compaction.md), and
  [ADR 0128](0128-tablet-movement-rtas-handoff.md)

## Context

RTAS movement transfers bind a Manifest generation and an opaque part-set checksum, but they do not
identify the physical CSEG objects that a target must possess. Manifest v2 already supplies the
required source-neutral Raft lineage, exact part descriptors, immutable object lengths, content
SHA-256 values, and retry outcomes. Creating a second descriptor grammar would duplicate those
rules and risk disagreement with local recovery.

CSEG files can be as large as 64 GiB. A control record therefore cannot concatenate physical files
or require them all in memory. The source needs a small canonical authority that a later transport
can use to stream and resume each immutable object independently.

## Decision

A Raft tablet physical snapshot is an exact canonical Manifest v2 projection containing one tablet,
the consecutive part descriptors owned by that tablet, and only its protected retry descriptors.
The projection retains the source database identity and Manifest generation, rewrites the tablet's
`first_part_index` to zero, omits the database-global WAL reclaim checkpoint, and otherwise preserves
every selected descriptor byte semantically. The tablet and all included objects must use the
expected nonzero Raft group as their source, and the tablet durable position must equal the requested
applied boundary.

`part_set_checksum` is SHA-256 over the exact canonical 224-byte Manifest v2 part-descriptor table in
descriptor order. It therefore binds part identity, table/tablet/schema, length, row and time
extrema, source lineage, CSEG version, and each object's existing content SHA-256. Empty part sets
use SHA-256 of the empty byte sequence. Manifest generation and applied index/term remain separately
bound by `SnapshotMetadata` and RTAS; they are deliberately not mixed into the part-set digest.

The destination validator exact-decodes untrusted bytes under caller limits and requires one Raft
tablet, no WAL checkpoint, exact group/table/tablet, exact Manifest generation and applied position,
complete zero-based part coverage, tablet-only retries, and an aggregate digest equal to the full
Raft snapshot metadata. Success authorizes only verified object transfer. It does not install CSEG
files, publish a local Manifest generation, advance movement readiness, or reclaim source data.

## Consequences and validation

The control image stays bounded by Manifest v2's one-GiB limit and owns descriptor/retry copies, not
CSEG bytes. Existing Manifest v2 decoders, source rules, checksums, and compatibility behavior remain
the only durable grammar. The same physical part set produces the same aggregate checksum even when
the enclosing Manifest generation changes.

Focused tests project one tablet out of a multi-tablet generation, prove foreign parts and retries
are omitted, exact-validate the resulting Raft authority, and reject a wrong source, applied
boundary, aggregate checksum, full-database manifest, and corrupted bytes. Streaming object
transfer, restartable destination staging, exact CSEG readback, atomic Manifest publication, and
post-placement source reclamation remain subsequent Phase 16 tasks.

Invariants 1–5, 8, 10, 11, 14, 16, and 18 apply.

## Migration and rollback

No new durable or wire format is assigned. Older binaries continue to read Manifest v2 and ignore
the projection API. Rollback must not interpret a one-tablet projection as an installed database
generation or treat its digest as proof that the named CSEG files are locally durable.

## References

- [Raft tablet physical snapshot projection](../formats/raft-tablet-physical-snapshot-v1.md)
- [Manifest v2](../formats/manifest-v2.md)
- [Raft Tablet Application Snapshot v1](../formats/raft-tablet-application-snapshot-v1.md)
