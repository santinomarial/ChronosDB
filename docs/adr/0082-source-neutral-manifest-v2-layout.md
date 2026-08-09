# ADR 0082: Source-neutral Manifest v2 layout

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB storage, recovery, and distributed-systems maintainers

## Context

Manifest v1 globally binds one WAL ID and records WAL-only part/retry positions. CSEG v2 and Raft
tablet application use explicit source namespaces, while independent tablets need independent
application and Raft-log reclamation boundaries. Reusing equal-width UUID fields would erase that
distinction. Remote immutable objects also need an exact byte identity stronger than a listing or
provider-specific ETag.

## Accepted decision

Adopt Manifest v2.0 as specified in [Manifest v2](../formats/manifest-v2.md). Each tablet, part, and
retry descriptor carries an explicit WAL/Raft source and source UUID. Tablet durable/reclaim
positions are source-relative. The header optionally retains one global physical WAL reclaim
coordinate; Raft reclamation remains per tablet. Part descriptors bind CSEG format version, source,
commit/event/system-time ranges, and SHA-256 of the exact installed bytes.

V2 retains immutable full generations, CRC32C framing, highest-generation/no-fallback selection,
and part-before-manifest durability ordering. V1 readers remain strict and V1 bytes are unchanged.
Migration writes a new V2 generation only after every referenced descriptor can be proven from
installed bytes; it does not reinterpret an old generation in place.

## Consequences and alternatives

Descriptors are larger and full-generation writes cost more. Exact content digests add one linear
read/hash pass, which is accepted at the installation trust boundary. Per-tablet boundaries make
multi-Raft progress explicit but require recovery to retain source-specific owners.

A single global Raft index was rejected because indexes are group-local. Removing the WAL physical
coordinate was rejected because per-tablet positions cannot prove a gap-free database WAL prefix.
Using a UUID without a source tag was rejected by ADR 0072. Relying on CSEG internal CRCs alone was
rejected because a descriptor must bind one exact immutable local/remote object, not merely any
internally valid part with matching metadata.

## Affected invariants and validation

Invariants 1–8, 10, 11, 13, 14, 16, and 18 apply. Implemented focused tests freeze every descriptor
size/offset, prove checked canonical layout, round-trip WAL and Raft generations, enforce exact
source relationships and limits, classify every truncation, and preserve strict v1 rejection. The
single-part admission boundary exact-decodes CSEG 2/0, validates all temporal semantics and schema
bindings, requires uniform source lineage, recomputes commit/event/system extrema, and binds exact
SHA-256 bytes. The ordinary successor validator now preserves source identity, advances application,
reclaim, and schema boundaries monotonically, and retains every prior part and retry descriptor
exactly. Independent golden Manifest bytes, expanded hostile decode, authorized retention and
compaction transitions, whole-generation coverage, v1-to-v2 migration, filesystem/object-store
crash matrices, Raft snapshot recovery, fuzzing, and performance evidence remain required
implementation work.
