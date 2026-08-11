# ADR 0147: Tablet physical part chunk v1

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB cluster, storage, and distributed-systems maintainers
- **Extends:** [ADR 0146](0146-raft-tablet-physical-snapshot-projection.md)

## Context

The physical snapshot projection identifies exact CSEG objects, but a CSEG may be nearly 64 GiB and
cannot be placed in one network frame or retained whole by transfer control state. Existing RTAS
chunks are capped at 1 GiB, use a whole-content CRC32C, and name an application-snapshot session;
relabeling them as physical objects would lose part identity and content SHA-256 semantics.

## Decision

Tablet Physical Part Chunk v1 is a canonical checksummed frame with a 192-byte header, nonempty
bounded payload, and four-byte complete-frame CRC32C trailer. Every chunk repeats the table, tablet,
Raft group, placement epoch, source and target nodes, Manifest generation, part identity, exact
64-bit object length, and expected content SHA-256. It also carries its exact object offset, payload
length, and payload CRC32C.

The object length is bounded by CSEG's 64-GiB-class maximum rather than `size_t`. Default payloads
are capped at 4 MiB and encoded frames at 16 MiB. Callers may lower those limits but cannot exceed
the frozen CSEG or frame maximums. The payload must fit wholly within the declared object; zero-byte
chunks are invalid. Header CRC32C is checked before identity or length fields control behavior, and
the complete CRC covers the header, payload, and stored payload checksum.

CRC32C detects framing damage; it is not object authentication. Only a later contiguous durable
owner may combine chunks, and completion requires streaming SHA-256 equality with the expected
digest repeated in every chunk. Receipt alone does not install a CSEG, publish a Manifest, advance
movement readiness, or authorize source deletion.

## Consequences and validation

Repeating the full session makes cross-tablet, cross-movement, and cross-object splicing detectable
without trusting a connection or directory name. The 192-byte overhead is accepted for restartable,
independently diagnosable pieces. Exact immutable retries and gap-free admission are storage-owner
rules, not codec claims.

Focused tests freeze deterministic round trip, exact session identity, object and caller bounds,
header/payload/trailer corruption, and checksum-valid unknown-version classification. Durable
installation/reopen, streaming completion, interruption matrices, transport backpressure, and
reclamation remain follow-up work.

Invariants 1–5, 8, 10, 11, 14, and 18 apply.

## Migration and rollback

No earlier physical-part chunk version exists. Readers reject unknown major or minor versions. A
rollback may discard unreferenced received chunks, but it must not convert them into RTAS chunks or
claim physical snapshot completion.

## References

- [Tablet Physical Part Chunk v1](../formats/tablet-physical-part-chunk-v1.md)
- [Raft tablet physical snapshot projection](../formats/raft-tablet-physical-snapshot-v1.md)
- [CSEG v2](../formats/cseg-v2.md)
