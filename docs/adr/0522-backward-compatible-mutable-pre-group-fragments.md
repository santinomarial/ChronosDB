# ADR 0522: Backward-Compatible Mutable Pre-Group Fragments

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB query execution and distributed protocol maintainers

## Context

ADR 0521 deliberately rejected serialization of an otherwise executable pre-group program. Remote
grouped workers need an exact owned program without changing or ambiguously reinterpreting accepted
v1 bytes.

## Decision

- Add Mutable Vector Fragment v2 with distinct magic `CHDMVFR2` and version `2.0`.
- Preserve the 248-byte header. The v1 reserved tail becomes a checked program length followed by a
  header checksum covering that length. Append the exact Pre-Group Program v1 frame after the
  result schema.
- Encode fragments without programs as byte-compatible v1. Encode fragments with programs as v2.
  Exact decoding accepts both pairs and rejects mixed magic/version identities.
- Increase outer fragment and existing request-carrier maxima by the hard 4 MiB program bound.
  The request carrier itself remains version 1 because its opaque payload semantics and framing do
  not change.
- Decode and reconstruct the nested program before structural plan/result validation. Binding and
  worker schema proofs from ADR 0521 remain mandatory after transport.

## Consequences

Computed grouped worker programs can cross the existing authenticated, checksummed, bounded mutable
query transport. Old v1 rows and grouped fragments retain their exact bytes and behavior. Receivers
that do not understand v2 fail on its distinct inner magic rather than silently dropping work.

## Validation plan

- Retain all v1 round-trip, corruption, and unknown-version tests.
- Round-trip a v2 program, reject independently damaged nested bytes under a refreshed outer CRC,
  and enforce caller nested-program bounds.
- Carry v2 through grouped request receiver authentication and worker publication.
- Sweep owned v2 encode/decode allocations and run full query/cluster, sanitizer, format, and
  applicable static-analysis gates.

## References

- [ADR 0520](0520-versioned-owned-pre-group-vector-program.md)
- [ADR 0521](0521-proof-bound-local-mutable-pre-group-execution.md)
- [Mutable Vector Fragment v2](../formats/distributed-mutable-vector-fragment-v2.md)
