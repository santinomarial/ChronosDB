# Distributed Grouped FLOAT64 Fragment Intent v1

> **Status:** accepted with implemented canonical owned encoding and exact bounded decoding. This
> is structural intent, not a bare executable worker request.

This envelope adds one projected nullable-FLOAT64 grouping-key index to an exact
[Distributed Aggregate Fragment v1](distributed-aggregate-fragment-v1.md) without changing those
frozen bytes. All integers are unsigned little-endian.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `CHDFGRP1` |
| 8 | 2 | Major version `1` |
| 10 | 2 | Minor version `0` |
| 12 | 4 | Header length `40` |
| 16 | 8 | Exact complete frame length |
| 24 | 4 | Group-key input index within the nested projection |
| 28 | 4 | Exact nested Fragment v1 length |
| 32 | 4 | Zero reserved bytes |
| 36 | 4 | CRC32C of bytes `[0, 36)` |
| 40 | nested length | One exact Distributed Aggregate Fragment v1 frame |
| final 4 | 4 | CRC32C of every preceding envelope byte |

The nested frame length is `224..16604`; the complete grouped envelope is `268..16648` bytes. The
group-key input index is less than the nested destination projection count. It may equal the nested
aggregate input index. The pinned destination schema, not this structural codec, must prove that
both selected inputs have supported FLOAT64 types and supplies the key's nullability contract.

Exact decoding validates the physical bound, magic, and header CRC before any peer length controls
slicing. It then validates the outer version, header length, reserved bytes, nested and complete
length relationship, and complete CRC before invoking exact nested decoding with the caller's
projection limit. The key index is checked only against the successfully decoded projection.
Checksum-valid unknown outer versions are unsupported. Damage and contradictory fields are
corruption; caller-limit excess remains resource exhaustion from the nested decoder.

Both encoded layers own their bytes. CRC32C provides accidental-corruption coverage, not
authentication. A group-scoped executable dispatch, local worker revalidation, and authenticated
carrier remain required before storage access.

The implemented authority binder proves both selected inputs against the same pinned destination
schema and returns this grouped intent with the exact Raft group as owned values. That in-memory
result still requires a canonical grouped dispatch and worker-local revalidation before execution.
