# Distributed Vector Grouped Aggregate Shuffle Authority v1

> **Status: accepted and implemented for exact encoding and bounded exact decoding.** The
> implemented reducer-job control envelope carries these bytes; its authenticated carrier remains
> an enclosing responsibility.

`CHDVGSA1` carries the complete immutable authority needed to reconstruct one grouped-shuffle job
in another process. It contains query identity, hash version, plan-order source tablets and nodes,
canonical partition destinations, typed group keys, and typed mergeable aggregate definitions. All
integers are unsigned little-endian, UUIDs use canonical network-order bytes, reserved bytes are
zero, and no native object representation is serialized.

The hard frame ceiling is 64 MiB plus the 96-byte header and four-byte trailer. Deployments may set
lower frame, source, partition, and retained-configuration limits.

## Header

| Offset | Size | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | `CHDVGSA1` |
| 8 | 2 | major | `1` |
| 10 | 2 | minor | `0` |
| 12 | 4 | header length | `96` |
| 16 | 8 | total length | Exact header + body + trailer |
| 24 | 16 | query ID | Nonzero UUID |
| 40 | 2 | hash version | `1` |
| 42 | 2 | reserved | Zero |
| 44 | 4 | source count | Nonzero and bounded |
| 48 | 4 | destination count | Nonzero and bounded |
| 52 | 4 | group-key count | Nonzero and bounded |
| 56 | 4 | aggregate count | Bounded; zero is valid |
| 60 | 8 | body length | Exact descriptor sum |
| 68 | 4 | header CRC32C | Bytes `[0,68)` |
| 72 | 24 | reserved | Zero |

## Body descriptors

Source descriptors are 24 bytes in plan order: tablet UUID followed by source node ID. Tablet and
node IDs are nonzero, and tablet IDs are unique.

Destination descriptors are 16 bytes: partition ID, four zero bytes, and destination node ID.
Partition IDs are contiguous from zero in body order; node IDs are nonzero.

Group-key descriptors are 24 bytes: 64-bit column ordinal, 16-bit logical type code, two 16-bit
type parameters, one-byte nullable flag, and nine zero bytes. The type and nullable flag must be
canonical.

Aggregate descriptors are 32 bytes: one-based operation code, one-byte input-present flag,
one-byte input-nullable flag, five zero bytes, 64-bit input ordinal, logical type code and two type
parameters, then ten zero bytes. A missing input requires zero nullable, ordinal, and type fields.
An input-present descriptor requires a canonical logical type. Operations map in declared v1 order
to COUNT(*), COUNT, SUM, AVG, MIN, MAX, VAR_POP, and VAR_SAMP.

The final four bytes are CRC32C over every preceding frame byte.

## Canonicality and limits

The decoder verifies fixed integrity and every allocation-driving count before reserving descriptor
storage. It requires exact body and total lengths, supported version/hash, zero reserved bytes,
canonical aggregate absence, hard key/aggregate maxima, and caller source/partition/frame limits.
It then reconstructs the public authority through its normal validator, which enforces source
uniqueness, canonical destinations, grouped type rules, and retained-memory accounting.

Wire damage and semantically invalid values are corruption. Unknown versions are unsupported.
Caller limits and allocation failures are resource exhaustion. Typed encoder misuse is an invalid
argument.

## Compatibility

Version 1.0 is the only accepted layout. Writers emit exactly 1.0. Changing hash semantics,
descriptor identity, operation mapping, or canonical ordering requires a new version.
