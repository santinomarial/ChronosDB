# Distributed Vector Plan Intent v1

> **Status: accepted and implemented.** This schema-neutral inner value requires a later
> authority-bound fragment and worker-side schema/type validation before execution.

All integers are unsigned little-endian. Reserved bytes are zero. The maximum frame length is
67,644 bytes. The header checksum is validated before counts or lengths control interpretation.

## Header

| Offset | Size | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | `CHDVPLN1` |
| 8 | 2 | major | `1` |
| 10 | 2 | minor | `0` |
| 12 | 4 | header length | `48` |
| 16 | 8 | frame length | Exact complete length |
| 24 | 1 | mode | `1` rows, `2` ungrouped aggregate, `3` grouped aggregate |
| 25 | 1 | flags | Bit 0 means LIMIT present; all others zero |
| 26 | 2 | reserved | Zero |
| 28 | 4 | row-output count | At most 4,096 |
| 32 | 4 | group-key count | At most 4,096 |
| 36 | 4 | aggregate count | At most 4,096 |
| 40 | 4 | order-key count | At most 256 |
| 44 | 4 | header CRC32C | CRC32C of bytes `[0,44)` |

The body contains, in order, all row-output descriptors, group-key descriptors, aggregate
descriptors, order-key descriptors, an optional LIMIT, and the final CRC32C.

## Descriptors

- A row-output or group-key descriptor is one 32-bit index into the later fragment's projected
  input. Row outputs retain order and may repeat. Group keys are unique.
- An aggregate descriptor is 8 bytes: one-byte operation, one-byte input-present flag, two zero
  bytes, and a 32-bit input index. Operation codes are `0` COUNT(*), `1` COUNT, `2` SUM, `3` AVG,
  `4` MIN, `5` MAX, `6` VAR_POP, and `7` VAR_SAMP. COUNT(*) alone omits input and stores zero;
  every other operation requires input.
- An order descriptor is 8 bytes: 32-bit final-output index, one-byte direction (`0` ascending,
  `1` descending), one-byte NULL placement (`0` first, `1` last), and two zero bytes. Output-order
  indices are unique.
- A present LIMIT is one 64-bit value. Zero is distinct from absence.
- The final 4 bytes are CRC32C of every preceding frame byte.

Rows mode requires at least one row output and no grouping or aggregates. Ungrouped mode requires
at least one aggregate and no row outputs or group keys. Grouped mode requires at least one group
key, allows zero or more aggregates, and has no row outputs. Grouped output is keys followed by
aggregates; ungrouped output is aggregates. Every input/output index and collection is subject to
lower caller limits during decode.

## Distributed execution rule

Ordering and LIMIT describe the final complete output. A coordinator must not apply them
independently to tablet fragments unless a separately specified algorithm proves that the global
result is preserved. In particular, this format alone does not authorize per-tablet top-N.

The intent carries no database, schema, snapshot, placement, Raft group, read proof, query
identity, resource limits, or authentication. It is nonexecutable until a later authority binder
matches every input index and aggregate operation to the exact committed schema and packages the
result with a proof-bound fragment. Unknown versions are unsupported; damaged or contradictory
bytes are corruption; lower caller bounds return resource exhaustion.
