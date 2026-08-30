# Distributed Vector Grouped Aggregate Shuffle Job Control v3

> **Status: accepted and implemented for authenticated reducer-job cancellation.** Header-first
> bounded reads, move-only partial writes, mutual-TLS exchange, finite retry, shared-endpoint
> dispatch, cancel-before-prepare tombstones, and whole-query cancellation ownership are
> implemented. PREPARE and SEAL remain version 1 actions; INSTALL_ROUTES remains version 2.

`CHDVGJC3` cancels one exact in-memory grouped-shuffle reducer job. It has one action: `CANCEL`.
The separate major version preserves the immutable v1 and v2 action registries. All integers are
unsigned little-endian, UUIDs use canonical network-order bytes, and reserved bytes are zero.

## Request

The request is a fixed 132-byte frame: a 128-byte header followed by a four-byte trailer.

| Offset | Size | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | `CHDVGJC3` |
| 8 | 2 | major | `3` |
| 10 | 2 | minor | `0` |
| 12 | 4 | header length | `128` |
| 16 | 8 | total length | `132` |
| 24 | 1 | action | `4` CANCEL |
| 25 | 7 | reserved | Zero |
| 32 | 8 | coordinator node ID | Nonzero |
| 40 | 8 | target reducer node ID | Nonzero and different from coordinator |
| 48 | 16 | query ID | Nonzero |
| 64 | 60 | reserved | Zero |
| 124 | 4 | header CRC32C | Bytes `[0,124)` |
| 128 | 4 | frame CRC32C | Bytes `[0,128)` |

The enclosing mutual-TLS carrier authenticates the claimed coordinator and authorizes its node
identity before service dispatch. The target must equal the receiving reducer node. No endpoint,
proof, schema, route, timeout, or wall-clock value is serialized.

## Service semantics

Cancellation is idempotent for the exact query/coordinator/target tuple. An installed receiving or
transmitting job closes destination ingress, source transport, and result transport before the
service returns success. A completed or already failed job is terminal and accepts cancellation as
a no-op. An already cancelled job returns the same success.

If the exact job is absent, the service installs a bounded in-memory cancellation tombstone before
returning success. The tombstone rejects a later matching PREPARE with `CANCELLED`, closing the
cross-connection race where CANCEL reaches the single-threaded service before an earlier PREPARE.
Conflicting coordinator reuse fails. Tombstone count and retention are deployment-bounded; full
admission returns `RESOURCE_EXHAUSTED`. Tombstones are not durable and expire after the configured
retention, which cannot exceed the v1 24-hour job-timeout ceiling. Coordinator retry remains bound
by its absolute query deadline.

## Response

`CHDVGJR3` is a fixed 100-byte correlated response:

| Offset | Size | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | `CHDVGJR3` |
| 8 | 2 | major | `3` |
| 10 | 2 | minor | `0` |
| 12 | 4 | header length | `96` |
| 16 | 8 | total length | `100` |
| 24 | 1 | action | `4` CANCEL |
| 25 | 1 | status code | Stable `common::StatusCode` numeric value |
| 26 | 6 | reserved | Zero |
| 32 | 8 | coordinator node ID | Exact request value |
| 40 | 8 | target reducer node ID | Exact request value |
| 48 | 16 | query ID | Exact request value |
| 64 | 28 | reserved | Zero; v3 never returns an endpoint |
| 92 | 4 | header CRC32C | Bytes `[0,92)` |
| 96 | 4 | frame CRC32C | Bytes `[0,96)` |

A sender treats only a fully read, checksum-valid, exact-correlated `OK` response as acknowledged
cancellation. Transport or application failure does not claim remote cleanup; the reducer's
original relative execution deadline remains the final bounded fallback.

## Compatibility

Version 3.0 accepts only CANCEL. Version 1.0 remains the only PREPARE/SEAL encoding, and version 2.0
remains the only INSTALL_ROUTES encoding. The shared reader selects by exact magic. Unknown
versions are unsupported, malformed or noncanonical bytes are corruption, and caller-limit excess
or allocation failure is resource exhaustion. New action semantics require another version.
