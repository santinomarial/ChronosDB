# Distributed Vector Grouped Aggregate Shuffle Job Control v2

> **Status: accepted and implemented for canonical route installation.** Header-first bounded
> reads, move-only partial writes, mutual-TLS exchange, finite retry, shared-endpoint dispatch, and
> reducer-service application are implemented. PREPARE and SEAL remain version 1 actions.

`CHDVGJC2` installs the complete numeric shuffle-listener map after every reducer has accepted the
same version 1 PREPARE. It has one action: `INSTALL_ROUTES`. The separate major version is required
because version 1 explicitly reserves action-semantic changes for a new version. All integers are
unsigned little-endian, UUIDs use canonical network-order bytes, and reserved bytes are zero.

The hard maximum is 4,096 route descriptors. Each descriptor is 16 bytes, so the maximum frame is
65,668 bytes: a 128-byte header, at most 65,536 payload bytes, and a four-byte trailer. A deployment
may impose lower frame and route-count limits.

## Request header

| Offset | Size | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | `CHDVGJC2` |
| 8 | 2 | major | `2` |
| 10 | 2 | minor | `0` |
| 12 | 4 | header length | `128` |
| 16 | 8 | total length | Exact header + route payload + trailer |
| 24 | 1 | action | `3` INSTALL_ROUTES |
| 25 | 7 | reserved | Zero |
| 32 | 8 | coordinator node ID | Nonzero |
| 40 | 8 | target reducer node ID | Nonzero and different from coordinator |
| 48 | 16 | query ID | Nonzero; exact previously prepared job |
| 64 | 16 | reserved | Zero |
| 80 | 8 | route payload length | Exact route count multiplied by 16 |
| 88 | 8 | route count | `0..4,096`, bounded further by the caller |
| 96 | 4 | route payload CRC32C | Complete route payload, including the canonical empty payload |
| 100 | 24 | reserved | Zero |
| 124 | 4 | header CRC32C | Bytes `[0,124)` |

The final four bytes are CRC32C over every preceding byte.

## Route descriptor

Descriptors are sorted by strictly increasing node ID. Duplicate nodes are forbidden.

| Offset | Size | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 8 | destination node ID | Nonzero |
| 8 | 4 | shuffle-listener IPv4 address | At least one nonzero byte |
| 12 | 2 | shuffle-listener port | Nonzero |
| 14 | 2 | reserved | Zero |

The route set contains exactly the destination nodes that require network ingress from at least one
source on a different node. An all-local destination therefore does not appear, and the complete
set may be empty. Every reducer receives the same set. The target reducer independently requires
its descriptor to equal the listener endpoint created by its accepted PREPARE; it never trusts the
coordinator to rewrite that local endpoint. Node-to-TLS-context resolution is deployment authority
and is not serialized.

An exact duplicate route installation is idempotent. Reuse of the query/coordinator/target tuple
with different route bytes is a conflict. No source worker may publish for the job until route
installation succeeds at every reducer.

## Response

`CHDVGJR2` is a fixed 100-byte response:

| Offset | Size | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | `CHDVGJR2` |
| 8 | 2 | major | `2` |
| 10 | 2 | minor | `0` |
| 12 | 4 | header length | `96` |
| 16 | 8 | total length | `100` |
| 24 | 1 | action | `3` INSTALL_ROUTES |
| 25 | 1 | status code | Stable `common::StatusCode` numeric value |
| 26 | 6 | reserved | Zero |
| 32 | 8 | coordinator node ID | Exact request value |
| 40 | 8 | target reducer node ID | Exact request value |
| 48 | 16 | query ID | Exact request value |
| 64 | 28 | reserved | Zero; v2 never returns an endpoint |
| 92 | 4 | header CRC32C | Bytes `[0,92)` |
| 96 | 4 | frame CRC32C | Bytes `[0,96)` |

A carrier exact-correlates action, query, coordinator, and target before treating status as
authoritative. CRC32C detects accidental damage; mutual TLS plus principal-to-node authorization
authenticates both sides.

## Compatibility

Version 2.0 accepts only INSTALL_ROUTES. Version 1.0 remains the only PREPARE/SEAL encoding and is
unchanged. The shared stream reader selects the decoder from exact magic and version; writers never
reinterpret either version. Unknown versions are unsupported. Malformed or noncanonical bytes are
corruption, caller-limit excess and allocation failure are resource exhaustion, and new action or
route semantics require another version.
