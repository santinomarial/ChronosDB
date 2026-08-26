# Distributed Vector Grouped Aggregate Shuffle Job Control v1

> **Status: accepted and implemented for exact prepare/seal request and correlated response
> encoding/decoding.** Authentication, partial-I/O ownership, job admission, and job progress remain
> enclosing service responsibilities.

`CHDVGJC1` binds a portable grouped-shuffle authority to its exact raw result schema and the
coordinator result route needed by one remote reducer daemon. It has two actions. `PREPARE`
installs immutable job configuration. `SEAL` names that already installed job and authorizes it to
close ingress and return complete reduced partitions. All integers are unsigned little-endian,
UUIDs use canonical network-order bytes, and reserved bytes are zero.

The hard maximum frame is 128 bytes plus the maximum authority frame, maximum result-schema frame,
and a four-byte trailer. The hard execution timeout is 24 hours. A deployment may impose lower
frame, authority, schema, and timeout limits.

## Header

| Offset | Size | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | `CHDVGJC1` |
| 8 | 2 | major | `1` |
| 10 | 2 | minor | `0` |
| 12 | 4 | header length | `128` |
| 16 | 8 | total length | Exact header + nested payloads + trailer |
| 24 | 1 | action | `1` PREPARE, `2` SEAL |
| 25 | 7 | reserved | Zero |
| 32 | 8 | coordinator node ID | Nonzero |
| 40 | 8 | target reducer node ID | Nonzero and different from coordinator |
| 48 | 16 | query ID | Nonzero; exact nested authority query for PREPARE |
| 64 | 4 | coordinator result IPv4 | Nonzero for PREPARE, zero for SEAL |
| 68 | 2 | coordinator result port | Nonzero for PREPARE, zero for SEAL |
| 70 | 2 | reserved | Zero |
| 72 | 8 | execution timeout milliseconds | `1..86,400,000` for PREPARE, zero for SEAL |
| 80 | 8 | authority length | Exact nested `CHDVGSA1` length for PREPARE, zero for SEAL |
| 88 | 8 | result-schema length | Exact nested `CHDVRSC1` length for PREPARE, zero for SEAL |
| 96 | 4 | authority CRC32C | Complete nested authority bytes, zero for SEAL |
| 100 | 4 | result-schema CRC32C | Complete nested schema bytes, zero for SEAL |
| 104 | 20 | reserved | Zero |
| 124 | 4 | header CRC32C | Bytes `[0,124)` |

PREPARE payload bytes are the authority frame followed immediately by the result-schema frame. The
final four bytes are CRC32C over every preceding byte. SEAL has no payload and therefore has the
canonical total length 132.

## Authority and lifecycle rules

PREPARE decoding first validates the outer header, lengths, and whole-frame integrity. It then
validates both nested checksums and exact-decodes each nested version under its own caller limits.
The target reducer must own at least one destination in the authority. The raw result schema must
be exactly the authority's key types/nullability followed by the derived aggregate output
types/nullability. Names remain the coordinator's bound SQL identities.

The timeout is a relative execution budget that starts only after successful receiver admission;
it is not a serialized `steady_clock` value and does not assume synchronized wall clocks. A
transport deadline must independently bound delivery and acknowledgment.

SEAL carries the exact query/coordinator/target tuple and no deployment or proof fields. A service
may accept it only for one installed PREPARE with that tuple. Duplicate identical PREPARE or SEAL
semantics and conflicting reuse are service-layer responsibilities.

CRC32C detects accidental damage but does not authenticate a coordinator. An enclosing mutual-TLS
carrier must authenticate the claimed coordinator, authorize the target node, and return a
correlated success or failure before a sender treats either action as complete.

## Response

`CHDVGJR1` is a fixed 100-byte correlated response. Its 96-byte header contains magic, version
1.0, header and total lengths, the echoed action, one stable status code, flags, coordinator and
target node IDs, query ID, and an optional reducer shuffle IPv4 endpoint. Bytes `[92,96)` hold the
header CRC32C, and the final four bytes hold the whole-frame CRC32C. All reserved bytes are zero.

Flag bit 0 means the endpoint is present; no other flag is assigned. Exactly one response shape
may carry an endpoint: successful PREPARE. That endpoint is the live reducer shuffle listener that
the coordinator may route source streams to. Failed PREPARE, successful or failed SEAL, and every
other response require zero address/port bytes and a clear endpoint flag. The response echoes the
exact query/coordinator/target/action tuple, so a carrier must validate correlation before treating
the status as authoritative.

## Compatibility

Version 1.0 is the only accepted layout. Writers emit exactly 1.0 and zero every reserved field.
Unknown versions are unsupported. Malformed and noncanonical wire values are corruption. Caller
limits and allocation failure are resource exhaustion. Changing action semantics, nested proof
identity, route identity, deadline meaning, response status mapping, or endpoint publication
requires a new version.
