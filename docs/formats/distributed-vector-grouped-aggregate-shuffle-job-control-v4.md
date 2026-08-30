# Distributed Vector Grouped Aggregate Shuffle Job Control v4

> **Status: accepted and implemented for authenticated coordinator-lease renewal.** Header-first
> bounded reads, move-only partial writes, mutual-TLS exchange, finite retry, shared-endpoint
> dispatch, reducer-side lease expiry, and coordinator/whole-query renewal ownership are
> implemented. PREPARE and SEAL remain version 1 actions, INSTALL_ROUTES remains version 2, and
> CANCEL remains version 3.

`CHDVGJC4` activates or renews one relative in-memory coordinator lease for an already prepared
grouped-shuffle reducer job. It has one action: `RENEW_LEASE`. The separate major version preserves
the immutable v1-v3 action registries. All integers are unsigned little-endian, UUIDs use canonical
network-order bytes, durations are relative monotonic intervals, and reserved bytes are zero.

## Request

The request is a fixed 132-byte frame: a 128-byte header followed by a four-byte trailer.

| Offset | Size | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | `CHDVGJC4` |
| 8 | 2 | major | `4` |
| 10 | 2 | minor | `0` |
| 12 | 4 | header length | `128` |
| 16 | 8 | total length | `132` |
| 24 | 1 | action | `5` RENEW_LEASE |
| 25 | 7 | reserved | Zero |
| 32 | 8 | coordinator node ID | Nonzero |
| 40 | 8 | target reducer node ID | Nonzero and different from coordinator |
| 48 | 16 | query ID | Nonzero; exact previously prepared job |
| 64 | 8 | lease duration milliseconds | `1..86,400,000` |
| 72 | 52 | reserved | Zero |
| 124 | 4 | header CRC32C | Bytes `[0,124)` |
| 128 | 4 | frame CRC32C | Bytes `[0,128)` |

The enclosing mutual-TLS carrier authenticates the claimed coordinator and authorizes its node
identity before service dispatch. The target must equal the receiving reducer node. No wall-clock
timestamp is serialized, and clocks need not be synchronized.

## Service semantics

The first exact request is accepted only after PREPARE and complete INSTALL_ROUTES. Its receipt
activates the lease and sets a process-local monotonic deadline to receipt time plus the requested
duration. Source workers cannot start until every reducer acknowledges this activation. A later
exact request with the same query, coordinator, target, and duration moves the deadline to that
request's receipt time plus the same duration. A different duration conflicts rather than silently
changing the active contract.

An authenticated renewal received after the current lease deadline cannot revive the job. Service
polling at or after that deadline closes destination ingress, source transport, and result
transport and marks the job cancelled. A failed or cancelled job returns its terminal status. A
completed job accepts renewal as an idempotent no-op while retained. If no job exists, the response
is `NOT_FOUND`; renewal never creates a job or tombstone.

The original v1 PREPARE execution deadline remains authoritative. Before activation it is the only
cleanup bound. After activation the effective active-job bound is the earlier of that original
deadline and the renewed lease deadline. Terminal retention still uses the original deadline.

## Response

`CHDVGJR4` is a fixed 100-byte correlated response:

| Offset | Size | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | `CHDVGJR4` |
| 8 | 2 | major | `4` |
| 10 | 2 | minor | `0` |
| 12 | 4 | header length | `96` |
| 16 | 8 | total length | `100` |
| 24 | 1 | action | `5` RENEW_LEASE |
| 25 | 1 | status code | Stable `common::StatusCode` numeric value |
| 26 | 6 | reserved | Zero |
| 32 | 8 | coordinator node ID | Exact request value |
| 40 | 8 | target reducer node ID | Exact request value |
| 48 | 16 | query ID | Exact request value |
| 64 | 28 | reserved | Zero; v4 never returns an endpoint |
| 92 | 4 | header CRC32C | Bytes `[0,92)` |
| 96 | 4 | frame CRC32C | Bytes `[0,96)` |

A sender treats only a fully read, checksum-valid, exact-correlated `OK` response as one accepted
lease. Each all-reducer renewal round has its own finite retry and delivery deadline. Failure of
any required renewal fails the whole query and enters v3 cancellation; it cannot expose partial
output or assume an unacknowledged reducer remains live.

## Compatibility

Version 4.0 accepts only RENEW_LEASE. Versions 1.0, 2.0, and 3.0 remain the only encodings for their
existing actions and are byte-for-byte unchanged. The shared reader selects by exact magic.
Unknown versions are unsupported, malformed or noncanonical bytes are corruption, and caller-limit
excess or allocation failure is resource exhaustion. New action semantics require another version.
