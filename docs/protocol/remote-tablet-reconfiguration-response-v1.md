# Remote Tablet Reconfiguration Response v1

## Purpose

This fixed internal control-plane response correlates receiver-local action admission to one exact
Remote Tablet Reconfiguration Request v1 route. It reports local status only. `OK` means the
receiver accepted the ledger-prepared action through its durable owner; it is not quorum commit,
state-machine application, or a promise that the node remains leader.

## Framing

All integers are unsigned little-endian. Every response is exactly 116 bytes: a 112-byte header and
a 4-byte whole-response CRC32C trailer.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | magic `CHRRTRS\0` |
| 8 | 2 | major `1` |
| 10 | 2 | minor `0` |
| 12 | 4 | header size `112` |
| 16 | 8 | total size `116` |
| 24 | 8 | nonzero source/receiver node ID |
| 32 | 8 | nonzero target/sender node ID, distinct from source |
| 40 | 8 | exact nonzero required leader term from the request |
| 48 | 16 | tablet UUID |
| 64 | 8 | nonzero movement epoch |
| 72 | 1 | action kind (`1` begin joint, `2` finalize, `3` publish placement) |
| 73 | 1 | stable status code |
| 74 | 2 | flags |
| 76 | 4 | zero reserved |
| 80 | 8 | suggested leader term, or zero |
| 88 | 8 | suggested leader node ID, or zero |
| 96 | 4 | header CRC32C with this field zero |
| 100 | 12 | zero reserved |

Flag bit 0 means the receiver's immutable action ledger already contained the exact action. Flag
bit 1 means both suggested-leader fields are present and nonzero. All other bits are zero.

The stable status registry is: `0 OK`, `1 CANCELLED`, `2 INVALID_ARGUMENT`, `3 OUT_OF_RANGE`,
`4 NOT_FOUND`, `5 ALREADY_EXISTS`, `6 CORRUPTION`, `7 IO_ERROR`, `8 RESOURCE_EXHAUSTED`,
`9 UNAVAILABLE`, `10 NOT_SUPPORTED`, `11 UNAUTHENTICATED`, and `12 INTERNAL`. This wire mapping is
explicit and does not depend on the C++ enum's object representation.

## Correlation and retry

The sender exact-matches response source to the attempted target, response target to itself, the
required term to that attempt, and tablet/epoch/kind to its sealed prepared action. A mismatch does
not consume the pending attempt.

`UNAVAILABLE`, `RESOURCE_EXHAUSTED`, and `IO_ERROR` are retryable within an explicit bounded attempt
budget. Every retry waits deterministic capped exponential backoff and requires the caller to
supply a fresh nonzero leader node/term route; a response hint is advisory only. Other errors are
terminal. Transport failure follows the same classification. No hidden retry queue, wall clock, or
I/O is owned by the retry state machine.

The implemented receiver adapter retains the request correlation beside its owning asynchronous
completion. Polling before readiness emits nothing. Readiness consumes the completion once and maps
either its top-level failure or sole per-operation status into this response. A leader hint is
included only when the caller supplies one from a separately ordered observation.

## Compatibility

Major 1, minor 0 is exact. Damage, unknown versions/statuses/flags, invalid identities, inconsistent
leader-hint presence, nonzero reserved bytes, or trailing data fail closed. The request and response
formats are independently framed so future response payloads cannot reinterpret v1 bytes.
