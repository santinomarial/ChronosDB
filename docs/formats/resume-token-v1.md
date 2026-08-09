# Resume Token v1

> **Status: implemented logical token format.** These opaque authenticated bytes are not a native
> Protocol v1 frame and do not change any existing Protocol v1 message bytes.

All integers are little-endian. The authenticated prefix is followed by a 32-byte HMAC-SHA256 over
that complete prefix. The MAC key is deployment state and is never encoded in the token.

## Header (128 bytes)

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `43 48 52 4e 52 53 4d 00` (`CHRNRSM\0`) |
| 8 | 2 | Major version, `1` |
| 10 | 2 | Minor version, `0` |
| 12 | 4 | Header size, `128` |
| 16 | 4 | Total token size including MAC |
| 20 | 4 | Source-position count, `1..4096` |
| 24 | 16 | Database UUID |
| 40 | 16 | Subscription UUID |
| 56 | 16 | Bound schema UUID |
| 72 | 8 | Bound schema version |
| 80 | 8 | Safely acknowledged delivery sequence |
| 88 | 32 | Query-plan fingerprint |
| 120 | 8 | Required-zero reserved bytes |

## Source positions

Each source uses 40 bytes: tablet UUID (16), WAL identity (16), and committed record sequence (8).
Positions follow the header in deterministic caller order. The single-tablet manager accepts
exactly one source. The multi-tablet coordinator emits and exact-validates canonical tablet-identity
order; other callers remain responsible for preserving their declared deterministic order.

## Validation and compatibility

The decoder checks the fixed minimum, authenticates the complete prefix in constant time, then
validates magic, version, size relationships, source bound, required-zero bytes, and nonzero
identities. Unknown major versions and newer minor versions fail as unsupported. Modification fails
as unauthenticated. A valid token is still rejected when database, tablet/WAL lineage, plan/schema,
or retained suffix is incompatible. External delivery remains at least once.
