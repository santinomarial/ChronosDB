# Resume Token v2

> **Status: implemented source-tagged logical token format.** New logical subscription tokens use
> v2. Resume Token v1 remains accepted for WAL-backed sources.

All integers are little-endian. The authenticated prefix is followed by a 32-byte HMAC-SHA256 over
that complete prefix. The deployment MAC key is never encoded. Decoders authenticate the prefix
before using its version or semantic fields.

## Header (128 bytes)

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `43 48 52 4e 52 53 4d 00` (`CHRNRSM\0`) |
| 8 | 2 | Major version, `2` |
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

Each source uses 48 bytes.

| Relative offset | Size | Field |
| ---: | ---: | --- |
| 0 | 16 | Tablet UUID |
| 16 | 1 | Source kind: `1` WAL, `2` Raft |
| 17 | 7 | Required zero |
| 24 | 16 | WAL ID for kind `1`; Raft group UUID for kind `2` |
| 40 | 8 | Committed WAL record sequence or Raft log index |

Tablet and selected source identities must be nonzero. The unused identity namespace does not exist
in the bytes and must not be inferred or aliased. Positions follow deterministic caller order; the
multi-tablet coordinator uses canonical tablet order.

## Validation and compatibility

The decoder bounds total bytes from its configured source limit, authenticates the complete prefix
in constant time, then validates magic, version, exact sizes, source count, required-zero bytes,
source kinds, and nonzero identities. Unknown major versions, newer minor versions, unknown source
kinds, and nonzero required fields fail as unsupported. Malformed known fields fail as corruption.
Modification fails as unauthenticated.

The compatibility decoder accepts authenticated v1 and v2 tokens. V1 positions are interpreted only
as WAL positions and retain their frozen 40-byte layout. The v1 encoder rejects a Raft position.
New issuance uses v2; this does not by itself add Raft positions to the WAL-only Native Protocol 1.1
subscription-change envelope or Multi-tablet Subscription Checkpoint v1.
