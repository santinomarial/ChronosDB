# Subscription Plan Definition v1

## Status and byte order

This document freezes major version 1, minor version 0. Every integer is fixed-width little-endian.
UUIDs and the SHA-256 plan fingerprint use their canonical bytes. The exact SQL byte string is
stored without normalization and the complete file ends with CRC32C over every preceding byte.

## Header

The header is exactly 128 bytes.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | ASCII magic `CHSUBPD1` |
| 8 | 2 | major version, `1` |
| 10 | 2 | minor version, `0` |
| 12 | 4 | header size, `128` |
| 16 | 8 | exact total file size including trailer |
| 24 | 8 | exact SQL byte length, nonzero |
| 32 | 16 | database UUID |
| 48 | 16 | bound table UUID |
| 64 | 16 | bound schema UUID |
| 80 | 8 | bound schema version |
| 88 | 32 | plan fingerprint |
| 120 | 8 | reserved zero |

Exactly `sql_length` bytes follow the header, then a 4-byte CRC32C of `[0, total_size - 4)`. The SQL
bytes may contain only what the bounded subscription parser accepts; the file codec itself preserves
them exactly.

## Validation and filesystem namespace

A decoder checks size, magic, version, header fields, configured SQL/total limits, and CRC before
allocating the SQL string. It then validates reserved bytes and all typed identities and rejects
trailing data. Unknown versions are unsupported and malformed or checksum-invalid bytes are
corruption.

A database-scoped locked directory stores immutable definitions as
`plan-<64 lowercase fingerprint hex>.subp`. Installation writes and exact-decodes the corresponding
`.tmp`, synchronizes it, closes it, renames without replacement, and synchronizes the directory.
Reopen removes only canonical temporaries. Loading verifies database and filename fingerprint, then
re-parses, re-binds, and re-lowers the SQL against the supplied catalog; recomputed table, schema,
version, and fingerprint must exactly equal the durable definition.
