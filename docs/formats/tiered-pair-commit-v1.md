# ChronosDB Tiered Pair Commit v1

> **Status: accepted codec, synchronized immutable installation, exact committed-component
> recovery, uncommitted-final isolation, and highest-generation/no-fallback selection are
> implemented. Physical power-loss qualification remains deferred.**

Tiered Pair Commit v1 is the aggregate crash authority for one exact Manifest v2 generation and its
optional Cold Location Manifest generation. All integers are little-endian. UUIDs and SHA-256 values
use canonical byte order. The record is exactly 256 bytes.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | magic `CHTRPAIR` |
| 8 | 2 | format major = 1 |
| 10 | 2 | format minor = 0 |
| 12 | 4 | record length = 256 |
| 16 | 4 | flags; bit 0 means cold generation present |
| 20 | 4 | zero |
| 24 | 8 | nonzero pair-commit generation |
| 32 | 8 | previous pair generation, zero only for generation 1 |
| 40 | 16 | database UUID |
| 56 | 16 | deployment object-store UUID |
| 72 | 8 | nonzero Manifest v2 generation |
| 80 | 8 | cold generation, zero when absent |
| 88 | 8 | exact Manifest v2 byte length |
| 96 | 8 | exact cold-manifest byte length, zero when absent |
| 104 | 32 | SHA-256 of exact Manifest v2 bytes |
| 136 | 32 | SHA-256 of exact cold bytes, zero when absent |
| 168 | 80 | zero |
| 248 | 4 | CRC32C of bytes 0–247 |
| 252 | 4 | CRC32C of bytes 0–251 |

Final names are `pair-00000000000000000001.tpc`; exact interrupted candidates append `.tmp`.
Generations begin at one and are consecutive. Equal component generation numbers must retain exact
length and digest. A successor advances each component by zero or one generation, cannot remove
previously present cold authority, and can introduce cold authority only at cold generation one.

Installation verifies both component owners before creating the marker. The final pair-directory
sync is the aggregate durability boundary. Recovery selects the highest pair final, exact-loads the
named component generations, and compares lengths, digests, identities, and Manifest-v2/cold
binding. Later component finals not named by that record are uncommitted orphans. Recovery never
searches lower pair generations after damage to the highest commit. No irreversible cleanup may use
a component generation until its pair commit is durable.

Major zero, invalid integrity, reserved bytes, invalid presence fields, broken generation
relationships, noncanonical names, gaps, and owner/name disagreement are corruption. A
checksum-valid unknown nonzero version or unknown required flag is unsupported.
