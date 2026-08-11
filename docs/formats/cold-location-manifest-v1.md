# ChronosDB Cold Location Manifest v1

> **Status: accepted canonical codec, exact Manifest v2 binding validation, and durable generation
> installation/no-fallback recovery are implemented. Atomic in-memory publication and durable
> cross-directory pair commit with Manifest v2 are implemented; local-source reclamation, remote
> deletion, and query-path integration remain separate work.**

Cold Location Manifest v1 is an immutable full-generation registry that maps exact CSEG identities
from one Manifest v2 database generation to one configured object-store namespace. It does not
replace Manifest v2 as logical row/version authority and never derives state from an object-store
listing. Manifest v2 bytes and reserved fields remain unchanged.

All integers are little-endian. UUID arrays and SHA-256 values use their canonical byte order rather
than a native object representation. File offsets and lengths are unsigned 64-bit values. The
maximum file length is 1 GiB, maximum descriptor count is 1,048,576, maximum aggregate key bytes is
64 MiB, and one object key is at most 1,024 bytes.

## Header

The header is 256 bytes.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | magic `CHCLDMF1` |
| 8 | 2 | format major = 1 |
| 10 | 2 | format minor = 0 |
| 12 | 4 | header length = 256 |
| 16 | 4 | file flags = 0 |
| 20 | 4 | zero |
| 24 | 8 | total file length |
| 32 | 8 | nonzero cold generation |
| 40 | 8 | previous cold generation, zero only for generation 1 |
| 48 | 8 | nonzero pinned Manifest v2 generation |
| 56 | 16 | database UUID |
| 72 | 16 | deployment-assigned object-store UUID |
| 88 | 8 | location descriptor count |
| 96 | 8 | location table offset = 256 |
| 104 | 8 | object-key table offset |
| 112 | 8 | object-key table byte length |
| 120 | 8 | aligned trailer offset |
| 128 | 120 | zero |
| 248 | 4 | CRC32C of bytes 0–247 |
| 252 | 4 | zero |

The object-store UUID identifies the configured endpoint/bucket namespace outside this byte format;
credentials and endpoint URLs are not durable fields. A runtime must map that UUID to exactly one
authorized `ObjectStore` configuration and fail closed when it cannot.

## Location descriptor

Descriptors are 96 bytes, strictly sorted by nonzero part UUID with no duplicate part or object-key
identity.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 16 | CSEG part UUID |
| 16 | 8 | exact CSEG file length, nonzero |
| 24 | 32 | SHA-256 of the complete CSEG bytes |
| 56 | 8 | object-key offset relative to the key table |
| 64 | 4 | object-key byte length |
| 68 | 4 | flags = 0 |
| 72 | 20 | zero |
| 92 | 4 | descriptor CRC32C of bytes 0–91 |

Key offsets form one exact contiguous prefix in descriptor order: the first is zero and every next
offset equals the preceding offset plus length. Keys are nonempty Unicode scalar-value UTF-8 with no
C0 control or DEL byte. The bytes are otherwise preserved exactly; they are not NUL terminated.

## Key table, alignment, and trailer

The key table immediately follows the descriptor table. Zero padding aligns the trailer to eight
bytes. The trailer is four zero bytes followed by CRC32C of every preceding byte. Decoding validates
header integrity before trusting counts or offsets, then validates the complete-file CRC before
allocating descriptor strings or interpreting key bytes. Exact decoding rejects trailing bytes.

## Binding and lifecycle

A valid cold manifest is usable only with a pinned Manifest v2 generation whose database UUID and
generation equal the header. Every cold descriptor must find one Manifest v2 part with equal part
UUID, file length, and content SHA-256. An equal key returned by object storage is still verified by
that digest; provider ETag and listing state are not authority.

The codec alone does not authorize deletion of the local CSEG. The durable installer uses canonical
`generation-00000000000000000001.clm` final names, exact `.tmp` candidates, synchronized complete
readback, no-replace rename, directory synchronization, and highest-consecutive/no-fallback
selection. The tiered publisher now atomically exposes and pins one compatible Manifest-v2/cold
pair in memory. Tiered Pair Commit v1 covers crash selection. A later reclamation owner must wait
for predecessor reader pins and only then reclaim a local source. Remote deletion additionally
requires proof that no
retained logical Manifest generation or cold generation references the object.

## Compatibility and rejection

Major zero, damaged integrity, invalid layout, noncanonical ordering/keys, unknown identities, and
reserved-byte damage are corruption. A checksum-valid unknown nonzero version or required flag is
unsupported. Configured limits are resource exhaustion. Prefix decoding reports the exact required
header or generation length for truncation. Readers never fall back from a damaged highest durable
cold generation without an explicit recovery contract.
