# Mergeable Vector Aggregate State v1

> **Status:** accepted with implemented canonical encoding, exact decoding, and bounded partial-I/O
> ownership.

This nested frame carries one all-type vector aggregate partial without finalizing it. It preserves
the exact input definition and the sufficient state required to merge COUNT, SUM, AVG, MIN/MAX, or
variance. It deliberately carries no query, tablet, group-key, sequence, or terminal identity; a
cross-service exchange must add and authenticate that enclosing correlation rather than treating
this value as a complete message.

All integers are little-endian. Floating fields preserve IEEE-754 bits. The maximum variable
extremum payload is 1,048,576 bytes, so the maximum frame is 1,048,692 bytes.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `CHDVAGS1` |
| 8 | 2 | Major version `1` |
| 10 | 2 | Minor version `0` |
| 12 | 4 | Header length `112` |
| 16 | 8 | Exact complete frame length |
| 24 | 1 | Aggregate operation, `0..7` in public enum order |
| 25 | 1 | Flags |
| 26 | 2 | Input logical-type code, or zero |
| 28 | 2 | Input type parameter 0, or zero |
| 30 | 2 | Input type parameter 1, or zero |
| 32 | 4 | Input column ordinal, `0..4095`, or zero |
| 36 | 4 | Extremum payload length |
| 40 | 8 | COUNT/AVG/variance count |
| 48 | 32 | Exact SUM signed-magnitude limbs, eight `uint32` limbs |
| 80 | 4 | FLOAT32 SUM bits |
| 84 | 4 | Zero reserved bytes |
| 88 | 8 | FLOAT64 SUM, AVG sum, or variance mean bits |
| 96 | 8 | Variance M2 bits |
| 104 | 4 | Zero reserved bytes |
| 108 | 4 | Header CRC32C of bytes `[0,108)` |
| 112 | variable | MIN/MAX scalar payload |
| final - 4 | 4 | Frame CRC32C of every preceding frame byte |

Flag bit 0 means input present, bit 1 means nullable input, bit 2 means SUM value or extremum
present, and bit 3 is the exact-SUM sign. Other bits are zero. COUNT(*) alone omits its input.
Absent input fields are all zero. Exact zero has a nonnegative sign.

## Operation-specific canonical state

- COUNT and COUNT(*) use only the count field and retain the SQL INT64 ceiling.
- exact integer and DECIMAL SUM use the presence bit, sign, and 256-bit magnitude;
- FLOAT32 SUM uses its 32-bit field, while FLOAT64 SUM uses the first 64-bit field;
- AVG uses count plus the first 64-bit sum field;
- population and sample variance use count, mean, and M2; and
- MIN/MAX use the presence bit plus one canonical scalar payload.

Every unused field is positive zero. Empty SUM, AVG, and variance states require positive-zero
numeric fields. A nonempty floating state preserves NaN, infinity, and signed-zero bits produced by
the local kernel. Fixed extrema use the physical widths BOOL 1, integers 1/2/4/8, FLOAT32 4,
FLOAT64 8, DECIMAL 16, TIMESTAMP_NS 8, DATE 4, and UUID 16. DECIMAL coefficients use canonical
little-endian two's complement and UUIDs use canonical UUID order. STRING and SYMBOL are exact
valid UTF-8 bytes; BINARY is opaque. The value-present flag distinguishes an empty variable value
from absent state.

## Validation and ownership

Exact decoding rejects truncation, trailing bytes, checksum failure, nonzero reserved bytes,
unknown or invalid definitions, noncanonical unused state, malformed fixed values, invalid UTF-8,
and inconsistent lengths. Header magic and CRC are checked before the frame length can allocate.
The complete CRC is checked before any variable payload allocation. Checksum-valid unknown versions
are unsupported. Caller frame and variable-payload limits may be lower than the hard limits.

Variable extrema reserve query memory before copying and retain that credit in the decoded state.
Failures release the new credit and expose no partial state. Fixed-width successful decode does not
allocate state payload storage.

The reader retains the fixed header first, then exactly one validated bounded frame. It reports the
exact consumed prefix, leaves a coalesced successor with the caller, and fails sticky. The move-only
write cursor owns one canonical frame, exposes only its unwritten suffix, rejects over-advance
without progress, and makes a moved-from cursor complete.

CRC32C detects accidental damage; it is not authentication. A future exchange envelope must bind
this nested frame to admitted schema, query/tablet identity, group keys, sequencing, and terminal
closure.
