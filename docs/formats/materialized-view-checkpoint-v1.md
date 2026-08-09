# Materialized View Checkpoint v1

> **Status:** format 1.0 codec is implemented. Durable filesystem installation is specified by a
> lock-protected owner; successful encoding alone is not a durability claim.

All integers are fixed-width little-endian. Floating values are stored as their exact IEEE-754
binary64 bits in little-endian byte order. The format contains no native structs, pointers, or ABI-
dependent padding.

## Fixed header

The 160-byte header is:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | magic `CHMVCP1\0` |
| 8 | 2 | major `1` |
| 10 | 2 | minor `0` |
| 12 | 4 | header size `160` |
| 16 | 4 | flags, zero |
| 20 | 4 | reserved, zero |
| 24 | 8 | exact total file bytes |
| 32 | 16 | source tablet ID |
| 48 | 16 | source WAL ID |
| 64 | 8 | applied WAL record sequence; zero is the initial boundary |
| 72 | 8 | window width |
| 80 | 8 | window slide |
| 88 | 8 | allowed lateness |
| 96 | 8 | declared maximum windows |
| 104 | 8 | declared maximum logical rows |
| 112 | 8 | watermark |
| 120 | 8 | global logical-row count |
| 128 | 8 | window count |
| 136 | 8 | total per-window contribution-row count |
| 144 | 4 | CRC32C of the complete header with this field zero |
| 148 | 12 | reserved, zero |

The source identities and window definition must pass the logical checkpoint contract. Counts and
declared bounds must fit both the file and caller decode limits.

## Logical rows

Global logical rows follow in strictly increasing row-identity order. Each 40-byte row is:

| Relative offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | nonzero row identity |
| 8 | 8 | signed event time |
| 16 | 8 | nonzero source order |
| 24 | 8 | finite value bits |
| 32 | 8 | finite weight bits |

The value-times-weight product must also be finite.

## Windows and contributions

Windows follow in increasing `(start, end)` order. Each fixed 88-byte window header is:

| Relative offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | signed start |
| 8 | 8 | signed end |
| 16 | 8 | nonzero output revision |
| 24 | 4 | bit 0 emitted, bit 1 finalized; all other bits zero |
| 28 | 4 | reserved, zero |
| 32 | 8 | contribution-row count |
| 40 | 8 | aggregate count |
| 48 | 8 | exact running sum bits |
| 56 | 8 | exact running weighted-sum bits |
| 64 | 8 | exact running weight-sum bits |
| 72 | 8 | exact Welford mean bits |
| 80 | 8 | exact Welford M2 bits |

Its contribution rows immediately follow in the same 40-byte layout and row-identity order. They
must equal exactly the global rows whose event time selects this window. Empty historical windows
remain representable. Window alignment, width, finalization at the stored watermark, revisions,
aggregate counts, and populated-window completeness receive semantic validation.

## Integrity and compatibility

The final four bytes are CRC32C over every preceding byte, including the stored header checksum.
No trailing bytes are allowed. The header checksum protects all counts and lengths before body
allocation; checked arithmetic proves their fixed-size body exactly equals the file size.

Unknown magic or major is unsupported. Any minor or header size other than 1.0/160 is unsupported.
Checksum, reserved-field, canonical-order, nested-state, or exact-size disagreement is corruption.
Caller size/count limits return resource exhaustion. Encoders reject noncanonical logical state and
emit only 1.0.

## Bound durable envelope

Filesystem owners use the Bound Materialized View Checkpoint v1 envelope rather than storing a bare
logical checkpoint. Its 160-byte header uses magic `CHMVCB1\0`, version 1.0, flags/reserved/total
fields at the same framing offsets, and then stores database ID at 32, view ID at 48, table ID at 64,
schema ID at 80, schema version at 96, the 32-byte plan fingerprint at 104, nested payload size at
136, header CRC32C at 144, and twelve reserved zero bytes at 148. The exact bare v1 checkpoint
follows, then a four-byte whole-envelope CRC32C.

All four durable identities and schema version must be nonzero. The outer header authenticates the
view/schema/plan binding and nested size before parsing. The nested checkpoint retains its own
header/file checksums and source tablet/WAL binding. A storage owner must exact-match the envelope
identity against its configured database/view definition; merely decoding a valid envelope never
selects it as authority.

Local durable envelopes use `checkpoint-<20-digit-WAL-sequence>.mvcp`. Installation writes and
exact-validates a deterministic `.tmp`, synchronizes it, atomically renames without replacement,
then synchronizes the directory. Existing identical bytes are an idempotent retry; different bytes
at one sequence are corruption. Recovery selects only a completely revalidated canonical file.
