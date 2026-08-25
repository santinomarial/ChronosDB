# Distributed Vector Pre-Group Program v1

> **Status: accepted and implemented.** This is an owned worker-program format. It is not yet
> embedded in a distributed mutable fragment; that is a separate protocol-version decision.

All integers are little-endian. The frame begins with magic `CHDVPGP1`, version `1.0`, a 64-byte
header, and ends with a four-byte complete-frame CRC32C. It contains `1..4096` ordered output
expressions and at most 65,536 total instructions. The complete frame is limited to 4 MiB.

The header carries exact frame length, expression count, total instruction count, payload length,
payload CRC32C, a CRC32C of bytes `[0,40)`, and zero reserved bytes. Every expression has a 16-byte
header containing its instruction count, exact byte length, CRC32C, and a zero reserved word.

Each instruction has a fixed 32-byte header followed by its constant payload. Tags `1..5` are input,
constant, unary, cast, and binary. Input ordinals and instruction operands are unsigned 32-bit
values. Logical types use the schema type code and two parameters. A single tag-specific flag
encodes input nullability or constant NULL. Other fields must be zero when unused. Constants use
canonical scalar bytes and an independent CRC32C; one constant is limited to 1 MiB. Unary operation
codes `1..8` are positive, negative, NOT, IS NULL, IS NOT NULL, ABS, lower ASCII, and upper ASCII.
Binary codes `1..15` are AND, OR, the six comparisons, the five arithmetic operations, COALESCE,
and time bucket.

Decoding validates hard and caller bounds, header and complete-frame checksums, payload and
expression checksums, canonical reserved fields, exact section consumption, logical types,
canonical scalar values, earlier-only operand references, and inferred expression shapes before
returning owned `VectorExpression` values. Unknown versions return `NOT_SUPPORTED`; damaged or
noncanonical bytes return `CORRUPTION`; caller-bound exhaustion returns `RESOURCE_EXHAUSTED`.
CRC32C is an integrity mechanism, not peer authentication.
