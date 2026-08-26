# Distributed Mutable Vector Fragment v2

> **Status: accepted and implemented.** Version 2 extends v1 only for grouped plans with one exact
> nested Distributed Vector Pre-Group Program v1. The v1 decoder path and v1 encoder output for
> fragments without a program remain supported.

All integers are little-endian. Magic is `CHDMVFR2`, version is `2.0`, and the header remains 248
bytes. Fields through the nested result-schema CRC32C at offset 240 are identical to v1. Version 2
uses bytes `[240,244)` for the exact pre-group program length and `[244,248)` for a CRC32C of header
bytes `[0,244)`. It has no reserved tail word.

Payload order is the projection ordinal vector, Distributed Vector Plan Intent v1, Distributed
Vector Result Schema v1, and Distributed Vector Pre-Group Program v1, followed by the existing
four-byte complete-frame CRC32C. The nested program length is `116..4,194,304` bytes. The maximum
outer frame length is the v1 maximum plus 4 MiB.

The nested program is valid only when the plan mode is grouped aggregate. Its ordered output shapes
replace raw projected source shapes as the plan input authority and must exactly produce the result
schema. Binding and workers independently prove every program input leaf's ordinal, logical type,
and nullability against the exact destination schema. Workers evaluate the program after exact
event-time filtering and before grouped aggregation.

Exact decoding verifies the version-specific magic/header checksum pair, complete-frame checksum,
all v1 fields, exact nested lengths, and the nested program's own checksums, bounds, canonical
instructions, scalar payloads, and typed DAG. Caller limits include an independent nested-program
limit. Unknown versions return `NOT_SUPPORTED`; damaged bytes return `CORRUPTION`; caller-bound
exhaustion returns `RESOURCE_EXHAUSTED`.
