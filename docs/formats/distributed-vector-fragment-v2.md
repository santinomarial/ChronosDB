# Distributed Vector Fragment v2

> **Status: accepted and implemented.** This wrapper preserves exact Fragment v1 bytes and adds one
> exact Distributed Vector Result Schema v1. V1 decoders do not accept it.

All integers are unsigned little-endian. The maximum frame is 4,344,224 bytes.

The 64-byte header contains: magic `CHDVFDS2` (8), version `2.0` (2 each), header length (4), exact
frame length (8), exact v1 dispatch length (8), exact result-schema length (8), CRC32C of each nested
value (4 each), CRC32C of bytes `[0,48)` (4), and 12 zero bytes. Exact v1 dispatch bytes and exact
result-schema bytes follow, then one CRC32C of every preceding wrapper byte.

Header integrity and hard/caller lengths pass before slicing. The complete and per-payload CRCs pass
before exact nested decode. Unknown versions are unsupported; truncation, trailing bytes, reserved
data, cross-protocol magic, and nested damage fail closed. Lower caller frame and nested limits
remain resource exhaustion.

The authority binder first constructs the unchanged proof-bound v1 dispatch. It then derives the
projected physical input shapes from that same committed destination schema and validates the result
descriptors against the plan before publishing one owning v2 value. This adds no execution or
result-cell semantics.

The streaming reader retains only the 64-byte header until its integrity, version, hard lengths,
caller outer and nested frame limits, and reserved bytes pass. It then owns exactly the declared
frame, leaves any coalesced successor caller-owned, exact-decodes both nested values before
publication, and retains sticky frame failure. The move-only write cursor owns canonical encoded
bytes, rejects over-acknowledgement without advancing, and leaves a moved-from cursor complete.

A compatible multi-tablet v2 owner pins one exact Manifest generation, retains the plan-ordered v1
authority dispatches, and owns the shared result schema once. Before publication, that schema is
proved against every tablet's exact committed projection. Pairing any exposed dispatch with the
shared schema forms its authorized v2 value without multiplying retained descriptor memory by the
tablet count. See [ADR 0367](../adr/0367-bounded-distributed-vector-fragment-v2-ownership.md).
