# ADR 0079: Temporal Mutation Command v1

- **Status:** accepted
- **Date:** 2026-08-08
- **Owners:** ChronosDB temporal-semantics and storage maintainers

## Context

The in-memory temporal provider implements bitemporal visibility but cannot recover corrections,
replacements, or tombstones. Re-encoding every SQL scalar independently would duplicate the frozen
columnar physical-type rules, while reusing ordinary append commands cannot distinguish version
semantics or audit times.

## Accepted decision

WAL application format 1 kind 3 contains one checksummed Temporal Mutation Command v1. Row values
reuse an exact Columnar Batch v1. A parallel bounded metadata section supplies unique logical
identities, mutation kinds, event times, and receive times. The command carries system commit time;
the enclosing WAL record sequence supplies authoritative system order and source identity.

The codec is exact and independently checksummed so the same logical bytes may later be carried by
a Raft entry without reinterpretation. It rejects duplicate identities, row/descriptor mismatch,
unknown versions or kinds, hostile lengths, schema-identity mismatch, and trailing bytes.

## Consequences and alternatives

Temporal rows retain the same physical column representation as ingestion and can be converted to
scalar or vector execution without a second durable scalar format. Tombstones retain a complete
schema-shaped row in v1; this costs space but keeps identity/schema validation uniform.

In-place updates and latest-only storage were rejected by ADR 0007. Extending COLUMNAR_APPEND v1
was rejected because that frozen command promises append/dedup behavior rather than multi-version
replacement semantics. JSON or native variants were rejected for noncanonical layout and unsafe
bounded recovery.

The implemented command-specific recovery owner validates retained schemas, applies enclosing WAL
identity/order, rejects impossible committed mutation history, and returns the locked reopened
writer. The live single-writer executor rejects invalid transitions before bounded coordinator
admission, preserves requested `ASYNC`/`LOCAL_SYNC` semantics, publishes only after completion, and
fails stale state closed after any post-admission uncertainty. A fresh provider can also atomically
restore canonical retained history: its first observed identity version may be any mutation kind,
because an earlier original can have expired, and the caller supplies an explicit proven
table-wide retention boundary rather than inferring one from physical extrema. A mixed database
application dispatcher, complete Manifest/WAL checkpoint
composition, vector publication, retention pins, and Raft application remain subsequent
integration tasks; these boundaries do not yet claim Phase 13.

## Affected invariants and validation

Invariants 1, 4, 6–10, 13, 14, and 18 apply. Focused tests cover exact round trip, nested schema
identity, correction metadata, checksum damage, duplicate-identity rejection, physical-cell
application, ordered original/correction replay, next-sequence reopen, and impossible-history
rejection. Live tests cover a covering `LOCAL_SYNC` frontier, recovery equivalence, pre-WAL semantic
rejection, sequence preservation, and post-admission fail-closed behavior. Independently generated
golden framing and CRC fixtures, a structure-aware sanitizer-backed fuzzer, and checksum-valid
hostile outer framing, count, metadata, caller-limit, and nested-batch matrices are implemented.
Unknown application identity and command versions are classified as unsupported, and repeatable
recovery over a mixed v1/future-format WAL fails during preflight without publishing partial state.
Exhaustive test-only allocation injection covers every codec-owned canonical encode/exact-decode
allocation and every owned allocation observed on the canonical single-table command-specific WAL
recovery path, including rollback and lock release. Canonical one-tablet Manifest-composed startup
sweeps cover both an empty tablet and authenticated single-CSEG restoration, return no state on
failure, and prove that both Manifest and WAL locks can be reacquired after each failure. The exact
golden fixture runs in the standard Linux GCC, Linux Clang/libc++, and macOS AppleClang CI matrix.
Sustained fuzzing, allocation injection for multi-part/multi-tablet Manifest startup, and crash
injection remain deferred to Phase 18.
