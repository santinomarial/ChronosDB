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
writer. Live WAL admission/acknowledgment, a mixed database application dispatcher, checkpoints,
vector/CSEG history persistence, retention pins, and Raft application remain subsequent integration
tasks; these boundaries do not yet claim Phase 13.

## Affected invariants and validation

Invariants 1, 4, 6–10, 13, 14, and 18 apply. Focused tests cover exact round trip, nested schema
identity, correction metadata, checksum damage, duplicate-identity rejection, physical-cell
application, ordered original/correction replay, next-sequence reopen, and impossible-history
rejection. Golden fixtures, fuzzing, broad hostile-length matrices, mixed-version recovery, and
crash injection are deferred to Phase 18.
