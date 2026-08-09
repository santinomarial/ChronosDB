# ADR 0090: Materialized View Checkpoint v1

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB live-query and durability maintainers
- **Extends:** [ADR 0089](0089-exact-logical-materialized-view-checkpoints.md)
- **Extended by:** [ADR 0091](0091-durable-materialized-view-checkpoint-storage.md)

## Context

The exact logical checkpoint contract cannot survive process restart until it has versioned,
bounded, integrity-protected bytes. The format must preserve floating running state bit-for-bit,
authenticate every allocation-driving count before allocation, and reject a window or contribution
set that no longer agrees with the global visible rows.

## Accepted decision

Materialized View Checkpoint v1 is the source-specific durable representation documented in
[`materialized-view-checkpoint-v1.md`](../formats/materialized-view-checkpoint-v1.md). A 160-byte
fixed header binds one tablet/WAL position, window definition, watermark, declared bounds, body
counts, and total bytes. CRC32C covers the header with its checksum slot zero; a final CRC32C covers
the complete preceding file.

Global rows and per-window contributions use fixed 40-byte records. Window headers store exact
revision/finalization state and the count/sum/VWAP/Welford running fields as IEEE-754 bits. All body
sizes are checked from authenticated counts before allocation. Decoding reconstructs the logical
owner, runs the complete ADR 0089 semantic validation, and exports canonical state.

A second 160-byte Bound Materialized View Checkpoint v1 envelope binds the nested state to exact
database, view, table, schema/version, and plan-fingerprint identity. Filesystem owners accept only
this envelope, preventing a valid source-state file from being silently adopted by another view.

## Consequences and alternatives

Per-window contributions duplicate global row fields. The duplication is bounded and permits direct
audit of each aggregate's membership; a later measured major version may compact the representation
without reinterpreting v1. Exact numeric fields preserve continuation even when a different replay
order would round differently.

Serializing private containers or native structs was rejected as ABI-dependent. Storing only output
snapshots was rejected because corrections require row contributions. A checksum only at the end was
rejected because hostile counts must be authenticated before they influence allocation.

This ADR freezes bytes and codec behavior only. A successful encode is not a durability claim.
ADR 0091 supplies lock, write/readback validation, file and directory synchronization, atomic
immutable installation, and exact recovery selection; source-retention ordering remains separate.

## Affected invariants and validation

Invariants 4, 8, 10, 12–15, and 17 apply. Focused tests round-trip exact state and post-decode
continuation, reject a complete-body bit flip, reject an authenticated unknown major, enforce caller
decode limits, and compile the public header independently. Golden fixtures, hostile field matrices,
fuzzing, cross-compiler bytes, allocation failure, durable installation, and process crashes remain
in the Phase 18 ledger.
