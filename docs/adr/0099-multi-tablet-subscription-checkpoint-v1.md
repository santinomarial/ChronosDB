# ADR 0099: Multi-tablet Subscription Checkpoint v1

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB live-query and durable-format maintainers
- **Extends:** [ADR 0098](0098-exact-multi-tablet-subscription-checkpoints.md)

## Context

ADR 0098 defines the exact logical coordinator state needed after restart. It needs portable,
bounded, corruption-detecting bytes before an atomic filesystem owner can install generations.

## Accepted decision

[Multi-tablet Subscription Checkpoint v1](../formats/multi-tablet-subscription-checkpoint-v1.md)
uses a 128-byte identity/count header, canonical 48-byte source entries, ordered variable-length
change records with fixed 80-byte envelopes, and a complete-file CRC32C trailer. All integers are
explicit little-endian. The format binds database, table, plan fingerprint, schema/version, exact
tablet/WAL lineages, latest/expiry frontiers, and retained admission order.

The decoder checks outer framing and CRC before decoded-state allocation, then reconstructs owned
logical state and reruns exact semantic validation. Count, total-byte, key, and payload limits are
finite and caller-configurable below an absolute 1 GiB ceiling.

## Consequences and alternatives

The format deliberately excludes token MAC keys and active subscriber/socket state. The storage
owner must supply secrets from protected configuration and clients must resume from authenticated
tokens. Format bytes alone are not durable until a later owner performs synchronized no-replace
installation and directory synchronization.

Serializing native structs was rejected for portability and padding safety. Per-record checksums
were rejected for v1 because the complete-file checksum detects every stored mutation before state
is exposed; future random-access recovery would require a new version. Reconstructing admission
order from per-tablet sequences remains forbidden.

## Affected invariants and validation

Invariants 10, 12, 14, 15, and 17 apply. Focused tests lock the complete fixture size and independent
FNV-1a fingerprint, round-trip exact logical state, reject a bit flip through CRC32C, and reject a
semantically discontinuous input before encoding. Sustained fuzzing, allocation sweeps,
cross-compiler golden verification, and durable crash installation remain Phase 18 work.
