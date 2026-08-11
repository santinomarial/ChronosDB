# ADR 0148: Durable physical-part chunk receipt

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB cluster, storage, and distributed-systems maintainers
- **Extends:** [ADR 0147](0147-tablet-physical-part-chunk-v1.md)

## Context

Valid physical-part chunk frames are not restart authority. A target must exclude concurrent owners,
admit only one gap-free object prefix, reject changed retries, reconstruct exactly after a process
restart, and verify the complete CSEG SHA-256 without retaining a possible 64-GiB object in memory.

## Decision

`TabletPhysicalPartChunkStorage` owns one exact transfer session in one existing directory under a
nonblocking exclusive `LOCK`. Immutable files are named
`part-chunk-<20-digit-offset>.pchk`; the only temporary form appends `.tmp`. A new chunk is accepted
only at the current durable prefix end. An existing offset is an idempotent retry only when its
complete canonical encoded bytes are identical.

Installation exclusively creates a temporary, writes and exact-decodes readback, synchronizes the
file, closes it, atomically renames without replacement, and synchronizes the directory before live
progress advances. Failure after rename but before directory synchronization poisons the owner.

Open removes only canonical regular temporaries and synchronizes cleanup. It parses final offsets,
orders them numerically, exact-decodes every file against the configured session and filename, and
requires a contiguous prefix beginning at zero. Gaps, renamed chunks, foreign sessions, malformed
recognized names, nonregular recognized entries, corruption, and configured chunk-count excess fail
closed.

Completion requires received length equal to the declared object length. It then reopens and
revalidates each chunk in offset order, feeds one bounded payload at a time to the maintained
incremental SHA-256 owner, and compares the result to the session digest. It returns only an owning
completion report. It does not concatenate the CSEG, install a final part, publish Manifest
ownership, advance movement readiness, or delete chunks.

## Consequences and validation

Per-chunk synchronization and files add metadata and write amplification, accepted for unambiguous
restart/retry behavior. A later measured optimization may use an append file only with an equally
explicit torn-tail and durable-progress protocol. Memory during completion is bounded by one encoded
chunk plus one decoded payload and hash state, independent of object length.

Real-filesystem tests cover exclusive ownership, contiguous installs, exact retry/conflict,
incomplete and complete finalization, reopen, streamed digest validation, foreign session, gap,
temporary cleanup, durable corruption, canonical names, and chunk-count exhaustion. Syscall fault
injection and subprocess crash points remain required hardening evidence.

Invariants 1–5, 8, 10, 11, 14, and 18 apply.

## Migration and rollback

No previous physical-part receipt namespace exists. Rollback may remove an unreferenced transfer
directory offline, but must not treat its chunk prefix as a final CSEG or installed Manifest
authority.

## References

- [Tablet Physical Part Chunk v1](../formats/tablet-physical-part-chunk-v1.md)
- [POSIX I/O learning guide](../learning/posix-io.md)
