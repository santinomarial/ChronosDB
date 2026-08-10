# ADR 0117: Tablet Movement Checkpoint v1

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB distributed-systems and storage maintainers

## Context

Tablet movement retained its phase, replica intent, snapshot metadata, and received snapshot prefix
only in memory. A process restart therefore lost whether transfer could resume and whether target
promotion or source removal had already crossed an authoritative boundary. The reconfiguration
coordinator needs an exact restart input before automated durable ownership is safe.

## Decision

Tablet Movement Checkpoint v1 is a canonical little-endian value containing the tablet identity,
placement epoch, source/target nodes, phase, sorted voter and learner sets, snapshot manifest/index/
term/size/checksum, received length, and the exact received snapshot prefix. A 64-byte versioned
header protects its interpretation with a header CRC, the payload has its own CRC, and a trailer CRC
covers the entire header and payload. Reserved bytes are zero and unknown versions fail closed.

Decode bounds total bytes, replica counts, and received bytes before allocation. It then applies the
same phase-specific semantic validation as `TabletMovement::recover`: pre-promotion phases retain
the source and target learner with capacity for promotion; caught-up phases require a complete
snapshot with matching content CRC; promoted/complete phases require the exact source/target voter
relationship and no learners. Recovery adopts owned bytes only after validation.

The checkpoint is an encoded value, not yet a filesystem installation protocol. A later owner must
write, synchronize, atomically install, and directory-synchronize generations before treating them
as restart authority.

## Detailed rationale

Embedding the received prefix makes one checkpoint self-contained and allows exact retry without
trusting an unrelated temporary file. This can amplify writes for large snapshots; a future
chunk-file/checkpoint design may replace the storage owner while retaining v1 decode support.
Separate header/payload/trailer integrity rejects damaged framing before semantic state is trusted.

## Alternatives considered

- **Persist phase only:** cannot resume or verify the received prefix.
- **Trust a temporary snapshot file by length:** cannot detect changed bytes or bind it to movement
  identity.
- **Serialize the native record representation:** is ABI-, padding-, and endianness-dependent.
- **Reuse a Raft or metadata command:** movement transfer progress is local orchestration state, not
  a replicated application command.

## Consequences

Every in-memory phase can round-trip and partial transfer resumes from the exact retained offset.
Checkpoint size is proportional to the received prefix and bounded by configured snapshot and
checkpoint limits. Durable file generations, cleanup, crash installation evidence, and write-
amplification optimization remain follow-up work.

## Affected invariants

Invariants 2, 4, 8, 10, 11, 14, and 18 apply. Checksummed canonical bytes and exact recovery support
idempotent restart without exposing a partially described snapshot or weakening movement ordering.

## Validation plan

Focused tests round-trip and reconstruct partial transfer, resume the suffix, finish/catch up,
round-trip empty and complete phases, and reject damage, truncation, inconsistent recovery, and
invalid limits. Golden fixtures, fuzzing, allocation failure, all phase mutations, filesystem crash
points, large prefixes, and cross-version tests remain deferred.

## Migration or rollback considerations

No prior durable movement checkpoint exists. Future versions use the major/minor policy and must
retain explicit v1 handling. Rollback may ignore uninstalled checkpoints but cannot safely resume
their movements without restarting the learner transfer.

## Unresolved questions

Generation naming, atomic file installation, prefix chunking, encryption, cleanup after completion,
and whether large transfers should use independently checksummed chunk files remain unresolved.

## References

- [Tablet Movement Checkpoint v1 format](../formats/tablet-movement-checkpoint-v1.md)
- [ADR 0116](0116-raft-metadata-tablet-reconfiguration.md)
