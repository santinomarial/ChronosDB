# ADR 0118: Durable tablet movement checkpoint generations

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB distributed-systems and storage maintainers
- **Extends:** [ADR 0117](0117-tablet-movement-checkpoint-v1.md)
- **Extended by:** [ADR 0126](0126-mixed-tablet-movement-checkpoint-generations.md)

## Context

Canonical checkpoint bytes are not restart authority until one filesystem owner can exclude
concurrent writers, bind recovery order into the durable bytes, reject copied or renamed states,
and complete an auditable file and directory synchronization protocol.

## Decision

One `TabletMovementCheckpointStorage` directory belongs to one exact tablet under a nonblocking
exclusive advisory `LOCK`. A versioned generation envelope binds one nonzero, monotonically
contiguous checkpoint generation to the nested Tablet Movement Checkpoint v1 bytes. Immutable files
are named `generation-<20-digit-generation>.movc`, beginning at one without gaps.

Installation accepts only the next generation, except that retrying an existing generation with
byte-identical content succeeds idempotently. The owner exclusively creates a canonical temporary,
writes all bytes, exact-reads and decodes the generation and nested checkpoint, synchronizes and
closes the file, atomically renames without replacement, and synchronizes the directory. Only the
final directory sync establishes durable success. Failure after rename but before that sync poisons
the live owner because the name's crash durability is uncertain.

Reopen acquires the same lock, removes only canonical regular temporaries, and synchronizes any
removal. Latest selection requires final generations to be contiguous from one, exact-decodes the
largest file, and requires the embedded generation and tablet identity to match its filename and
configured owner. Unrelated names are ignored; malformed names in the recognized namespace,
foreign tablets, checksum damage, gaps, or a valid generation renamed to another coordinate fail
closed.

## Rationale and alternatives

The outer envelope keeps the logical checkpoint format reusable while preventing a valid older
checkpoint from masquerading as a later recovery point. Retaining immutable generations avoids a
mutable latest-pointer file and preserves prior evidence for future repair or diagnosis.

Replacing one fixed path was rejected because a crash could destroy the only recovery point.
Filename-only generations were rejected because copied bytes could silently claim a false order.
Deleting old generations immediately was rejected until a retention policy proves the selected
recovery point cannot be lost.

## Consequences

A successful installation is restartable under the existing Linux POSIX durability assumptions,
and reopen deterministically selects the same latest valid generation. Storage grows with every
checkpoint and each self-contained prefix, so reclamation and chunked snapshot persistence remain
separate work. The owner serializes filesystem mutation but does not itself drive reconfiguration or
decide when a new checkpoint is required.

## Affected invariants and validation

Invariants 1, 2, 4, 8, 10, 11, 14, and 18 apply. Real-filesystem tests cover exclusive ownership,
exact next-generation admission, byte-identical retry, conflicting retry, generation/name binding,
tablet binding, latest selection, close/reopen reconstruction, canonical temporary cleanup, and
installed corruption. Syscall fault injection, process-kill crash points, power-loss qualification,
generation reclamation, permission matrices, and chunked-prefix storage remain deferred.

## Migration and rollback

No earlier durable movement-generation namespace exists. A rollback can ignore these files, but it
cannot safely resume their movements. Future envelope versions must retain explicit v1 decode or
provide an offline migration that preserves embedded generation and nested checkpoint bytes.

## References

- [Tablet Movement Checkpoint v1 format](../formats/tablet-movement-checkpoint-v1.md)
- [POSIX I/O learning guide](../learning/posix-io.md)
- [ADR 0116](0116-raft-metadata-tablet-reconfiguration.md)
