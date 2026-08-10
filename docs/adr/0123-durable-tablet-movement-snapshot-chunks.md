# ADR 0123: Durable tablet movement snapshot chunks

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB distributed-systems and storage maintainers
- **Extends:** [ADR 0122](0122-tablet-movement-snapshot-chunk-v1.md)
- **Extended by:** [ADR 0124](0124-tablet-movement-external-prefix-reference-v1.md)

## Context

Canonical chunk bytes are not resumable authority until one filesystem owner excludes concurrent
writers, admits only a gap-free prefix, rejects changed retries, and reconstructs the same progress
after restart. A complete transfer also must prove the whole snapshot checksum before its bytes can
be handed to the application-snapshot installer.

## Decision

One `TabletMovementSnapshotChunkStorage` directory belongs to one exact tablet/placement-epoch/
source/target/snapshot session under a nonblocking exclusive advisory `LOCK`. Immutable files are
named `chunk-<20-digit-offset>.mchk`; a single `.tmp` suffix is the recognized interrupted install.
The first offset is zero and each later offset must equal the end of the installed prefix.

Installation encodes the requested chunk, rejects another session or a gap, exact-loads any
existing offset, and accepts only byte-identical retry. A new temporary is exclusively created,
written, exact-read and decoded, synchronized and closed, renamed without replacement, and followed
by directory synchronization. Only then does live progress advance. Directory-sync failure after
rename poisons the owner because crash durability of the visible name is uncertain.

Opening ownership removes only canonical regular temporaries and synchronizes that cleanup. It
parses recognized finals, orders them numerically, exact-decodes every file, requires filename,
session, and offset agreement, and reconstructs one contiguous prefix from zero. Recognized
malformed names, non-regular entries, gaps, renamed valid bytes, foreign sessions, or corruption
fail closed. A configured chunk-count limit, capped at 1,048,576, bounds accepted recovery work.

`load_received_prefix()` revalidates all durable chunks before assembly. `finalize()` additionally
requires the exact declared snapshot length and its whole-content CRC32C. Returned bytes are still
not RTAS/Manifest/CSEG installation authority; the caller must validate and durably install the
appropriate application snapshot using the existing storage protocol.

## Rationale and alternatives

Offset-addressed immutable files make exact retry and crash recovery simple and preserve every
installed piece for diagnosis. Repeating the session in every chunk prevents directory or filename
metadata from authorizing cross-transfer splicing.

A mutable append file was rejected because torn-tail repair and synchronized progress metadata
would introduce another recovery format. Sparse or out-of-order admission was rejected because it
would require a durable range map and complicate unambiguous resume. Trusting per-chunk CRCs alone
was rejected because reordered individually valid payloads could otherwise escape end-to-end
validation.

## Consequences and validation

Each successfully returned install is durable under the existing POSIX assumptions and reopening
reconstructs the same exact prefix. Small chunks incur one file and synchronization sequence each;
chunk sizing is therefore a transport and benchmark choice within the configured bound. Chunks are
retained after finalization until a later checkpoint-integrated reclamation policy proves they are
unreferenced.

Invariants 1, 2, 3, 8, 10, 11, 14, and 18 apply. Real-filesystem tests cover exclusive ownership,
sequential installation, exact retry/conflict, incomplete and complete assembly, close/reopen
reconstruction, temporary cleanup, wrong session, durable damage, gaps, renamed offsets, chunk-count
limits, and whole-snapshot checksum mismatch. Syscall fault injection, process-kill crash points,
power-loss qualification, permission matrices, checkpoint integration, final RTAS installation,
reclamation, fuzzing, and large-transfer testing remain deferred.

## Migration and rollback

No earlier durable chunk namespace exists. A rollback can ignore these files but cannot safely
claim their transfer progress. Future filename or chunk versions must retain exact v1 recognition
or provide an offline migration that preserves session and offset binding.

## References

- [Tablet Movement Snapshot Chunk v1 format](../formats/tablet-movement-snapshot-chunk-v1.md)
- [ADR 0086](0086-durable-raft-tablet-snapshot-installation.md)
- [POSIX I/O learning guide](../learning/posix-io.md)
