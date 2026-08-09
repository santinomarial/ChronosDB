# ADR 0091: Durable materialized-view checkpoint storage

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB live-query and durability maintainers
- **Extends:** [ADR 0090](0090-materialized-view-checkpoint-v1.md)
- **Extended by:** [ADR 0092](0092-materialized-view-checkpoint-generations.md) and
  [ADR 0093](0093-durable-windowed-materialized-view-owner.md)

## Context

Versioned bound checkpoint bytes are not durable merely because they encode successfully. One owner
must exclude concurrent writers, recover interrupted temporaries, prevent sequence reuse with
different bytes, validate exact identity on every load, and prove directory-entry durability before
a source suffix can eventually be released.

## Accepted decision

`MaterializedViewCheckpointStorage` owns one existing directory and one exact database/view/table/
schema/version/plan identity under a nonblocking exclusive advisory `LOCK`. Legacy minor-0 files are
named `checkpoint-<20-digit-WAL-sequence>.mvcp`; ADR 0092 adds generated production files. Either
name plus `.tmp` is a recognized installation temporary. Sequence zero remains valid for an initial
legacy logical boundary.

Installation validates and encodes the bound checkpoint, exact-loads any existing final file for an
idempotent same-byte retry, removes and directory-synchronizes a recognized prior temporary, creates
without replacement, writes, exact-reads, decodes, and compares the complete checkpoint, then
synchronizes and closes the file. It atomically renames without replacement and synchronizes the
directory before returning success. Directory-sync failure after rename poisons the owner because
name durability is uncertain.

Open removes only canonical regular temporaries and synchronizes that cleanup. Exact and latest
loads revalidate file size, both envelope and nested checksums, logical state, complete configured
identity, and filename/source-sequence agreement. Latest selection parses recognized final names and
never treats directory order or an unvalidated file as authority.

## Consequences and alternatives

Different bytes at one sequence are corruption. Checkpoints remain append-only until a separate
retention/pin owner proves an older file reclaimable. A copied valid file from another view fails
identity matching even when its nested source bytes decode.

Replacing a final path was rejected because it makes crash outcome ambiguous. Trusting only the
write result was rejected because durable recovery will depend on bytes that must survive readback.
Using a latest-name pointer was unnecessary: canonical numeric names plus validation select the
highest durable boundary without a second mutable truth.

This storage owner does not itself decide when to checkpoint, release source retention, or apply a
post-checkpoint suffix. ADR 0093 integrates checkpoint scheduling and the durable source frontier in
the materialized-view application owner; service-level suffix derivation and retention coordination
remain external.

## Affected invariants and validation

Invariants 1, 4, 8, 10–15, and 17 apply. Real-filesystem tests cover exclusive ownership, install,
same-byte retry, conflicting-byte rejection, higher-sequence selection, close/reopen, interrupted-
temporary cleanup, and installed corruption. Syscall faults, directory-sync uncertainty, process
crashes, permission matrices, retention release, and old-file reclamation remain in the Phase 18
ledger.
