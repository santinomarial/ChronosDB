# ADR 0057: Bounded Checksummed External Sort

- **Status:** accepted
- **Date:** 2026-08-07
- **Owners:** ChronosDB query-execution and storage-I/O maintainers

## Context

ADR 0044 deliberately bounds an in-memory sort to one output chunk. Phase 9 also requires an exact
spill path for a finite input that is larger than that run bound. Spill cannot turn process memory,
temporary-file count, or disk consumption into an unbounded escape hatch, and it cannot replace SQL
tie identity with run arrival order. Query intermediates are identity-free physical columns, so the
durable schema-identified Columnar Batch format is not an honest encoding for them.

## Decision

- `SpillSortOperator` forms contiguous input runs, sorts each with `SortOperator`, writes each run to
  one exclusively created directory-relative file, and performs a pull-based k-way merge into
  independently owned bounded chunks.
- Every configured limit is finite: total selected rows, rows per run, run count, spill bytes,
  serialized bytes per row, retained configuration, keys, and output chunk bytes. An input chunk
  larger than a run is explicitly unsupported because silently splitting a selected chunk would
  introduce another materialization policy.
- Equal configured keys select the lower run ordinal. Runs are contiguous input intervals and each
  run sort is stable, so this reproduces global logical input order exactly. This remains only the
  physical fallback; SQL lowering must still append its authoritative logical/version or group-key
  identity.
- Run bytes use an ephemeral version 1 envelope with fixed little-endian fields, a shape checksum,
  header CRC32C, and a CRC32C for every row payload. Cells retain canonical fixed-width bytes,
  Boolean values, NULL markers, and length-prefixed variable bytes. Readers validate bounds,
  checksums, canonical cells, declared row counts, and the absence of trailing bytes before use.
- The run format is not durable, installed, or recoverable. Files are not synchronized. A query
  restart recomputes them; no compatibility promise extends beyond one operator lifetime.
- The caller supplies an already opened `PosixDirectory` and a bounded basename prefix. The
  operator creates only exact exclusive names derived from that prefix, owns those files, and
  removes them on normal completion, failure, cancellation, LIMIT-driven destruction, or other
  early destruction. Normal cleanup errors are observable; destructor cleanup is best effort.
- One pre-pull query reservation covers bounded run metadata, merge references, type shape, and two
  reusable maximum-record scratch buffers. Input chunks, in-memory run sort, and each output chunk
  retain their own existing credit. All allocation failures are `RESOURCE_EXHAUSTED` and unwind
  files and credit through RAII.
- The first implementation uses a simple scan of run heads and explicit-offset POSIX reads. It does
  not claim async I/O, mapping, replacement selection, a tournament tree, or parallel merge without
  later profiles and equivalent correctness evidence.

This decision changes no durable or network format and adds no dependency.

## Consequences

ChronosDB can now sort a larger finite input without retaining every row in memory. The baseline is
intentionally I/O-heavy: comparisons reread bounded records and output uses a measure-then-copy
pass so exact chunk credit is reserved before allocation. It provides a correctness oracle and
measurement baseline for later optimizer selection.

Spill availability is not automatic SQL plan selection. A future validated optimizer chooses
between in-memory and external sort only when it has an explicit spill directory and policy. File
names may collide with an existing query prefix and fail rather than replacing another owner.

## Affected invariants

This decision supports invariants [9, 10, 11, 14, and 18](../architecture/invariants.md): memory,
disk, file count, and record lengths are bounded; malformed bytes are rejected; ownership cleanup
is explicit; encoding is versioned and checksummed; and the spill path preserves exact ordering.

## Validation plan

- Deterministic and random-model tests force many runs and output boundaries across multiple keys,
  direction, NULL placement, variable bytes, and stable ties.
- Hostile tests cover oversized chunks, rows, records, run counts, disk quota, shape changes,
  foreign credit, collisions, corruption, cancellation, early destruction, and exact final runs.
- Allocation-failure injection covers construction, run formation, encoding, merge planning, and
  output ownership with zero leaked credit or files.
- A spill fuzzer varies values, directions, run and output widths, and hostile limits against a
  stable independent model. ASan/UBSan and applicable TSan runs cover ownership and parsing.
- Microbenchmarks report rows, run width, runs, allocation bytes, and actual spill bytes read and
  written; source construction is excluded while POSIX run I/O is included.

## Unresolved questions

Optimizer selection, top-N runs, replacement selection, mapped/asynchronous reads, tournament-tree
merge, parallel run formation, operator-level disk admission shared across queries, and crash-time
orphan scavenging remain future work. The current caller-owned directory is expected to be a
query-lifetime temporary namespace.

## References

- [ADR 0021](0021-query-resource-accounting-and-cooperative-cancellation.md)
- [ADR 0022](0022-pull-based-physical-operator-lifecycle.md)
- [ADR 0044](0044-query-accounted-bounded-physical-sort.md)
- [Architecture invariants](../architecture/invariants.md)
- [Phase 9 roadmap](../roadmap.md#phase-9--vectorized-execution-and-parallel-scheduling)
