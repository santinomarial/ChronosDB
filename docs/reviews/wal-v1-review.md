# WAL v1 Exit Review

> **Review result: implementation exit gate passed with one correctness fix and documentation/tooling
> hardening.** This review freezes no new bytes and makes no performance claim. The reviewed baseline
> was clean `main` at `fc84d1c78374b56cce9dc79d5b331289f8515143` before review edits.

## Scope and method

The review treated [WAL v1](../formats/wal-v1.md), the accepted
[recovery contract](../architecture/wal-recovery.md), and ADRs 0004, 0006, 0012, and 0013 as
authoritative. It also checked the portability/dependency decisions in ADRs 0002 and 0011. The
complete reachable Git history and exact baseline commit were inspected before editing.

The code audit covered common checked binary primitives; WAL codecs and paths; POSIX file,
directory, rename, synchronization, truncation, locking, and injectable syscall layers; segment
installation and rotation; discovery, verification, repair, replay, reopening, and continued
append; the bounded commit coordinator; subprocess crash protocol and failpoints; corruption and
idempotence matrices; fuzz targets; benchmarks and operator tools; public headers; CMake/install
rules; scripts; and CI workflows. Documentation reviewed included the architecture, product,
format, recovery, correctness, benchmark, learning, build, and crash-harness contracts named in the
exit-review request.

The physical constants, byte offsets, checksum coverage, segment headers, record framing,
application envelope, filenames, limits, and compatibility rules were not changed.

## Confirmed findings and disposition

### WAL-EXIT-001: complete flagged records bypassed full-record integrity classification

**Severity:** correctness/diagnostic ordering; no observed acceptance or silent repair.

The recovery scanner called the public record-header decoder before reading the framed record. That
decoder correctly rejects nonzero required record flags for ordinary callers, but in recovery this
meant a complete record with both an unsupported flag and corrupt payload/trailer was reported as
`NOT_SUPPORTED` without verifying its full-record CRC32C. This contradicted WAL v1's rule that a
complete record establishes structural integrity before an unknown required feature controls
compatibility. Recovery still stopped, so the defect did not skip, replay, or repair the bytes, but
it masked corruption and could direct an operator toward the wrong remedy.

The scanner now uses a private structural header decoder solely to obtain already-bounded framing.
It reads the complete record and lets the normal full decoder validate padding and full-record
CRC32C before applying required-flag policy. An incomplete flagged record remains unsupported, not
a qualifying repairable tail, because integrity of the required-feature record cannot be
established. The public decoder contract is unchanged. A regression constructs a checksum-valid
flagged header, recomputes the complete-record checksum, corrupts payload afterward, and requires a
CRC32C corruption result.

### WAL-EXIT-002: implemented subsystem and installation documentation was stale

The build guide still described acknowledgment coordination as planned and omitted the implemented
crash harness and installed benchmark/operator surface. The project-foundation graph also omitted
current WAL dependencies and executables. These statements are updated so operators do not infer a
weaker implementation or incomplete install layout. The install-layout test now requires
`chronos-walbench` as well as `chronosctl` and `chronos-waldump`.

### WAL-EXIT-003: no reviewable end-to-end WAL measurement artifact existed

The existing Google Benchmark target measures common in-memory primitives, not production WAL
acknowledgment and recovery. `chronos-walbench` and `scripts/benchmark-wal.sh` now provide an
optimized, bounded, overwrite-safe harness with raw samples, complete source/build/workload/host
metadata, exact identity/sequence recovery reconciliation, separate durability modes, and retained
WAL images. CI exercises tiny `ASYNC` and `LOCAL_SYNC` smoke runs and artifact validation. Smoke
output is explicitly not a performance result.

## Adversarial conclusions

- Segment installation preserves the file-sync, no-replace rename, and directory-sync ordering;
  rotation synchronizes the predecessor before installing and using its successor.
- Full-write completion and synchronization frontiers remain distinct. The coordinator preserves
  FIFO admission ownership, bounded request/byte accounting, non-weakened durability, and terminal
  failure fan-out. The concurrency argument remains mutex/condition-variable based rather than
  depending on undocumented relaxed-atomic publication.
- Verification rejects gaps, identity mismatch, middle damage, malformed reserved entries, and
  unsupported semantics. Only a qualifying final incomplete suffix is repairable, and repair,
  replay, reopen, and next-sequence behavior have dedicated idempotence/crash coverage.
- Writer locking spans verification, repair, replay, reopening, and live ownership; the read-only
  operator does not race a live writer.
- Linux and macOS remain the explicitly guarded POSIX platforms. The benchmark uses only those
  already-supported interfaces and does not add a production dependency.
- WAL growth remains unbounded because checkpoint-backed deletion is out of scope. This is a known
  product limitation, not a defect to hide inside this exit task.

No additional byte-format, durability-ordering, acknowledged-write, recovery-mutation, locking, or
queue-accounting defect was confirmed during this review. That is a bounded audit conclusion, not a
proof of absence.

## Verification evidence

The exit review requires clean results from the ordinary unit/integration suite, the focused crash
and corruption suites, benchmark smoke tests, formatting/static analysis, supported sanitizer
configurations, bounded fuzz smoke runs, installation layout, and final diff inspection. Commands
and their actual outcomes are recorded in the review handoff; no throughput or latency value from a
smoke run is retained as a claim.

The deterministic subprocess suite provides process-crash evidence over real host syscalls. It does
not prove behavior under every physical power loss, storage-firmware or controller-cache failure,
filesystem bug, hypervisor failure, or network-filesystem failure. A qualified storage campaign is
therefore not silently promoted into this software exit gate.

## Exit decision and remaining limits

The WAL v1 software subsystem is fit to exit its current implementation phase once the verification
commands in the handoff pass. Future work still needs application-kind idempotency semantics and,
in later accepted phases, checkpoints and reclamation. Those omissions prevent claiming a complete
database but do not justify speculative implementation in this WAL hardening review.
