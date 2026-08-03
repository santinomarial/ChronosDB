# WAL Crash and Recovery Harness

> **Status: implemented deterministic subprocess harness.** The harness exercises the production
> WAL codec, POSIX operations, segmented writer, commit coordinator, recovery, repair, reopening,
> and writer lock. It provides process-termination and crash-image evidence; it is not a physical
> power-loss or storage-stack qualification suite.

## Purpose

The harness tests the boundaries that cannot be established by an in-process mock alone. A parent
test process starts a dedicated WAL child, receives bounded protocol events over a pipe, terminates
the child with `SIGKILL` at a selected boundary, and runs the public recovery APIs over the files the
child left behind.

The primary oracle is:

> Every `LOCAL_SYNC` request whose successful completion event reached the parent before abrupt
> termination appears exactly once, in global record-sequence order, after recovery.

The oracle deliberately distinguishes five events:

1. the parent sent a request;
2. the coordinator admitted it;
3. the writer completed its physical record write;
4. a synchronization operation covered it; and
5. the coordinator published a completion that the parent actually read.

A record may be written or synchronized without reaching step 5. The WAL does not contain a bit
stating whether the response pipe delivered an event.

## Process and ownership model

```text
parent GoogleTest process
  ├── creates and parent-syncs a dedicated wal/ directory
  ├── posix_spawn + command/event pipes
  │
  ├── child process
  │     ├── real WalWriter creation or reopening
  │     ├── real WalCommitCoordinator and sole worker thread
  │     ├── real POSIX backend through a test-only forwarding observer
  │     └── bounded completion-event publication
  │
  ├── optional SIGKILL after a received event/failpoint
  └── public scan_wal / recover_wal / WalWriter::open_existing
        └── sequence, identity, multiplicity, classification, and byte-image oracle
```

Only the child owns the writer while it is live. The parent verifies that its scan and another child
writer receive lock contention, then verifies that both graceful shutdown and process death release
the advisory lock.

## Child protocol

The protocol is line-oriented ASCII. Lines and commands are limited to 4096 bytes. Decimal fields
are parsed with checked `uint64` arithmetic, unknown commands are rejected, and the parent applies a
finite timeout to every expected event. Protocol writes are serialized so completion threads cannot
interleave a line.

Parent commands are:

```text
SUBMIT <request-id> ASYNC
SUBMIT <request-id> LOCAL_SYNC
SHUTDOWN
```

Important child events are:

```text
READY
ADMITTED <request-id>
COMPLETED <request-id> <mode> <record-sequence> <admission-sequence>
FAILED <request-id> <status-code>
FAILPOINT <name> <occurrence>
METRICS <sync-attempts> <local-batches> <local-requests> <async-acks> <local-acks>
SHUTDOWN OK
```

The parent counts a request as acknowledged only after it has parsed the corresponding `COMPLETED`
line. A child-side successful wait or attempted pipe write that the parent did not receive is not
part of the acknowledged set.

Each test request uses a synthetic 24-byte application payload: the ordinary 16-byte WAL
application envelope followed by a little-endian 64-bit test request identity. This is test data,
not an allocation of production application kind `1` or a durable application-format contract.
Recovery sinks validate and copy the identity only for the duration of the callback's borrowed
payload view.

## Real-syscall failpoints

Creation-mode and test reopen-mode children use the existing internal `PosixSyscalls` seam with a
forwarding observer. The observer calls the production system backend first. Only after that real
operation returns success can it publish and block at the selected failpoint. Killing the blocked
process therefore leaves bytes and namespace state produced by actual `pwrite`, synchronization,
and rename operations on the host filesystem.

The matrix uses these boundaries:

| Failpoint | Successful operation completed before the child blocks |
| --- | --- |
| `after_segment_header_write` | Complete 64-byte temporary segment header write |
| `after_segment_file_sync` | Full synchronization of the temporary segment or startup active file |
| `after_segment_rename` | Atomic same-directory no-replace rename |
| `after_segment_directory_sync` | WAL-directory synchronization after install or startup |
| `after_record_write` | Complete physical record write |
| `after_short_record_write` | Configured real prefix write of a record; the production full-write loop has not resumed |
| `after_data_sync` | Successful active-file data synchronization before the call returns to the coordinator |

Occurrence numbers disambiguate startup synchronization from later successor installation. The
short-write failpoint forwards only the configured prefix to the real backend and then blocks inside
the syscall adapter. It creates a genuine short final file suffix while still exercising the
production explicit-offset write path and recovery implementation.

No failpoint is compiled into `chronos_wal`, exported in a public header, or enabled in a production
binary. The observer, child protocol, and fixed identity generator are test-only code.

## Recovery oracle

For every clean or repaired image, the collecting sink requires:

- physical record sequences equal exactly `1, 2, ..., N`;
- every recovered synthetic request identity is unique;
- replay order equals physical global sequence order;
- every parent-observed `LOCAL_SYNC` identity is present once;
- an `ASYNC`-acknowledged identity may be present or absent;
- an admitted but unacknowledged identity may be present when its complete bytes reached the file;
  and
- a synchronization failpoint may leave a durable record whose completion was never published.

The current deterministic matrix covers:

- initial segment header write, file sync, rename, and directory sync;
- prior-segment sync and every successor installation boundary during rotation;
- complete and short record writes;
- ASYNC and LOCAL_SYNC completion;
- mixed durability and grouped LOCAL_SYNC requests;
- rotation-provided durability for prior-segment waiters;
- graceful drain and abrupt process termination;
- strict scan and recovery after middle/header/payload/trailer corruption and a segment gap;
- explicit incomplete-final-tail repair and repeated convergent recovery;
- exact sequence continuation after ordinary recovery and after repair; and
- cross-process lock exclusion and release.

For corruption, the harness snapshots the damaged named files, attempts both read-only scan and
repair-authorized recovery, requires `CORRUPTION`, and verifies that recovery changed no bytes.
Repair tests first prove that unauthorized recovery leaves the image unchanged, then compare the
successfully repaired image with a repeated recovery.

## Determinism, bounds, and diagnostics

The matrix uses fixed request identities, fixed segment targets, named failpoints, and explicit
failpoint occurrence numbers. It does not depend on random scheduler yields. Long group windows are
closed deterministically by count or graceful shutdown, while private timeouts detect a stuck or
misbehaving child. Unexpected protocol events are retained until the matching event is requested.

Admission remains bounded by the production coordinator configuration. The child retains at most
128 accepted requests and an 8 MiB encoded-byte budget. Parent protocol buffers are bounded per line,
each scenario owns a temporary directory, and a process guard sends `SIGKILL` and reaps a child if a
test exits early. A failure reports the GoogleTest case and raw child event that failed to parse or
arrive.

The focused local reproduction command is:

```bash
build/debug/chronos_wal_tests --gtest_filter='*WalCrash*:*WalProcessLock*:*WalRecoveryIdempotence*:*WalCorruptionMatrix*'
```

The suites are also discovered by CTest and run in the ordinary compiler and sanitizer matrices.

## What this evidence does not prove

`SIGKILL` terminates one process. It does not clear the kernel page cache, reset a machine, interrupt
storage firmware, or remove power. Consequently:

- a surviving ASYNC record in these tests does not make ASYNC durable;
- visibility of a rename after process death does not prove it would survive power loss before a
  completed directory sync;
- macOS execution remains correctness-development evidence, not qualification of `fsync` versus
  `F_FULLFSYNC` for power loss;
- Linux execution is meaningful only under the local-filesystem/device assumptions in the
  [recovery architecture](../architecture/wal-recovery.md); and
- the harness does not cover lying drive caches, controller loss, filesystem/kernel bugs,
  hypervisor failures, device loss, network filesystems, malicious modification, or correlated
  infrastructure failure.

Power-cut testing, virtual-machine reset testing, filesystem/device qualification, and retained
crash images from those environments remain separate evidence tiers. They must use the same
acknowledged-set and recovered-prefix oracle rather than weakening it.
