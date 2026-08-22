# Raft Persistent-Log Crash Matrix

> **Status: implemented deterministic subprocess harness.** The matrix exercises initial log
> installation, segment rotation, checkpoint publication, recovery-anchor publication, and physical
> prefix reclamation through the production POSIX path. It provides `SIGKILL` and process-restart
> evidence; it is not physical power-loss or storage-stack qualification.

## Purpose and process model

The matrix complements injected syscall failures with real crash images. A parent GoogleTest process
creates a dedicated directory and starts a small test-only child with `posix_spawn`. The child runs
`RaftPersistentLog` through a forwarding POSIX observer. The observer first lets the selected real
operation succeed, prints `FAILPOINT <name>`, and blocks. The parent reads that event, sends
`SIGKILL`, reaps the child, and invokes only the public creation and recovery APIs over the resulting
filesystem image.

The forwarding observer and failpoint names are linked only into the test child. The production
library has no crash callback, environment-variable hook, or failpoint branch.

## Crash cuts

The 31-point matrix covers the authority-changing and data-bearing persistence boundaries in three
scenarios:

| Scenario | Cuts | Successful operations observed before termination |
| --- | ---: | --- |
| Initial installation | 6 | `LOCK` creation, lock-directory sync, initial header write, initial file sync, initial rename, final directory sync |
| Rotation | 8 | predecessor data sync and close; successor header write, file sync, rename, and directory sync; rotated record write and data sync |
| Checkpoint and reclamation | 17 | rotation's six pre-record operations; checkpoint record write and data sync; anchor write, file sync, rename, directory sync, and close; obsolete-segment unlink and directory sync; obsolete-anchor unlink and directory sync |

Every cut names a successful kernel-visible operation rather than an intended source-code step. An
operation that returns failure cannot publish its failpoint. Exclusive temporary-file creation is
not a distinct cut: termination there produces the same recognized-temporary authority class as the
subsequent pre-rename write cut, and recovery never interprets temporary contents.

## Recovery oracle

The parent requires one exact authority for each image:

- Before initial-segment rename, `open_existing` reports the empty segment set and `create_new`
  restarts initialization. While holding `LOCK`, it may remove only the exact regular
  `raft-00000000000000000001.tmp`; unrelated, duplicate, unknown, or nonregular entries still fail
  closed. After rename, `open_existing` adopts the checksummed empty segment 1.
- Before successor rename, rotation recovers the synchronized predecessor only. After rename it also
  adopts the valid empty successor. Once the complete record write is visible, it recovers that
  record and its physical sequence.
- Before anchor rename, the older anchor remains authoritative and any complete later checkpoint
  record is ordinary contiguous history. From anchor rename onward, the new base and its complete
  checkpoint are authoritative; partial removal of older segments or anchors is cleanup residue.

For every case, recovery must report exact base segment, segment count, record count, durable and
written physical sequence, and latest full state per group. The first successful recovery must
remove recognized temporary residue. A second open must return the identical state, and appending
and synchronizing the next physical sequence must succeed. This repeated-open and continuation check
guards both recovery idempotence and stale in-memory authority.

The focused reproduction command is:

```bash
build/debug/chronos_raft_tests --gtest_filter='EveryPersistentTransition/PersistentLogCrashMatrixTest.*'
```

The matrix is also discovered by the normal CTest and sanitizer configurations. Its child process
inherits the matching instrumentation.

## Companion retained-byte corruption campaign

The same persistent-log test binary builds a canonical two-group checkpoint spanning two 277-byte
retained segments and one 64-byte authoritative anchor. A bounded in-process campaign creates 1,239
distinct damaged images:

- one low-bit flip at each of the 618 authority-byte positions;
- every strict-prefix truncation of those three files, for another 618 images; and
- removal of each authority file in turn.

Every image is opened once in strict mode and once with incomplete-final-tail repair authorized.
Both opens must return `CORRUPTION`, release `LOCK`, and leave all remaining names and bytes exactly
unchanged. Restoring the original artifact must reconstruct base segment 3, both records and group
states, and physical sequence 4. The campaign is exhaustive for one low-bit mutation and every
truncation of this canonical image; it does not model coordinated checksum-preserving modification.

Its focused reproduction command is:

```bash
build/debug/chronos_raft_tests --gtest_filter='RaftPersistentLogCorruptionCampaignTest.*'
```

## Evidence boundary

`SIGKILL` terminates one process but leaves the host kernel, page cache, filesystem, and storage
device running. Therefore a complete write or visible rename in this matrix does not independently
establish stable-media durability. In particular, visibility before the corresponding file or
directory synchronization is a permitted process-restart outcome, not an acknowledgment guarantee.

Power-cut and virtual-machine-reset campaigns, filesystem/device qualification, lying-cache
testing, and retained crash images from those environments remain separate evidence tiers. They
must reuse the same exact-authority, idempotent-reopen, and sequence-continuation oracle.
