# WAL Segment Lifecycle and Recovery

> **Status: accepted design, physical recovery and acknowledgment coordination implemented.** The pure in-memory physical codec from
> [WAL v1](../formats/wal-v1.md), reusable blocking POSIX primitives, exclusive writer,
> crash-safe segment installation, append, explicit synchronization, rotation, frontier tracking,
> terminal write/sync failure behavior, locked discovery, verification, explicit repair,
> sink-directed semantic preflight/replay, startup barriers, reopening, bounded multi-producer
> admission, single-worker append ordering, `ASYNC`/`LOCAL_SYNC` completion, group commit, and
> coordinator metrics are implemented. Application-kind codecs and server-level acknowledgment
> transport are not. This document defines those boundaries without repeating the format tables.

## Safety goals and scope

The single-node WAL must provide one ordered physical history, must never replay past unverified
bytes, and must not acknowledge a `LOCAL_SYNC` entry before its bytes can survive the failures in
the platform contract below. Recovery must distinguish the one permitted incomplete-tail case from
corruption and must validate the whole log before applying any entry.

This design covers process termination and ordinary operating-system crashes on the supported local
filesystem/storage contract. It does not claim survival of device loss, lying volatile drive caches,
filesystem or kernel defects, operator deletion, malicious modification, or power loss outside the
documented storage stack. Replication and `QUORUM_SYNC` are unavailable.

## Ownership and lock

One logical writer owns a WAL directory. The process opens or creates `wal/LOCK` without following a
symlink and acquires a nonblocking whole-file exclusive advisory lock for the complete lifetime of
creation, recovery, repair, append, rotation, and shutdown. On the Linux reference backend this is a
POSIX `fcntl` write lock. Failure to acquire it is `WRITER_LOCKED`; the process must not inspect and
then mutate the directory as though it were the writer.

Because traditional POSIX record locks are process-associated and may be released when that process
closes another descriptor for the same inode, the WAL lock manager exclusively owns all opens of
`LOCK` and keeps its descriptor open until WAL shutdown. Code in the process MUST NOT independently
open and close `LOCK`. The lock is checked for successful acquisition, not inferred from diagnostic
bytes or a process identifier stored in the file.

The advisory lock prevents cooperating ChronosDB processes from writing simultaneously; directory
permissions and deployment isolation remain necessary because advisory locking cannot stop an
uncooperative program. Threads inside the owning process may prepare work concurrently, but one
serialized WAL writer orders file writes, segment installation, synchronization, sequence
assignment, and acknowledgment eligibility.

A read-only diagnostic tool must either acquire the same exclusive lock or explicitly label its
result a racy observation. Only a locked scan may claim authoritative recovery classification.

## WAL directory prerequisite

The `wal/` directory is itself durable state. Database creation must create it relative to an open
database-root directory, require a successful `fsync` of that parent directory, and only then install
segment 1. No record may be appended or acknowledged before the parent-directory boundary. Opening
an existing database assumes this creation boundary previously completed; a missing `wal/` or
missing segment history is a surfaced failure, never an instruction to manufacture a new history.

This prerequisite is distinct from synchronizing `wal/` after a segment rename: the parent sync
protects the `wal/` name, while the WAL-directory sync protects segment names inside it.

## Segment states

The state of a segment is inferred from its name and the set of higher final names:

```text
absent
  │ create temporary file and complete header
  ▼
temporary
  │ synchronize file → same-directory atomic rename → synchronize directory
  ▼
installed active
  │ append complete records; data-sync as required
  │ synchronize prior segment before installing successor
  ▼
closed immutable
```

There is exactly one installed active segment: the highest-numbered final segment. Every lower
segment is closed and immutable. A closed segment has no footer and is not renamed; the existence of
its valid successor closes it. The active segment may change only by append or explicit
recovery-tail repair.

Recognized temporary files are never log members. Recovery removes rather than promotes them,
because their crash point cannot prove that the intended final-name directory entry crossed its
durability boundary.

## Creating and installing a segment

Creation uses a directory file descriptor and relative operations that reject symlinks. For segment
`N` with expected first record sequence `R`:

1. If `N > 1`, stop appending to segment `N - 1` and successfully data-synchronize it. A failure
   leaves the writer failed and prevents installation of `N`.
2. Create the exact temporary name from the format specification with exclusive creation. It must
   be a new regular file on the same filesystem and in the same `wal/` directory as its final name.
   A name collision is never opened, truncated, or reused; the creator chooses another nonce.
3. Encode the complete 64-byte segment header in memory, write all 64 bytes with a checked full-write
   loop, and verify the file size is exactly 64. No record is written yet.
4. Call `fsync` on the temporary file and require success.
5. Atomically rename the temporary name to the final segment name in the same directory. Replacement
   of an existing destination is forbidden.
6. Call `fsync` on the open `wal/` directory and require success.
7. Only after step 6 may the segment become active and accept records.

The boundary after step 6 is the **segment installation durability boundary**. No record in that
segment may be acknowledged before it. This design deliberately orders installation before record
append rather than merely delaying acknowledgments for records written into an uninstalled name.

The rename primitive must combine same-directory atomic rename with no-replace semantics in one
operation. On the Linux reference backend this is `renameat2(..., RENAME_NOREPLACE)` (or an
equivalent kernel operation with the same guarantee). A check-then-plain-rename sequence is not an
equivalent fallback because it can overwrite a destination after a race. A backend without an
atomic no-replace primitive must report the platform unsupported for writable WAL operation.

Initial WAL creation applies the same steps to segment 1. Opening an existing database never invokes
initial creation as a fallback for missing history.

The current new-history API requires the already installed `wal/` directory to be empty except for
an optional regular `LOCK`. It performs a read-only content classification before consuming an
identity and repeats that classification after acquiring the lock. It rejects rather than deletes
recognized orphan temporaries; only the existing-history recovery opener may remove them after the
named history verifies and then synchronize that cleanup. Malformed reserved names, unrelated
entries, symlinks, and nonregular entries also fail closed. The creation API never creates `wal/`,
because it does not own the database-root descriptor or the required parent-directory
synchronization boundary.

### Rotation

The writer computes the complete next record length before writing. If it would exceed the current
writer's validated runtime target, the writer installs the next consecutive segment using the
procedure above and places the entire record there. The runtime target may be below the v1 64 MiB
format limit, is not serialized, and must fit one maximum configured record. The new header's first
sequence is the sequence assigned to that record. Because the prior segment is data-synchronized
first, installing a successor cannot make an older `ASYNC` prefix less durable; it may make that
prefix durable incidentally.

An installation error never permits append to the temporary or ambiguously installed segment.
Recovery later observes either the previous complete directory state or the new valid final name
under the platform assumptions. Since no record was accepted into the successor before directory
sync, disappearance of an unsynchronized rename cannot lose an acknowledged successor record.

If the new final name survives a crash that occurred before the original directory sync returned,
its valid header may be discovered on restart even though persistence of that name is not yet proved
for another crash. The writer-startup namespace barrier below re-establishes that proof before any
new record becomes acknowledgment-eligible.

## Record append and acknowledgment

The writer assigns the next WAL-wide record sequence, encodes the complete physical record in
memory, and writes it at the known active end offset. `EINTR` and short writes are retried for only
the unwritten suffix. Arithmetic and offsets are checked against the physical record and segment
limits before the first write.

If a hard error occurs after a prefix is written, the writer enters a **poisoned** state. It returns
an I/O error, acknowledges no request covered by the incomplete record, performs no later append,
and requires locked recovery before reopening. Continuing after the prefix would turn a final-tail
failure into middle-of-log corruption.

### `ASYNC`

An `ASYNC` request becomes acknowledgment-eligible only after every byte of its complete record has
been accepted successfully by the operating system through the WAL file write path. It does not
wait for data synchronization. The logical operation may become current-process committed and
visible at that boundary, but process/OS crash and storage failure may lose it. `ASYNC` is not a
durable mode.

### `LOCAL_SYNC`

A `LOCAL_SYNC` request becomes acknowledgment-eligible only when:

1. its segment installation durability boundary has completed;
2. every byte of its record has completed the write path; and
3. a successful WAL data-synchronization operation covers the record's ending file offset.

On Linux, the data-synchronization operation is `fdatasync` on the active regular-file descriptor;
`fsync` is an allowed stronger operation. A backend without an equivalent `fdatasync` uses a
documented operation at least as strong. Success is required—submission or completion of an
unrelated write is not enough.

Group commit may capture a stable **sync frontier** containing the active segment number, covered end
offset, and final covered record sequence, call one data synchronization, and release every waiting
`LOCAL_SYNC` request in that segment whose complete record end is at or before the frontier. Offsets
from different segment files are never compared. Rotation's mandatory synchronization closes the
prior segment and can release its covered waiters before the successor becomes active. Appends that
race after frontier capture wait for a later sync. An `ASYNC` request does not wait but may be
incidentally covered. A sync error fails all requests waiting on that attempt, poisons the writer,
and is never converted into `ASYNC` success.

The response names requested and effective mode. `QUORUM_SYNC` is rejected as unavailable until the
replication specification is implemented; no downgrade is permitted.

### Current commit coordinator

`WalCommitCoordinator` transfers an open `WalWriter` to one worker thread. Producer threads copy
application payloads into a mutex-protected FIFO, and the mutex acquisition that admits a request
assigns its admission sequence and linearizes physical append order. The worker alone calls append,
synchronize, observe-frontier, and close operations on the writer. This is a focused MPSC boundary
for WAL persistence, not a replacement for the reactor-to-shard SPSC topology in ADR 0004.

Admission is nonblocking and bounded by both unfinished request count and exact encoded WAL bytes.
The charge remains until completion, including while a request is in the worker or waiting for
`LOCAL_SYNC`; popping the FIFO therefore cannot evade the bound. Full admission returns
`RESOURCE_EXHAUSTED`. An accepted payload is owned by the coordinator until append finishes, after
which its storage is released even when the completion still waits for synchronization.

A sync window begins after the first `LOCAL_SYNC` record completes its write. The worker admits
subsequent FIFO records into that window until the configured physical request count, encoded-byte
limit, or delay expires. Intervening `ASYNC` records count toward the physical batch limits but
complete immediately after their own writes. Waiting `LOCAL_SYNC` records complete only after a
successful covering frontier. Rotation's mandatory prior-file synchronization releases covered
prior-segment waiters without an unnecessary second sync; cross-file offsets are never compared.

Shutdown closes admission, drains the FIFO, synchronizes a partial final group, closes the writer,
and joins the worker. A terminal writer failure preserves already completed `ASYNC` results and any
`LOCAL_SYNC` result whose sequence is already covered, then fails all other accepted requests with
the retained root status. The coordinator performs no retry or downgrade.

### Acknowledgment ambiguity

If the process crashes or the connection fails after the durable boundary but before the client
receives the response, the record may replay even though the client observed no acknowledgment.
This is expected and is why application records must durably couple client-batch identity, digest,
outcome, and mutation under their future kind-specific specification.

## Platform persistence contract

The Linux production contract requires all of the following:

- a local filesystem that implements successful regular-file `write`/`pwrite`, `fdatasync`/`fsync`,
  same-directory atomic `rename`, and directory `fsync` with their documented crash-persistence
  semantics;
- durable installation of the `wal/` directory itself through synchronization of its database-root
  parent before any record acknowledgment;
- one filesystem for temporary and final names;
- a storage stack that truthfully honors completed cache flushes needed by those calls;
- no concurrent or out-of-band modification of the locked WAL directory; and
- errors from write, synchronization, rename, directory synchronization, close where material, and
  truncation are surfaced rather than ignored.

Under that contract, successful file data synchronization preserves the covered record bytes and
required file size across process termination and ordinary OS crash. `fsync` of the directory after
rename preserves the final segment name. Hardware power-loss behavior is covered only when the
deployed filesystem/device stack documents and truthfully honors the same completed persistence
operations; WAL v1 does not claim protection from devices or firmware that lie.

macOS is a correctness-development platform, not the production durability reference. A future
macOS persistence backend must document whether `fsync`, `F_FULLFSYNC`, or another sequence is
required before advertising a power-loss envelope. Passing portable codec tests alone is not a
durability claim.

## Recovery phases

Normal service remains unavailable through all recovery phases:

```text
acquire lock
  → discover and classify directory entries
  → verify every segment header and physical record
  → classify clean end or incomplete final tail
  → optionally perform explicit synchronized tail repair
  → re-verify the complete physical WAL
  → preflight every record/application version and kind
  → replay from sequence 1 into fresh or resettable logical state
  → synchronize the WAL directory as the writer-startup namespace barrier
  → publish recovered state and enable writes
```

No application entry is replayed during physical verification. A scanner may retain bounded record
descriptors or make a second immutable pass under the lock; a second pass must recheck integrity
rather than trusting mutable buffers.

### Directory discovery

Discovery follows the exact entry and filename policy in WAL v1. Final segments are sorted by their
numeric filename field. Recovery rejects an empty existing history, gaps, duplicates, malformed or
unrecognized entries, non-regular files, symlinks, and a final filename/header mismatch.

Recognized temporary files are recorded but not opened as history. Recovery SHOULD delay their
deletion until the named WAL has verified, so a corruption report does not perform unrelated
cleanup first. Temp cleanup is idempotent; `ENOENT` after a prior successful removal is harmless,
and directory synchronization follows any cleanup batch before writes are enabled.

The implementation follows that policy: read-only scans retain and report recognized temporaries;
writer recovery removes them only after physical verification (and any authorized repair), then
synchronizes the directory. This prevents an orphan from colliding with a later segment install.

### Physical verification

Recovery verifies:

- segment 1 through the highest segment are consecutive and share one nonzero WAL identity and one
  supported physical format;
- each segment header and file length satisfy WAL v1;
- first-record sequence declarations and every physical record sequence form exactly `1, 2, ...`;
- every complete record satisfies its header, checked-length, padding, and full-record CRC rules;
- all non-final segments end cleanly on a record boundary and are nonempty; and
- only the highest segment qualifies for the incomplete-tail classification.

The scanner stops at the first fault for the primary classification but MAY continue in a separate
diagnostic mode that never repairs or replays. Diagnostic continuation must not resynchronize by
searching for record magic and must not relabel later bytes valid history.

## Recovery classifications

| Classification | Examples | Normal action |
| --- | --- | --- |
| `CLEAN` | All headers and records verify; active file ends at a record boundary, possibly immediately after its header. | Continue to semantic preflight. |
| `INCOMPLETE_FINAL_TAIL` | At the verified end of the highest segment, 1–39 bytes remain, or a valid complete record header declares bytes beyond EOF. | Report valid end; repair only when explicitly authorized. |
| `CORRUPTION` | Bad CRC/magic/length/padding/sequence, truncated non-final segment, gap, mixed identity, unexpected entry, invalid assigned application envelope/body, or complete final record with bad CRC. | Fail closed; never truncate or skip. |
| `UNSUPPORTED` | Checksum-valid unknown segment/record format or flags, unknown physical type, or unsupported application format/kind/flags. | Fail before replay; retain bytes for a compatible implementation/tool. |
| `IO_ERROR` | Read, metadata, synchronization, rename, cleanup, or repair operation fails. | Fail closed and preserve the original diagnostic/error. |
| `WRITER_LOCKED` | Exclusive advisory lock is held elsewhere. | Do not recover or write. |
| `MISSING_HISTORY` | Existing database expects a WAL but no final segment exists. | Fail; do not create a replacement history. |

The exact incomplete-tail conditions are normative in [WAL v1](../formats/wal-v1.md#clean-end-incomplete-final-tail-and-corruption).
They are deliberately narrow. Middle-of-log corruption is never skipped, and a full checksum
mismatch is never interpreted as an ordinary torn tail.

## Explicit tail repair

Verification is read-only by default. Writer startup may request `repair_incomplete_final_tail`; an
operator-facing verifier must require an equally explicit repair mode. Repair is permitted only for
the `INCOMPLETE_FINAL_TAIL` classification and only on the highest final segment.

With the exclusive lock still held:

1. record the segment identity, observed file size, verified end offset, and classification;
2. reopen/confirm the same regular file without following symlinks and recheck identity/size so the
   target cannot have changed;
3. truncate the file to the verified end with `ftruncate` (never grow it);
4. call `fsync` on the segment and require success;
5. call `fsync` on the WAL directory and require success;
6. re-run complete physical verification from segment 1;
7. only after a clean result continue to semantic preflight and replay.

Any repair error leaves normal service disabled. Repeating the procedure is idempotent: after a
successful truncation the next scan is `CLEAN`, and truncating to the same verified end does not
remove another valid record. Recovery never rewrites checksums, patches headers, promotes temps, or
truncates at a guessed magic byte.

## Writer-startup namespace barrier

After successful verification, semantic preflight, replay, and any temporary cleanup/repair, writer
startup calls `fsync` on the WAL directory and requires success before publishing recovered state,
appending, or acknowledging a new record. This barrier is required even when recovery made no
directory change. It makes a valid final segment name that survived an earlier crash durable for the
next covered crash, including the ambiguous rename-before-directory-sync case. Failure leaves normal
service and writing disabled.

A read-only verifier does not perform this barrier and cannot enable a writer. Initial creation and
ordinary rotation already cross their own installation directory sync; the startup barrier is a
conservative re-establishment, not a substitute for those transitions.

The reference recovery implementation additionally calls full-file synchronization on the final
active segment before the directory barrier. This conservative strengthening makes surviving
process-crash page-cache bytes and a prior interrupted repair durable before it reports the
recovered written/durable frontier; it does not change the WAL v1 format or acknowledgment modes.

## Semantic preflight and replay

After physical verification and any repair, recovery examines every physical record type and every
application envelope without applying it. Unknown required semantics fail `UNSUPPORTED` before any
logical state changes. A known application kind whose body violates its accepted codec is
`CORRUPTION` with an `INVALID_APPLICATION_RECORD` diagnostic; CRC validity does not make an invalid
operation meaningful.

Only a fully preflighted WAL is replayed in increasing record sequence. Per-tablet order is the
subsequence of that global order selected by the kind-specific application body. Replay targets
fresh or explicitly resettable state and cannot publish query-visible state until the entire replay
completes. Repeating recovery over unchanged bytes must produce the same logical state and must not
duplicate external side effects.

Every complete valid record may replay, including a record whose client never received an
acknowledgment and an `ASYNC` record that happened to survive. Conversely, an acknowledged `ASYNC`
record may be absent or removed as an incomplete final tail after a covered crash. Higher-layer
idempotency makes client retry deterministic.

## Why `LOCAL_SYNC` cannot be lost inside the covered envelope

For a `LOCAL_SYNC` acknowledgment:

1. the segment header was file-synchronized, and its final name crossed either the original install
   directory sync or the recovery writer-startup namespace barrier before append;
2. the complete record crossed the WAL write path;
3. successful data synchronization covered its complete end offset before acknowledgment;
4. rotation synchronizes the prior segment before a successor becomes active;
5. recovery accepts only a sequence of fully checksummed records and can truncate only bytes after
   the last verified record in the final segment; and
6. corruption before or within the synchronized record fails recovery rather than silently erasing it.

Therefore, under the platform persistence contract, a covered process termination or ordinary OS
crash cannot turn that acknowledged record into a missing or repairable partial record. Later
unsynchronized records may form an incomplete final tail, but repair stops after the last valid
record and retains the synchronized prefix. A counterexample under covered assumptions is a
severity-one durability defect, not an allowed recovery outcome.

## Observability

Implementation must expose at least:

- WAL identity, active segment number, active byte offset, and next record sequence;
- segment create/install/rotate counts and durations;
- bytes written and acknowledged by effective durability mode;
- current sync frontier, unsynchronized bytes/age, sync counts/durations/failures, and group size;
- poisoned-writer state and the first I/O failure;
- recovery segment/record/byte counts, valid end, classification, and exact failing file/offset;
- tail-repair old/new sizes and synchronization result; and
- unsupported physical/application versions, types, kinds, and flags.

Logs and metrics must not print application payloads by default because future payloads may contain
user data. Diagnostics report identities, lengths, checksums, and bounded context.

## Deferred work

Deployment tuning and the server default for the implemented group-commit size/delay policy,
logical application kinds, checkpoint
format and old-segment removal, CSEG/manifest integration, encryption, compression, direct I/O,
memory-mapped writing, `io_uring`, replication, and broader filesystem qualification remain future
work. None may weaken the lifecycle or acknowledgment boundaries silently.
