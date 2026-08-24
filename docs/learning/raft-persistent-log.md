# Segmented Multi-Raft Persistent Log

## Purpose and public interface

`RaftPersistentLog` turns the full-state record codec into one node-level physical append stream.
It owns the dedicated directory, advisory `LOCK`, active segment, write frontier, local durable
frontier, and recovered latest state per logical group. `create_new` accepts only an empty dedicated
directory (optionally containing the regular lock file). It may also restart its own interrupted
pre-rename installation by removing only the exact regular segment-1 temporary while holding the
lock; every other nonempty namespace fails closed. `open_existing` verifies the complete history
before returning. `append` and `synchronize` deliberately expose different guarantees.

## Data structures and invariants

Every segment has a checksummed 64-byte versioned header followed by complete multiplexed records.
Segment number and node-global physical sequence are contiguous; group UUID, logical term/index,
commit/apply state, and snapshot metadata remain inside each record. A physical offset never becomes
a logical Raft index. Recovery stores only the latest full state per group while still validating
every earlier record.

Bounds apply before allocation: segment size is at most 1 GiB, record size is at most 16 MiB, and
configuration limits total segments, records, and recovered groups. Public headers contain no POSIX
types. The minor-1 snapshot voter count is physically `u32` but cannot exceed the 65,535-voter
Membership Command v1 ceiling; decode rejects a larger checksummed count before reserving storage,
and encode rejects a larger source vector. Exact-limit and over-limit tests include repeated
fail-closed disk recovery with byte preservation. Minor-1 persistent-state payload accounting is
exact:
`112 + 8 * snapshot_voters + sum(32 + entry_payload_bytes)`. The core checks that aggregate rather
than relying only on per-entry and entry-count bounds.
Legacy snapshots without an encoded voter checkpoint are canonicalized from bootstrap membership
before that accounting runs. This prevents a minor-0-shaped input from passing a smaller check and
then becoming an over-budget minor-1 state once its voters are owned in memory.

## Ownership, lifetime, and synchronization

The owner is move-only and not internally synchronized. One caller serializes append, sync,
observation, and close. Borrowed state is encoded during `append` and need not outlive the call.
`LOCK` excludes another process or in-process owner. A write, sync, rotation, or installation error
poisons the owner; later I/O is rejected with the retained root cause. For an ambiguous append or
synchronization error, the complete record may nevertheless exist on disk. The caller still
receives failure and must not release its transition or outbound messages; recovery, rather than
the failed in-memory owner, decides which complete checksummed prefix exists. Focused
asynchronous-owner tests execute the real record write or data synchronization before returning
`EIO`, fan that status out to all accepted work, and then reopen the exact complete term/vote record
without treating the failed call as an acknowledgment.

The composed tablet-snapshot completion tests also stop one completion record after a real 16-byte
prefix write. Strict durable-runtime reopen returns corruption without changing the segment.
Repair-authorized reopen truncates and synchronizes exactly those 16 bytes, restores the preceding
group state, and lets the application owner retry the completion. This exercises the public
composition of failed append, explicit tail-repair authority, and persist-before-response behavior.
The same composed image injects failure at repair size inspection and ambiguous real truncate,
file-sync, and directory-sync operations. Each failed runtime open releases the durable lock. A
fresh open validates either the original incomplete suffix or the already-truncated prefix before
the application retries completion, so no in-memory recovery progress becomes authority.
Mixed-owner schedules retain the same rule after a preceding RTAS failure: the Raft fault remains
armed across runtime reopen, then its definite or partial write failure withholds the second
response and leaves recovery—not retry memory—to select the next authority.
The repeated-fault lifecycle performs that incomplete-record rejection and authorized 16-byte
repair twice in succession. Each fresh runtime reconstructs the same pre-completion group state;
only the later complete record becomes snapshot authority.
The composed reopen matrix also pairs both RTAS cleanup outcomes with every repair failure. Repair
may fail at the final size check with the incomplete tail intact, after the real truncate has
removed it, or after either synchronization boundary. Every path releases `LOCK`; the next recovery
validates the observed file rather than assuming which operation happened.

Rotation first data-synchronizes and closes the predecessor. Either result may be ambiguous, so a
reported error poisons the writer even if the kernel operation completed. Deterministic tests inject
`EIO` after each real predecessor operation, verify that later appends return the retained root
cause, close the remaining physical ownership, and reopen only the exact predecessor record prefix.
The reopened writer can then retry the rotation normally.

Successor installation has five ordered stages: exclusive temporary creation, complete header
write, full-file synchronization, no-replace rename, and directory synchronization. Focused fault
injection covers each stage. A failure before rename leaves either no successor or a recognized
temporary that reopen removes while holding `LOCK`. An ambiguous error after the real rename or
directory sync can leave the valid empty successor visible, so reopen adopts it. Neither case
recovers the unattempted record: the exact predecessor prefix remains authoritative and the next
sequence appends successfully to segment 2. These process-level tests do not qualify power-loss
behavior.

Recovery-anchor publication follows the same temporary-write-sync-rename-directory-sync sequence
and then closes the anchor file. Occurrence-selective injection reaches all six anchor boundaries
after the complete checkpoint is synchronized. Before rename, reopen removes any temporary and
retains the old base, treating the checkpoint segment as contiguous later history. After the real
rename, reopen treats the valid anchor as authoritative and removes the old segment only after the
checkpoint validates. Both paths recover identical latest group states and continue at the same
next physical sequence. This establishes process-restart arbitration, not power-loss qualification.

After anchor publication, reclamation removes obsolete segments and synchronizes the directory,
then removes older anchors and synchronizes again. Ambiguous real-unlink and real-directory-sync
injection covers both cleanup epochs, including a second checkpoint with an older anchor. The live
owner retains the reported error before updating its public reclamation counters, while restart
uses the already-published newest anchor, validates its exact checkpoint, observes the completed
cleanup, and continues at the next sequence.

Recovery owns the directory and advisory lock before it enumerates the namespace. Failure injection
now covers directory and file opens; every directory, lock, anchor, segment, and final-active
metadata inspection; anchor, segment-header, record-header, and record-body reads; namespace
enumeration; ambiguous stale-temporary unlink and cleanup directory sync; and the final active-file
and directory synchronization gates. Incomplete-tail repair additionally injects the repair-only
size check and ambiguous real truncate, file-sync, and directory-sync results. Every failed open
unwinds all temporary handles and releases `LOCK`. A fresh owner validates either the unchanged
incomplete suffix or the already-truncated complete prefix, recovers the same exact logical state,
and accepts the next physical sequence. A combined namespace matrix adds four recognized
temporaries, two obsolete segments, and one obsolete anchor below an authoritative base. Injection
at every cleanup unlink and at the directory sync proves each completed removal is monotonic. A
worst-case schedule repeats failed recovery six times, removing one artifact per attempt, then
reports an ambiguous error after the final cleanup sync; every attempt releases `LOCK`, and the
next open still recovers the authoritative checkpoint exactly.

The [Raft persistent-log crash matrix](../testing/raft-persistent-log-crash-harness.md) complements
those return-value injections with 31 real subprocess images. It sends `SIGKILL` after successful
POSIX operations across initialization, rotation, checkpoint and anchor publication, and both
cleanup epochs. Each image must recover one exact authority twice and accept the next sequence.
Because the host kernel and storage stack remain alive, this is process-restart evidence rather than
power-loss qualification.

The sustained corruption campaign uses a canonical two-group checkpoint split across two retained
segments plus one authoritative anchor. Its 618 single-bit images, 618 strict-prefix truncations,
and three missing-artifact images are each opened in strict and repair-authorized modes. Every open
must return `CORRUPTION`, release `LOCK`, and preserve the damaged byte image and namespace exactly.
After each original file is restored, clean recovery must reproduce base 3, both checkpoint
records, both latest group states, and physical sequence 4. This is exhaustive for single low-bit
mutations and truncations of that bounded image, not for coordinated adversarial checksum repair.

Linux-only packaged-daemon coverage adds the aggregate-owner boundary. It damages one
checksum-covered segment-number byte in the established metadata-Raft segment header, requires the
exact header-checksum failure before native socket admission, and compares the complete segment
after process rejection. The database owner does not patch the segment or proceed to WAL recovery.
A companion case damages the first complete multiplexed record payload after its headers, requires
the payload-checksum failure, and proves the complete segment is still unchanged. A third appends
the minimal one-byte incomplete suffix; packaged startup has no repair authorization, reports the
incomplete final record, and preserves the segment byte-for-byte.
The adjacent namespace case synchronizes one unrecognized regular file beside the established
segment. Packaged startup reports the exact unknown-entry corruption before native socket admission
and preserves both files, proving cleanup remains limited to recognized temporary and obsolete
artifacts. A second namespace case synchronizes a symlink to the established segment. Directory
enumeration classifies it without following; packaged recovery reports the exact non-regular-entry
corruption and preserves both the link target text and segment bytes.
The anchor companion reopens the daemon's metadata log through `RaftPersistentLog`, checkpoints its
exact recovered group state into a fresh retained base, and proves one clean packaged reopen. It
then damages a checksum-covered base field; packaged startup rejects the highest anchor without
fallback and preserves both the 64-byte image and retained checkpoint segment.
A portable owner companion truncates that anchor to 63 bytes. Repeated recovery rejects its exact
invalid-size corruption, releases ownership, preserves the truncated anchor and retained segment,
and never consults reclaimed history as fallback authority.
A portable database-owner companion removes that anchor after segment 1 has been reclaimed. Without
the anchor, recovery requires base segment 1, rejects the surviving segment-2 checkpoint as an absent
base, and leaves it unchanged rather than treating its filename as authority.
A complementary database-owner case retains the anchor but damages the selected segment-2 header.
Repeated recovery validates the anchor, rejects the exact retained-header checksum, releases the
owner, and preserves both images; anchor selection cannot bypass retained-byte integrity.
A retained-record companion damages the first complete checkpoint payload instead. Repeated
recovery rejects the exact payload checksum, releases ownership, preserves both images, and does not
recreate segment 1; anchor selection cannot turn complete record corruption into repairable tail.
The incomplete-record companion derives that checkpoint's encoded end, truncates it by one byte,
and explicitly authorizes ordinary final-tail repair. Recovery still returns the exact incomplete
checkpoint corruption twice and preserves the image, because discarding any anchor-required record
would destroy the only retained state for its logical group.

The recovery-layout matrix checks a different dimension: target-size arithmetic and latest-state
selection. A canonical state encodes to 213 bytes. Eight targets straddle the exact one-, two-,
three-, and four-record capacities after the 64-byte header, and each runs with 1, 3, and 8 groups.
For every one of the 24 cases, a small independent model advances only when
`active_end + 213 > target`, while checkpointing resets it to a fresh base. Reopen must match the
model's segment number/count, end offset, record count, physical frontier, and UUID-sorted latest
states before and after ordinary continuation, checkpoint reclamation, and a checkpoint tail.
Variable payload sizes and production-scale histories remain separate campaigns.

Explicit close adds no synchronization boundary. It invalidates the active segment, advisory lock,
and directory in that order, continues after an error, and returns the first physical close error.
Because POSIX close can release a descriptor even when it reports failure, none of those descriptor
numbers is retried; a repeated log close is instead a no-op. Deterministic real-filesystem tests
inject every nonempty combination of ambiguous errors after the underlying closes and prove that
the first error is retained, the lock is released, and the already-synchronized state reopens
exactly.

`append` completes all bytes but makes no power-loss claim. `synchronize` data-synchronizes the
active file and advances the durable physical sequence. Rotation synchronizes the predecessor and
durably installs the successor header before using it, so records never enter an ambiguously named
segment. `DurableMultiRaftRuntime` appends several groups from a bounded caller-provided batch, calls
one synchronization, and withholds every associated transition and outbound message until that
boundary completes. The same rule now covers persisted `applied_index` advancement.
The core owns both the in-memory replacement and returned post-apply state before it advances that
index. Resource exhaustion therefore leaves the committed-unapplied range unchanged and cannot make
the runtime lose a required persistence transition.
Ordinary proposals use the same boundary around the entire prospective node. The appended entry,
self progress, possible immediate commit and membership derivation, full replication batch, and
returned state all exist before the live leader changes, so the durable owner never observes a
failed call after hidden proposal progress.
Current-term progress no-ops share that internal append mechanism. A prior-term exact-retained retry
therefore cannot leak its required proof entry or consume an index unless the complete durable and
replication transition can be returned.
The deterministic core pre-owns the exact post-term persistent state before any canonical
higher-term response can demote a node. Allocation failure therefore leaves the group unchanged;
success always gives this owner the complete state it must append and synchronize.
Vote admission uses the same ownership rule: its prospective grant/rejection response and any term
or vote persistence change exist before the deterministic node publishes the decision.
Local election start likewise owns the next term/vote state and complete candidate or immediate-
leader transition before mutation, so the durable owner never receives a partially constructed
election result.
While candidate acknowledgements do not change persistent state, they obey the same atomic
transition boundary: the replacement vote set and, at quorum, every leader replication map and
initial heartbeat are owned before candidacy changes. Resource exhaustion therefore neither leaks
a counted vote nor exposes partially initialized leadership.
AppendEntries acknowledgements extend that boundary to follower progress and commit. The leader
prepares replacement match/next maps, derives any newly committed membership, copies the exact
post-commit durable state, and constructs every resulting replication message before publication.
A failed rejection retry similarly leaves its prior next index intact. The durable owner therefore
never receives an error after an unreported commit mutation.
Snapshot completion acknowledgements also prepare follower progress and the next replication
message together. Until a successful acknowledgement's suffix message is owned, the leader keeps
selecting the installed snapshot boundary for that follower; rejected acknowledgements likewise
own their snapshot retry before returning.
Inbound AppendEntries validation produces the candidate retained log, prospective commit, and
membership once. The node then owns both copies needed by the in-memory state and returned durable
transition, plus the exact feedback response, before it publishes a higher term, suffix replacement,
or commit. The persistence owner never has to reconstruct a partially applied follower transition.
Inbound InstallSnapshot requests use the same boundary. Feedback paths own their response and any
post-term persistent state first. A new installation owns one metadata copy for the core's pending
completion identity and another for the externally returned task before it changes role or leader;
the application owner therefore cannot receive work that the core failed to retain, nor can the core
retain work that was never returned.

The maximum physical sequence is a terminal record identity. Once recovered or emitted, the
Multi-Raft owner rejects and fails closed before invoking another deterministic group transition;
otherwise the group could change in memory before discovering that no persistence identity remains.
The same fail-before-transition rule applies to the node-local outbound bound: configuration must
hold the exact maximum fanout implied by `maximum_voters`, including one response for a single-voter
configuration.
The durable owner also derives a per-group state budget from the segment target by subtracting the
segment header and record header/trailer. The core applies the resulting smaller of the configured
and physical budgets to recovery, proposals, follower suffix replacement, membership commands,
leader no-ops, snapshot completion, and compaction before changing state. Recovery includes any
bootstrap voters added while canonicalizing a legacy nonempty snapshot.
The durable batch owner applies that rule cumulatively before dispatch. Observation, applied-index,
and local-compaction operations reserve no outbound messages; snapshot completion reserves one;
every other operation reserves the configured maximum core fanout. Capacity rejection is
nonterminal because no group transition or log append has started.

Snapshot installation is deliberately two-stage. A received request exposes pending metadata but
sends no success response. After the application owner durably installs the named manifest/part
state, a completion operation persists the compacted Raft state and only then releases success.
The completion operation first owns its retained suffix, membership projection, two exact copies of
the new persistent state, commit notification, and response. Rejection likewise owns its response
before releasing the pending identity. Allocation failure therefore leaves the same completion
authority available for retry without exposing a partially installed Raft snapshot.
Local compaction similarly requires an already applied exact term/index and stable configuration.
It prepares the canonical voter checkpoint, retained suffix, replacement base voters, and returned
durable state before erasing the live prefix, so resource exhaustion leaves the uncompacted state
available for exact retry. Checkpoint voters come from replay through the compaction boundary rather
than the node's later live state. A boundary inside a joint transition is rejected; a stable earlier
boundary can retain the complete joint/final pair and recover the later configuration.

For a committed or joint configuration, a leader may issue a `QuorumSyncReceipt` after its
synchronized commit index covers an entry. Raft commit embodies either the committed majority or
both old and new majorities, and every counted follower response was withheld until that follower
synchronized its persistent transition. Tablet application adds a separate coverage check before
the proof can authorize a query-visible write acknowledgment.

## Recovery and failure behavior

Recovery holds the lock and rejects retained gaps, unknown entries, symlinks/non-regular files,
invalid headers, noncontiguous sequences, hostile lengths, and checksum failure. Explicit repair
truncates only a structurally incomplete suffix in the highest segment, then synchronizes file and
directory. It never treats a complete checksum-invalid record as a tail. After reclamation, the
highest immutable recovery anchor names the exact retained base and a complete one-record-per-group
checkpoint. Lower segments are removed only after that checkpoint validates. Repeating recovery
over the repaired or unchanged bytes produces the same group states and positions.

## Complexity and tradeoffs

Recovery is linear in retained physical bytes and records, with memory proportional to one bounded
record plus the latest full state of each group. Full-state checkpoints simplify audit and reopen
but may amplify writes. The node-wide reclamation operation proves that every resident group has a
fresh durable checkpoint before deleting a shared prefix; deleting records merely because one group
advanced remains unsafe.

`chronos-raftbench` provides evidence about this tradeoff without changing the production path. It
times closed-loop batches of deterministic full-state records in APPEND_ONLY and LOCAL_SYNC modes,
then closes and exactly reopens each retained image. Its batch-size, payload-size, group-count, and
segment-size controls expose physical write and synchronization cost, but they do not model Raft
quorum commit, transport, catch-up, application, or power loss. See the
[Raft persistent-log benchmark contract](../benchmarks/raft-persistence.md).

## Likely interview questions

- Why are append completion and data synchronization different frontiers?
- Why must the successor header be durable before its first record?
- Why can only an incomplete highest-segment suffix be repaired?
- Why does a physical sequence not replace a group's logical Raft index?
- Why must aggregate persistent-state size be checked before a deterministic transition mutates the
  group?
- What proof is required before reclaiming an old shared segment?
- Why must the recovery anchor become durable before the first old segment is removed?
