# Segmented Multi-Raft Persistent Log

## Purpose and public interface

`RaftPersistentLog` turns the full-state record codec into one node-level physical append stream.
It owns the dedicated directory, advisory `LOCK`, active segment, write frontier, local durable
frontier, and recovered latest state per logical group. `create_new` accepts only an empty dedicated
directory (optionally containing the regular lock file); `open_existing` verifies the complete
history before returning. `append` and `synchronize` deliberately expose different guarantees.

## Data structures and invariants

Every segment has a checksummed 64-byte versioned header followed by complete multiplexed records.
Segment number and node-global physical sequence are contiguous; group UUID, logical term/index,
commit/apply state, and snapshot metadata remain inside each record. A physical offset never becomes
a logical Raft index. Recovery stores only the latest full state per group while still validating
every earlier record.

Bounds apply before allocation: segment size is at most 1 GiB, record size is at most 16 MiB, and
configuration limits total segments, records, and recovered groups. Public headers contain no POSIX
types.

## Ownership, lifetime, and synchronization

The owner is move-only and not internally synchronized. One caller serializes append, sync,
observation, and close. Borrowed state is encoded during `append` and need not outlive the call.
`LOCK` excludes another process or in-process owner. A write, sync, rotation, or installation error
poisons the owner; later I/O is rejected with the retained root cause.

`append` completes all bytes but makes no power-loss claim. `synchronize` data-synchronizes the
active file and advances the durable physical sequence. Rotation synchronizes the predecessor and
durably installs the successor header before using it, so records never enter an ambiguously named
segment. `DurableMultiRaftRuntime` appends several groups from a bounded caller-provided batch, calls
one synchronization, and withholds every associated transition and outbound message until that
boundary completes. The same rule now covers persisted `applied_index` advancement.

Snapshot installation is deliberately two-stage. A received request exposes pending metadata but
sends no success response. After the application owner durably installs the named manifest/part
state, a completion operation persists the compacted Raft state and only then releases success.
Local compaction similarly requires an already applied exact term/index and stable configuration.

For a committed or joint configuration, a leader may issue a `QuorumSyncReceipt` after its
synchronized commit index covers an entry. Raft commit embodies either the committed majority or
both old and new majorities, and every counted follower response was withheld until that follower
synchronized its persistent transition. Tablet application adds a separate coverage check before
the proof can authorize a query-visible write acknowledgment.

## Recovery and failure behavior

Recovery holds the lock and rejects gaps, unknown entries, symlinks/non-regular files, invalid
headers, noncontiguous sequences, hostile lengths, and checksum failure. Explicit repair truncates
only a structurally incomplete suffix in the highest segment, then synchronizes file and directory.
It never treats a complete checksum-invalid record as a tail. Repeating recovery over the repaired
or unchanged bytes produces the same group states and positions.

## Complexity and tradeoffs

Recovery is linear in physical bytes and records, with memory proportional to one bounded record
plus the latest full state of each group. Full-state checkpoints simplify audit and reopen but may
amplify writes. Per-group snapshot checkpoints now exist, but segment-prefix reclamation still
requires proving that every group represented in a shared prefix has a later durable checkpoint;
deleting records merely because one group advanced would remain unsafe.

## Likely interview questions

- Why are append completion and data synchronization different frontiers?
- Why must the successor header be durable before its first record?
- Why can only an incomplete highest-segment suffix be repaired?
- Why does a physical sequence not replace a group's logical Raft index?
- What proof is required before reclaiming an old shared segment?
