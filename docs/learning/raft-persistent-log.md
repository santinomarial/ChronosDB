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
types. Minor-1 persistent-state payload accounting is exact:
`112 + 8 * snapshot_voters + sum(32 + entry_payload_bytes)`. The core checks that aggregate rather
than relying only on per-entry and entry-count bounds.

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

The maximum physical sequence is a terminal record identity. Once recovered or emitted, the
Multi-Raft owner rejects and fails closed before invoking another deterministic group transition;
otherwise the group could change in memory before discovering that no persistence identity remains.
The same fail-before-transition rule applies to the node-local outbound bound: configuration must
hold the exact maximum fanout implied by `maximum_voters`, including one response for a single-voter
configuration.
The durable owner also derives a per-group state budget from the segment target by subtracting the
segment header and record header/trailer. The core applies the resulting smaller of the configured
and physical budgets to recovery, proposals, follower suffix replacement, membership commands,
leader no-ops, snapshot completion, and compaction before changing state.
The durable batch owner applies that rule cumulatively before dispatch. Observation, applied-index,
and local-compaction operations reserve no outbound messages; snapshot completion reserves one;
every other operation reserves the configured maximum core fanout. Capacity rejection is
nonterminal because no group transition or log append has started.

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

## Likely interview questions

- Why are append completion and data synchronization different frontiers?
- Why must the successor header be durable before its first record?
- Why can only an incomplete highest-segment suffix be repaired?
- Why does a physical sequence not replace a group's logical Raft index?
- Why must aggregate persistent-state size be checked before a deterministic transition mutates the
  group?
- What proof is required before reclaiming an old shared segment?
- Why must the recovery anchor become durable before the first old segment is removed?
