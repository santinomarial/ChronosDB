# Deterministic Raft Simulation

## Purpose and public interface

`DeterministicRaftSimulator` turns distributed nondeterminism into a sequence of owned
`RaftSimulationAction` values. Callers can execute one action, replay a retained trace, generate a
seeded schedule, inspect active and durable node state, list queued message routes, or shrink a
failing trace. The simulator uses the production deterministic `RaftNode`; it does not contain a
second consensus implementation. `RaftSeededSimulationSchedule` names the seed and finite action
budget so two width-compatible integral arguments cannot be transposed at a call site.

## Data structures and ownership

Each configured node has two distinct owners: an optional live core and a durable `PersistentState`
image. Crash destroys only the live core. Restart reconstructs it from the image. The virtual
network is a preallocated vector of optional slots containing exact message identity, source, and
owned outbound message. A preallocated directional-link matrix decides delivery at the moment a
message is selected. Queued messages survive sender crashes, just as bytes already handed to a real
network may outlive a process.

The action trace owns proposal and membership payloads. Message and action identities are stable
within a replay. A repository-defined fixed PRNG makes seeded choices independent of the standard
library. Seeded generation first derives bounded valid candidates from current state. That permits
automatic joint-membership begin/finalize and stable local snapshot compaction without emitting an
invalid action that would terminate the schedule. Eligible leaders may also begin current-term read
barriers; the simulator counts completed barriers while their exact request/response traffic remains
subject to delay, duplication, loss, partitions, and replay. Membership transitions wait for the
virtual network to drain, and election/read-barrier sources must be admitted by every target's
current configuration. If a chosen action class is unavailable, progress prefers message delivery,
a leader heartbeat, an election, or restart before another link mutation. One caller thread
exclusively owns nodes, links, queues, durable images, the trace, and safety-model state.

## Persistence and failure behavior

A transition's full persistent state is copied into a candidate, installed as the durable image,
and only then are its outbound messages enqueued. `RaftSimulationFailNextPersistence` instead drops
those messages, destroys volatile state, and leaves the old image unchanged. A restart therefore
cannot observe half of a transition. This represents the logical atomic persistence contract; the
segmented log's write/sync/rotation failure points require separate file-backed campaigns.

Disabled links, delivery to crashed nodes, and explicit drops consume a selected message as loss.
Messages left queued are delayed and can be delivered in any order. Duplication allocates a new
identity for the same value message. Ordinary capacity failures are statuses, never silent growth.
A terminal step error is sticky so its trace remains a stable reproducer.

## Safety model

After each successful action the simulator checks:

- no two different nodes have ever been observed as leader in one term;
- durable term and commit index never regress across restart;
- applied index never exceeds commit and commit never exceeds the local durable log end;
- replicas that share an index/term share every comparable retained prefix entry;
- every retained committed index has one canonical entry across replicas; and
- every later-term leader retains or snapshots every known earlier committed entry.

The committed-entry map is a small independent reference model. Snapshot-covered entries are
accepted as compacted; equal snapshot positions must retain the same membership checkpoint, while
node-local physical manifest fields may differ.

## Complexity and tradeoffs

Step cost includes safety checking. Durable-state and committed-prefix checks are linear in retained
entries; pairwise log matching is intentionally quadratic in simulated nodes and comparable log
length. Network lookup is linear in the configured message bound. These choices make ownership and
failure reproduction obvious. Optimizing them requires measured simulation-rate evidence.

Deletion shrinking is also deliberately simple: replay candidates with one action removed and keep
the deletion when the original failure status code remains. It is bounded by
`maximum_shrink_replays`; semantic dependency-aware and chunk-based shrinking can be added after
corpus evidence shows a need.

## Verification and likely interview questions

Focused coverage includes partition, duplicate, commit propagation, crash/restart, atomic
persistence failure, exact replay, seeded schedules that automatically produce joint-membership and
local-compaction churn plus completed read barriers, generated completion of a pending external
snapshot install, explicit joint membership and compaction, trace shrinking, and bound validation.
The production core additionally rejects AppendEntries predecessors below an
installed snapshot without interpreting the compacted entry as retained log storage, and its
higher-term regression requires persistence before the negative response. The same core serializes
pending external installation against local compaction so one simulator node cannot create a second
snapshot authority before resolving the first. Duplicate snapshot requests coalesce around that
owner; different requests fail negatively without replacing it, including persist-before-response
handling for a higher term. A committed-prefix conflict also preserves the pending identity so the
external owner can explicitly reject it. An impossible snapshot whose last-included term exceeds
the request term fails before the node observes that request term or publishes external
installation work. Recovery
also rejects such impossible snapshot history relative to current term. Failed append responses
must name a nonzero conflict index, and their optional conflict term cannot exceed the response term,
so malformed higher-term feedback cannot step down a candidate. Recovered term-zero state has no
vote, and an index-zero snapshot has no external or membership identity. Long seed campaigns,
exhaustive schedules, timer clock changes, physical disk faults, and minimized corpus retention
remain in the hardening ledger. Snapshot index `UINT64_MAX` is rejected before external installation
because installing it would create a durable state the exhaustion-aware recovery path cannot reopen.
Higher-term AppendEntries requests cannot replace a committed entry by changing its term or by
retaining its term while changing type or payload bytes; both failures preserve the complete local
persistent state and role because validation precedes term observation.
Vote requests reject the same reserved last-log index before term or vote observation because no
canonical candidate can own an entry at that position.
AppendEntries requests reject it as a predecessor even for an entry-free heartbeat because no
canonical leader can own the predecessor that such a heartbeat claims.
They also reject it as `leader_commit` before term or role observation because no canonical leader
can have committed the reserved index.
AppendEntries responses reject it as `match_index` before observation because no follower can own
an entry at the reserved position, even when reporting a failed replication attempt.
InstallSnapshot responses likewise reject it because a follower cannot have installed a snapshot at
the reserved boundary; a failed response may still report the zero empty-snapshot boundary.

Useful questions include: why can a queued message survive a sender crash; why must durable state be
installed before outbound admission; why does replay use explicit message IDs; why is a snapshot
allowed to hide a committed entry; and why does seeded generation avoid `std::uniform_distribution`?
