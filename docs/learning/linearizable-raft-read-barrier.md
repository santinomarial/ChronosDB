# Linearizable Raft Read Barriers

## Purpose and public interface

`RaftNode::begin_read_barrier` lets a current leader establish a safe committed index for a
linearizable read without appending one entry per query. The returned transition sends bounded
leadership probes. A later `receive` transition carries `read_barrier_ready` after quorum
confirmation. Query code must still wait for `applied_index` to reach the barrier's `read_index`.
`MultiRaftRuntime` tags that result with its group, and `DurableMultiRaftRuntime` accepts a
`BeginReadBarrierOperation` through the same serialized batch interface.

The packaged replicated query path composes that primitive through `ReplicatedReadBarrier`. Its
transport poll owner first submits `CommitCurrentTermOperation`, then begins each barrier through
the ordered application lane. It records the submission sequence for the initial transition and
the exact group, term, and context for later peer responses. A query-thread waiter has one finite
deadline; a timed-out request releases its bounded slot, and a stale completion cannot match a new
context. Local one-voter mode executes the same no-op/barrier pair directly.
`await_authority` additionally owns the exact current-leader observation that validated each
completed barrier. Transported mode captures it from the correlated completion; local mode appends
one ordered observation to the same durable batch. The barrier-only `await` path does neither.

## Data structures and invariants

One pending barrier stores its leader term, nonzero context, committed index, frozen stable or joint
voter configuration, and a set of acknowledgements. Starting requires a committed entry in the
leader's current term: this prevents a newly elected leader from relying on a commit index that may
only reflect an older configuration or term. Responses count only when term, context, acceptance,
and source voter all match. Recipients likewise require the request source to be an active voter
before observing its term. Learners may receive log and snapshot replication, but cannot assert
leadership through this exception. During joint consensus both old and new majorities are required.

Barrier completion is not application visibility. The read index names a committed prefix, while
`applied_index` names the prefix installed in the state machine. Serving before the latter reaches
the former would violate the committed-and-applied read contract.

`ReplicatedIngestDatabase::acquire_query_snapshot` closes that second half of the proof. It checks
the metadata catalog's applied index and each tablet publication's matching Raft commit position
against the returned group vector before pinning query state. Because worker-extension application
finishes before asynchronous completion publication, the check observes either a covering immutable
publication or a fail-closed error.
For distributed fragment construction, the authority vector removes a reobservation race: the
metadata-backed binder receives the exact serving-node/term observation paired with the barrier,
then independently checks stable membership, committed placement, and Manifest durability.

## Ownership, lifetime, and synchronization

The deterministic node owns barrier state and uses no clock, socket, thread, or lock. Its runtime
serializes calls just as it does for elections and replication. Request/response values may outlive
a transition, but a response cannot complete a later barrier unless its exact term and context are
pending. Leadership changes synchronously discard pending state.

Barrier issuance is prepare-before-publish. The node first owns the frozen voter vectors, local
acknowledgement set, and complete outbound transition in temporaries. Only after that fallible work
succeeds does it move the pending barrier into node state and advance the context. That final move
is required to be non-throwing, so allocation failure leaves no hidden pending owner and an exact
retry uses the same context.

The Multi-Raft wrapper preserves the group on outbound probes and completion. A barrier start or
ordinary same-term response has no durable state of its own, so the durable owner does not invent a
physical-log record. If a recipient observes a higher term, its normal persistent transition is
still appended and synchronized before the response can leave the durable batch. The recipient
prepares both its response capacity and an exact post-term persistent-state value before changing
the deterministic node, so later publication on this path is allocation-free. A leader observing a
higher-term barrier response prepares the same exact persistent-state value before it demotes and
discards the pending barrier.

## Failure behavior and complexity

Followers persist a higher term before the runtime sends their response under the transition
contract. Rejections, stale terms, stale contexts, duplicates, and nonmatching responses do not
complete a barrier. Loss or partition leaves the one barrier pending and causes a later attempt to
return `UNAVAILABLE`; the owner may resolve this through its request deadline or a leadership
change. Allocation or container-limit failure while issuing a barrier returns `RESOURCE_EXHAUSTED`
without changing role, term, leader identity, persistent state, pending ownership, or the next
context. The acknowledgement set insertion has the same strong guarantee: allocation failure
returns `RESOURCE_EXHAUSTED` without counting the responder, and an exact retry can add it once.
Recipient-side response or persistent-state preparation failure also returns
`RESOURCE_EXHAUSTED` before an admitted request can change term, vote, role, leader identity, or
durable state. Retrying then returns the exact higher-term state that must be synchronized together
with the accepted response. Higher-term response preparation has the same failure guarantee and
also preserves the pending barrier until the complete demotion transition can be returned.
Each barrier sends `O(voters)` messages and retains `O(voters)` bounded state.

The deterministic simulator exposes barrier issuance as a replayable action. Seeded generation
selects only leaders with a current-term committed entry, no pending barrier, and voter targets that
all admit the leader as a source. Requests and responses then traverse the ordinary virtual network,
so delay, loss, duplication, partition, crash, and completion remain part of the retained trace.

## Tradeoffs and likely interview questions

An explicit probe avoids coupling reads to log catch-up and avoids clock assumptions, at the cost of
one quorum round per non-coalesced read. A future runtime may safely coalesce callers onto one
pending barrier while preserving the same read index, but this core deliberately exposes only one
owner-controlled operation.

- Why must the leader first commit an entry in its current term?
- Why does quorum confirmation not by itself make state query-visible?
- Why are voter sets frozen at barrier issuance?
- Why can a lagging follower safely acknowledge without matching the leader's log?
- What makes an old response harmless after restart or reelection?
