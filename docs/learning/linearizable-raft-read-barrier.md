# Linearizable Raft Read Barriers

## Purpose and public interface

`RaftNode::begin_read_barrier` lets a current leader establish a safe committed index for a
linearizable read without appending one entry per query. The returned transition sends bounded
leadership probes. A later `receive` transition carries `read_barrier_ready` after quorum
confirmation. Query code must still wait for `applied_index` to reach the barrier's `read_index`.
`MultiRaftRuntime` tags that result with its group, and `DurableMultiRaftRuntime` accepts a
`BeginReadBarrierOperation` through the same serialized batch interface.

## Data structures and invariants

One pending barrier stores its leader term, nonzero context, committed index, frozen stable or joint
voter configuration, and a set of acknowledgements. Starting requires a committed entry in the
leader's current term: this prevents a newly elected leader from relying on a commit index that may
only reflect an older configuration or term. Responses count only when term, context, acceptance,
and source voter all match. During joint consensus both old and new majorities are required.

Barrier completion is not application visibility. The read index names a committed prefix, while
`applied_index` names the prefix installed in the state machine. Serving before the latter reaches
the former would violate the committed-and-applied read contract.

## Ownership, lifetime, and synchronization

The deterministic node owns barrier state and uses no clock, socket, thread, or lock. Its runtime
serializes calls just as it does for elections and replication. Request/response values may outlive
a transition, but a response cannot complete a later barrier unless its exact term and context are
pending. Leadership changes synchronously discard pending state.

The Multi-Raft wrapper preserves the group on outbound probes and completion. A barrier start or
ordinary same-term response has no durable state of its own, so the durable owner does not invent a
physical-log record. If a recipient observes a higher term, its normal persistent transition is
still appended and synchronized before the response can leave the durable batch.

## Failure behavior and complexity

Followers persist a higher term before the runtime sends their response under the transition
contract. Rejections, stale terms, stale contexts, duplicates, and nonmatching responses do not
complete a barrier. Loss or partition leaves the one barrier pending and causes a later attempt to
return `UNAVAILABLE`; the owner may resolve this through its request deadline or a leadership
change. Each barrier sends `O(voters)` messages and retains `O(voters)` bounded state.

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
