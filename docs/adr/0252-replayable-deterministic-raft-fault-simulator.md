# ADR 0252: Replayable Deterministic Raft Fault Simulator

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB Raft and correctness maintainers

## Context

Example-based Raft tests cannot systematically reorder messages or reproduce a rare combination of
partition, duplicate delivery, crash, and persistence failure. Wall-clock and socket-based tests
also hide the exact schedule that caused a failure. ADRs 0010, 0012, and 0069 require a deterministic
laboratory before broader production integration claims.

## Decision

`DeterministicRaftSimulator` owns a fixed configured set of `RaftNode` instances, a bounded virtual
network, a durable full-state image per node, directional links, and a bounded action trace. Explicit
actions start elections, propose, heartbeat, deliver/drop/duplicate exact message identities,
partition links, crash/restart nodes, fail the next persistent transition, apply entries, change
membership, and complete or compact snapshots. Message delivery order is virtual network time:
leaving a message queued delays it without consulting a wall clock.

Configuration may start every node from canonical term-zero state or supply exactly one complete
recovered persistent image per node in sorted node-ID order. Construction validates each image
through the production core and then runs the cross-node safety checker before releasing the
simulator. Crash/restart therefore uses the exact supplied durable image, including boundary states
that cannot be reached economically by generating a trace from term zero.

For each core transition, the simulator first checks virtual-network capacity and routes. If the
transition contains persistent state, that complete state becomes the node's durable image before
any outbound message is admitted. An armed persistence failure discards the transition's messages,
crashes the volatile node, and retains the prior durable image. Restart constructs a new core only
from that image. This models an atomic full-state persistence boundary; it does not pretend to model
individual file syscalls or torn physical records.

Every successful action runs an independent safety checker. It retains the first observed leader
per term, canonical committed entries, and each node's maximum durable term and commit index. It
checks historical election safety, monotonic durable state, applied-at-or-below-commit, retained-log
matching, committed-prefix agreement, future-leader completeness, and membership agreement at equal
snapshot positions. A snapshot may have node-local physical manifest identity, so equal consensus
positions do not require byte-identical physical snapshot descriptors.

`run_seeded` uses a repository-defined xorshift64* sequence rather than implementation-defined
standard-library distributions. It records every generated action, so a seed run and direct replay
reach identical durable states and counters. Each step derives bounded valid candidates from the
current state for elections, joint-membership begin/finalize actions, local snapshot compaction,
completion of already-pending snapshot installations, and current-term linearizable read barriers.
Membership transitions wait for the virtual network to drain so an old configuration cannot make
an already-admitted message structurally invalid. Election and read-barrier candidates also require
their sources to be admitted by every target's current configuration. When the selected action
class has no candidate, the generator prefers delivery, heartbeat, election, or restart progress
before a link mutation. `shrink_failing_trace` performs bounded deletion-based replay and retains
only candidates with the original failure status code. It starts with deterministic coarse chunks,
reduces granularity toward individual actions, and never exceeds `maximum_shrink_replays`.

`explore_fault_schedules` accepts one valid setup trace and exhaustively branches every queued
message into delivery or explicit loss through a bounded suffix depth. Callers may opt into a third
duplicate-message branch, directional link changes, and live-state node lifecycle branches. Message
identities are visited in ascending order and each identity's actions are ordered delivery, loss,
then optional duplication. Optional link branches follow in ascending source/destination order,
exclude self-links, and toggle the replayed state so each action is a partition or healing rather
than a no-op. Finally, ascending node IDs contribute exactly crash for an active node or restart for
an inactive node. `maximum_replays` bounds retained frontier work and replayed prefixes; the result
distinguishes a completed search from a truncated search and retains the exact first failing trace
and status. The one setup-validation replay is outside that exploration count.

## Consequences

Partitions, arbitrary delay/reordering, duplication, loss, process crashes, atomic persistence
faults, application advancement, membership, and snapshot actions are now reproducible without
threads, clocks, sockets, or disks. The simulator and core are single-thread-affine; no atomics or
memory ordering are involved.

The virtual network and trace reserve their declared capacities up front. Message payloads and
action payloads remain bounded by Raft and trace limits. Safety checking intentionally favors a
clear oracle over speed and compares retained prefixes quadratically across the small simulated
cluster. Simulation-rate measurement must precede optimization.

This foundation does not close the full Phase 14 campaign. Broader exhaustive action enumeration,
long seed matrices, injected timer/clock changes, physical segmented-log syscall faults, and stored
minimized corpora remain Phase 18 work.

## Validation

Focused tests reproduce a partitioned and duplicated election/commit schedule, fail a persistent
election transition and restart from the prior term, replay the complete trace to identical durable
states, run eight seeds for 4,000 generated fault actions twice with exact trace/state equality,
run 32 longer seeds that automatically generate and replay joint-membership begin/finalize, local
snapshot-compaction, and read-barrier actions with observed barrier completion, generate and replay
completion from an already-pending external snapshot install, drive explicit joint membership plus
local snapshot compaction, shrink an irrelevant-prefix failure to one essential action, and reduce a
65-action failure to at most four actions with only four candidate replays. A two-node election
exhaustively covers all delivery/loss prefixes through depth two, reports replay-bound truncation,
rejects invalid setup, and retains a membership-removal stale-message failure for exact replay.
Opt-in duplication exhausts all three depth-one outcomes and retains exact queue-exhaustion replay.
One-node lifecycle coverage completely enumerates crash then restart through depth two and reports
replay-bound truncation when the restart prefix cannot be retained.
Two-node link coverage exhausts both directional toggles through depth two, including healing, and
reports truncation one replay below the complete seven-prefix tree.
Recovered-image coverage restarts a node at terminal term, rejects the next election without state
mutation, rejects image-count mismatch and invalid local state, and rejects two individually valid
images whose same-term log entries violate log matching. Independent boundary schedules elect from
the penultimate term with a penultimate-index snapshot: one rejects another proposal at the reserved
next index, while the other restarts at terminal term and rejects another election without mutation.

## References

- [ADR 0010](0010-tablets-raft-and-multiplexed-log-storage.md)
- [ADR 0012](0012-correctness-testing-and-performance-evidence.md)
- [ADR 0069](0069-deterministic-raft-and-multiplexed-state-record.md)
- [ADR 0076](0076-joint-consensus-raft-membership.md)
