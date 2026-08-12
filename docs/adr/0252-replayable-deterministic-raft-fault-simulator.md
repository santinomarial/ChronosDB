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
reach identical durable states and counters. `shrink_failing_trace` performs bounded deletion-based
replay and retains only candidates with the original failure status code.

## Consequences

Partitions, arbitrary delay/reordering, duplication, loss, process crashes, atomic persistence
faults, application advancement, membership, and snapshot actions are now reproducible without
threads, clocks, sockets, or disks. The simulator and core are single-thread-affine; no atomics or
memory ordering are involved.

The virtual network and trace reserve their declared capacities up front. Message payloads and
action payloads remain bounded by Raft and trace limits. Safety checking intentionally favors a
clear oracle over speed and compares retained prefixes quadratically across the small simulated
cluster. Simulation-rate measurement must precede optimization.

This foundation does not close the full Phase 14 campaign. Exhaustive bounded enumeration, long
seed matrices, injected timer/clock changes, physical segmented-log syscall faults, automated
membership/snapshot random generation, and stored minimized corpora remain Phase 18 work.

## Validation

Focused tests reproduce a partitioned and duplicated election/commit schedule, fail a persistent
election transition and restart from the prior term, replay the complete trace to identical durable
states, run eight seeds for 4,000 generated fault actions twice with exact trace/state equality,
drive joint membership plus local snapshot compaction, and shrink an irrelevant-prefix failure to
one essential action.

## References

- [ADR 0010](0010-tablets-raft-and-multiplexed-log-storage.md)
- [ADR 0012](0012-correctness-testing-and-performance-evidence.md)
- [ADR 0069](0069-deterministic-raft-and-multiplexed-state-record.md)
- [ADR 0076](0076-joint-consensus-raft-membership.md)

