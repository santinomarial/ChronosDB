# ADR 0460: Three-daemon mutable SQL failover qualification

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB Raft transport, replicated service, query, Native Protocol, and operations
  maintainers
- **Extends:** [ADR 0265](0265-unified-raft-transport-runtime.md),
  [ADR 0279](0279-authoritative-tablet-group-binding.md),
  [ADR 0437](0437-correlated-replicated-mutable-fragment-binding.md), and
  [ADR 0459](0459-bounded-row-backed-distributed-grouped-sql.md)

## Context

The packaged three-daemon Linux gate previously proved replicated ingest and a direct mutable-row
query, but not the complete row, aggregate, expression, and grouped SQL surfaces through abrupt
leader loss. Extending that gate exposed several composition defects that focused owners could not
reveal: later-ready durable completions could pass an earlier outstanding completion, one
disconnected peer could prevent RequestVote delivery to every connected peer, final QueryResult
frames were not marked end-of-stream, distributed-mode followers did not use the already-safe
common-leader redirect, and advisory placement leader hints were treated as current authority.
Plaintext loopback replicated ingest also rejected the reactor's intentional anonymous principal
identifier, and read-barrier timeout messages hid the stalled phase.

## Decision

The unified Raft runtime selects the lowest outstanding durable submission sequence across inbound,
timer, and application lanes. It consumes that sequence only when its owning completion is ready;
later ready work waits. Each component therefore exposes its lowest outstanding sequence separately
from its lowest completed sequence.

Peer-manager routing validates every outbound destination against immutable configuration, then
atomically queues the transition to the subset currently connected. A configured reconnecting or
disconnected destination does not block delivery to live peers. Previously queued complete frames
remain owned by reconnect, while newly omitted Raft messages rely on the protocol's ordinary
heartbeat/election retransmission and duplicate-safe term/index rules. An unknown destination,
connected-route capacity failure, or malformed result still fails closed.

The Native service marks the final QueryResult frame with `kFrameFlagEndStream` before appending
QueryEnd. The existing complete-group redirect resolver now runs in distributed mutable-query mode
as well: it redirects only when every required group has the same stable remote leader and fails
closed for split or unknown authority. Mutable fragment binding and worker execution continue to
require a current correlated Raft observation, barrier, membership, placement, and publication;
the committed placement `leader_hint` is checked only as an advisory replica identifier and cannot
override that current proof.

Replicated ingest accepts principal identifier zero as a structurally valid loopback-plaintext
identity. Authentication and remote principal authorization remain reactor/transport boundaries;
connection and request correlation remain mandatory. Read-barrier timeouts report the exact commit
admission, commit completion, barrier admission, barrier completion, or quorum-confirmation stage,
and retain the most recent bounded-admission failure.

`chronosd --raft-election-timeout-ms MILLISECONDS` accepts 101 through 60000 only with the complete
peer-transport bundle and sets one fixed local election timeout. The randomized 300--600 ms default
is unchanged. The Linux qualification assigns distinct 300/600/900 ms values so both resident
groups elect one common leader deterministically, then one common replacement after abrupt loss.

This decision qualifies only that controlled common-leader topology. It does not implement
cross-process acquisition from independently led groups, a globally atomic cross-group instant,
multi-key/all-type sufficient-state shuffle, multi-process real-CSEG scans, automatic movement, or
packet/partition chaos.

## Consequences

One real gate now starts three mutually authenticated daemons, commits and exactly retries a
QUORUM_SYNC batch, executes row, global aggregate, constant, row-expression/predicate, and multi-key
all-type grouped SQL, kills the common leader, elects a replacement on the two survivors, repeats
the retry and SQL checks, shuts down cleanly, and verifies every retained root.

Strict outstanding FIFO can delay ready work behind earlier durable work; that is required for the
existing sequence contract. Dropping a new message for a disconnected configured peer may delay
progress until the next Raft retransmission, but prevents one failed peer from stopping quorum
traffic. Fixed election timeouts are an operations/test control, not a production tuning
recommendation: equal fixed values can increase repeated election collisions, so deployments should
retain randomized defaults unless a measured topology requires otherwise.

All affected owners remain single-thread-affine. Durable completion publication continues through
the completion owner's mutex followed by pipe notification; this decision adds no new shared-memory
publication or memory-ordering edge.

## Affected invariants

- [Invariant 4](../architecture/invariants.md): strict outstanding FIFO preserves application and
  observation order across all durable completion sources.
- [Invariant 5](../architecture/invariants.md): current Raft proof, not advisory metadata, remains the
  mutable-query visibility authority.
- [Invariant 6](../architecture/invariants.md): safe redirect requires one common stable leader for
  the complete required group vector; split authority fails closed.
- [Invariant 8](../architecture/invariants.md) and
  [Invariant 9](../architecture/invariants.md): the failover gate repeats the exact ingest identity
  and verifies identical retained-root recovery.
- [Invariant 14](../architecture/invariants.md): Native and Raft wire bytes are unchanged; existing
  terminal framing is now emitted according to its protocol contract.
- [Invariant 18](../architecture/invariants.md): deterministic election control changes timing only,
  not durability, authority, snapshot, or query semantics.

## Validation

Focused tests cover outstanding-order Raft runtime intake, disconnected-peer routing, loopback
principal-zero replicated admission, common-leader redirect with the distributed query owner,
advisory leader-hint rebinding, bounded read-barrier timeout behavior, and terminal Native result
framing. The Linux process gate is the acceptance test for authenticated three-process composition
and abrupt leader loss.

Before this decision was committed, the complete normal query, cluster, and service suites passed
411, 206, and 106 tests. Their allocation-failure suites passed 57, 28, and 3 tests. Focused
ASan/UBSan runs passed 2 query, 5 Raft transport, and 6 replicated-service tests. Both Linux
replicated process cases passed, including the three-daemon gate in 1.33 seconds in that run. All 11
changed production sources passed repository-pinned clang-tidy 18; all changed C++ files passed
clang-format 18; CLI negative checks rejected an out-of-range timeout and a timeout without peer
transport; and the diff passed whitespace review. The Linux warning-enabled build still reports
unrelated existing missing-field-initializer warnings, so this decision does not claim a
warning-clean tree.

## Migration or rollback considerations

No durable or network format changes. Mixed pre-alpha binaries are not qualified. Removing the
fixed-timeout option restores randomized elections. Rolling back the completion ordering, redirect,
framing, hint, or connected-peer behavior reintroduces process-gate failures and is not a safe
operational rollback; roll back the complete binary before writing new state instead.

## Unresolved questions

- Design cross-process authority acquisition and routing when metadata and tablet groups have
  different leaders.
- Qualify partition schedules, election collision/backoff, snapshot transfer, rolling upgrades,
  and real multi-process CSEG execution.
- Replace row-backed grouping with a versioned bounded sufficient-state shuffle only after its
  skew, ownership, and differential contracts are specified and measured.

## References

- [Unified Raft transport runtime](../learning/raft-transport-runtime.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Native server operations](../operations/native-server.md)
- [Phase 16 roadmap](../roadmap.md#phase-16--distributed-query-execution-and-rebalancing)
