# ADR 0547: Split-leader three-daemon process qualification

- **Status:** accepted
- **Date:** 2026-08-30
- **Owners:** ChronosDB Raft transport, replicated service, Native query, operations, and test
  maintainers
- **Extends:** [ADR 0460](0460-three-daemon-mutable-sql-failover-qualification.md) and
  [ADR 0469](0469-split-leader-native-read-authority-coordination.md)

## Context

The packaged Linux process gate intentionally elected the metadata and tablet groups on one common
leader. Focused service coverage proved that a Native coordinator could combine local metadata
authority with authenticated remote tablet authority, but it did not prove the same composition
across three real daemon processes or through abrupt loss of the remote tablet leader. One global
fixed election timeout per node could deterministically choose a common leader but could not create
independent group leaders.

## Decision

The Raft transport runtime accepts an optional bounded set of exact local per-group election timeout
overrides. Each override identifies one nonnil resident group and a timeout greater than the
heartbeat interval and no greater than 60 seconds. Duplicate, nonresident, zero, or otherwise invalid
overrides reject runtime creation. Omitted groups retain the existing global fixed timeout or
randomized default. Deadline arithmetic saturates at the steady-clock maximum.

`chronosd --raft-group-election-timeout GROUP_UUID=MILLISECONDS` exposes the same control only with
the complete authenticated peer-transport bundle. The canonical lowercase UUID and decimal timeout
are parsed strictly; the option is repeatable and duplicate validation remains authoritative in the
runtime. This is a deterministic qualification and measured-topology control, not a deployment
tuning recommendation.

The three-daemon Linux gate assigns metadata/tablet timeouts of 300/600 ms on node 1, 600/300 ms on
node 2, and 900/900 ms on node 3. The recovered persistent votes therefore prove that node 1 first
won metadata while node 2 first won the tablet group. The test sends supported SQL directly to node
1 and forbids a redirect, so completion requires local metadata authority plus authenticated remote
tablet authority. It then abruptly kills node 2, requires node 1 to win the higher tablet term,
replays the exact ingest identity as `MATCHING_RETRY`, and repeats the SQL surface without redirect.

## Consequences

Packaged process evidence now covers independent metadata/tablet leaders, remote read authority and
mutable fragment execution, abrupt remote tablet-leader loss, whole-attempt authority rebinding,
higher-term exact retry, orderly survivor shutdown, and recovery of every retained root. It remains
a controlled two-group, three-node loopback topology. It does not establish a globally atomic
cross-group instant, partition behavior, rolling upgrades, snapshot transfer, multi-process
real-CSEG scans, durable query-job recovery, or skew and latency bounds.

Per-group fixed timeouts can create repeatable split leadership, but careless equal or tightly
clustered values can also create election collisions. Ordinary deployments should retain randomized
timeouts unless measurements justify an override. The change affects only local timer selection; it
adds no durable or network bytes and no shared-memory publication edge.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): every visible query publication still requires its
  exact current-term quorum barrier and covered application publication.
- [Invariant 6](../architecture/invariants.md): split authority is combined only after the complete
  all-group attempt succeeds; partial output remains hidden.
- [Invariant 8](../architecture/invariants.md) and
  [Invariant 9](../architecture/invariants.md): abrupt leader loss repeats one exact durable ingest
  identity and every retained database root must recover.
- [Invariant 18](../architecture/invariants.md): election overrides alter timing only; authenticated
  node identity, committed membership, placement, and endpoint authority remain unchanged.

## Validation

Focused runtime tests cover one exact override, duplicate rejection, and nonresident-group rejection.
CLI checks cover help text, malformed values, and use without the complete transport bundle. The
Linux process gate proves the split leader identities, direct no-redirect SQL before and after tablet
leader loss, higher-term matching retry, persistent initial votes, and retained-root recovery. The
complete Linux process suite also covers the surrounding packaged startup, corruption, SQL,
security, subscription, and common single-process boundaries. In the qualifying run, all 21 Linux
process tests and all 115 normal and ASan/UBSan service tests passed; all four Raft transport runtime
tests passed under TSan. Apple's sanitizer run disabled unsupported leak detection. CLI checks and
changed-file formatting passed. Repository-pinned clang-tidy 18 remains blocked by its known
incompatibility with the macOS 26 libc++ generic bit-count builtins; its additional `chronosd`
findings are pre-existing outside this change.

## Migration or rollback considerations

No durable or network migration is required. Mixed pre-alpha binaries are not qualified. Removing
the per-group option restores the prior global fixed or randomized timer selection. Rolling back the
split-leader process test reduces deployment evidence but does not change stored state.

## Unresolved questions

- Qualify packet partitions, election collision/backoff, snapshot transfer, and rolling upgrades.
- Run multi-process real-CSEG and reducer process-loss gates with durable replacement/recovery.
- Measure failover latency, authority-rebinding latency, throughput, and skew under declared bounds.

## References

- [Native server operations baseline](../operations/native-server.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
