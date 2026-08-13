# ADR 0315: Canonical multi-pair Raft observation acquisition

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB cluster, query, and networking maintainers
- **Extends:** [ADR 0313](0313-correlated-raft-observation-pair-fan-out.md)

## Context

A selected pair owner could acquire authority for one Raft group, but a distributed query may span
many groups. Embedding loops could block on the first group before starting the rest, sleep past
another group's deadline, expose a partial authority vector, accept duplicate group evidence, or
leave unrelated descriptors alive after one group failed.

## Decision

`RaftObservationTcpBatchAcquisition` owns a canonical unique group-sorted vector of selected pair
configs under a positive hard pair limit capped by the distributed plan limit. Construction creates
every pair owner and one fixed poll-descriptor vector without opening sockets.

Each batch poll drives every pair with zero wait before blocking. It then collects at most two
targets per running pair into the fixed poll vector and shortens the caller wait to the earliest
connect, handshake, exchange, or retry-backoff deadline across the batch. After readiness it drives
every pair again. Thus all groups begin before the batch consumes a blocking wait.

The result boundary is all-or-nothing. One pair failure cancels every running survivor. Explicit
cancellation does the same. Only when every pair is complete does the owner copy their owning
authorities in canonical group order and publish the vector. Metrics distinguish total, completed,
and active nonterminal pairs.

## Consequences and validation

Retained memory and descriptors are `O(selected groups)` with hard bounds; per poll work is linear
in the configured pairs and active descriptors. No partial authority can reach the packaged query
binder, and duplicate groups cannot overwrite or ambiguously order evidence.

A focused two-server mutual-TLS test acquires two groups through four target exchanges, proves no
early result, exact once-per-group service calls, canonical complete results, and exact terminal
metrics. The same test starts another batch and proves cancellation clears every active pair, then
rejects duplicate group configuration before I/O. It requires approved host execution where
sandbox policy forbids loopback bind.

Automatic leader/follower selection from committed placement, construction of pair configs and
correlations from a distributed plan, packaged bounded-stale query composition, process integration,
and broader failure matrices remain incomplete.

Invariants 4–6, 10, 11, 14, 15, and 18 apply.

## References

- [Correlated Raft observation pair fan-out](0313-correlated-raft-observation-pair-fan-out.md)
- [Catalog-backed Raft observation route resolution](0314-catalog-backed-raft-observation-route-resolution.md)
- [Correlated follower read proof binding](0303-correlated-follower-read-proof-binding.md)
