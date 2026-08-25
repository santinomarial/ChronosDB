# ADR 0476: Portable pinned grouped sufficient-state execution owner

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB query, cluster, and distributed-query maintainers
- **Extends:** [ADR 0395](0395-pinned-definition-bound-vector-aggregate-query-v2-execution-owner.md),
  [ADR 0473](0473-bounded-all-tablet-grouped-state-coordinator.md), and
  [ADR 0475](0475-owned-cross-tablet-grouped-vector-authority.md)

## Context

The compatible Fragment-v2 snapshot owned one cross-tablet-proved grouped key and aggregate
authority, workers produced canonical sufficient-state frames, and the grouped coordinator could
retain and merge complete tablet streams. An embedding still had to keep the Manifest epoch pinned,
decode worker bytes under a finite resource authority, correlate every batch with a planned tablet,
and ensure a partially admitted batch could never become a successful query.

## Decision

`DistributedVectorGroupedAggregateQueryExecutionV2` is a move-only, portable, single-threaded
owner. Creation accepts only a nonempty compatible grouped snapshot with nonempty key authority,
empty ungrouped authority, one common query/database/generation/plan identity, unique plan-ordered
tablets, valid decode limits, and a finite decode-memory ceiling. It retains that snapshot and
Manifest pin, one shared decode `QueryResourceContext`, and one all-tablet grouped coordinator built
from copies of the exact retained key and aggregate definitions.

The caller owns worker scheduling and transport and supplies one complete vector of canonical
encoded frames for one planned tablet. The owner exact-decodes every frame against the retained
authority before coordinator admission. It rejects a foreign tablet, malformed bytes, authority
drift, gaps, conflicts, and a batch whose last frame is not terminal. Any failure after a planned
batch begins is retained locally and reported to the coordinator as the worker's terminal failure.
The local sticky failure remains authoritative even when the coordinator had already observed a
terminal frame, so no failed retry or partially admitted prefix can later publish rows. Exact
complete-batch retries retain the coordinator's byte-identical idempotence.

`finish()` remains unavailable until every planned stream closes. Resource exhaustion during the
coordinator's merge stays retryable because the canonical retained frames remain owned. A
successful finish seals input and enables pull-based `next()` output; rows cannot be observed
before that global gate. The owner has no socket, retry clock, thread, callback, or cancellation
policy.

## Alternatives considered

- **Accept decoded messages:** rejected because the execution boundary must parse potentially
  corrupted worker bytes under its own limit and exact authority.
- **Decode directly inside the coordinator only:** rejected because the coordinator deliberately
  accepts typed messages and independently canonicalizes them for retry identity.
- **Let an incomplete batch remain retryable:** rejected because a retained prefix plus later
  transport failure could otherwise be mistaken for a valid tablet contribution.
- **Compose authenticated transport now:** rejected because this increment establishes the
  transport-independent publication and lifetime boundary first.

## Consequences

Creation is `O(tablets log tablets)` and retains one Manifest pin, one tablet index, one decode
resource handle, and the bounded coordinator. Each admitted frame is decoded once at the owner and
canonically re-encoded once by the coordinator; final merge decodes the retained canonical frame
again. This deliberate validation cost keeps independent trust boundaries and bounded ownership.
The owner releases temporary decoded key/state reservations after each admission. One thread owns
all mutation, so no inter-thread memory-ordering argument applies.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): only the checksummed grouped-state format crosses
  the encoded worker boundary.
- [Invariant 6](../architecture/invariants.md): decode memory, frame shape, retained bytes, messages,
  group state, and output remain independently finite.
- [Invariant 10](../architecture/invariants.md): every decode and coordinator uses the exact pinned
  cross-tablet grouped authority.
- [Invariant 11](../architecture/invariants.md): the Manifest pin outlives worker admission, global
  merge, and row output.
- [Invariant 14](../architecture/invariants.md): query, tablet, sequence, group ordinal/count, and
  terminal identity are revalidated before admission.
- [Invariant 18](../architecture/invariants.md): one owner controls snapshot, resource, coordinator,
  failure, and output lifetimes.

## Validation

A two-tablet compatible grouped snapshot proves pinned authority, foreign-tablet rejection,
unavailable partial finish, byte-identical whole-batch retry, global equal-key COUNT merge, sealed
input, and sticky end-of-stream. Companion cases prove an incomplete batch becomes a sticky
failure, later worker errors cannot replace the first failure, row-mode authority is rejected, and
zero decode memory is invalid. The normal cluster build and all 224 cluster tests pass, including
real loopback/mTLS cases. Both focused cases pass under ASan/UBSan; the public header compiles by
itself, formatting and whitespace checks pass. Direct LLVM 18 clang-tidy could not complete because
that tool cannot parse the installed macOS 26 libc++ headers (`__builtin_clzg`); after fixing its
two reported local findings, the rerun reported only that external compiler/header mismatch.

## Migration and rollback

No durable or wire bytes change. Rollback removes this portable owner, but later scheduling must
restore an equivalent pinned all-or-none boundary rather than assembling snapshot, authority,
decoder, and coordinator lifetimes independently.

## Unresolved questions

- Authenticated grouped request/response transport, finite retries, cancellation, and TCP
  scheduling around this owner.
- Computed pre-group physical-plan splitting and final grouped Native SQL integration.
- Partitioned shuffle routing and multi-process fault qualification.

## References

- [Canonical multi-key grouped sufficient-state exchange](0470-canonical-multi-key-grouped-sufficient-state-exchange.md)
- [Bounded all-tablet grouped-state coordinator](0473-bounded-all-tablet-grouped-state-coordinator.md)
- [Owned cross-tablet grouped vector authority](0475-owned-cross-tablet-grouped-vector-authority.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
