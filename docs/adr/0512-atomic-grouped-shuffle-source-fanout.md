# ADR 0512: Atomic grouped shuffle source fan-out

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB query and distributed-query maintainers
- **Extends:** [ADR 0501](0501-canonical-bounded-grouped-partition-splitting.md),
  [ADR 0507](0507-finite-immutable-route-grouped-shuffle-retry.md),
  [ADR 0511](0511-proof-bound-grouped-shuffle-destination-selection.md)

## Context

The partitioner, authority, local reducer, and remote retry owner existed independently. A packaged
source still had to join them correctly for every partition, distinguish the valid in-process
self-route from remote transport, preserve one shared query resource authority, and avoid exposing
a subset of edges if later partition construction failed.

## Decision

Add one move-only, single-thread-affine source-plan owner. It accepts the immutable complete shuffle
authority, one authorized source tablet, one caller-owned complete canonical grouped stream, a
shared query resource context, and finite partition/retry/outer-byte limits. It copies exact grouped
definitions into the canonical partitioner and privately constructs all partition outputs before
publishing the owner.

Each partition receives the authority-bound edge `(tablet, partition, source node, destination
node, hash version)`. If source and destination nodes match, the plan bypasses the network carrier,
exact-decodes the re-ordinalized nested stream, rechecks query/tablet and canonical hash route, and
owns one `CompleteStream` ready for the local reducer. Otherwise it creates the established finite
whole-stream retry owner, which revalidates the complete remote stream before admission. Every
partition is represented, including empty terminals.

The source plan owns separate local-stream and remote-retry vectors and allows each vector to be
moved out once without allocation. Remote retries borrow the immutable authority, which must
outlive the plan and extracted retries. Local decoded messages own their query memory reservations.
The input span is never retained. Metrics expose local/remote edge counts and exact total nested and
outer encoded extents.

Partition count, input groups, skew, nested bytes, per-stream outer bytes, retry count/backoff, and
total outer bytes are bounded. The total outer default is 512 MiB with a one-GiB hard ceiling. Any
unknown source, authority drift, route error, malformed input, cancellation/resource failure,
overflow, or allocation failure discards the private partial vectors and publishes no plan.

## Detailed rationale

Local edges are valid shuffle edges but invalid network frames. Representing them directly as the
same complete-stream value consumed by the reducer preserves all reducer checks without fabricating
a loopback peer identity. Remote edges retain the byte-identical retry contract and can enter the
existing TCP/mTLS client later. Whole-source atomic construction prevents a scheduler from sending
some partitions before discovering that another cannot be represented within limits.

## Alternatives considered

- **Send self-routes over loopback TLS.** Rejected because the frozen carrier explicitly forbids
  identical source and destination nodes and no peer hop exists to authenticate.
- **Return raw partition vectors.** Rejected because callers could omit edge authority, retry
  validation, or total outer-byte accounting.
- **Publish edges incrementally.** Rejected because failure would require compensating cancellation
  after externally visible partial fan-out.
- **Merge local groups before partitioning.** Rejected because every source must still emit an
  explicit terminal to every reducer.

## Consequences

One complete tablet-local worker stream can now be transformed atomically into every local and
remote source edge required by the proof-bound shuffle authority. The remaining package must obtain
all source streams, deliver local edges, schedule remote retry/TCP clients, drain destination
listeners into reducers, propagate cancellation, and gather disjoint partition results.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): every emitted edge is derived from one immutable
  query/source/destination/hash authority.
- [Invariant 10](../architecture/invariants.md): local and remote paths both exact-decode and
  authority-check complete nested streams before publication.
- [Invariant 11](../architecture/invariants.md): caller input, owned local messages, borrowed
  authority, shared query accounting, and extracted retry lifetimes are explicit.
- [Invariant 15](../architecture/invariants.md): partition, group, skew, nested, stream, retry, and
  total outer byte influence are finite.
- [Invariant 18](../architecture/invariants.md): fan-out uses canonical hash-v1 routing, complete
  empty edges, and unchanged reducer equality.

## Validation plan

A focused two-partition case routes one key locally and one remotely, moves each delivery vector
out exactly once, finishes the local reducer, pulls its exact grouped result, and starts the remote
byte-producing retry attempt. Negative cases reject an unknown source, query drift, total outer
exhaustion, and an invalid hard limit. Allocation injection sweeps partition decode/re-encode,
local decode retention, remote retry validation, both edge vectors, and proves all failed attempts
release query reservations. Header self-containment, warning-as-error ASan/UBSan suites, formatting,
changed-source clang-tidy, and final diff review are required. The warning-as-error ASan/UBSan
build, all 289 cluster tests, and all 50 cluster allocation-failure tests pass. Changed C++ files
pass LLVM 18 formatting. The repository-wide format check reaches one unchanged pre-existing
grouped-query TLS header violation. Changed-source clang-tidy reaches only the known LLVM 18/macOS
26 libc++ builtin incompatibility without a ChronosDB-source finding. Whitespace and scope review
pass.

## Migration or rollback considerations

No durable or wire bytes change. Rollback removes packaged source fan-out while retaining each
lower-level partition, transport, retry, and reducer primitive; no caller may partially re-create
the contract and claim packaged shuffle completion.

## Unresolved questions

- Add bounded all-remote-edge polling, address rotation, cancellation, and completion ownership.
- Feed destination listener completions into every local reducer without losing retryable admission.
- Gather partition outputs for global final projection, ORDER BY, LIMIT, and Native publication.

## References

- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
