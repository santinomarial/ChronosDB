# ADR 0504: Atomic authorized grouped shuffle streams

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB cluster, distributed-query, and security maintainers
- **Extends:** [ADR 0502](0502-complete-node-bound-grouped-shuffle-authority.md) and
  [ADR 0503](0503-authority-bound-grouped-shuffle-frame.md)

## Context

One `CHDVGSF1` frame can prove its route and nested key partition, but a destination reducer must
consume a complete contiguous source-partition stream. Publishing decoded groups one at a time
would let a disconnect, byte limit, allocation failure, duplicate ordinal, or changed edge expose a
prefix. Frame integrity also does not connect the claimed source node to the already authenticated
connection principal. The source needs a symmetric boundary that proves the complete partitioner
output and constructs every outer frame before exposing its first byte.

## Decision

Add single-thread-affine stream sender and receiver owners around `CHDVGSF1`. The sender
exact-decodes an already complete canonical partition stream under caller query resources,
requires one empty terminal or contiguous ordinal/sequence/group-count closure, binds every message
to one exact nonlocal authority edge, and privately constructs every outer write cursor before
exposing bytes. Frame count and total encoded bytes are independently bounded.

The receiver requires an already authenticated nonzero principal before accepting input. After the
first exact frame identifies a source, the borrowed principal authorizer must bind that principal
to the claimed source node; the target must be the configured local node. Every later frame must
retain the identical source tablet/node, destination partition/node, and hash version. The receiver
privately owns decoded query-accounted messages until one canonical terminal closes. Missing
terminal, duplicate or skipped position, count drift, changed edge, byte/count overflow, terminal
suffix, decode failure, and allocation failure are sticky and destroy the complete prefix.

Successful extraction is move-only and exact-once. The result owns the edge, complete decoded
message vector, and accepted outer byte count. The authority, authorizer, and shared query resource
state have explicit lifetimes. One caller serializes each owner, so this decision introduces no
shared concurrent algorithm or memory-ordering obligation.

This is an application stream boundary, not a connected transport. It consumes an authentication
result produced elsewhere and does not perform TLS, authorize the destination certificate at the
sender, return a terminal acknowledgment, persist duplicate state, retry, resolve addresses, or
install data into a reducer.

## Detailed rationale

Validating and encoding the complete source stream before exposing bytes prevents a late malformed
group from producing a sendable prefix. Withholding receiver output until terminal turns socket
failure into discard rather than partial reducer mutation. Locking the edge on the first frame
supports one connection per source-partition stream and makes principal authorization exact without
trusting connection-local address identity.

## Alternatives considered

- **Publish each frame directly to the reducer.** Rejected because later corruption or disconnect
  would require rollback of already merged sufficient state.
- **Authorize only the target node.** Rejected because any authenticated cluster principal could
  then claim another source tablet/node edge.
- **Treat EOF or timeout as stream closure.** Rejected because empty edges and lost terminals would
  become indistinguishable.
- **Combine multiple edges on one stream immediately.** Deferred because independent edge closure,
  backpressure, retry, and acknowledgment semantics need evidence before multiplexing.

## Consequences

Both source and destination now have an all-or-none complete-stream boundary reusable by local
tests and a future TLS carrier. Receiver memory is the fixed/header-first frame reader plus the
complete query-accounted partition stream; sender memory is every complete encoded outer frame.
This favors correctness and retry simplicity over streaming peak-memory efficiency. The hard
whole-stream ceiling is one GiB and deployments may lower both count and byte limits.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): one immutable query edge and complete contiguous
  source stream are retained through terminal extraction.
- [Invariant 10](../architecture/invariants.md): only exact-decoded nested and outer frames can
  enter the retained stream.
- [Invariant 11](../architecture/invariants.md): frame buffering, decoded query credit, unpublished
  prefixes, and extracted results have explicit nonoverlapping owners.
- [Invariant 14](../architecture/invariants.md): the stream composes existing independently
  versioned outer and nested frames without inventing EOF framing.
- [Invariant 15](../architecture/invariants.md): peer identity, source authorization, frame count,
  total bytes, per-frame limits, key payload, group width, and state widths are finite.
- [Invariant 18](../architecture/invariants.md): all-or-none stream construction preserves
  canonical routing, equality, sequence, and terminal rules.

## Validation plan

Focused tests drive every outer frame through seven-byte fragments, verify one authorization call,
prove no preterminal extraction, publish a complete two-group stream, release query credit, and
accept the distinct empty terminal. Negative cases cover an authenticated but unauthorized source,
incomplete sender and receiver streams, duplicate position, coalesced terminal suffix, whole-stream
byte overflow, and forbidden local network edges. Allocation injection sweeps sender construction,
receiver construction, and receive/decode retention while proving no prefix or query-credit leak.
The warning-as-error build, 274 cluster tests, 42 cluster allocation-failure tests, and focused
ASan/UBSan cases pass. Changed-source clang-tidy reaches only the known LLVM 18/macOS 26 libc++
builtin incompatibility after its two project findings were fixed.

## Migration or rollback considerations

No durable or network bytes change. Rollback removes the stream owners while retaining the outer
frame codec, source partitioner, and immutable authority. A future transport must not bypass this
all-or-none boundary when retrying or delivering a remote stream.

## Unresolved questions

- Add mutual-TLS client/server ownership with exact destination-principal authorization.
- Define a correlated terminal acknowledgment and byte-identical finite retry behavior.
- Transfer exact-once complete streams into deterministic partition reducers.

## References

- [Distributed Vector Grouped Aggregate Shuffle Frame v1](../formats/distributed-vector-grouped-aggregate-shuffle-frame-v1.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
