# ADR 0478: Authenticated complete grouped-state attempts

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB query, cluster, protocol, and security maintainers
- **Extends:** [ADR 0390](0390-authenticated-vector-aggregate-v2-receiver-and-finite-sender.md),
  [ADR 0476](0476-portable-pinned-grouped-sufficient-state-execution-owner.md), and
  [ADR 0477](0477-distinct-grouped-sufficient-state-response-v2.md)

## Context

The grouped-state response codec could preserve one canonical sufficient-state frame, but it did
not decide who authenticated a request, rebound current local authority, rejected authority drift,
or withheld a partial worker stream. The portable multi-tablet execution owner likewise needed a
finite one-tablet sender that could turn authenticated response objects back into canonical nested
frames without retaining a prefix across failures or retries.

Grouped responses differ from ungrouped aggregate responses: the number of groups is data-dependent,
an empty tablet still emits one distinct terminal, and every nonempty frame repeats a declared total
group count. Frame count, total bytes, nested decode limits, and decoded variable-key memory therefore
need independent bounds.

## Decision

Add a single-thread-affine authenticated receiver around `CHDVREQ2` and `CHDVGRP2`. Peer
authentication and principal-to-source authorization precede route, plan, binding, or execution.
The embedding binds fresh proof-derived grouped authority separately from worker execution. The
receiver validates that authority against the exact Fragment-v2 grouped plan and result schema and
requires the executed authority to remain identical.

The receiver exact-decodes the entire worker result under request-local query memory and validates
one empty terminal or a contiguous nonempty stream with exact group count, ordinal, sequence,
terminal, query, and tablet fields. It constructs every outer response under independent frame and
total-byte limits before returning the vector. Execution failures become one correlated failure;
only `UNAVAILABLE` may carry a fresh advisory leader hint. Authentication, route, binding, decode,
and worker-contract errors are not converted into successful publications.

Add a finite sender that owns one immutable encoded request, complete grouped authority, query
resource context, and bounded exponential retry policy. Each attempt copies the same bytes and
targets the admitted serving node. Failure statuses and transport errors may schedule a whole
attempt retry; leader hints never alter authority. A success vector is route-correlated, shape-
validated, canonically outer-encoded and exact-decoded, and re-encoded as nested grouped frames
before atomic publication. Any malformed, partial, oversized, or allocation-failed vector leaves no
retained prefix.

## Consequences

Authentication and fresh local proof binding occur before execution. The portable execution owner
can consume the sender's published nested frames without trusting transport-created typed objects.
Receiver work is linear in the complete worker stream; sender reconstruction is linear in the
complete response vector and temporarily decodes variable keys and extrema under query accounting.
Retries can repeat work but never combine frames from different attempts.

The receiver and sender are serialized by one caller. They contain no shared concurrent algorithm,
so no inter-thread memory-ordering argument applies. They still do not own TLS, sockets, route
rotation, deadlines, cancellation, multi-tablet scheduling, or process integration.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): response frame count, total bytes, nested limits,
  and decoded query memory are independently bounded.
- [Invariant 10](../architecture/invariants.md): current proof-derived grouped authority is bound
  before execution and required again during canonical reconstruction.
- [Invariant 14](../architecture/invariants.md): authenticated source, target, query, tablet, group
  count, ordinal, sequence, terminal, and retry attempt correlation are exact.
- [Invariant 18](../architecture/invariants.md): request bytes, authority, decoded reservations,
  attempt state, and published response vectors have explicit owners.

## Validation

Focused functional coverage proves authentication before binding, source authorization, target and
mode rejection, exact authority validation, successful complete-stream publication, leader-hinted
correlated failure, authority drift and missing-terminal rejection without prefixes, byte bounds,
exception containment, canonical sender reconstruction, incomplete and wrong-sequence rejection,
immutable attempt replay, advisory hints, finite backoff, and terminal states. Allocation-injection
sweeps cover receiver binding/execution/publication and sender reconstruction while proving
temporary decoded key credit returns to zero. All 11 focused grouped transport/execution cases and
all three focused allocation cases pass under ASan/UBSan. The complete cluster suite passes 233 of
233, and the complete cluster allocation-failure suite passes 31 of 31. Header self-containment,
formatting, and whitespace checks pass. LLVM 18 static analysis remains blocked by incompatibility
with the installed macOS 26 libc++ headers; after its two project-local findings were corrected, its
source pass reported no further project-local finding before those compiler errors.

## Migration and rollback

This adds policy owners around existing wire formats and does not change either magic or byte
layout. Rollback disables grouped remote receiver/sender use while leaving the carrier and portable
in-process execution available. A deployment must not substitute the ungrouped receiver or sender.

## Unresolved questions

- Mutual-TLS attempt/session and nonblocking TCP ownership.
- Deadline, cancellation, multi-address route, and all-tablet scheduling composition.
- Native SQL and multi-process compatibility qualification.

## References

- [Distributed Vector Grouped Aggregate Query Transport v2](../formats/distributed-vector-grouped-aggregate-query-transport-v2.md)
- [Distributed Vector Grouped Aggregate Exchange v1](../formats/distributed-vector-grouped-aggregate-exchange-v1.md)
- [Distributed Vector Fragment v2](../formats/distributed-vector-fragment-v2.md)
