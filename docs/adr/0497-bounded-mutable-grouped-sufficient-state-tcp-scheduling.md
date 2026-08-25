# ADR 0497: Bounded mutable grouped sufficient-state TCP scheduling

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB query, cluster, and networking maintainers
- **Extends:** [ADR 0483](0483-pinned-grouped-sufficient-state-tcp-scheduling.md),
  [ADR 0493](0493-deadline-bound-mutable-grouped-sufficient-state-tcp-client.md), and
  [ADR 0496](0496-portable-mutable-grouped-sufficient-state-execution.md)

## Context

The mutable grouped path owned exact applied-head fragments, finite senders, complete grouped
authority, a global sufficient-state coordinator, and one deadline-bound TCP attempt. No owner
joined those pieces across all mutable tablets. Production callers would otherwise have to invent
route completeness, retry/address rotation, polling, deadline arbitration, cancellation, terminal
failure publication, and all-or-nothing output policy.

## Decision

`DistributedMutableVectorGroupedAggregateQueryTcpExecution` owns the portable execution and one
stable slot per tablet. Each slot has one immutable target route and at most one active mutable
grouped TCP client. Construction validates authentication dependencies, every nested carrier bound,
grouped authority width, a finite unique route table, nonzero unique endpoints, and complete target
coverage before opening a descriptor.

One thread calls `poll_once`. The scheduler starts only ready or due-backoff attempts, selects an
endpoint by attempt number modulo the target's ordered finite address list, and maintains
preallocated descriptor and slot-index arrays. Its wait is capped by the caller, whole-query
deadline, and due retry. Retryable connection or carrier failures discard the entire attempt and
return policy to the portable sender; local exhaustion and terminal sender failure poison the whole
execution. Advisory leader hints remain observable but never rewrite immutable target authority.

Only a completed client's canonical response vector enters the portable execution. The scheduler
becomes complete and enables physical output only after every sender succeeds and the global
coordinator seals. Explicit cancellation, deadline expiry, or failure destroys every active client
and resets the active-attempt metric. A finite explicit rebind is permitted only after a retryable
terminal failure and only when logical query identity, grouped key/aggregate authority, deadline,
and rebinding budget remain unchanged; cumulative transport metrics survive replacement.

This owner does not encode Native result batches, split computed pre-group programs, or route a
partitioned shuffle.

## Consequences

Mutable grouped sufficient-state execution now has bounded all-tablet TCP scheduling without
inventing a Manifest generation. Exact mutable publication proofs, key/state authority, immutable
target identity, shared decode accounting, and complete-attempt retry semantics survive every
connection. Output remains unavailable until global terminal closure.

Memory is bounded by finite fragments, routes, addresses, senders, carrier frames/bytes, decode
credit, coordinator retention, and preallocated poll arrays. One thread serializes all calls; no
inter-thread synchronization or memory-ordering algorithm is introduced.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): scheduling does not expose uncommitted or partially
  applied mutable state and changes no durable bytes.
- [Invariant 6](../architecture/invariants.md): exact applied positions, schema, placement, group,
  and barrier authority remain fixed across attempts.
- [Invariant 10](../architecture/invariants.md): every response is decoded under the complete
  grouped key/state authority before coordinator admission.
- [Invariant 11](../architecture/invariants.md): attempt and scheduler destruction release carrier,
  query-resource, and descriptor ownership in dependency order.
- [Invariant 14](../architecture/invariants.md): existing versioned `CHDMREQ1` and `CHDVGRP2` bytes
  retain exact route, query, tablet, and authority correlation.
- [Invariant 15](../architecture/invariants.md): mutual authentication and target authorization
  precede application request write.
- [Invariant 18](../architecture/invariants.md): retry does not weaken proof, bounds, or atomic
  publication guarantees.

## Validation

The loopback integration case starts two bounded mutable grouped servers, injects a refused first
endpoint, rotates only that tablet to its second endpoint, completes mutual TLS, binds and executes
each worker once, and exposes one globally merged group only after both terminal streams close.
Metrics prove three attempts, one retry, two completed transports, one failed transport, and zero
active attempts. Negative coverage rejects incomplete routes and an invalid nested state bound
before I/O, expires before any attempt, and cancels active clients. Deterministic allocation
injection walks route-table, poll-array, and owner construction until success.

The focused two scheduler tests and one allocation sweep pass normally and under ASan/UBSan with
leak detection disabled. The complete cluster suite passes 263 tests and the complete cluster
allocation-failure suite passes 38 tests. Changed-file LLVM 18 formatting, the warning-as-error
build, and whitespace validation pass. Static analysis is blocked by the installed LLVM 18/macOS
26 libc++ mismatch (`__builtin_clzg`, `__builtin_ctzg`, and related substitutions); the one
project-local compiler warning observed before that failure was corrected.

## Migration and rollback

This change is additive and changes no durable or wire format. Rollback removes the mutable grouped
TCP execution owner while retaining the portable execution, one-attempt client, and bounded server.
Callers must not substitute the Manifest-pinned grouped scheduler or publish a response prefix.

## Unresolved questions

- Atomic mutable grouped Native finalization and scheduler publication.
- Computed pre-group physical-plan splitting.
- Partitioned shuffle routing, skew policy, and broader process/fault/measurement evidence.

## References

- [Pinned grouped sufficient-state TCP scheduling](0483-pinned-grouped-sufficient-state-tcp-scheduling.md)
- [Deadline-bound mutable grouped sufficient-state TCP client](0493-deadline-bound-mutable-grouped-sufficient-state-tcp-client.md)
- [Portable mutable grouped sufficient-state execution](0496-portable-mutable-grouped-sufficient-state-execution.md)
