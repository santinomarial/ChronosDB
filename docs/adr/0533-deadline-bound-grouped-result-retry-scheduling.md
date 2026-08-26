# ADR 0533: Deadline-bound grouped result retry scheduling

- **Status:** accepted
- **Date:** 2026-08-26
- **Owners:** ChronosDB distributed-query and networking maintainers
- **Extends:** [ADR 0529](0529-deadline-bound-grouped-shuffle-result-tcp-client.md)

## Context

One result TCP client owns exactly one connection attempt, while the immutable retry owner decides
whether another whole attempt is permitted. Without a scheduler between them, reducer processes
cannot rotate coordinator addresses, honor retry backoff, or apply one query deadline without
embedding policy in an ad hoc event loop.

## Decision

Add a move-only, single-thread-affine grouped-result TCP execution. Creation borrows the exact
shuffle authority and raw result schema, consumes a finite nonempty set of prepared result retries,
prevalidates all coordinator routes and TLS contexts, and preallocates one client slot, poll record,
and poll-to-slot index per partition.

Every retry must borrow the exact supplied authority and schema objects, name a unique authority
partition, claim its authority-derived reducer node, target the same nonzero coordinator, and have
one configured coordinator route. An attempt rotates deterministically through that route's finite
IPv4 address list. Only a fully authenticated correlated result receipt marks the retry successful;
connect, TLS, authentication, I/O, or receipt failure returns to the existing finite retry policy.

Polling starts due attempts, bounds its wait by active carrier deadlines, retry wakeups, the caller
limit, and the optional execution deadline, and advances each ready client at most once. Permanent
or exhausted partition failure fails the whole owner and closes every active client. Explicit
cancellation and deadline expiry do the same without opening further attempts. Metrics expose
attempts, retries, completed and failed transports, active attempts, and succeeded/total
partitions.

The owner runs inside one reducer process and does not own the coordinator listener, collector, or
global finalizer. Those inbound responsibilities remain a separate lifecycle boundary.

## Consequences

Reduced partition bytes now have one finite route from immutable retry policy through real TCP and
mutual TLS to receipt-proven coordinator acceptance. Lost receipts resend the exact stream and rely
on coordinator deduplication; no partial write offset is resumed.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): exact authority, schema, reducer, partition, and
  coordinator identities are rechecked before scheduling.
- [Invariant 11](../architecture/invariants.md): each slot owns at most one client and its descriptor
  while all proof and security dependencies are explicitly borrowed.
- [Invariant 15](../architecture/invariants.md): partition count, routes, addresses, attempts,
  backoff, carrier deadlines, query deadline, descriptors, and poll work are finite.
- [Invariant 18](../architecture/invariants.md): address rotation reconstructs whole immutable
  streams and receipt validation alone reports success.

## Validation plan

Route one reducer result first to a refused address and then to a live coordinator server; require
one retry, mutual authentication, retained complete result, and receipt-gated success. Reject empty
coverage and copied authority or schema objects, cancel before opening a descriptor, and sweep
allocation failure through retry reconstruction, route validation, and scheduler preallocation.
Run cluster, allocation-failure, sanitizer, formatting, static-analysis, and diff gates.

## Migration or rollback considerations

No durable or wire format changes. Rollback requires reducer embeddings to fail after one TCP
attempt or retain their own equivalent finite scheduling policy; unbounded or partial-offset retry
is not allowed.

## Unresolved questions

- Compose coordinator server progress, idempotent collection, materialization, cancellation, and
  atomic Native finalization under one lifecycle.
- Qualify multiple reducer processes returning distinct partitions to one coordinator process.

## References

- [Result retry decision](0528-finite-immutable-grouped-shuffle-result-retry.md)
- [Result TCP client decision](0529-deadline-bound-grouped-shuffle-result-tcp-client.md)
- [Accounted result finalization](0532-accounted-independent-grouped-result-finalization.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
