# ADR 0063: Bounded Reactor-to-Shard SPSC Routing

- **Status:** accepted
- **Date:** 2026-08-08
- **Owners:** ChronosDB networking and shard-runtime maintainers

## Context

ADR 0009 forbids socket progress from mutating tablet state. A reactor therefore needs a finite
ownership handoff to exactly one shard consumer, with a memory-order argument proving that the
consumer sees a fully initialized request frame.

## Accepted decision

Each reactor/shard pair uses a fixed-capacity SPSC ring. A task owns its connection ID and decoded
frame. Creation allocates `capacity + 1` optional cells; one remains empty to distinguish full from
empty. Capacity is nonzero and capped at 1,048,576.

Only the producer writes `producer_index` and a free cell. It loads its own index relaxed, acquires
`consumer_index` before cell reuse, initializes the task, then release-stores the next producer
index. Only the consumer writes `consumer_index`; it loads its own index relaxed, acquires
`producer_index`, moves and destroys the task, then release-stores the next consumer index. These
pairs establish publication and safe reuse. Indices occupy separate cache lines.

Full admission returns `false` immediately. There is no side queue, wait, overwrite, or allocation
after construction. Moving is allowed only before publication; destruction requires both owners
to be joined.

## Detailed rationale

SPSC matches the accepted reactor/shard topology and admits a short complete proof. MPMC would add
contention and a larger correctness surface without a current consumer.

## Alternatives considered

- A mutex queue adds blocking and still needs a bound.
- MPSC/MPMC routing is unnecessary for the fixed pair ownership.
- Relaxed-only indices do not publish initialization or protect reuse.
- Overwriting oldest work makes overload silent.

## Consequences

Saturation becomes an explicit overload/connection decision. A connection routed to another shard
uses another queue pair; occupied cells never migrate.

## Affected invariants

This supports invariants 4, 11, 15, and 17 through single-shard ownership, complete lifetime,
finite influence, and explicit admission.

## Validation plan

FIFO/wrap/full/empty tests, 100,000-task concurrent publication, saturation, allocation failure,
installed consumption, TSan, and reactor/shard stall tests.

## Deferred decisions

Shard selection, wakeup descriptors, response routing, and overload/disconnect policy remain later
Phase 10 increments.

## Migration or reversal implications

Changing producer/consumer cardinality or memory orders requires a superseding ADR and race proof.

## References

- [ADR 0004](0004-thread-ownership-and-ingress-concurrency.md)
- [ADR 0009](0009-network-reactor-strategy.md)
- [Architecture invariants](../architecture/invariants.md)
