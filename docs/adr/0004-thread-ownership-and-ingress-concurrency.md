# ADR 0004: Thread Ownership and Ingress Concurrency

- **Status:** accepted
- **Date:** 2026-08-01
- **Owners:** ChronosDB runtime and storage maintainers

## Context

Network connections and mutable tablet state have different concurrency needs. Reactors must progress many sockets without blocking, while tablet mutation needs a clear serialization point for validation, log order, head publication, and live operators. A design centered on shared mutable structures or one globally contended queue would obscure ownership and make overload behavior depend on scheduler timing.

## Accepted decision

ChronosDB uses a small set of event-loop reactor threads for connections and a set of shard workers that partition mutable storage ownership. A mutable tablet has exactly one owning shard worker at a time. Ownership transfer, when introduced, is a control-plane protocol with a quiescent boundary; two workers never concurrently own mutation rights.

A reactor decodes and validates frame-level structure, resolves routing metadata, and submits an immutable or reference-counted write batch to the owning shard worker. It does not mutate tablet state. Each relevant reactor-to-shard relationship uses a bounded single-producer/single-consumer queue; one globally contended MPSC queue is not the default architecture.

The producer owns an unpublished queue slot and its batch reference until publication. The consumer gains access only after observing publication and becomes responsible for releasing or transferring the batch reference after processing. Batch storage must outlive both queue transit and shard processing; stack-backed or reactor-reused bytes cannot be published without an owning lifetime object.

Queue-full behavior is explicit and bounded. Depending on the future protocol/admission specification, a reactor may stop reading that connection, defer routing within a bounded per-connection budget, or reject the request with an overload result. It may not allocate an unbounded side queue or block the event loop indefinitely.

Shutdown first stops admission, then prevents new publication, drains or deterministically rejects published work, and finally destroys queues only after both endpoints have quiesced. Tablet ownership and acknowledgment state determine which accepted operations must complete or recover.

For a conventional ring queue, producer initialization followed by a release publication and consumer acquire observation is likely sufficient to make a completed slot visible; the consumer can similarly release reclaimed capacity to the producer. This is architectural guidance, not a blanket ordering prescription. The final algorithm must justify every atomic access, wraparound rule, lifetime transition, and progress claim against its concrete representation.

Cold control-plane paths may use locks. A structure may be called lock-free only when every operation covered by the claimed progress model satisfies that property. Cache-line separation and thread-affine allocation are optional measured optimizations, not design slogans.

## Detailed rationale

Single-writer tablet ownership turns ordering and mutable-head updates into local operations and sharply reduces synchronization surfaces. Per-relationship SPSC queues have explicit endpoints, bounded capacity, and a small memory-order proof. They also isolate a stalled shard or reactor better than one global queue and make overload attributable.

This design does not claim that the database is globally lock-free. Locks are often clearer for schema, placement, and other cold state. Restricting lock-free techniques to measured hot paths reduces reclamation and shutdown complexity.

## Alternatives considered

- **Thread per connection:** simple blocking control flow but poor memory/scheduling behavior at high connection counts and no natural tablet ownership boundary.
- **One global MPSC ingest queue:** easy routing but introduces shared contention, weakens locality, and lets unrelated shards interfere at one choke point.
- **Direct reactor mutation of tablets:** removes a hop but creates multiple writers, couples socket scheduling to storage serialization, and complicates ownership and recovery order.
- **A lock around each mutable tablet:** can be correct, but makes contention and thread affinity unpredictable on the main path; locks remain acceptable for cold paths.
- **Unbounded queues:** hide overload temporarily while converting it into uncontrolled memory growth and tail latency.

## Consequences

- Reactor count, shard count, routing, and queue topology become important configuration and measurement dimensions.
- Cross-shard operations require explicit coordination rather than shared mutation.
- Batch objects need auditable ownership and bounded allocation policies.
- Overload is visible to clients and operations instead of being absorbed indefinitely.
- Shutdown and ownership transfer require state machines and tests, not destructor-order assumptions.

## Affected invariants

This decision owns part of invariants [4, 9, 15, and 16](../architecture/invariants.md): ordered per-tablet application, durable retry identity across handoff, bounded subscriber/ingress influence, and complete row publication. Its implementation must also satisfy invariant 18 when atomics or cache optimizations are changed.

## Validation plan

- Model and property-test queue wraparound, full/empty transitions, and batch lifetime.
- Force queue-full, connection pause/reject, shutdown, and ownership-transfer interleavings under deterministic scheduling.
- Run ThreadSanitizer where feasible and use poisoned/recycled buffers to expose premature lifetime release.
- Document a happens-before and progress argument beside the selected queue implementation.
- Benchmark per-reactor/per-shard throughput, fairness, cache behavior, and tail latency before adding padding, custom allocation, or weaker atomics.

## Deferred decisions

Ring-buffer ABI, capacity selection, counter width/wrap technique, wake-up mechanism, batching policy, exact overload response, ownership-transfer protocol, NUMA placement, and allocator design remain deferred to the relevant specifications and measurements.

## Migration or reversal implications

No ABI exists yet. Queue representation may change behind its interface before release. Replacing single-shard ownership or bounded handoff would affect WAL ordering, head publication, live processing, and testing assumptions and therefore requires a superseding ADR.

## References

- [Architecture write plane](../architecture/overview.md)
- [Roadmap Phase 4](../roadmap.md)
- [Invariant 16 publication rule](../architecture/invariants.md)
