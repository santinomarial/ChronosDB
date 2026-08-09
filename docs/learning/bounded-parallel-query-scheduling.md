# Bounded Parallel Query Scheduling

## Purpose and public interface

`ParallelMergeOperator` runs independent, unordered physical pipelines concurrently and merges
their already-accounted chunks through a bounded queue. `create` receives the query resource
context, complete task owners, explicit maxima for tasks, workers, ready chunks, and retained
configuration bytes, and optionally one exact placement per selected worker. `next` keeps the normal
physical pull contract. Metrics report tasks started and completed, chunks published, and peak queue
occupancy.

`QuerySharedMemoryReservation` complements the scheduler. Unlike move-only chunk credit, it names
one immutable shared allocation/lifetime. Copying the token changes no counters; the last token
returns the charge.

## Data structures and invariants

The shared scheduler state owns the task vector, a fixed vector of optional chunk slots, one shared
configuration reservation, mutex/conditions, counters, and the selected terminal error. The root
operator owns bounded `std::thread` handles. There is no detached work.

Each worker claims the next task ordinal under the mutex and drains that entire operator on the same
thread. Producers wait when the ring is full. The consumer waits when it is empty. Installing a
complete chunk before unlocking and observing it after locking creates the required happens-before
edge; relaxed query accounting atomics do not publish payloads.

Before any claim, every worker applies its own optional CPU/NUMA placement and reports through a
two-stage startup barrier. Registration must be closed and every registered worker must report
success before the first pipeline pull. The creator observes that same mutex-protected boundary
before returning. A placement failure stops and joins all workers without cancelling the query;
concurrent failures select the lowest worker ordinal.

The merge order is intentionally unspecified. A downstream full sort may establish SQL order, but
the scheduler itself cannot sit where arrival order would become `ORDER BY`, LATEST, ASOF, LIMIT,
or another order-sensitive semantic decision.

## Ownership, cancellation, and failure

The configuration credit conservatively covers scheduler/task/queue/worker storage and allocation
overhead. Every queued chunk keeps its own move-only reservation. Clearing a queue destroys those
owners immediately.

The first failure stops new claims, but all already-running workers may report. Arbitration prefers
a semantic failure over cancellation and then the smallest task ordinal. The consumer waits for
all workers before returning that stable error. Cancellation wakes both empty and full waits.
Destroying an unfinished merge requests cancellation and joins; destroying a completely drained
merge merely releases ownership.

Allocation and length failures during construction or worker pulls become resource exhaustion.
Operating-system worker creation failure becomes unavailable. A placement count mismatch is
invalid; an unsupported or failed OS placement is returned directly from creation before any task
runs. A foreign resource context is an invalid argument and cancels the owned work before returning.

## Complexity and tradeoffs

For `T` tasks, `W` workers, queue capacity `Q`, and `C` emitted chunks, setup storage is
`O(T + W + Q)` and each publish/consume transition is `O(1)` under one mutex. Total operator work is
unchanged; elapsed time can improve only when task work is independent and large enough to amortize
thread creation and synchronization.

One mutex gives a short, auditable ownership proof and bounded backpressure. Per-instance threads
avoid inventing a premature global pool but are too expensive to select blindly. Skew can leave one
worker draining the final task; work stealing inside a pipeline would break thread affinity.

## Verification and benchmark methodology

Deterministic tests gate two task starts, force queue capacity one, and compare sorted output with a
serial multiset. They record thread affinity, coordinate racing errors, exercise foreign contexts
and hostile limits, and destroy a producer blocked on a full queue. Allocation injection sweeps all
caller-owned construction allocations; an explicit throwing source covers worker exception
classification. Placement tests cover count mismatch, unsupported NUMA, no-pull failure gating,
credit release, and explicit empty-placement success. The fuzzer varies null tasks, all finite
limits, resource admission, cancellation, and worker failures.

`merge_independent_chunks` measures scheduler creation, bounded publication, root consumption, and
join for one and four tasks across one, two, and four workers. Setup chunks are built while timing is
paused. Results describe scheduler overhead in the current debug/release configuration; they are
not a scalability claim. The bounded physical selector separately measures its selection and
requires an explicit complete-pipeline order-independence proof before choosing this scheduler.

## Likely review questions

**Why copy shared credit but not chunk credit?** One scheduler configuration is genuinely shared;
each chunk is a distinct movable owner whose release obligation must remain singular.

**Why wait for every worker before returning an error?** Returning early would leave concurrent
owners running behind a terminal API result and would make later error arrival affect cleanup.

**Why is output unordered?** Worker completion is schedule-dependent. SQL ordering is established
only by explicit physical keys and a full ordering operator.

**Why not use the query atomics for publication?** They track bytes and cancellation only. They do
not cover the chunk object or its backing writes; the queue mutex does.
