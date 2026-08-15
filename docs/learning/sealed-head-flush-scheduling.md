# Sealed-Head Flush Scheduling

## Purpose and boundary

`chronos::ingest::SealedHeadFlushQueue` is the bounded ownership handoff between shard writers and
the single storage worker. It schedules immutable sealed generations; it does not sort rows, encode
CSEG, perform filesystem I/O, build or install a Manifest, publish database visibility, or reclaim
WAL. Those operations belong to the durable flush coordinator.

The safety requirement is simple but strict: queue pressure must be known before a shard changes
topology or admits a new WAL command, and a scheduled head must never disappear because processing
failed. The queue therefore separates capacity reservation, readiness, retry, and authorized
completion.

## Public interfaces and ownership

- `SealedHeadFlushQueue::create` allocates a fixed number of slots once and returns shared queue
  ownership suitable for multiple tablet configurations and one storage consumer.
- `TabletStateConfig::flush_queue` optionally attaches that owner. A nonempty generation rotation
  reserves capacity before creating/sealing the successor topology.
- `try_acquire` returns at most one move-only `SealedHeadFlushWork`. The work owns a copied
  `HeadSnapshot`, which pins the exact immutable publication and underlying column storage.
- `release_for_retry`, or destruction of live work, returns the same queue item to ready state.
  `complete` requires the exact `SealedGenerationRetirementReceipt` issued after aggregate durable
  publication and validates tablet, schema, generation, row count, WAL identity, and sequence
  extrema before releasing capacity.
- `metrics` reports capacity, occupied/reserved/ready/in-flight counts, accepted/completed/retry and
  capacity-rejection counters, and the monotonic age of the oldest ready or in-flight item.

Production queues borrow the process-lifetime `SystemTimeSource`. Deterministic tests inject a
typed source whose lifetime exceeds the queue and every reservation/work object retaining its
shared state. Only the monotonic domain is consulted; wall-clock adjustment cannot make queue age
negative or expire durable authority.

Public accessors on a moved, completed, or released work object remain safe: `is_valid` is false and
`snapshot` returns null. Queue work and metrics do not borrow the `SealedHeadFlushQueue` wrapper;
their internal shared state remains alive independently.

## Data structures and invariants

Creation allocates a vector of exactly `capacity` slots. A slot is one of `free`, `reserved`,
`ready`, or `in-flight` and carries a non-reused 64-bit reservation sequence. Reservation scans for
a free slot under one mutex, marks it reserved, and increments occupied capacity. It performs no
tablet mutation.

After fallible rotation preparation succeeds, the shard seals the old head and stages its owning
snapshot while the slot remains invisible to the consumer. It then release-publishes the complete
outer tablet topology and changes the reservation to ready under the queue mutex. If the operation
returns earlier, the move-only reservation destructor frees the slot. Thus a consumer cannot
observe a head before the tablet epoch that names it, and an ordinary pre-publication failure cannot
leak capacity.

The consumer selects the lowest occupied sequence. A ready later sequence cannot pass an earlier
reserved sequence. Only one item may be in flight, matching the accepted single storage-worker
ownership model. Retry changes only `in-flight -> ready`; it does not allocate, reorder, or reset
the enqueue timestamp. Completion changes `in-flight -> free` only after exact receipt validation.

## Synchronization argument

Producers and the consumer serialize slot state, counters, snapshots, and timestamps with one
mutex. The shard's outer tablet release store is sequenced before the mutex-protected ready
transition. A consumer that later locks the mutex observes the staged snapshot and cannot acquire
the item before that transition. `HeadSnapshot` itself retains the mutable-head publication and
storage through shared ownership.

The mutex-based implementation is deliberately not claimed to be lock-free. It makes the MPSC
ordering and lifetime proof direct, while all blocking encoding and I/O remain outside the shard
critical path. Replacing it requires profile/benchmark evidence and an equally explicit reclamation
argument.

## Failure behavior and complexity

Zero capacity is invalid. A full queue or exhausted reservation sequence reports
`ResourceExhausted`; a full queue increments the rejection counter. Expected failures after a
reservation destroy it and restore capacity. A hostile completion receipt reports
`InvalidArgument` and leaves the work valid and in flight, so it can retry. Destructor retry is
non-allocating and cannot drop the pin.

With configured capacity `Q`, creation is `O(Q)` memory/time. Reservation, acquisition, completion,
and metrics are `O(Q)` scans; staging, readiness, and retry are `O(1)`. Queue memory is `O(Q)` and no
per-row allocation occurs. This correctness-first fixed-slot design avoids an allocation after a
reservation has authorized topology change.

## Evidence and measurement

Tests cover zero capacity, later-producer non-overtaking, deterministic fake-clock age, retry age
preservation, hostile completion receipts, exact TabletState handoff, pre-WAL full-queue rejection,
capacity restoration after a post-reservation preparation failure, and concurrent producers with
exactly-once pins. Header self-containment, installed external-consumer signatures, ASan/UBSan, and
TSan cover the public and concurrency boundaries.

`chronos_ingest_benchmarks` measures complete topology rotation plus queue handoff/acquire/retry at
64, 1,024, and 65,536 rows, and isolates repeated single-consumer acquire/retry overhead. Fixture
creation and first-head materialization occur outside timed loops. Results are microbenchmarks, not
production latency claims.

## Likely review questions

- Why reserve before sealing? A full queue must reject before WAL or topology mutation; checking
  and enqueueing later would race among producers or strand an unscheduled sealed head.
- Why can a ready item wait behind a reservation? Reservation sequence defines deterministic FIFO
  ownership. Overtaking could reorder same-shard generations when producers interleave.
- Why does work destruction retry? RAII makes every early return preserve scheduling ownership.
- Why require a retirement receipt to complete? It prevents an encoder or I/O failure from freeing
  the only queue slot that schedules a still-visible sealed generation.
- What remains? The durable coordinator and integrated filesystem crash matrix now compose encoding,
  installation, publication, retirement, and completion. Persistent catalog/service activation and
  reviewed device campaigns remain outside this scheduling primitive.
