# WAL Commit Coordination

> **Status: implemented.** This document explains the bounded in-process coordinator above the
> segmented `WalWriter`. The durable byte layout remains authoritative in
> [WAL v1](../formats/wal-v1.md), and acknowledgment boundaries remain authoritative in the
> [WAL recovery architecture](../architecture/wal-recovery.md) and
> [consistency contract](../product/consistency-and-durability.md).

## Purpose and public interface

`WalWriter` deliberately has no internal synchronization. `WalCommitCoordinator` adds the smallest
shared-thread boundary needed to accept application entries from concurrent producers without
creating multiple physical writers.

`WalCommitCoordinator::start` consumes an already open writer and starts exactly one worker.
`try_submit(payload, mode)` copies the payload into coordinator ownership or immediately rejects it
when count/byte capacity is full. It returns an owning `WalCommitCompletion`; `wait()` returns either
the retained failure or a `WalCommitResult` containing admission sequence, requested/effective
mode, append positions, and synchronization position/record-sequence frontiers only for
`LOCAL_SYNC`.

Only `ASYNC` and `LOCAL_SYNC` are representable. `QUORUM_SYNC` remains outside this API until
replication has an accepted persistence and commit contract. No request is silently downgraded.

## Ownership and lifetime

The coordinator takes exclusive ownership of the writer. From worker start until shutdown, only
that worker calls append, synchronize, position observation, or close. Producer payload bytes need
remain alive only through `try_submit`, which copies them before returning success. The copy is
released after its append finishes; the completion state remains independently reference-counted
and may outlive the coordinator.

Each accepted request remains charged against both admission bounds until success or failure. The
charge includes FIFO residence, the current append, and time waiting in a local-sync group. This is
stricter than bounding only queue nodes: removing a node for work cannot create unaccounted memory
or an unbounded set of synchronization waiters.

## Ordering and happens-before argument

One mutex protects FIFO insertion, admission-sequence assignment, lifecycle flags, accounting, and
metrics. Successful insertions are totally ordered by their critical sections. The worker removes
the FIFO head under the same mutex and invokes `WalWriter` serially, so physical append order equals
the admission order of valid records. A request rejected before append consumes no WAL sequence;
a nonterminal validation error affects only that request.

The worker stores a result while holding the completion state's mutex and then notifies its
condition variable. `wait()` observes readiness under that same mutex. Mutex unlock/lock therefore
establishes the happens-before edge from completed file operation and result initialization to the
waiting producer. Metrics use the coordinator mutex and are returned as one internally consistent
snapshot. No field is advertised as lock-free.

The initial worker gate used by tests is private. It allows requests and injected outcomes to be
installed before the worker runs, making concurrency and batching assertions independent of host
scheduler timing.

## Admission and backpressure

Configuration bounds unfinished request count and exact encoded WAL bytes. Exact bytes come from
the checked WAL v1 layout calculation, including header, padding, and trailer. Admission rejects:

- a full request or byte budget with `RESOURCE_EXHAUSTED`;
- a physical length outside WAL v1 limits;
- a `LOCAL_SYNC` record that cannot fit the configured sync-batch byte ceiling;
- an unknown durability value; and
- requests after shutdown or terminal failure.

Admission is intentionally nonblocking. A caller may apply its own bounded retry or upstream
backpressure policy, but the coordinator never creates a side queue. The cumulative metrics are
unsigned saturating counters; current pending values and high-water values remain exact within the
configured `size_t` limits.

## Mixed durability and batching

An `ASYNC` completion is published immediately after the writer reports the complete record write.
It contains no synchronization position and makes no crash-survival claim.

After the first `LOCAL_SYNC` append, the worker opens a sync window. It continues with FIFO records
until the earliest of:

- the maximum physical request count;
- the maximum exact encoded bytes;
- the maximum delay measured from completion of the first local append; or
- draining shutdown.

Every physical record appended while the window is open counts toward count/byte limits. An
intervening `ASYNC` request still completes after write and does not wait for the local group. A
`LOCAL_SYNC` completion remains pending until `WalWriter::synchronize()` succeeds with a durable
record-sequence frontier covering it.

Rotation is a special but already proved boundary: `WalWriter` synchronizes the prior file before
installing the successor. After a rotating append returns, the coordinator releases prior-segment
local waiters covered by the writer's durable record sequence and starts any successor group anew.
The reported physical synchronization position can therefore be the installed successor header;
callers never compare offsets from different files.

## Failure and shutdown

Validation errors that occur before writer I/O complete only the affected request with an error and
do not poison the coordinator. A hard append, rotation, or synchronization failure retains the
writer's first failure, closes admission, and fails every accepted request not already covered.
Previously completed `ASYNC` requests remain successful. If rotation synchronized older local
records before a later append failed, those covered records complete successfully before the
terminal failure is propagated.

`shutdown()` is idempotent. It closes admission, drains accepted FIFO work, forces a partial final
local group to synchronize, closes the writer, and joins the worker. The destructor performs the
same drain best-effort, but callers use explicit shutdown when close errors matter. Completion
objects remain usable after the coordinator has joined and been destroyed.

## Metrics

`WalCommitMetrics` reports admitted/rejected/appended requests and bytes, acknowledged requests and
encoded bytes by mode, failed requests and encoded bytes, exact current and high-water admission
usage, explicit coordinator sync attempts and outcomes, and local batches released by durable
frontiers. Rotation sync is not mislabeled as an explicit coordinator `synchronize()` call, but any
local requests it releases form a reported local-sync batch.

Metrics are process-local observability, not durable recovery evidence. Filesystem-sync latency
histograms, oldest-wait age, crash reconciliation, and server export remain higher-level work.

## Complexity and tradeoffs

Admission copies `O(payload)` bytes and performs constant expected queue work while holding one
mutex. The worker performs `O(record length)` codec/write work and serial filesystem operations.
Memory is bounded by configured unfinished encoded bytes plus bounded request/completion metadata.

A mutex and condition variable were selected over a lock-free MPSC structure because this boundary
needs blocking wakeup, exact multi-field capacity accounting, lifecycle coordination, and no
measured evidence justifies a more fragile algorithm. This focused queue does not replace ADR
0004's future reactor-to-shard SPSC topology; it serializes only requests that already target one
physical WAL owner.

## Validation and remaining risks

Deterministic tests cover count/byte backpressure, concurrent producers, admission/physical order,
mixed-mode completion while sync is blocked, count/byte/delay triggers, rotation frontiers,
nonterminal validation, append and sync failure, graceful drain, and metric conservation. Public
header compilation and ThreadSanitizer are part of the validation matrix.

The subprocess crash harness now reconciles parent-received completion events with records recovered
after `SIGKILL`, including grouped `LOCAL_SYNC`, rotation, complete writes, and the synchronized but
not yet published window. This is process-termination evidence, not power-loss persistence.
Qualified Linux filesystem/device testing remains necessary for the full `LOCAL_SYNC` failure
envelope. The coordinator also does not define application mutation
kinds, idempotency bodies, server response delivery, checkpoint reclamation, or replication.

## Likely interview questions

- Why must admission bytes remain charged after the worker pops a request?
- What linearizes physical append order across producer threads?
- Why can an `ASYNC` request after a pending `LOCAL_SYNC` request complete first?
- How can rotation acknowledge a prior local group without another sync call?
- Why does the synchronization result use record sequence instead of comparing cross-file offsets?
- Which failures poison the coordinator, and which affect only one request?
- What happens to accepted work during shutdown?
- Why is this mutex queue compatible with the future SPSC ingress architecture?
