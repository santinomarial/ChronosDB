# Retry Reservation Directory

> **Status: implemented process-local primitive.** The directory implements live, database-wide
> reservation and committed-outcome lookup for `COLUMNAR_APPEND`. The blocking single-tablet
> executor connects it to WAL submission and tablet publication. Retained-lineage recovery rebuilds
> committed outcomes into fresh replacement state; routing/admission and an idempotency-retention
> policy remain unimplemented.

## Purpose and public interface

The ordered pair `(ClientId, ClientBatchId)` is a database-wide retry identity. Before the first
attempt may submit a WAL record, it must own that identity. A later attempt must not submit a second
record while the first is active, and a committed retry must return the exact immutable outcome
already published by tablet state.

`chronos::ingest::RetryDirectory` provides that narrow coordination boundary:

- `create` requires an explicit maximum entry count;
- `try_reserve` returns a move-only reservation, an immediate in-flight observation, a matching
  committed outcome, or a conflict;
- `RetryReservation::mark_wal_started` crosses the point after which automatic cancellation is
  forbidden;
- `cancel_before_wal` removes a harmless pre-WAL rejection;
- `commit_published` records the exact shared outcome object supplied by tablet publication; and
- `metrics` reports the configured bound, current state counts, and successful-reservation high
  water mark.

The mutation identity compared for a committed lookup contains `TableId`, `TabletId`, and the
canonical SHA-256 request digest. Matching therefore requires the same logical mutation and target.
The directory does not recompute the digest; the command decoder/admission layer owns that proof.

## State machine

```text
                         mark_wal_started
 absent ── try_reserve ─────────────────────► WAL-started in-flight
   ▲              │                                      │
   │              │ cancel / pre-WAL handle drop         │ commit exact published outcome
   └──────────────┘                                      ▼
                                                     committed
```

The implementation internally distinguishes pre-WAL and WAL-started reservations even though both
are observed by contenders as `kInFlight`:

- pre-WAL destruction is rollback-safe and removes the entry;
- after `mark_wal_started`, destruction deliberately leaves the entry blocking;
- only a valid outcome matching the reserved mutation can become committed; and
- committed entries are immutable and are not reclaimed by this phase.

A contender never waits inside `try_reserve`. It receives `kInFlight` immediately, which is the
current bounded-busy policy allowed by the accepted architecture. Transport code will later map
that decision to a retryable response or an independently bounded wait outside this primitive.

## Ownership and lifetime

The directory owns a reference-counted internal state object. A `RetryReservation` keeps that state
alive, so a reservation may safely outlive the public `RetryDirectory` owner. The reservation is
move-only and has one logical owner. It may be transferred between threads, but concurrent calls on
the same handle are unsupported.

Committed entries hold `shared_ptr<const ColumnarAppendRetryOutcome>`. `commit_published` does not
copy or reconstruct the tablet result: matching retries receive the same shared pointer. This is
what permits the future global directory and tablet retry table to name one immutable logical
outcome instead of maintaining two outcome authorities.

## Concurrency and ordering argument

One mutex protects the ordered map, reservation tokens, state transitions, outcome pointers, and
metrics. The successful map operation or state transition performed while holding that mutex is
the operation's linearization point. Unlock followed by a later successful lock establishes the
C++ synchronization edge by which contenders observe the transition and initialized immutable
outcome.

The implementation makes no lock-free claim and uses no atomics. Distinct reservations and
directory calls may execute concurrently because they converge on the same mutex. The single lock
also makes database-wide conflicts independent of target tablet. A future partitioned directory
would require a proof that the same retry key has exactly one partition and that metrics and
retention preserve the accepted semantics.

## Bounds, allocation, and failure behavior

`maximum_entries` bounds the sum of in-flight and committed map entries. It is deliberately
required rather than defaulted because no accepted idempotency horizon or reclamation policy exists
yet. Reaching the bound returns `kResourceExhausted` without changing state. It bounds entry count,
not allocator overhead or the memory retained by outcome objects.

An absent-to-reserved transition allocates a map node and reservation handle. If handle allocation
throws, the just-inserted map entry is erased before the public boundary converts the failure to
`RESOURCE_EXHAUSTED`, so an attempt that never received ownership cannot strand an identity.
Directory construction and map-node failure use the same explicit classification. State changes
after WAL start modify an existing entry and assign one shared pointer; they do not add a map node.
The eventual ingestion coordinator must still reserve tablet/head/publication resources before WAL
admission as required by the architecture.

Invalid outcome pointers, mismatched mutation identities, invalid source-specific commit positions,
zero row counts, and invalid or stale handles fail without changing the owned entry. A committed
outcome retains either its WAL ID or its Raft group ID, never both. A dropped WAL-started handle is
intentionally fail-closed: the entry remains in-flight until the whole process-local state is
discarded and reconstructed by future recovery. This primitive does not pretend that it can infer
whether an ambiguous WAL operation became durable.

## Complexity and performance

The correctness-first `std::map` implementation makes reserve and transition operations `O(log N)`
for `N` retained identities. Metrics collection is `O(N)` and holds the same mutex while counting
states. Current entry count is explicitly bounded, and no throughput claim is made.

There is no standalone microbenchmark for this unit because it introduced no performance
optimization or format hot path. The single-tablet executor microbenchmark keeps digest, WAL,
publication, and durability work enabled for first attempts. A separate steady matching-retry case
re-encodes and re-digests 64-, 1,024-, and 65,536-row batches against one pre-established committed
identity while asserting exact outcome-pointer reuse and the absence of a second WAL result. The
executor also measures 50/50 and 10/90 first-attempt/retry operation ratios at 64 and 1,024 rows,
with one real first apply per timed cycle.

A dedicated retry-directory benchmark constructs 64-, 4,096-, and 65,536-entry committed
populations, cycles deterministic matching lookups, and checks that cardinality never changes.
Shared 4,096- and 65,536-entry cases run 1, 2, and 4 threads against the same mutex-protected
directory. Benchmark-only instrumentation reports the number of regular allocation calls and total
requested bytes during population construction. That total includes transient reservation handles
and is deliberately not labeled retained memory or allocator/RSS overhead. Those measurements are
local evidence for the current `std::map`, not a justification to replace it from one machine.

## Verification strategy

Deterministic unit tests cover capacity, automatic and explicit pre-WAL cancellation, the
irreversible WAL boundary, exact shared-outcome identity, conflicts across targets, nominal-key
scope, and reservation lifetime. A seeded operation generator compares thousands of transitions
with an independent reference state machine. A concurrent start barrier proves that one of many
contenders owns the absent-to-in-flight transition while all others observe in-flight. TSan is the
required local evidence for the shared-state boundary; ASan/UBSan cover lifetime and undefined
behavior. A separate test-only allocator fails directory construction, map-node allocation, and
reservation-handle allocation in turn and proves `RESOURCE_EXHAUSTED` plus complete rollback.
Installation testing compiles the public header through the exported `chronos::ingest`
target.

## Deferred integration

The directory alone does not provide exactly-once ingestion. The live executor now composes bounded
head capacity, WAL coordinator submission, batch-atomic tablet publication, and post-WAL failure
ownership for one already-routed tablet. Tablet preparation now enforces APPEND_ROWS logical-key
conflicts and retained-lineage recovery rebuilds fresh state. Remaining Phase 4 work includes
routing/admission, retention and pruning, and checkpoint-aware crash reconciliation.
In particular, startup must reconstruct committed retry entries in global WAL order before
publishing recovered database state.

## Likely review questions

**Why not erase a reservation after an ambiguous WAL error?** Once I/O starts, the command may be
present in the durable history. Erasing would allow a duplicate record and possibly a different
commit position. Blocking until recovery is the safe classification.

**Why return in-flight even when the contender has a different digest?** The owner has not yet
published the authoritative outcome. Both matching and conflicting contenders must avoid a second
WAL submission; conflict classification can occur after the first transition commits or through a
future bounded coordination path.

**Why store a shared outcome pointer?** Rows, tablet retry state, and the outcome must be one logical
publication. Reusing that immutable object prevents the global directory from manufacturing an
independent result.

**Why not prune committed entries at capacity?** Safe pruning requires the advertised idempotency
horizon and durable checkpoint/catalog ownership. Evicting opportunistically would silently weaken
the client contract.
