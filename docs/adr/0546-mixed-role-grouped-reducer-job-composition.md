# ADR 0546: Mixed-role grouped reducer-job composition

- **Status:** accepted
- **Date:** 2026-08-30
- **Owners:** ChronosDB distributed-query, service, network, and Native protocol maintainers
- **Extends:** [ADR 0500](0500-packaged-mutable-grouped-native-execution.md),
  [ADR 0512](0512-atomic-grouped-shuffle-source-fanout.md),
  [ADR 0534](0534-atomic-grouped-result-coordinator-lifecycle.md),
  [ADR 0543](0543-packaged-grouped-shuffle-job-lifecycle.md), and
  [ADR 0545](0545-authenticated-grouped-reducer-coordinator-leases.md)

## Context

The packaged reducer-job lifecycle selected only gateway coordinators. A daemon that coordinated a
query and also led one tablet was necessarily both a source and a reducer because canonical
destination selection uses the serving-node set. Job Control and result-return frames correctly
reject identical source/target nodes: no network peer exists to authenticate on an in-process
edge. Sending those operations through loopback TLS would therefore require a fabricated principal
or a weakened frozen protocol.

The packaged Native query thread and private query-control poll thread are distinct. Enabling local
source publication and local reducer control also requires an explicit synchronization boundary
around the reducer service and every single-threaded job object it owns.

## Decision

Keep all Job Control v1-v4, shuffle, and result-return wire formats unchanged. The reducer-set
coordinator now classifies each canonical reducer route by node identity:

1. a different node retains the existing finite address-rotating TCP/mTLS acquisition;
2. the coordinator node requires a borrowed, stable in-process reducer-job service;
3. local PREPARE, INSTALL_ROUTES, RENEW_LEASE, SEAL, and CANCEL accept only
   `coordinator == target == local_node` and never construct a peer authentication result; and
4. local SEAL readiness uses the same bounded retry/backoff/deadline policy as a correlated remote
   `UNAVAILABLE` response.

The service materializes coordinator-local partition results directly into an in-memory complete
result stream. That stream revalidates query, partition owner, exact raw schema, canonical Native
batch decoding, frame count, and the equivalent bounded encoded extent. The all-partition collector
and accounted materializer accept this explicit self-owned form while continuing to require the
unchanged authenticated wire form for remote partitions. Local and remote partitions still share
one collector and one atomic final projection/order/limit/Native publication boundary.

The reducer-job service owns one mutex covering control receipt, source publication, destination
progress, result handoff, cancellation, lease expiry, tombstones, metrics, and cleanup. The
query-control poll thread and Native query thread may enter through different public methods, but
no contained reducer, socket, TLS owner, retry owner, or resource context is called concurrently.
Unlock synchronizes-with the next successful lock, so every handoff observes the preceding job
mutation. No lock-free algorithm or additional atomic memory order is introduced. Poll holds the
mutex for at most its caller-supplied finite wait; the packaged caller uses ten milliseconds.

The packaged server exposes a stable borrowed pointer to its installed job service. The Native
provider selects mixed-role queries only when this pointer exists; embeddings without it retain the
established direct grouped lifecycle. Teardown closes query execution and the shared listener
before releasing the service.

## Consequences

All-local, remote-only, and mixed local/remote grouped reducer jobs now use one whole-query owner.
Local workers publish through the existing proof-revalidating source decorator, remote workers use
the shared mTLS endpoint, local/remote reducers exchange opposite shuffle edges, and local/remote
partition results finalize together. No durable or network compatibility migration is required.

The mutex intentionally favors correctness and bounded ownership over parallel reducer progress.
Contention and tail latency require measurement before introducing per-job locking or queues.
Abrupt daemon loss still destroys both the local coordinator and its local reducer; leases protect
surviving remote reducers, while whole-query retry/recovery remains the higher-level policy.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): local control, source, reducer, result, and remote
  peers retain one exact query/authority product.
- [Invariant 10](../architecture/invariants.md): local results exact-decode the same canonical
  Native batches and validate equivalent bounded extents before admission.
- [Invariant 11](../architecture/invariants.md): the packaged owner keeps the borrowed service
  stable; one mutex defines cross-thread ownership transfer and exclusion.
- [Invariant 14](../architecture/invariants.md): every frozen wire format remains unchanged and
  continues rejecting self-routes.
- [Invariant 15](../architecture/invariants.md): local SEAL retry, lock wait, result bytes, frames,
  memory, lease, and whole execution remain finite.
- [Invariant 18](../architecture/invariants.md): no local or remote subset can publish before every
  authority partition reaches the common finalizer.

## Validation

Focused coverage proves an all-local coordinator/worker/reducer/result lifecycle without any TCP
self-route and a two-node mixed-role lifecycle with one local and one authenticated remote worker,
opposite shuffle edges, two reducers, local plus mTLS result collection, leases, and the exact merged
COUNT. Local control rejects foreign identity and retains cancellation tombstones. Provider tests
require the explicit service before selecting a coordinator-local query. Allocation injection
covers local PREPARE ownership and local-result validation while preserving caller streams.

The milestone gate runs warning-as-error cluster/service builds, complete cluster/service and
allocation suites, focused ASan/UBSan and TSan, changed-file LLVM 18 formatting/static analysis,
workflow action validation, and final diff review. Packaged multi-daemon split-leader process-loss,
network-partition, skew, and measurement gates remain separate.

## Migration and rollback

This is an additive in-memory deployment seam. Rollback removes local reducer acquisition and
result handoff and makes the provider select the direct grouped lifecycle whenever the coordinator
owns a fragment. No stored or network bytes change.
