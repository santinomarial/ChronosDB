# ADR 0188: Proof-gated tiered distributed-query loading

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB query, tiering, storage, and distributed-systems maintainers
- **Extends:** [ADR 0167](0167-proof-revalidated-distributed-aggregate-worker.md), [ADR 0187](0187-manifest-bound-tiered-cseg-loading.md)

## Context

The distributed aggregate worker already revalidates dispatch, placement, Raft barrier, Manifest,
tablet, and schema authority before reading local parts. Calling a tiered loader outside that gate
would permit remote I/O for a stale or unauthorized request. Making the query library depend on a
specific object-store implementation would also collapse the separation between logical execution
and byte location.

## Decision

The query worker exposes a synchronous `DistributedTemporalPartBatchLoader` seam. The ordinary
entry point constructs its existing local Manifest-backed implementation. An overload accepts a
custom loader but invokes it only after every existing authority and structural gate passes. A
successful loader must synchronously invoke one consumer exactly once while its fully validated
part views are alive; omission or repetition fails closed. The consumer performs the unchanged
Manifest-v2 temporal winner resolution, predicate evaluation, and aggregate accumulation.

`execute_tiered_distributed_aggregate_fragment` lives in `chronos_tiering`. Its adapter calls the
Manifest-bound tiered loader and holds all local or remote image owners through the synchronous
consumer call. Both before execution and inside the loader, it requires the query request's
`selected_manifest()` shared owner to equal the owner embedded in the aggregate tiered snapshot.
Equal generation numbers or equal bytes from an independently loaded owner are insufficient. This
prevents cold authority from being paired with a request built from another acquisition event.

The worker's part-validation limits always control semantic validation. Tiered count and aggregate
byte limits additionally bound remote loading. The local-only public entry point remains source and
behavior compatible.

## Consequences and validation

The query library owns only an abstract synchronous batch contract; the tiering library depends on
query and supplies the object-store implementation, so there is no dependency cycle. One virtual
load and one virtual consume call occur per nonempty tablet, not per row. Part views remain borrowed
only for the consume call, while concrete images retain their exact publication epochs.

The full query suite proves the unchanged local path. A tiering integration test installs a real
Float64 temporal CSEG, Manifest, cold authority, and aggregate publication, removes the local final,
and obtains the same filtered aggregate through the remote object. It also rejects an independently
loaded Manifest owner even though its generation and bytes match. Installed-consumer compilation
covers both public entry points.

Invariants 4–6, 8, 10, 11, 14, and 18 apply.

## Alternatives considered

- **Perform remote loading before calling the worker:** rejected because unauthorized dispatches
  could cause I/O and because proof state might change between validation and execution.
- **Add object-store fields directly to the query request:** rejected because logical query code
  should depend on validated part batches, not storage-provider details.
- **Accept equal Manifest generations/bytes:** rejected because it permits authority mixing across
  independent snapshot acquisitions.
- **Return borrowed views from the loader:** rejected because the tiered image and cold-route owners
  must remain alive through resolution.

## Migration and rollback

Existing callers continue to use `execute_distributed_aggregate_fragment(request)` and local
storage. Tier-enabled callers acquire one aggregate snapshot, construct the ordinary worker request
from that snapshot's embedded Manifest reference, and call the tiered entry point. Rolling back the
tiered adapter remains safe only while all referenced local finals exist.

## References

- [Tiered CSEG loading](../learning/tiered-cseg-loading.md)
- [Atomic Manifest v2 and cold publication](0185-atomic-tiered-storage-publication.md)
- [Architecture invariants](../architecture/invariants.md)
