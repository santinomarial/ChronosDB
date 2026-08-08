# Shared Snapshot Publication Credit

## Purpose

One aggregate database snapshot can feed many part images, chunks, tablet sources, and ASOF aliases.
They all retain one immutable publication object. Increment 41 charges that publication once per
query while continuing to charge every independently owned image, decoded page, operator, and
output buffer locally.

The implementation uses the last-owner shared reservation accepted in ADR 0056. This is lifetime
deduplication, not an estimate or optimizer discount: every owner that retains the publication also
retains a copy of the exact same query-credit control block.

## Public interfaces and accounting split

`SnapshotPartImage` exposes publication, owned, and complete retained-byte counts.
`CsegPartPinRetainedBytes` carries the complete/shared split into `CsegPartPin`. Shared CSEG, head,
part-scan, and tablet-scan factories accept a `QuerySharedMemoryReservation`; the original factories
remain conservative standalone boundaries.

`AccountedVectorChunk` can combine one local reservation with one shared reservation. Its complete
charge is their checked sum, both credits must belong to the same query, and projection/filter
operators transfer both with the chunk.

```text
query shared publication reservation (one charge)
  ├── tablet/ASOF source parent
  ├── durable CSEG source ── returned borrowed CSEG chunks
  ├── other durable parts and chunks
  └── sealed/active head sources

local reservations (one per independent owner)
  ├── part image bytes and metadata
  ├── decoded/synthesized CSEG buffers
  ├── head materialization and output
  └── operator/container/allocation allowances
```

The shared reservation is not interchangeable with local credit. A part pin declares how much of
its complete retained count is the common publication. A shared CSEG factory requires exact
coverage for that amount. A head factory permits the aggregate publication reservation to be
larger than one head's retained count because a complete epoch owns multiple heads and durable
metadata.

## Ownership and lifetime

The aggregate snapshot owns the physical publication token. Query credit does not extend storage
lifetime by itself, and the storage token does not consume query budget by itself; both travel
together through source state and borrowed CSEG backing.

A raw CSEG chunk may outlive its source, so its `AccountedVectorChunk` copies the shared reservation.
The final copy releases the publication credit only after the last such source or chunk is gone.
Mutable-head rows are copied into canonical output buffers, so a returned head chunk owns no input
publication and retains only local output credit.

The complete tablet parent also keeps the shared credit while it owns unopened sequential children.
Snapshot ASOF creates one reservation before constructing any source, then copies it across all
source aliases. Partial construction destroys every copy and returns the original failure.

## Failure and cancellation behavior

Foreign-query shared reservations, insufficient coverage, impossible complete/shared splits, and
snapshot provenance mismatches are rejected explicitly. Query-budget and allocation failures are
`RESOURCE_EXHAUSTED`. Cancellation, LIMIT, child failure, and early root destruction use ordinary
RAII; no special release protocol or reference cycle exists.

Shared reservation copies allocate no new control block. The initial reservation allocates its
last-owner state once, after the query budget is admitted. Exhaustive allocation injection covers
that owner plus all newly composed operator paths.

## Complexity and measurement

Creating the first shared reservation is `O(1)` and copying it is `O(1)` reference-counted owner
work. Total charged publication bytes are `O(1)` per query snapshot rather than `O(parts × live
chunks + sources)`. Local retained memory remains proportional to actual images and outputs.

`publish_shared_reservation` and `reserve_independent_pins` measure fanout at
1, 8, and 64 owners. They report operations and publication bytes; they do not include storage I/O
or claim cross-query cache savings.

## Verification

Deterministic tests hold chunks from multiple parts after their sources end and compare the sum of
independent chunk coverage with actual query credit. The only difference is repeated logical views
of one publication. Head tests prove final materialized output drops input publication credit.
Hostile tests reject cross-query and incomplete coverage. Vector-chunk fuzzing varies valid local/
shared splits, and installed-consumer compilation checks every new public boundary.

Allocation-failure, ASan/UBSan, TSan, and full repository checks remain part of the increment gate;
only executed commands are reported as evidence.

## Tradeoffs and review questions

The API exposes several explicit composition factories, but keeps the ownership proof visible and
prevents implicit global accounting. It also retains conservative complete publication bytes even
when a query touches one small part; deciding which aggregate metadata can be independently pinned
would require a different storage-publication contract.

**Why does every chunk copy the shared reservation?** It may outlive every source. Last-owner credit
must survive exactly as long as the backing's publication token.

**Why does head output not copy it?** Head materialization owns canonical bytes and borrows nothing
from the snapshot after the pull.

**Can two queries share one reservation?** No. Query budget and cancellation identity are isolated.
They may retain the same storage publication through separate, independently charged query owners.

**Why keep the old factories?** A standalone caller may have no authoritative aggregate snapshot.
Its complete local pin remains safe and preserves API compatibility.
