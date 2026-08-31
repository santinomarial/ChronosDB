# ADR 0230: Live Single-Node Sealed-Head Flush

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB single-node, ingestion, Manifest, and service maintainers

## Context

Manifest-aware startup could consume durable CSEG generations, but the live owner configured every
tablet without a flush queue. Heads therefore remained WAL-only until their bounded sealed-head
limit rejected later ingestion, and the service could not create the advanced generations that it
was able to recover.

## Decision

Each local tablet receives its own bounded `SealedHeadFlushQueue`, sized by the frozen database
bootstrap sealed-generation limit. The owner retains one `SealedHeadFlushCoordinator` per queue;
the coordinators borrow the aggregate Manifest storage and publisher and are destroyed before that
recovery owner.

`TabletSnapshot::retry_entries()` returns a bounded identity-sorted copy of the exact pinned retry
publication. When queue work is ready, the owner reads the oldest visible sealed generation,
derives its distinct WAL record sequences from authenticated row metadata, and requires one matching
WAL retry outcome per sequence. It supplies those descriptors, all retained tablet lineage bindings,
and three distinct UUIDs to the existing durable flush coordinator. ADR 0564 makes that outer
allocation injectable and finite: it rejects nil/same-operation values, `PartId` values already
present as final or temporary namespace entries, and exact operation-temporary collisions before
the coordinator acquires work or mutates the filesystem.

Native ingest and SQL INSERT drain ready work after successful idempotent WAL application. A flush
failure is returned as a request error even though WAL application may already be durable; the same
client retry identity safely resolves that ambiguity. Shutdown drains queues before stopping WAL,
then destroys flush coordinators before releasing Manifest ownership.

## Consequences

Ordinary ingestion can now rotate a full head into a CSEG part, install the next Manifest, atomically
replace query storage authority, retire the sealed head, and restart from only the uncovered WAL
suffix. Queues remain bounded and per-tablet ownership avoids cross-tablet dequeue probing.

Flush execution is currently synchronous on the service owner and uses uncompressed CSEG pages.
ADR 0231 subsequently routes native SELECT through the aggregate CSEG/head snapshot. Checkpoint
advancement and restart reclamation are composed by ADR 0232. Background scheduling, compression
policy, abrupt-stop checkpointing, and flush metrics export remain separate work.

## Validation

Focused ingestion coverage verifies exact retry publication enumeration. A service integration test
forces three two-row appends through a four-row head, observes Manifest generation 2 with one
four-row part and two-row live suffix, then restarts and observes the same durable/head split.

## References

- [ADR 0017](0017-manifest-generations-installation-and-checkpoints.md)
- [ADR 0229](0229-manifest-aware-single-node-startup.md)
- [ADR 0564](0564-authoritative-bounded-live-flush-identity-allocation.md)
- [Durable sealed-head flush coordination](../learning/sealed-head-flush-coordinator.md)
