# ADR 0278: Worker-affine asynchronous metadata application

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB metadata, Raft, routing, and runtime maintainers

## Context

`DurableMetadataStateMachine` reconstructs and advances the authoritative catalog only while it
borrows the synchronous `DurableMultiRaftRuntime`. The asynchronous runtime owns that object on its
private worker, so recovering metadata on the embedding thread would create an invalid cross-thread
borrow. Packaged routing also needs stable metadata after each committed update without borrowing
the mutable state machine or blocking on durable storage.

The worker-extension set from ADR 0277 allows tablet and metadata application to share the worker,
but a concrete metadata extension and publication boundary were still absent.

## Decision

`AsyncRaftMetadataApplication` configures exactly one nonnil metadata group plus optional
application-snapshot storage and the existing metadata/schema codec limits. During extension
initialization it recovers `DurableMetadataStateMachine` on the durable worker, including exact
installed-snapshot validation and committed-suffix replay, and builds an owning
`MetadataCatalogSnapshot` before admission opens.

Each batch context records only its request count and whether the metadata group was touched. After
the durable runtime completes such a batch, the extension applies every newly committed metadata
entry, persists `applied_index`, and then builds and publishes a
`shared_ptr<const MetadataCatalogSnapshot>`. Publication occurs under one mutex after the complete
projection is allocated. Untouched batches do no catalog work and retain the prior snapshot
identity.

Readers briefly acquire that mutex only to pin the immutable shared projection. They never borrow
the mutable state machine, synchronous runtime, or worker context. A retained snapshot remains
valid independently of later publication or owner shutdown. New acquisition fails after shutdown.

Recovery, decoding, applied-index persistence, or projection failure poisons the extension and
therefore fails the asynchronous runtime closed. A durable transition may already exist, but no
successful batch completion is published; restart reconstructs from the authoritative snapshot and
Raft log. Shutdown destroys the state machine on the durable worker before the log closes.

## Consequences

- Metadata recovery and application now share the exact thread and persistence order of Raft.
- Routing and catalog composition can use stable, owning, applied metadata without copying the
whole catalog for every lookup.
- The projection includes committed node endpoints as well as tablet replica identities, schemas,
  and policies; it still does not invent a tablet-to-group identity absent from metadata authority.
- Readers may briefly observe the previous complete applied snapshot while a newer batch is being
  durably applied; they never observe a partial catalog.
- Snapshot publication is proportional to catalog size. Incremental immutable projections require
  measurements and a separate design.
- This owner does not infer tablet-to-Raft-group mappings absent from committed metadata and does
  not treat leader hints as leases.

## Affected invariants and validation

Invariants 1, 4–6, 8, 11, 14, 15, and 18 apply. Focused tests prove pre-admission empty and retained
catalog publication, exact installed application-snapshot recovery, application before completion,
untouched-group snapshot reuse, pinned lifetime across shutdown, nil-group rejection, and terminal
corrupt-command handling.

Allocation fault injection, concurrent publication stress, TSan, large-catalog profiles, exact
routing integration, and packaged multi-process recovery remain Phase 18 work.

## References

- [ADR 0075](0075-durable-metadata-raft-commands.md)
- [ADR 0268](0268-owned-metadata-snapshot-compaction.md)
- [ADR 0272](0272-worker-affine-raft-application-extension.md)
- [ADR 0277](0277-bounded-worker-extension-composition.md)
