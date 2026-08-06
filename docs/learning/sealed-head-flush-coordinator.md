# Durable Sealed-Head Flush Coordination

> **Status: implemented Phase 6 composition.** `SealedHeadFlushCoordinator` connects the bounded
> flush queue to sealed-head encoding, immutable part installation, Manifest generation building
> and installation, aggregate database publication, TabletState retirement, and receipt-gated
> queue completion. A subprocess SIGKILL/reopen matrix covers every part and Manifest filesystem
> transition.

## Purpose and boundary

The individual Phase 6 primitives deliberately do not grant one another authority. Encoding a
sealed head does not make it durable; installing a part does not make it selected; installing a
Manifest does not update existing readers; publishing a new database epoch does not by itself
release TabletState or queue ownership. The coordinator performs those transitions in the only
safe order:

1. acquire the oldest ready queue item and retain its exact immutable head pin;
2. exact-encode and validate one CSEG part;
3. reconcile the selected on-disk Manifest with the aggregate published generation;
4. refresh the aggregate tablet epoch before durable mutation;
5. install the part and build/install the next full Manifest generation;
6. reload that directory-synchronized generation and atomically replace the exact sealed head;
7. consume the publisher-issued retirement receipt in TabletState; and
8. use the same receipt to release the queue slot.

This class does not choose file identities, retain retry history, advance the WAL checkpoint, or
delete WAL/CSEG/Manifest files. The single storage owner supplies fresh nonzero part and operation
nonces, the exact retry descriptors, and retained schema lineages. Checkpoint proof and WAL
reclamation remain explicit later operations over the newly selected generation.

## Public interface and ownership

`create(queue, storage, publisher)` retains shared ownership of the queue and borrows the
single-threaded `ManifestStorage` and `DatabaseStoragePublisher` owners. Those owners must outlive
the coordinator. `try_flush_one(tablet, operation)` is synchronous and borrows its TabletState,
retry descriptors, and schema bindings only for the call.

An empty successful result means no FIFO work was ready. A completion reports the exact part,
Manifest generation, queue sequence, row count, and whether the call resumed a successor that was
already durable. The API makes filesystem identities explicit rather than hiding random-number or
process-global policy in a storage primitive.

The coordinator is single-threaded. The caller must serialize it with the shard writer for the
supplied TabletState and with other mutations through the storage and aggregate publication owners.
Readers remain lock-free: their acquired database snapshots retain the old Manifest/head epoch
until they release it.

## Failure and retry state machine

Before the Manifest directory synchronization boundary, a failure returns an error and destruction
of the in-flight work lease restores the same queue item. A successfully installed but unselected
part is a safe immutable orphan. A retry uses a fresh part identity, because ordinary installation
never treats a final-name collision as success.

At the start of every call, the coordinator compares the highest selected on-disk generation with
the aggregate publication:

- equal generations with no matching part begin a fresh flush;
- disk exactly one generation ahead with an exact matching part resumes aggregate publication;
- equal generations already containing the exact part resume retirement/queue completion; and
- any other relationship is corruption or unsupported concurrent ownership and fails closed.

Once a successor Manifest is known directory-synchronized, an unexpected failure poisons the
coordinator and calls `TabletState::fail_closed()`. The durable generation remains the recovery
authority. `DatabaseStoragePublisher::publish_manifest` independently fails itself closed if it
cannot publish a successor it has received. Restart selection can then reconstruct one coherent
owner without guessing whether older process memory is authoritative.

## Invariants

- The selected predecessor is exact-decoded and catalog-bound before generation construction.
- A successor can add only the one part derived from the queued head and exact retry outcomes.
- The final part descriptor must equal the converter-derived descriptor, including row and record
  bounds.
- Aggregate replacement yields a non-forgeable receipt matching table, tablet, schema, generation,
  rows, WAL, and record extrema.
- Queue capacity is released only after both aggregate publication and TabletState retirement.
- Failed pre-Manifest attempts preserve work age/order and never remove a visible head.
- The coordinator never advances the predecessor reclaim checkpoint implicitly.

## Complexity and observability

The coordinator adds linear orchestration overhead around the existing costs. Head conversion is
`O(rows log rows)` because it applies physical ordering; Manifest construction and validation are
linear in retained descriptors and referenced parts; installation cost is linear in bytes plus
the required file and directory synchronization latency.

Metrics report attempts, empty polls, failures, completions, durable-resume completions, encoded
rows/bytes, and fail-closed state. Storage, publication, queue, and TabletState retain their own
component metrics; callers should observe them together when diagnosing backpressure.

## Verification and interview questions

Integration tests cover the complete part-to-publication path, empty polling, a generation-builder
failure that leaves an orphan while restoring queue work, and restart-style resume after Manifest
installation but before aggregate publication. A subprocess matrix stops the real storage protocol
after each part/Manifest write, file sync, rename, and directory sync, sends `SIGKILL`, then opens a
new storage owner, cleans recognized temporaries, validates old-or-new selection, and repeats
selection byte-for-byte. Header self-containment and installed external-consumer compilation protect
the public API.

- Why is an orphan part safe? No selected Manifest references it, and final files are immutable.
- Why reload after Manifest installation? Publication must retain the exact bytes selected by the
  durable namespace, not merely trust the pre-write candidate object.
- Why not complete the queue immediately after directory sync? Readers and TabletState would still
  expose the head, while its only scheduled ownership had been discarded.
- Why preserve the checkpoint? Coverage proof is database-wide and WAL-aware; one local flush
  cannot infer a globally consecutive reclaim frontier.
