# Recoverable single-node database owner

## Purpose and public interfaces

`SingleNodeDatabase` is the first owner that turns existing ChronosDB subsystems into one recoverable
database lifetime. The startup config supplies an existing root plus proposed values used only if no
final bootstrap exists. After success, callers can inspect the immutable query catalog, find a table
lineage or tablet, execute routed appends through one database-owned method, and take tablet
snapshots for vector query execution.

For a pre-Manifest root, the owner opens the WAL only long enough to validate it and obtain its
durable identity, then installs exact empty generation 1. Every live startup proceeds through
`recover_manifest_columnar_database()`: it validates the selected Manifest/CSEGs, restores durable
retry/tablet boundaries, replays only the WAL suffix, and retains one aggregate Manifest/part/head
publication plus `manifest/LOCK`. Native queries instantiate their physical pipeline from that exact
aggregate epoch, including selected CSEG descriptors and current head boundaries.

The owner accepts initial local `CREATE TABLE` as three exact-retained metadata proposals. A table is
visible only when metadata contains its complete schema tail, complete policy, and local placement.
Schema-only or schema+policy crash prefixes remain invisible and a matching retry reuses their
durable identities before completing publication.

`NativeProtocolService` supplies the first transport-to-owner boundary. It exactly decodes an
accepted Protocol v1 ingest request, resolves only the active durable schema and local tablet, copies
borrowed canonical column buffers into immutable ownership, and executes through the database-owned
append boundary. A new append acknowledges its real WAL record start and exact requested/effective
durability. A matching retry performs no second WAL operation and therefore returns zero position
fields.

The append boundary retains the immutable input until the executor returns. Only `kApplied` invokes
the optional borrowed committed-append observer, with the exact tablet/WAL position and committed
outcome. Failed work and matching retries never notify. The callback cannot change an already
committed result and must contain live overload or evaluation failure internally rather than throw
or report a false write failure.

For SELECT, the same adapter parses and binds against the owner's immutable catalog, acquires one
aggregate Manifest publication, lowers the supported unary or ASOF SQL subset, and places each
physical source above every local tablet/generation of its bound table. Bound output descriptors and
canonical vector cells become Protocol v1 result batches. Query memory, total rows, batches, and
aggregate encoded payload bytes all have independent finite caps. A successful empty query still
emits a described zero-row result before `QUERY_END`; a local failure discards accumulated frames
and returns one terminal error.

The owner currently publishes append-only Manifest v1 state rather than temporal CSEG/Manifest v2
history. Native `FOR SYSTEM_TIME AS OF` therefore fails explicitly before snapshot acquisition; it
must never reuse the current source and return present rows for a historical request.

For CREATE TABLE, token dispatch selects the DDL parser/binder and requires an injected identity
generator. The adapter rejects nil or duplicate UUIDs before handing explicit table, schema, tablet,
and per-column identities to the owner's restartable creation path. Completion is one described row
containing those durable identities, the applied metadata index, and whether an incomplete prefix
was resumed. The reusable service does not choose entropy policy; its process owner must inject a
secure generator. The complete identity vector is generated and validated before the database owner
receives the operation. An entropy error after any generated prefix therefore emits no metadata
proposal and cannot create an incomplete durable table prefix.

For INSERT VALUES, token dispatch selects the INSERT parser/binder, evaluates source-free constant
rows, and transposes them into canonical immutable columnar ownership. The adapter currently
requires exactly one local tablet, allocates distinct nonnil client/batch identities through the
same injected generator, and executes the canonical append with `LOCAL_SYNC`. Its described result
reports the row count, real WAL location, and retry outcome before `QUERY_END`; the rows are already
query-visible when it returns. Because Protocol v1's SQL query envelope carries no durable client
mutation key, an ambiguous SQL INSERT response is not safe to retry. Canonical ingest remains the
retry-safe surface.

## Startup ownership and data flow

```text
database root lock + bootstrap
  -> one-node metadata Raft open/election/replay
  -> owning complete-table catalog projection
  -> retained schema lineages + immutable query catalog
  -> classify final Manifest OR initialize generation 1 from the opened WAL identity
  -> validate selected Manifest/CSEGs + derive durable seeds
  -> replay required WAL suffix into bounded RetryDirectory + TabletState publications
  -> aggregate Manifest/part/head publication + retained manifest/LOCK
  -> one bounded sealed-head queue and durable flush coordinator per local tablet
  -> live WalCommitCoordinator admission
```

The metadata group and WAL keep their own subsystem locks while the database root lock supplies the
aggregate process-ownership boundary. WAL recovery receives exact schema lineages and per-version
head capacities derived from the durable bootstrap. It publishes nothing until whole-log preflight
and replay succeed.

`RecoveredManifestColumnarState` owns the startup retry directory and tablets and releases only the
verified/repositioned writer to the coordinator. Tables created later retain separate tablet owners
but publish their empty epochs into the same aggregate storage owner. Every tablet uses a separate
bounded flush queue. Native writes synchronously drain ready work through CSEG installation,
Manifest installation, aggregate replacement, tablet retirement, and queue completion. They also
refresh the aggregate tablet epoch when no flush is ready, so later queries see the latest active
head boundary.

## Shutdown and failures

`shutdown()` drains ready sealed heads, closes admission, finishes the last required local
synchronization, and joins the WAL worker. With the WAL lock released, it proves the longest covered
prefix from the exact selected parts and publishes a checkpoint-only successor when that coordinate
advances. It then destroys flush coordinators before releasing Manifest ownership, closing Raft, and
releasing the root lock. The next startup enables conservative deletion of wholly covered closed
segments. The first failure is returned while later close operations are still attempted. Calls
after shutdown are outside the live service contract; repeated shutdown itself succeeds.

Unknown WAL tablets, damaged log bytes, inconsistent lineage tails, missing active definitions,
and nonlocal placement all fail startup. Incomplete metadata-only table prefixes remain invisible.
No fallback schema, policy, durability downgrade, or empty-success response exists.

A new root can fail after Bootstrap v1 and metadata Raft are ready but before the initial WAL
identity is installed. The owner returns that contextual entropy error without exposing a service.
On the next start, the final bootstrap remains authoritative; the owner reopens metadata, creates
the still-empty WAL and Manifest namespace, and enters the ordinary recovery path. Linux process
coverage requires that recovery and a second established-root reopen through the shipped daemon.
If the final bootstrap instead fails checksum validation, the owner stops before opening later
subsystems. It neither substitutes newly proposed identities nor rewrites the damaged descriptor;
packaged process coverage verifies the exact 128 bytes are preserved.
If bootstrap is valid but the active WAL segment header fails CRC32C validation, recovery stops
before replay or service publication. It does not invoke incomplete-tail repair or patch the header;
packaged process coverage verifies the entire damaged segment is preserved.
A checksum-invalid complete application record fails at the same pre-replay boundary even when it
is the last record in the active segment. The owner neither truncates it nor publishes the table's
recovered rows; packaged coverage compares the entire segment after rejection.
An actual incomplete final suffix has a different classification but the same default availability
result: this owner configures no repair authorization, returns the explicit-repair requirement, and
leaves the segment unchanged. Repair remains an intentional operator/tool boundary.
Metadata Raft opens before WAL. A checksum-invalid Raft segment header therefore stops startup at
that earlier authority boundary: no catalog is projected and no WAL recovery or service publication
begins. Packaged coverage proves the complete damaged segment is preserved. The adjacent
complete-record case corrupts the multiplexed payload and proves the same pre-catalog rejection and
segment preservation. A structurally incomplete final Raft suffix could be repaired only when the
caller opts in; the packaged owner does not, so it rejects and preserves the suffix before catalog
projection.

Malformed transport/command bytes become client-invalid protocol errors. Database corruption stays
distinguishable as an internal error; I/O, unavailability, and unsupported operations are execution
failures. The adapter is synchronous and thread-affine: it retains a complete result sequence within
its explicit cap, while the daemon worker owns queue backpressure and will later own
cancellation/concurrency policy.

DDL is thread-affine. The bound statement must retain the exact current query-catalog pointer; a
catalog update makes older bound DDL stale. After all metadata applies, the owner constructs the new
tablet state before replacing its immutable query catalog. Existing bound query plans keep their
shared schema/catalog ownership and already-instantiated head scans keep generation pins.
SQL INSERT is also thread-affine and rejects multi-tablet targets rather than guessing event-time or
shard routing. Parser, binder, columnar, WAL, head, and response bounds fail before success is
reported; after WAL begins, the append executor's existing fail-closed rules remain authoritative.
The configured committed-append observer must outlive the database and is called on the same owner
thread. Database startup replay completes before the observer can receive online work and does not
re-emit historical notifications. The bounded live fan-out is one concrete observer: its borrowed
plans, coordinators, and query resource contexts must also outlive it. It routes only exact
table/tablet/WAL matches and contains evaluation or publication failure by expiring the affected
plan's old replay continuity; it never changes the applied write result.

Because executable plans are recovered only after database startup, the daemon passes a stable
`SingleNodeCommittedAppendRouter` as the database observer. Before admitting requests it binds one
heap-stable `SingleNodeSubscriptionRuntime` fan-out. That runtime and its service mutate their
coordinator only on the database worker thread and detach before any borrowed plan, coordinator, or
database storage context is destroyed. `subscription_snapshot_context` exposes only the exact
borrowed Manifest storage, aggregate publisher, and lineage needed for the historical half.

## Complexity, tradeoffs, and review questions

Startup is linear in retained metadata history, current catalog size, and verified WAL history.
Current catalog and tablet lookup are linear vectors under explicit metadata limits; indexing is a
later profile-driven optimization. Head variable-byte capacity is allocated per variable column and
schema generation, so operators must configure finite durable values rather than rely on defaults.

Reviewers should ask why metadata opens before WAL, which catalog states are routable, whether an
unknown durable append can be ignored, which owner shuts down first, and which durability mode
covers an acknowledgement. Today the answers are: schema authority first; complete local tables
only; never; WAL coordinator first; and the exact requested `ASYNC` or `LOCAL_SYNC` mode.
