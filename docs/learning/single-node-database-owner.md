# Recoverable single-node database owner

## Purpose and public interfaces

`SingleNodeDatabase` is the first owner that turns existing ChronosDB subsystems into one recoverable
database lifetime. The startup config supplies an existing root plus proposed values used only if no
final bootstrap exists. After success, callers can inspect the immutable query catalog, find a table
lineage or tablet, execute appends through the global retry directory and WAL coordinator, and take
tablet snapshots for vector query execution.

The owner accepts initial local `CREATE TABLE` as three exact-retained metadata proposals. A table is
visible only when metadata contains its complete schema tail, complete policy, and local placement.
Schema-only or schema+policy crash prefixes remain invisible and a matching retry reuses their
durable identities before completing publication.

`NativeProtocolService` supplies the first transport-to-owner boundary. It exactly decodes an
accepted Protocol v1 ingest request, resolves only the active durable schema and local tablet, copies
borrowed canonical column buffers into immutable ownership, and executes through the same retry
directory and WAL coordinator used by direct callers. A new append acknowledges its real WAL record
start and exact requested/effective durability. A matching retry performs no second WAL operation
and therefore returns zero position fields.

For SELECT, the same adapter parses and binds against the owner's immutable catalog, acquires stable
publications for every local tablet in the bound table, lowers the supported SQL subset, and places
one physical pipeline above the full tablet/generation source. Bound output descriptors and
canonical vector cells become Protocol v1 result batches. Query memory, total rows, batches, and
aggregate encoded payload bytes all have independent finite caps. A successful empty query still
emits a described zero-row result before `QUERY_END`; a local failure discards accumulated frames
and returns one terminal error.

For CREATE TABLE, token dispatch selects the DDL parser/binder and requires an injected identity
generator. The adapter rejects nil or duplicate UUIDs before handing explicit table, schema, tablet,
and per-column identities to the owner's restartable creation path. Completion is one described row
containing those durable identities, the applied metadata index, and whether an incomplete prefix
was resumed. The reusable service does not choose entropy policy; its process owner must inject a
secure generator.

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
  -> WAL create OR whole-log semantic preflight/replay
  -> bounded RetryDirectory + TabletState publications
  -> live WalCommitCoordinator admission
```

The metadata group and WAL keep their own subsystem locks while the database root lock supplies the
aggregate process-ownership boundary. WAL recovery receives exact schema lineages and per-version
head capacities derived from the durable bootstrap. It publishes nothing until whole-log preflight
and replay succeed.

For a new or logically empty WAL, the owner creates the global retry directory and tablet states
directly. For a nonempty configured WAL, `RecoveredColumnarAppendState` owns them and releases only
the verified/repositioned writer to the coordinator. Lookup hides this representation difference.

## Shutdown and failures

`shutdown()` closes admission, drains requests, finishes the last required local synchronization,
and joins the WAL worker before closing Raft and releasing the root lock. The first failure is
returned while later close operations are still attempted. Calls after shutdown are outside the
live service contract; repeated shutdown itself succeeds.

Unknown WAL tablets, damaged log bytes, inconsistent lineage tails, missing active definitions,
and nonlocal placement all fail startup. Incomplete metadata-only table prefixes remain invisible.
No fallback schema, policy, durability downgrade, or empty-success response exists.

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

## Complexity, tradeoffs, and review questions

Startup is linear in retained metadata history, current catalog size, and verified WAL history.
Current catalog and tablet lookup are linear vectors under explicit metadata limits; indexing is a
later profile-driven optimization. Head variable-byte capacity is allocated per variable column and
schema generation, so operators must configure finite durable values rather than rely on defaults.

Reviewers should ask why metadata opens before WAL, which catalog states are routable, whether an
unknown durable append can be ignored, which owner shuts down first, and which durability mode
covers an acknowledgement. Today the answers are: schema authority first; complete local tables
only; never; WAL coordinator first; and the exact requested `ASYNC` or `LOCAL_SYNC` mode.
