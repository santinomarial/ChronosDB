# Recoverable single-node database owner

## Purpose and public interfaces

`SingleNodeDatabase` is the first owner that turns existing ChronosDB subsystems into one recoverable
database lifetime. The startup config supplies an existing root plus proposed values used only if no
final bootstrap exists. After success, callers can inspect the immutable query catalog, find a table
lineage or tablet, execute appends through the global retry directory and WAL coordinator, and take
tablet snapshots for vector query execution.

The owner does not accept arbitrary metadata mutation yet. A table is visible only when recovered
metadata contains its complete schema tail, complete policy, and local placement. This boundary
prevents a crash prefix of future multi-entry table creation from becoming a partially configured
runtime table.

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

## Complexity, tradeoffs, and review questions

Startup is linear in retained metadata history, current catalog size, and verified WAL history.
Current catalog and tablet lookup are linear vectors under explicit metadata limits; indexing is a
later profile-driven optimization. Head variable-byte capacity is allocated per variable column and
schema generation, so operators must configure finite durable values rather than rely on defaults.

Reviewers should ask why metadata opens before WAL, which catalog states are routable, whether an
unknown durable append can be ignored, which owner shuts down first, and which durability mode
covers an acknowledgement. Today the answers are: schema authority first; complete local tables
only; never; WAL coordinator first; and the exact requested `ASYNC` or `LOCAL_SYNC` mode.
