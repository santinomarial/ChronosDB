# Columnar Append Execution

> **Status: blocking live single-tablet execution implemented.**
> `chronos::ingest::execute_columnar_append` composes the immutable batch and command codecs, the
> database-wide process-local retry directory, one already-routed tablet state, and the bounded WAL
> commit coordinator. Registered successor generations and APPEND_ROWS logical-key conflict checks
> are supported, while catalog and routing admission, retention, transport acknowledgment, and multi-tablet
> coordination remain outside this API.

## Purpose and public boundary

The component closes the first live Phase 4 gap between durable command bytes and batch-atomic
tablet visibility. Its input owns one immutable `OwnedColumnarBatch`, the nominal client identities,
and an explicit `ASYNC` or `LOCAL_SYNC` request. The caller supplies the already-selected
`TabletState`, global `RetryDirectory`, and live `WalCommitCoordinator`.

The function is synchronous. It must run under the tablet's single shard-writer ownership rule; it
does not add internal serialization around `TabletState`. The retry directory and WAL coordinator
remain safe for calls from distinct producer threads under their own documented contracts.

Success returns:

- `kApplied` with the exact tablet-published immutable outcome and the WAL completion that
  authorized publication; or
- `kMatchingRetry` with the exact previously published outcome and no new WAL completion.

The latter deliberately appends no record. Its requested durability field describes the attempt,
not a newly established persistence frontier. A future transport contract must decide how a retry
that asks for a stronger durability mode reports or establishes that mode; this executor does not
fabricate an effective frontier.

## Ordered transition

For a first-time retry identity, execution follows this order:

1. Reject a null batch or unknown durability mode and acquire the target tablet's current identity.
2. Canonically encode and validate Columnar Batch v1 bytes.
3. Compute the frozen `COLUMNAR_APPEND` request digest from the exact batch bytes and target.
4. Reserve the database-wide `(ClientId, ClientBatchId)` identity.
5. Encode the exact WAL application payload and prepare all bounded tablet/head/publication state;
   if the batch uses the next registered schema, seal the ancestor and activate an empty successor
   generation before WAL admission.
6. Submit the owned payload to bounded WAL coordinator admission.
7. Once admission succeeds, make both retry and tablet reservations irreversible and wait for the
   coordinator completion.
8. Validate that the completion preserves the requested/effective mode and returns a valid append
   position; `LOCAL_SYNC` must include a covering durable frontier.
9. Publish the batch, applied position, tablet retry entry, and outcome in one outer tablet epoch.
10. Commit that exact shared outcome pointer into the global retry directory.

The function returns `kApplied` only after step 10. A future transport may use that return as its
logical-publication boundary, but the executor itself sends no response and makes no network
acknowledgment.

An existing retry identity branches after step 4. A matching committed mutation returns the stored
outcome without WAL or tablet mutation. An in-flight identity returns `UNAVAILABLE` immediately.
A different committed mutation returns `ALREADY_EXISTS`. None of those paths can submit a duplicate
WAL record.

## Ownership and allocation boundaries

`ColumnarAppendExecutionInput` holds a shared owner for the immutable batch. Canonical batch and
application encodings are local owned values retained through WAL admission. The coordinator copies
the application payload into its bounded queue before accepting it, so the local command owner may
die after `try_submit` returns.

The global `RetryReservation` and `PreparedTabletAppend` handles remain owned by the executing call.
Tablet preparation allocates the head descriptor, immutable retry-map copy, outcome, and outer
publication before WAL admission. Expected post-WAL publication uses reserved storage and does not
add a retry-directory node. The exact `shared_ptr<const ColumnarAppendRetryOutcome>` created for the
tablet is installed in both the tablet publication and global directory; the executor verifies
pointer identity instead of constructing a second result.

## Failure and durability behavior

Failures before successful WAL admission are rollback-safe for rows, retries, and applied position.
Command encoding, tablet capacity, global retry capacity, and coordinator queue rejection leave no
logical mutation. A pre-WAL registered schema activation may leave the empty successor generation
active because it reflects the caller's catalog handoff; the prepared row range is still canceled.
If coordinator admission rejects after both reservations exist, the executor explicitly cancels
both.

After admission succeeds, the retry identity must never become absent while the live state keeps
processing. A WAL append or synchronization error therefore leaves the global entry in flight. The
prepared tablet handle fails its tablet and head closed while preserving the last complete outer
publication. Later mutations are rejected until fresh-state recovery exists.

An `ASYNC` completion proves only a complete WAL write; publication after it has no crash-survival
guarantee. A `LOCAL_SYNC` completion must name the same valid WAL append and a durable record
sequence covering it before publication. The executor rejects an impossible downgraded, mismatched,
or malformed completion and fails the tablet closed.

Tablet publication precedes global retry commit because the global directory may point only to an
outcome already authoritative in tablet state. Contenders still observe the global identity as
in-flight during that small interval and cannot submit another command. The expected global commit
is allocation-free and validates the pre-reserved mutation. An impossible cross-component failure
after tablet publication fails the tablet closed and returns no logical success rather than trying
to roll back an epoch that a reader may already have acquired.

## Complexity and measurement

For encoded batch size `B`, `C` columns, `R` rows, and `T` retained tablet retry entries, the live
first-attempt path performs `O(B)` canonical encoding/digest/copy work, `O(C + T)` preparation, and
`O(C × R + B)` head materialization, plus WAL write and optional synchronization latency. Matching
retry lookup still recomputes `O(B)` canonical bytes and digest so the server does not trust a
client assertion, but performs no WAL or tablet write.

`chronos_ingest_benchmarks` measures first-attempt execution for 64- and 1,024-row timestamp batches
in both durability modes. Per-iteration temporary-directory, writer, coordinator, retry-directory,
and tablet construction and destruction are outside the timed interval. The timed path retains
canonical encoding, retry coordination, the real host-filesystem WAL write, `fdatasync` for
`LOCAL_SYNC`, and tablet publication. Results are local microbenchmarks, not device-qualified
durability or database throughput claims.

The same executable measures steady matching retries for 64, 1,024, and 65,536 rows after one real
first apply establishes the immutable outcome. Each timed retry still performs canonical encoding,
SHA-256 digesting, and directory lookup, while correctness guards require the matching-retry kind,
the exact original outcome pointer, and no second `WalCommitResult`.

## Verification

The executor integration suite covers:

- two ordered real-WAL appends using `ASYNC` then `LOCAL_SYNC`;
- WAL inspection and command decoding proving exact physical sequence and identities;
- one registered successor append sealing the ancestor plus a later exact ancestor retry with no
  second record;
- matching retry pointer identity with no second record;
- conflicting and in-flight identity rejection without tablet mutation;
- deterministic bounded coordinator-admission rejection with both pre-WAL reservations released;
- injected accepted-WAL write failure leaving the identity in flight and tablet failed closed; and
- invalid durability rejection before retry lookup.

The public header compiles by itself, the installed external consumer takes the exported function
address through `chronos::ingest`, and the same tests run in ordinary and sanitizer configurations.

## Deferred integration

This API assumes that its batch was already authorized and routed to exactly one tablet. It does not
enforce event-time admission, active ingest schema selection, or shard scheduling. Tablet
preparation rejects duplicate logical keys within the batch and conflicts against every visible
active or sealed generation before WAL; it still does not decide which registered schema version
the catalog has activated.
The separate retained-lineage
[columnar append recovery](columnar-append-recovery.md) owner can rebuild a fresh replacement from
the WAL; this live call still does not initiate recovery itself. Retry wait policy,
retention/pruning, catalog reconstruction, transport response fields, and operational metrics
aggregation also remain future work.

Likely review questions include:

- Why must both tablet and global retry resources exist before WAL admission?
- Why does accepted WAL failure retain an in-flight identity rather than erase it?
- Why does global retry commit occur after tablet publication, and what blocks contenders between
  those steps?
- Which exact proof distinguishes an `ASYNC` completion from a valid `LOCAL_SYNC` completion?
- Why can a matching retry return no `WalCommitResult`?
- Which omitted admission checks prevent this function from being a network-facing ingest API?
