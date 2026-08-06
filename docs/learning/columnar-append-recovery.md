# Columnar Append Recovery

> **Status: retained-lineage fresh-state recovery implemented.**
> `chronos::ingest::recover_columnar_append_wal` connects the accepted WAL recovery passes to the
> `COLUMNAR_APPEND` codec, immutable batches, retry directory, and tablet publications, including
> APPEND_ROWS logical-key conflict enforcement. Catalog persistence, routing reconstruction, retry
> pruning, CSEG/manifest/checkpoint state, and multi-kind application dispatch remain outside this boundary.

## Purpose and public boundary

The recovery owner turns one existing WAL directory into a new in-memory state without exposing a
verified prefix as a database. The caller supplies:

- the existing `WalWriterConfig` and explicit tail-repair policy;
- one retained linear schema lineage, per-version head capacities, and bounded `TabletStateConfig`
  for every configured tablet;
- one database-wide retry-directory bound; and
- command and embedded-batch decode limits.

Success returns a move-only `RecoveredColumnarAppendState`. It owns the reconstructed global retry
directory, every configured tablet, and the locked `WalWriter` reopened at the exact next global
record sequence. Tablet pointers remain stable for the owner's lifetime. `release_writer()` moves
the writer out exactly once so a live `WalCommitCoordinator` can take ownership without discarding
the recovered tablets or retry outcomes.

This API is intentionally not a catalog. The first schema config is the earliest version that
retained WAL may append. Each successor must be a direct accepted v1 transition and carries the
capacity for one empty generation of its shape. A command naming an unconfigured tablet or absent
retained schema fails with `NOT_FOUND`; a known schema whose immutable definition disagrees with
the command or embedded batch is corruption. An empty or duplicate tablet configuration, invalid
lineage, invalid limits, or invalid bounded state is a caller configuration error.

## Whole-history ordering

The implementation delegates physical authority to `WalWriter::open_existing`:

1. lock and discover the WAL directory;
2. verify the complete physical history;
3. optionally repair only the accepted incomplete final-tail case and reverify;
4. run semantic preflight over every verified record;
5. replay every record in increasing global sequence;
6. synchronize the active segment and WAL-directory startup barrier; and
7. construct the live writer at the recovered identity, offset, and next sequence.

The recovery state, replay sink, and writer remain private local ownership through all seven steps.
If any callback or startup barrier fails, destructors discard every fresh retry entry, tablet row,
head generation, publication epoch, and lock. Only the completed owner crosses the API boundary.

Preflight decodes the already integrity-verified WAL record through
`decode_columnar_append_v1_record`, which still checks application format/kind, exact payload
length, command fields, SHA-256 digest, embedded batch CRC/structure, and duplicated metadata. It
then resolves the command's schema identity in the configured tablet lineage and binds the decoded
physical schema exactly to that immutable version. It performs no tablet or retry mutation.

## Applying one record

Replay decodes and schema-binds again because WAL callback payload spans are valid only during that
callback. It constructs the database-wide retry identity and canonical mutation identity from the
authenticated command.

For a first occurrence, replay:

1. reserves the absent global retry key;
2. copies each borrowed decoded column buffer into an `OwnedColumnVector`;
3. constructs one schema-shaped immutable `OwnedColumnarBatch` under the decode bounds;
4. prepares all tablet/head/retry publication state, sealing the active ancestor and allocating a
   successor generation when this is the first command under the next registered version;
5. marks both reservations as having crossed the existing WAL boundary;
6. publishes using the record's authenticated `(wal_id, record_sequence)`; and
7. commits the exact tablet-published outcome pointer into the global directory.

No WAL bytes are written during replay. Copying physical buffers is deliberate: decoded record
payload storage is callback-scoped, while head preparation and snapshots require immutable owned
input. The copy is per buffer, not per row.

For an exact repeated retry identity and digest, replay verifies the original outcome and target,
adds no rows, adds no retry entry, and keeps the original outcome pointer. This remains true when
the duplicate names a retained ancestor schema after a successor was activated. It publishes only
a new outer tablet epoch whose applied position names the later duplicate record. The active head's
schema, row boundary, and last row-producing position remain unchanged. A different digest,
impossible in-flight state, inconsistent outcome, non-increasing tablet position, or first-time
return to an ancestor schema is corruption and fails the complete recovery.

## Failure classification and bounds

Physical WAL errors retain the WAL recovery classification. At the application layer:

- unknown format, kind, flags, or required semantics are `NOT_SUPPORTED`;
- checksum, digest, metadata, schema, retry, outcome, or ordering contradictions are `CORRUPTION`;
- a logically truncated command inside a physically complete WAL record is `CORRUPTION`, not an
  incomplete WAL-tail repair opportunity;
- an absent configured tablet/schema is `NOT_FOUND`;
- configured decode, retry, head, sealed-generation, or allocation bounds report
  `RESOURCE_EXHAUSTED`; and
- lost internal ownership or impossible cross-component pointer substitution is `INTERNAL`.

Capacity exhaustion can make a valid WAL unrecoverable under a particular runtime configuration.
That is explicit configuration pressure, not corrupt durable bytes. The caller may retry from the
same WAL with a valid larger bounded configuration; no partial owner escaped the failed attempt.

## Complexity and measurement

For `N` records with total encoded bytes `B`, physical verification and semantic preflight are
`O(B)`. Replay is another `O(B)` decode/copy pass plus ordered-map retry work of `O(log K)` per
record for at most `K` bounded identities. First occurrences materialize their rows; exact retry
hits validate bytes and publish one small outer position epoch without copying rows into the head.
Memory is bounded by registered schema versions, configured retry entries, tablet/head generations,
and one callback record plus one record's transient owned batch.

`chronos_ingest_benchmarks` measures verified reopen, whole-log preflight, and fresh replay for
unique commands and alternating first/retry command pairs across record and row-count sizes. It
keeps CRC, SHA-256, schema validation, owned-buffer copy, retry lookup, tablet publication, startup
barriers, and real filesystem reads enabled. Per-iteration writer close and recovered-state teardown
are excluded. Warm filesystem cache is allowed and labeled, so these are local recovery-path
microbenchmarks rather than cold-device startup claims.

## Verification evidence

The focused suite uses real WAL files to prove:

- two tablets replay in global order while an exact duplicate adds no rows;
- the duplicate advances only the outer tablet position and retains the exact original outcome;
- a direct schema successor seals the ancestor generation and publishes rows under its own shape;
- an exact ancestor-schema retry after activation remains a no-row position advance;
- a new retry identity targeting an existing logical row is corruption while an exact request retry
  remains a no-row position advance;
- a first-time ancestor regression is corruption and an absent retained schema is `NOT_FOUND`;
- global and tablet retry tables share that outcome pointer;
- repeating recovery from unchanged bytes reconstructs the same rows, retry counts, and positions;
- the released writer continues with the exact next record sequence and supports live append;
- a conflicting identity fails repeatably without returning partial state;
- an unconfigured target fails during whole-log preflight;
- a command truncated inside a complete WAL record is corruption; and
- an unknown application kind remains unsupported.

The public header is self-contained, the target installs and exports through `chronos::ingest`, and
the external-consumer check takes the recovery function address from the installed package.
Existing command/batch hostile-byte fuzzers exercise the pure decoders used by both recovery passes;
the recovery integration adds state-machine and real-file evidence rather than a second byte parser.

## Deferred integration and review questions

This owner handles only `COLUMNAR_APPEND` application records and caller-supplied linear schema
lineages. A future database recovery coordinator must provide durable catalog/tablet-map
reconstruction, dispatch multiple application kinds, reconcile CSEG/checkpoint coverage, and
publish a database-wide catalog/root atomically. Retry retention and pruning must remain coupled to
checkpoint and idempotency-horizon policy.

Likely review questions include:

- Why must every semantic preflight finish before the first tablet mutation?
- Why is callback-scoped decoded storage copied before head publication?
- Why does a duplicate advance the tablet position but not the active head's row position?
- Why may an ancestor-schema retry remain valid after a successor is active while a first-time
  ancestor append is corruption?
- Why is a short command inside a complete WAL record corruption rather than repairable tail?
- Which state is destroyed when replay fails after earlier records were applied?
- Why can the reopened writer be released without invalidating recovered snapshots?
