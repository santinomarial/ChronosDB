# Non-Negotiable Invariants

These properties define correctness for every future ChronosDB implementation. A roadmap phase cannot pass while it violates an applicable invariant. Tests named here are required future validation strategies, not tests that currently exist. The [correctness strategy](../testing/correctness-strategy.md) maps every invariant to future executable test types. Terms follow the [glossary](../glossary.md), and component boundaries follow the [overview](overview.md).

## 1. Acknowledged durability survives covered failures

No acknowledged durable write disappears after any failure covered by the selected durability mode.

- **Owner:** write admission, WAL or Raft-log persistence, commit tracking, recovery, and the network acknowledgment path.
- **How it could be violated:** an acknowledgment is sent before the required sync/quorum boundary; a mode is ambiguous; a torn record is treated as durable; recovery skips a committed record; or checkpoint reclamation removes the only durable copy.
- **Eventual tests:** enumerate each durability mode and inject process crashes, power-loss approximations, short writes, sync failures, segment rotation failures, and later replica loss at every acknowledgment boundary. Recovered committed identities must match the acknowledged set allowed by that mode.

For the partially implemented single-node WAL, [WAL recovery](wal-recovery.md) fixes the exact
`ASYNC` and `LOCAL_SYNC` eligibility boundaries and the covered Linux persistence assumptions. The
writer exposes complete-write and explicit synchronization frontiers, the commit coordinator
releases `ASYNC` and `LOCAL_SYNC` requests only at their respective boundaries, and locked recovery
verifies, optionally repairs, replays, and reopens existing history. A `LOCAL_SYNC` record cannot be
discarded as an incomplete tail under the completed
contract; encountering such an outcome is a durability defect or an excluded platform failure, not
permitted loss.

## 2. Manifests reference only completely installed durable parts

A manifest version may name a part only after every required byte and piece of metadata for that part has been written, validated, and made durable according to the installation protocol.

- **Owner:** part writer, filesystem installation protocol, manifest/version-edit writer, and recovery.
- **How it could be violated:** publishing a version edit before file sync or atomic rename, trusting an object-store listing, omitting directory durability where required, or accepting a footer whose pages are incomplete.
- **Eventual tests:** crash and fault injection after every write/sync/rename/version-edit step; corrupt, truncate, remove, and reorder candidate files; verify recovery selects a complete prior or new generation and never opens a partial referenced part.

## 3. Immutable parts are never modified in place

Once installed, a CSEG part's bytes and logical contents never change. Replacement, correction, compaction, and tiering create new identified objects and atomically update manifest references.

- **Owner:** CSEG writer, flush, compaction, tiering, repair tools, and manifest management.
- **How it could be violated:** patching page metadata, rewriting a file under the same identity, reusing an object key, or allowing a background job to mutate data visible to an active snapshot.
- **Eventual tests:** record content hashes after installation; exercise flush, compaction, correction, cache, and tiering operations; assert installed identities retain bytes until safe reclamation and that replacements receive distinct identities.

## 4. Per-tablet committed application follows log order

Committed operations are applied to each tablet's mutable state, live operators, and materialized views in authoritative log order.

- **Owner:** shard-worker scheduler, commit/apply loop, WAL replay, and later Raft state machine.
- **How it could be violated:** parallel validation publishes out of order, replay uses file completion order, live operators race storage apply, or Raft entries apply before earlier committed entries.
- **Eventual tests:** generate order-sensitive mutations and corrections under delays, batching, restart, and future leader changes; compare storage and live state with a serial reference model at every applied position.

## 5. Uncommitted Raft entries are never query-visible

No scan, point lookup, live result, materialized view, or system-time history may expose an entry until it is committed by its Raft group and applied under the selected read contract.

- **Owner:** Raft commit tracker, deterministic state machine, snapshot acquisition, head publication, and live plane.
- **How it could be violated:** reading a leader's appended tail, publishing prepared head rows early, confusing physical-log durability with quorum commit, or serving from a follower beyond its applied position.
- **Eventual tests:** deterministic simulations with partitions, lost leaders, divergent tails, delayed apply, membership changes, and concurrent reads; tag every returned version with its origin and assert it was committed and applied at the read boundary.

## 6. Queries observe stable snapshots

A query observes one stable combination of relevant heads, parts, schema versions, row-version visibility rules, and per-tablet committed positions for the declared consistency level.

- **Owner:** catalog, snapshot manager, tablet read interface, manifest generations, head publication, and distributed read coordinator.
- **How it could be violated:** mixing old and new schemas, scanning a head past its captured row boundary, losing a part during compaction, acquiring tablets at unintended positions, or changing version visibility mid-query.
- **Eventual tests:** run long queries while ingesting, sealing, flushing, compacting, evolving schemas, reclaiming files, and later moving leadership; compare results with a model evaluated at the recorded snapshot descriptor.

## 7. Compaction preserves visible logical rows

For all supported snapshots and system-time rules, compaction cannot add, lose, or duplicate visible logical rows.

- **Owner:** compaction planner and executor, row-version resolver, CSEG reader/writer, manifest installer, and garbage collector.
- **How it could be violated:** overlapping range boundaries, incorrect deduplication, dropping a history version still visible to a snapshot, applying late corrections in the wrong order, or installing both replacement and input as simultaneously active.
- **Eventual tests:** property-generate overlapping base/delta parts with duplicates, tombstones, corrections, and history; enumerate snapshots before and after compaction; compare logical query results and multiplicities with uncompacted inputs, including injected crashes.

## 8. Recovery is idempotent

Running recovery repeatedly over unchanged durable bytes yields the same durable and logical state and does not duplicate side effects.

- **Owner:** WAL replay, manifest recovery, checkpoint handling, part finalization, materialized-view recovery, and later Raft snapshot/log recovery.
- **How it could be violated:** replay appends a second row, temporary-file cleanup changes the chosen generation, version edits are applied twice, or view output is emitted again without a defined replay contract.
- **Eventual tests:** crash recovery itself at every mutation point, restart repeatedly from copied disk images, and compare manifests, applied positions, logical rows, and retained files to a single successful recovery.

WAL v1 recovery concretizes the first part of this obligation: physical verification and optional
tail repair complete before replay, repair is synchronized and repeatable, and replay does not
publish state until whole-log semantic preflight succeeds.

## 9. Idempotent batch retry does not duplicate logical input

Retrying a batch with the same idempotency identity and compatible content cannot create duplicate logical input. Reuse of an identity with conflicting content must follow an explicit deterministic error or correction contract.

- **Owner:** ingest protocol, identity index/state, shard validation, WAL/log record model, expiration policy, and recovery.
- **How it could be violated:** deduplication exists only in reactor memory, identity scope is ambiguous, crash loses the identity but keeps rows, partial batch apply is retried as a whole, or retention forgets identities sooner than promised.
- **Eventual tests:** retry before/after acknowledgment, connection loss, restart, flush, compaction, and future failover; inject partial I/O and conflicting payloads; compare logical identities and row versions with a reference state machine.

## 10. Durable records and pages have integrity coverage

Every durable log record and CSEG data page is covered by an integrity check that includes the fields needed to establish safe framing and interpretation.

- **Owner:** WAL and CSEG codecs, format specifications, readers, writers, recovery, compaction, and tiering.
- **How it could be violated:** checksumming only payload but trusting corrupt lengths, leaving metadata unprotected, failing open on mismatch, or verifying compressed bytes after unsafe allocation or decoding.
- **Eventual tests:** bit-flip, truncate, splice, reorder, and replace each field and payload region; fuzz parsers with hostile lengths and offsets; require a bounded clean error before unchecked data influences memory access or durable state.

The authoritative [WAL v1 format](../formats/wal-v1.md) satisfies the design obligation for log
framing by protecting segment interpretation fields, record framing fields, and each complete stored
record with specified CRC32C ranges. The in-memory codec now has golden, boundary, corruption,
property-style, sanitizer, and fuzz-target coverage. Locked physical verification, corruption
classification, explicit final-tail repair, ordered replay, and a process-kill crash-image matrix
have deterministic tests. Physical power-loss and storage-stack qualification remain required.

## 11. Referenced storage is not reclaimed

A file or memory region cannot be reclaimed, unmapped, overwritten, or reused while any active reader can reference it.

- **Owner:** snapshot/reference manager, head-generation lifecycle, manifest generation pins, compaction/tiering garbage collector, query cancellation, and subscription retention.
- **How it could be violated:** deleting compacted inputs immediately after manifest swap, reusing a head arena while a scan holds spans, failing to release or acquire a pin atomically, or losing a pin during cancellation.
- **Eventual tests:** pause readers at every dereference boundary while sealing, compacting, canceling, evicting, and reclaiming; use sanitizers and deterministic scheduling; assert deletion/reuse occurs only after the final pin is released.

## 12. Resume tokens name deterministic committed boundaries

A subscription resume position identifies one deterministic committed boundary and enough database, query/definition, tablet-set, epoch, and format identity to reject ambiguous or incompatible resumption.

- **Owner:** subscription protocol, token codec, catalog/query-definition versioning, commit-position mapping, retention, and future topology management.
- **How it could be violated:** encoding wall-clock time alone, omitting tablet or history identity, resuming from an uncommitted index, silently mapping across an incompatible schema, or accepting a token whose required log range is gone.
- **Eventual tests:** round-trip and tamper tokens; resume across restart, compaction, schema changes, retention expiry, future leader changes and rebalancing; require the same suffix or a precise non-resumable error.

## 13. Late corrections preserve event-time and system-time semantics

A late event or correction retains the event time to which its business meaning applies while receiving a distinct committed system-time position/version. Queries select versions according to their stated temporal semantics.

- **Owner:** schema/binder, ingestion and correction model, version storage, compaction, historical execution, and live window operators.
- **How it could be violated:** overwriting the original row, substituting ingestion time for event time, assigning system time before commit, dropping an older version needed by history, or revising a finalized window without a documented change record.
- **Eventual tests:** generate late originals, duplicate retries, multi-step corrections, cancellations, and watermark crossings; compare event-time and as-of-system-time queries plus live correction streams with a bitemporal reference model before and after compaction.

## 14. Durable and network formats are versioned from release one

Every durable file/record and network frame carries or inherits an unambiguous format/protocol version with a specified compatibility and rejection policy.

- **Owner:** format and protocol specifications, codec registries, upgrade tooling, handshake, and recovery.
- **How it could be violated:** inferring layout from file size, dumping native structs, reinterpreting an old field, accepting unknown required flags, or changing semantics without changing the version/feature negotiation.
- **Eventual tests:** maintain golden fixtures for every released version; test supported upgrade/downgrade matrices, unknown versions and flags, byte order, mixed-version connections, and corrupted version headers.

WAL physical format 1.0 now has an immutable compatibility and rejection policy. Logical application
kinds remain unavailable until their independently versioned payload specifications are accepted.

## 15. Slow subscribers cannot block ingestion indefinitely

Every subscription has bounded in-memory influence on the write path and a specified overflow behavior. An unresponsive consumer cannot indefinitely prevent tablet commit, head rotation, compaction, or required log reclamation.

- **Owner:** live operator scheduler, subscription buffers, network reactor, retention manager, admission control, and resume protocol.
- **How it could be violated:** synchronous socket writes on a shard worker, an unbounded result queue, permanent log pins, global backpressure from one connection, or spill without quota.
- **Eventual tests:** halt and throttle subscribers under sustained ingestion and fan-out; verify bounded memory and shard latency, deterministic disconnect/spill/backpressure behavior, explicit retention-expired errors, and successful resume where retained.

## 16. Mutable rows publish only after full initialization

A writer publishes a mutable-head row to readers only after every column value, null bit, offset, and referenced variable-length byte for that row is initialized and has a lifetime covering all readers.

- **Owner:** mutable-head layout and append API, shard worker, allocator, and snapshot scan implementation.
- **How it could be violated:** incrementing the visible row count first, publishing an offset before its payload, reallocating a backing buffer visible through spans, or using insufficient release/acquire ordering.
- **Eventual tests:** deterministic interleavings at every column write and publication step, high-contention scans under ThreadSanitizer, poisoned uninitialized memory, and assertions that readers see either the previous boundary or the complete new row. The implementation must document and test its memory-ordering argument.

## 17. Snapshot-to-stream handoff omits no committed change

After selecting snapshot position `P`, the historical result reflects the defined state at `P` and the continuation includes every matching committed change after `P`, subject only to explicitly documented filtering and retention errors.

- **Owner:** snapshot manager, commit/change retention, subscription registration, query executor, shard live plane, and resume protocol.
- **How it could be violated:** registering after releasing a log pin, using event time as the boundary, starting at `P + 2`, racing tablet commits during multi-tablet registration, or losing buffered changes while historical output is sent.
- **Eventual tests:** force commits at every handoff step, disconnect during history and live output, vary tablet skew, restart and later fail over; compare the concatenated snapshot plus deduplicated continuation with a reference committed log. No post-`P` identity may be absent.

## 18. Optimization cannot weaken guarantees

Performance optimizations cannot reduce documented durability, integrity, ordering, snapshot, temporal, or visibility guarantees. A different tradeoff is a separately named mode with an explicit contract, never a silent fast path.

- **Owner:** every subsystem owner, benchmark reviewers, configuration/protocol designers, and ADR process.
- **How it could be violated:** skipping sync or checksums in a default path, reading uncommitted memory, using approximate pruning that drops rows, relaxing atomics without proof, or benchmarking a weaker mode as though it were equivalent.
- **Eventual tests:** run the same invariant, fault, differential, and crash suites against optimized and reference paths and every documented mode; compare semantics before accepting benchmark results. Code review must connect each optimization to evidence and its preserved invariant set.

## Applying these invariants

Specifications and ADRs must cite affected invariant numbers. Implementations must convert the eventual strategies above into concrete automated tests appropriate to their phase, including negative and fault-injection cases. When two requirements appear to conflict, the engineering priority in the root [`AGENTS.md`](../../AGENTS.md) applies; performance or scope must yield before correctness and recoverability.
