# Feature Completion Pass Review

## Repository state

- **Starting HEAD:** `7c46d1427a0c0b1cc4cfbe7a864b140b7ae17a0b` (`build: close phase 10 Linux portability gate`).
- **Starting tree:** `main...origin/main` with two pre-existing untracked headers:
  `include/chronos/live/subscription.hpp` and `include/chronos/live/resume_token.hpp`.
- **Concurrent owner activity:** while this pass was running, the repository owner created and
  pushed `f35c140`, `30ba6bd42f84aae821f426cc194f344b32114de9`, and
  `034273eb74b0040820be4e82587797c77b217fac`, committing the Phase 11–17, runtime, integration, and
  documentation files produced during the pass. The agent did not run `git commit` or `git push`.
- **Reviewed implementation boundary:** `ce3e47cfdcda5c02a80c9995de7eaf5808f29af0`
  (`Reclaim obsolete Raft application snapshots`), pushed to `main`; the working tree was clean before
  this review reconciliation.
- **Continuation policy:** the repository owner subsequently requested one verified commit and push
  per completed task, overriding the original no-commit instruction for that continuation.

This is a truthful feature-architecture checkpoint, not a declaration that Phases 11–17 have passed
their full roadmap exit gates or that ChronosDB is a production three-node database.

## Implemented phase slices

### Phase 11 — live subscriptions and materialized views

`chronos_live` implements:

- Resume Token v2 issuance with fixed source-tagged WAL/Raft positions, HMAC-SHA256, constant-time
  MAC comparison, and v1 WAL-token decoding compatibility;
- single-source register-before-boundary handoff, snapshot-phase buffering, live transition,
  at-least-once poll, acknowledgment checkpoints, retained-suffix resume, cancellation, and bounded
  fail-closed overflow that never rejects an already committed source change;
- removable count, sum, min, max, VWAP, OHLC, and Welford population/sample variance state; and
- tumbling/sliding window materialized state, watermark finalization, corrections/tombstones,
  revisions, consecutive committed progress, and finite row/window bounds;
- plan-bound single-tablet snapshot execution, schema-bound `SUBSCRIBE SELECT` identity, and a
  durable exact-SQL registry that reprepares and verifies executable fingerprints after restart;
- canonical multi-tablet delivery order, exact source-tagged logical checkpoint/restore,
  Checkpoint v2 generation issuance, and v1 WAL-generation recovery compatibility; and
- negotiated Protocol 1.2 source-tagged WAL/Raft changes with frozen 1.1 and 2.0 WAL-only
  compatibility, exact negotiated-context service propagation, and fail-closed cross-version
  decoding; and
- lock-owning, exact-next-generation filesystem installation, fail-closed latest selection, and a
  durable coordinator owner that publishes retention frontiers only after synchronized install;
  plus exact multi-tablet historical execution through one global physical pipeline, started
  directly from a durably recovered executable without mutable-manager escape. Snapshot teardown
  abandons state without token allocation, while client cancellation still returns a safe token;
  and committed schema incompatibility has a distinct terminal phase, precise subscription reason,
  invalidated resume state, and durable checkpoint/reopen representation; and a bounded reactor-
  facing service owns SQL validation, snapshot/READY/live/ack/cancel/resume transitions, exact
  response-ring backpressure retry, disconnect cleanup, and resumable shutdown drain.

The source-retention boundary now component-wise intersects storage/Raft safety with every durable
plan frontier and validates committed placement epochs and local replica membership before invoking
a source-specific batch reclaimer. Logical subscription positions are not fabricated into physical
WAL offsets. The WAL-backed implementation now validates the complete topology-bound batch, maps
each distinct durable writer frontier through a full retained-namespace scan, uses the minimum for
tablets sharing a WAL, and only then performs idempotent whole-segment cleanup.

Homogeneous Raft-backed historical subscriptions now register before acquiring exact immutable
applied tablet snapshots and execute one global vector pipeline. The Raft reclaimer binds the fixed
tablet/group/epoch topology to the worker-hosted application, requires both its publication and
durable application-snapshot boundary to cover every authorized index, and then schedules the
implemented node-wide all-group checkpoint/reclamation on the sole durable worker. Mixed WAL/Raft
historical execution now treats the registered continuation vector as the cross-authority product
boundary and executes one global pipeline over canonical raw sources without inventing a scalar
epoch. The retention authority now supports bounded dynamic plan registration before service
activation and explicit retirement after service drain while preserving monotonic reclamation.

### Phase 12 — performance architecture and io_uring

Portable `ReactorBackend` selection retains epoll as the reference. The opt-in Linux/liburing owner
now submits accept, receive, send, and response-wakeup operations directly through io_uring while
preserving the portable connection state, bounded buffers, shard queues, and partial-I/O semantics.
One operation per connection and CQE-before-reclamation ownership keep native references bounded;
unsupported builds, kernels, or host policies fail explicitly without fallback.
`ThreadPlacement` adds optional CPU and NUMA hooks; empty configuration is correctness-neutral and
unsupported NUMA/portable affinity fails explicitly. Parallel query workers accept one exact
placement per selected worker and use an all-worker startup gate: no pipeline executes unless every
placement succeeds, and concurrent failures resolve by lowest worker ordinal.

A focused Ubuntu 24.04/GCC/liburing 2.5 production build passed with warnings as errors. Focused
Linux 6.12 tests passed for fragmented accept/read, handshake/query routing, response wakeup,
ordered result/terminal sends, and shutdown. Dense non-null identity timestamp filters now dispatch
between exact scalar, AVX2, and AArch64 NEON kernels; NEON differential tests ran locally and AVX2
passed an x86_64 warnings-as-errors compile-only check. No epoll/io_uring or SIMD comparison, NUMA
experiment, allocation profile, or performance campaign ran.

### Phase 13 — system-time history and corrections

The existing SQL parser/binder/executor already carried `FOR SYSTEM_TIME AS OF` into a snapshot
provider. `TemporalSnapshotProvider` now supplies real committed logical history for one exact
schema: atomic commit batches, distinct event/receive/system time, original/correction/replacement/
tombstone versions, current and historical winner selection, copied stable snapshots, finite state,
and precise history-expired failure.

CSEG v1 remains correctly frozen to `APPEND_ROWS`. Temporal Mutation Command v1, CSEG v2, and
Manifest v2 now durably encode, validate, install, select, resolve, and recover WAL/Raft-neutral
version histories without reinterpreting v1 bytes. The WAL startup owner restores every selected
distinct-table tablet under one global checkpoint, exact-verifies routed overlap, applies only each
tablet's suffix, and retains all providers/generation/locks until complete unpublished recovery.
Same-table multi-tablet routing fails explicitly because command v1 lacks tablet identity. Vector
publication now has a bounded query-accounted scalar-snapshot source that copies current/as-of
winners into canonical owned chunks. Direct vector winner resolution/lowering, Raft/mixed-source
composition, v1 migration, and authorized compaction/retention integration remain, so the complete
Phase 13 exit is not claimed. In-memory compaction now keeps the global time-index predecessor needed
at the exact retained boundary, rejects both frontier regressions, and reports exact removals; this
does not yet authorize durable part replacement or reclamation.

### Phase 14 — deterministic Raft

`RaftNode` implements follower/candidate/leader roles, term/vote/log/snapshot/commit/applied state,
runtime-triggered elections, log freshness, RequestVote, heartbeat/AppendEntries, log match,
conflict hints/rewind, next/match indexes, majority commit with current-term restriction, stale-term
rejection, leader demotion, bounded proposals, and committed-unapplied exposure. Transitions carry
an explicit persist-before-send state copy.

The focused minimum gate passes for 3-node election, one committed command, leader loss, replacement
leader, a second command, stale-leader rejection, and restarted follower catch-up. Joint membership,
two-stage snapshot installation, and a current-term quorum read barrier are implemented. Physical
disk persistence fault matrices, production tablet read integration, broader snapshot transfer, and
long/exhaustive randomized simulation remain incomplete.

Later work accepted and implemented a bounded canonical group/source/destination Raft transport
envelope for every current vote, append, snapshot, and read-barrier message.
Header-first stream ownership now validates the fixed header before exact frame allocation, returns
precise consumed prefixes for coalesced input, and owns complete output across short writes.
An authenticated receiver now rejects trust and route failures before durable runtime admission and
encodes responses only from the asynchronous owner's post-synchronization completion.
A persistent inbound mutual-TLS carrier now processes fragmented sequential frames with one bounded
durable operation/result in flight and leaves multi-peer output and snapshot routing to its owner.
A bounded TCP listener/poll table now admits those persistent sessions and retains each complete
post-sync result until explicit embedding pickup.
Stable connection IDs and borrowed descriptor interests now let the same server run under one outer
poll owner without exposing compacting table indexes.
Each authenticated inbound message now executes with an immediately following observation in one
durable FIFO batch and retains that exact post-message role and term for timer rearming.
A persistent outbound mutual-TLS carrier now validates exact peer routes, bounds FIFO frames and
bytes, and returns complete original frames for duplicate-safe reconnect after failure.
A bounded exact-peer carrier pool now preflights every durable-result destination and aggregate
queue demand before routing, and returns failed carriers with their complete retry frames instead
of silently dropping or replacing them.
One nonblocking exact-route TCP attempt now retains complete retry frames through connection,
creates TLS only after authoritative connect completion, and transfers descriptor/carrier ownership
together into that pool. One exact peer now has capped monotonic reconnect backoff and complete frame
custody between attempts. A fixed multi-peer manager exposes exact descriptor interests, installs
and recycles pairs, and rejects unroutable fresh results without consuming them.
A bounded injected-time scheduler now emits generation-tagged election and heartbeat actions,
preserves due work across admission backpressure, and rejects stale completion rearming.
A bounded driver now submits each timer action plus its ordered observation through the asynchronous
durable owner and retains every post-sync transition for embedding-owned transport/snapshot routing.
The asynchronous durable owner now publishes a portable nonblocking completion descriptor after
each owning result, allowing one event loop to wait on storage progress and sockets together.
Timer, connect, reconnect, TLS, peer, and inbound-server owners now expose exact earliest monotonic
deadlines so that wait cannot overrun a consensus or transport timeout.
Nonzero runtime-lifetime FIFO submission identities now propagate through timer and inbound results,
preventing reusable slots or connection-table order from reversing durable work.
Inbound terminal closure now preserves an already admitted durable operation until its owning result
is taken, rather than discarding possibly persisted state with the socket.
Outbound terminal closure now immediately transfers whole retry frames into capped reconnect instead
of spinning on a terminal descriptor until its later TLS deadline.
Outbound Raft TLS now starts with write readiness so a real poll loop emits ClientHello before
following OpenSSL's subsequent readiness state.
One bounded unified runtime now polls durable wakeups, listener/inbound sessions, and outbound peers;
clamps waits to exact deadlines; merges inbound/timer results by FIFO submission identity; rearms
activity; routes with backpressure; and retains application/snapshot/read work for explicit pickup.
Focused gates cover synchronized election wakeup and a real two-node mutual-TLS vote round trip.
A bounded deterministic simulator now records explicit and seeded virtual-network, crash/restart,
atomic persistence-fault, application, membership, and snapshot actions; it runs an independent
election/log/commit/leader-completeness checker after each step, replays exact traces, and performs
bounded deletion shrinking. Focused coverage runs 4,000 generated actions reproducibly; long and
exhaustive campaigns, clock changes, and physical-log syscall faults remain hardening work.
The core now also rejects zero terms and noncanonical vote/append predecessor and response state
before observing a higher term, preserving the persist-before-response contract on invalid input.

Metadata Application Snapshot v1 now provides a bounded canonical structural codec for exact
metadata/schema application entries and internal Raft gaps under one complete snapshot membership
identity. A locked storage owner exact-installs those bytes, and the metadata application owner now
installs them before Raft compaction and exact-reopens a compacted catalog from the validated
snapshot plus committed suffix. Node-wide physical-log reclamation now installs a checksummed
all-group recovery anchor before removing old segments. Tablet and metadata snapshot owners also
retain only the exact durable Raft authority and reclaim older or crash-orphaned future files.
Additive Snapshot 1.1 retains exact Tablet Group Binding v1 type-4 entries, giving routing an
immutable committed tablet-to-group identity without changing Metadata Command v1 or minor-0 bytes.
Replicated-ingest coordination now derives that route from committed metadata, obtains an ordered
local role/term observation, exact-compares stable membership with placement, and term-fences the
proposal. It also binds the complete command shape to the committed active schema on both sides of
the observation. Callers no longer supply group, term, or schema authority.
An address-stable outer service owner now composes tablet and metadata applications, their shared
durable worker, and that coordinator through create, shutdown, and exact reopen.
A bounded queue-facing adapter now connects negotiated reactor tasks to that coordinator, preserves
one exact response across response-ring saturation, exposes wakeup information, and drains admitted
work after shutdown closes new admission. Packaged daemon configuration remains external.
A database-level recovery owner now holds Database Bootstrap authority, uses explicit resident group
membership to recover the committed global metadata catalog, reconstructs only local tablet/retry
owners, and reopens the asynchronous runtime before exposing service access.
The external resident group/voter set now has a strict bounded canonical text parser; daemon file
loading and explicit mode selection now compose it with Protocol 2 advertisement, local single-voter
election, reactor routing/wakeup, and ordered shutdown.
Authenticated Raft deployment routes now also have a separate strict bounded text parser binding
ordered node IDs to unique IPv4 endpoints, TLS server identities, and unique leaf-certificate
SHA-256 fingerprints. One immutable authority maps an exact verified fingerprint/address pair to
that configured node ID for both inbound and outbound carriers. An address-stable service owner now
composes those authorities and per-peer TLS contexts with the existing receiver, listener,
reconnect pool, randomized timers, and unified poll runtime, arming each group only from a durable
ordered observation. The packaged daemon now loads the complete peer/TLS option bundle, validates
voter coverage, runs the transport on its sole poll thread, and destroys it before the durable
database. Snapshot-install and read-barrier results remain explicit fail-closed process gaps.

### Phase 15 — Multi-Raft tablets and metadata

`MultiRaftRuntime` multiplexes bounded logical groups on one owner with group-tagged messages,
node-global physical persistence sequences, persist-before-send batches, independent application,
and reopen state. The checksummed Multiplexed Raft Persistent-State Record v1 encodes group identity,
term, vote, logical log, commit/applied indexes, manifest generation, and part-set checksum without
native struct serialization.

`MetadataStateMachine` applies nodes, complete immutable table-schema generations and SQL names,
complete partition/retention/history/lateness/retry policy, tablet
placement/replicas/epochs/leader hints, and legacy retention only at consecutive committed
metadata-group indexes. Focused tests cover deterministic schema bytes, strict damage rejection,
legal schema succession, different leaders, group isolation, one-node loss, reopen, metadata order,
and record corruption.

A single-owner segmented file/fsync/recovery log, bounded asynchronous persistence worker, committed
tablet state-machine adapter, joint-consensus membership, exact application snapshots, and checked
leader quorum-sync/application receipts are implemented. The shared physical log now performs
caller-triggered all-group checkpoint reclamation without crossing group authority. Protocol 2.0
now negotiates QUORUM_SYNC and carries the exact receipt-shaped acknowledgement without changing
v1 bytes; replicated service advertisement/execution remains. Measured group-aware scheduling is
deferred. Database namespaces, catalog tombstones, and placement-driven membership orchestration
still need accepted durable/runtime composition before being added.

### Phase 16 — distributed query and rebalancing

The query target implements event-time tablet pruning, explicit read-consistency enum values,
mergeable COUNT/SUM/MIN/MAX/Welford partial state, bounded MPMC exchange, backpressure, cancellation,
duplicate detection, worker failure, and a coordinator that refuses partial success.

`TabletMovement` enforces add target as learner, bounded checksummed retryable snapshot transfer,
catch-up through the snapshot index, placement-epoch-checked target promotion, and only then source
removal. Corrupt, gapped, or conflicting retry chunks fail closed.

Subsequent work added proof-bound dispatch/response protocols, authenticated TCP/mTLS clients and
servers, compatible pinned multi-tablet scheduling, whole-query cancellation/deadlines, finite
explicit rebinding, authenticated leader hints, durable physical movement ownership, joint Raft
membership coordination, and reader-pinned source retirement/reclamation. A focused real-mTLS gate
now returns the identical aggregate before and after a learner-first movement. Committed metadata
now supplies the active schema, placement, immutable tablet-to-group authority, and exact IPv4/TLS
routes used by distributed execution. Leader-linearizable construction obtains ordered
group-correlated quorum read-barrier observations, requires metadata and tablet publications to
cover those barriers, binds one compatible Manifest epoch, and creates the complete TCP execution
lifecycle. A distinct bounded-stale constructor applies the same catalog, Manifest, route, and
execution gates to stable same-term leader/follower observation pairs. General vector-plan row
fragments/exchanges and multi-key grouping remain incomplete. The supported FLOAT64
grouped surface now applies key ORDER BY, explicit null placement, and LIMIT only after global merge,
providing correct top-N for that key. It also orders globally merged COUNT/SUM/extrema/mean/
population variance with deterministic group-key ties before LIMIT. A distinct canonical frame now carries one nullable
FLOAT64 group key and mergeable partial with SQL-equivalent signed-zero/NaN canonicalization. An
authenticated mutual-TLS carrier owns its bounded ordered response stream, and a deadline-bound
outbound TCP composite owns one validated connection attempt. A bounded inbound TCP server owns
finite admission and stable carrier/descriptor lifetime, and a production owner composes it with
the real-CSEG grouped worker. A finite sender owns complete response-vector correlation and
whole-attempt retry/backoff. A compatible grouped binder retains one pinned Manifest epoch across
every plan-ordered, schema-proved dispatch. A portable execution owner now retains that snapshot,
owns one finite sender per tablet, delivers complete terminal streams once, and preserves the
coordinator's all-tablet result boundary. A pinned multi-tablet TCP scheduler prevalidates complete
routes, drives plan-ordered attempts and due retries, rotates bounded address candidates, releases
all clients on terminal failure/deadline/cancellation, and publishes only the complete grouped
result. A packaged leader-linearizable grouped constructor now carries correlated barriers and one
committed catalog/Manifest authority through exact FLOAT64 specialization, route resolution,
execution, and TCP ownership. A distinct bounded-stale grouped constructor carries canonical
leader/follower authority through the follower binder and same lifecycle while preserving the
proved follower target. A packaged lifecycle now retains plan/Manifest ownership through
placement-backed authenticated remote observation acquisition and transfers only its complete
canonical authority vector into grouped execution. Retryable grouped failure now permits only a
finite explicit whole-query replacement with identical logical shape and group-key, a nonregressing
Manifest generation, unchanged deadline/budget, discarded old partials, and cumulative metrics.
A distinct bounded multi-key/all-type group-state frame now covers exact key tuples, nested
sufficient states, empty tablets, and partial-I/O ownership; worker accumulation, stream
coordination, transport, and global merge remain incomplete. Replicated Native SQL now
executes that broader grouped surface through a bounded row-backed coordinator physical pipeline,
including computed keys/inputs, global ordering, and LIMIT, while retaining the existing exchange
bytes and all-tablet authority gate. A distinct
canonical observation protocol, authenticated receiver, mTLS clients/servers, finite multi-address
acquisition, correlated
leader/follower pairs, canonical all-group batches, placement-backed construction, and packaged
query lifecycle now provide remote follower authority acquisition. Committed numeric or
lowercase-DNS routes acquire a fresh
bounded ordered unique IPv4 candidate set before polling, and finite sender retries rotate
candidates without changing node/proof/TLS authority. Live DNS churn/latency/cache policy, IPv6, a
packaged multi-process runtime and broad failure/measurement evidence remain incomplete; the Phase
16 exit gate is not claimed. A focused in-process gate now transfers a checksummed real CSEG,
reopens it from a distinct target root, and queries the promoted target through production mTLS.

### Phase 17 — object storage and interoperability

`chronos_tiering` defines S3-compatible immutable `put_if_absent`/`stat`/`get_range` semantics and a
deterministic memory implementation. A subsequent production libcurl backend adds SigV4,
TLS-by-default, finite timeouts and response bounds, checksum metadata, conditional immutable PUT,
exact retry verification, and exact range responses. `TieredPartManager` checks SHA-256 content,
verifies remote metadata, calls the atomic manifest installer before allowing local release, rejects
part/key identity conflicts, caches bounded complete objects with eviction, and supports
authenticated range reads for larger objects. Upload admission now reuses the full Manifest-v1
CSEG validator with the exact schema, tablet, descriptor, and WAL source before any remote request
or manifest callback. A smoke test exposed and fixed a 32-bit constant-expression overflow that
made the default 4 GiB object limit zero.

Manifest v1/v2 bytes remain unchanged. A subsequent Cold Location Manifest v1 codec binds bounded
object keys and deployment store identity to exact Manifest v2 part length/SHA-256 without trusting
listings. A dedicated locked storage owner now exact-readback installs synchronized immutable
add-only generations and recovers the highest consecutive generation without fallback, always
binding it to an exact Manifest v2 value. One release/acquire tiered publication now exposes and
retains a complete compatible Manifest-v2/cold pair to concurrent readers. A fixed checksummed pair
commit record now makes exact already-durable component generations crash-selectable while ignoring
higher uncommitted finals. Reader-pinned remote reclamation requires selected authority to omit the
logical part, route, and key, then preflights metadata before exact conditional deletion. After a
crash, a startup-only pass uses immutable consecutive cold generations as the durable garbage
journal, exact-binds each one to its historical Manifest/catalog authority, and retries partially
completed deletion without bucket listings. The S3 carrier now gives every replay-safe operation a
finite capped-backoff attempt budget and fresh signature, and a concurrent caller-supplied provider
is force-refreshed after 401/403. Delta-seconds Retry-After hints are honored only within the
configured maximum backoff. An explicit built-in provider now snapshots and validates the
standard AWS environment credentials and fails closed on forced refresh. Ambient proxy variables
are disabled; proxy use requires one bounded credential-free HTTP(S) authority. Workload/instance
and ordered-chain provider integrations, retry jitter, authenticated proxies, parallel multipart
scheduling, and broader Arrow/Parquet external fixture and resource-fault qualification remain incomplete. The
bounded full-object LRU now permits concurrent post-install reads without holding
its cache mutex across object-store I/O. It is intentionally volatile: restart transactionally
restores an exact-metadata-preflighted authoritative catalog and rebuilds cache bytes only on
verified demand, so no second durable cache index is required. Large objects now
use sequential signed parts, conditional completion, exact final verification, and failure-path
abort. HTTP-200 completion bodies are now strictly classified as one top-level success result or an
embedded/malformed error before exact HEAD reconciliation. The optional Arrow/Parquet
provider now round-trips all current logical types through an exact
caller-supplied schema and keeps CSEG as the primary store.

Reader-pinned local reclamation now accepts both WAL- and Raft-owned temporal parts after proving
exact part/tablet source agreement. The WAL case has focused remote validation, synchronized
deletion, tier-aware restart recovery, and post-restart remote query coverage; standalone local-only
recovery remains intentionally invalid after either source is reclaimed.

## End-to-end integration state

`chronos_feature_smoke_tests` connects committed metadata, temporal visibility, live handoff,
distributed partial aggregation, a committed single-group Raft command, verified object upload,
manifest callback, cache/range read, and byte-identical result in one process. Separate deterministic
tests cover the requested 3-node Raft failover and Multi-Raft different-leader cases.

A packaged `chronosd` lifecycle now owns the bounded native reactor and worker handoff. Its default
and single-node modes preserve Protocol 1 behavior. An explicit replicated mode securely loads
resident group membership, recovers committed metadata/tablets, advertises Protocol 2 QUORUM_SYNC,
and routes bounded ingest/cancellation/completion work. A Linux-only process gate performs a real
loopback applied write and matching retry after daemon restart. A subsequent Linux-only gate starts
three actual daemons over distinct mutual-TLS identities, obtains a quorum-applied write, kills its
tablet leader, requires a higher-term matching retry from the surviving quorum, and reopens the
same applied/retry state from all three retained roots. The broader requested
three-process/data-plane workflow remains incomplete. A later focused
gate uses real mutual-TLS query sockets around the complete movement state machine, but simulates the
externally committed promotion/removal milestones and deterministic worker aggregates. A separate
one-process service gate now carries a checksummed real CSEG through movement, installs and reopens
it from a distinct target root, and queries the promoted target through the production mTLS worker
stack. No gate executes the broader historical SQL, distributed query, subscription, movement,
CSEG, and object-storage sequence across those three processes. Those remain high-priority
integration and hardening tasks, not passed checks.

## Public APIs and formats

Important new public targets are `chronos::live`, `chronos::runtime`, `chronos::raft`, and
`chronos::tiering`; `chronos::query` gained temporal/distributed APIs and `chronos::network` gained
explicit backend selection.

Important APIs include `SubscriptionManager`, `WindowedMaterializedView`,
`IncrementalAggregateSet`, `TemporalSnapshotProvider`, `RaftNode`, `MultiRaftRuntime`,
`MetadataStateMachine`, `TabletMovement`, `BoundedExchange`, `DistributedAggregateCoordinator`,
`ObjectStore`, `TieredPartManager`, `Reactor`, and `apply_current_thread_placement`.

Accepted formats added during and after the pass include authenticated Resume Token v1/v2,
source-tagged Multi-tablet Subscription Checkpoint v2, and Native Protocol 1.2 source-tagged
subscription changes,
Multiplexed Raft Persistent-State Record v1, Raft transport, distributed exchange, temporal
CSEG/Manifest v2, cold-location authority, tablet and metadata application snapshots, and the
node-wide Raft recovery anchor. Native Protocol 2.0 adds feature-gated QUORUM_SYNC without
reinterpreting WAL v1, Columnar Batch v1, CSEG v1, Manifest v1, or native Protocol v1 bytes.

## Checks actually performed

Targeted configure/build commands used the existing `dev` preset and `--parallel 4`:

- `cmake --preset dev`
- `cmake --build --preset dev --target chronos_live_tests --parallel 4`
- `cmake --build --preset dev --target chronos_query_tests --parallel 4`
- `cmake --build --preset dev --target chronos_raft_tests --parallel 4`
- `cmake --build --preset dev --target chronos_tiering_tests --parallel 4`
- `cmake --build --preset dev --target chronos_network_tests chronos_runtime_tests --parallel 4`
- `cmake --build --preset dev --target chronos_feature_smoke_tests --parallel 4`
- Ubuntu 24.04/GCC 13: `chronos_network` with `CHRONOS_ENABLE_IO_URING=ON` and warnings as errors.

Focused executions passed:

- `chronos_live_tests`: 10 tests;
- `chronos_query_tests --gtest_filter=TemporalSnapshotTest.*:DistributedQueryTest.*`: 4 tests;
- `chronos_raft_tests`: 12 tests after metadata integration and final hostile-input audit fixes;
- `chronos_tiering_tests`: 17 tests;
- `chronos_network_tests`: 31 tests, including the backend-selection test;
- `chronos_runtime_tests`: 1 test; and
- `chronos_feature_smoke_tests`: 1 test.
- Linux 6.12/liburing 2.5 `IoUringReactorTest.*`: 2 focused tests.
- Continued Raft work: `chronos_raft_tests`, 123 tests, including transport composition, metadata
  snapshot recovery, shared-log reclamation, and application-snapshot reclamation.
- Continued ingest work: `chronos_ingest_tests`, 95 tests, including repeated tablet snapshot
  compaction/recovery and authoritative file reclamation.
- Continued lightweight integration: `chronos_feature_smoke_tests`, 1 test.
- Protocol 2.0 continuation: 52 non-socket network tests, 22 service tests, 5 runtime tests, and the
  1-test feature smoke passed. The three loopback `TcpSocketTest` cases could not bind inside the
  workspace sandbox and were excluded after their environment-specific failure was confirmed.
- Worker-affine Raft continuation: all 125 Raft tests, 3 non-socket authenticated receiver tests,
  5 focused synchronous tablet application tests, and 8 concrete asynchronous tablet owner tests
  passed. Both real-socket unified transport tests rebuilt but could not bind/listen inside the
  workspace sandbox.
- Replicated ingest continuation: 2 focused service tests passed for exact applied/matching-retry
  acknowledgements and required-leader-term rejection.
- Replicated coordinator continuation: 2 focused service tests passed for bounded admission,
  cancellation, correlated completion, deadline error, metrics, and negotiation rejection.
- Negotiated task continuation: the SPSC context test and affected non-socket network tests passed;
  real-socket protocol-v2 propagation remains in the deferred environment matrix.
- Worker-extension composition continuation: 11 focused Raft tests passed for bounded definitions,
  direct child identity, ordered batch callbacks, reverse shutdown, partial-initialization cleanup,
  throwing-shutdown continuation, and terminal completion failure.
- Worker-affine metadata continuation: 5 focused Raft tests passed for applied publication,
  untouched-group isolation, pre-admission retained-log reconstruction, exact installed-snapshot
  recovery, retained immutable snapshot lifetime, and terminal corrupt-command handling.
- Tablet-group authority continuation: 20 focused Raft tests passed for binding codec damage and
  version rejection, immutable ordered application, retained-log and Snapshot 1.1 recovery,
  minor-0 isolation, compaction, owning projection, and asynchronous publication.
- Packaged replicated-ingest continuation: all 39 service tests passed, `chronosd` built, and the
  Linux-only real-socket process test source passed a standalone syntax check on macOS. The new
  process case itself remains a Linux execution gate.
- Replicated distributed-query continuation: all 339 query tests and 54 service tests passed after
  committed metadata binding, exact route resolution, group-correlated read authority, compatible
  Manifest binding, and packaged leader-linearizable and bounded-stale TCP lifecycle construction.
  The full incremental build and installed external-consumer check also passed.
- DNS/multi-address query routing continuation: `chronos_network_tests` and `chronos_cluster_tests`
  built cleanly; all 11 affected `TcpSocketTest` and `DistributedQueryTcpExecutionTest` cases passed,
  including fresh `localhost` resolution and real-mTLS retry from a refused first address to a live
  second address. The socket subset required the approved host execution boundary because the
  workspace sandbox prohibits loopback bind. The full incremental `dev` build and installed
  external-consumer check also passed after the public resolver was added to that consumer.
- Raft-observation transport continuation: `chronos_cluster_tests` built cleanly and all 4 focused
  codec/receiver cases passed for correlation, canonical observation state, damage/version/bounds,
  authentication/authorization ordering, service failure, and exception containment. The full
  `dev` build and installed external-consumer check passed. Stream/TLS carrier and multi-node pair
  acquisition were not claimed.
- Raft-observation partial-I/O continuation: all 6 focused observation cases passed, including
  every request/success/failure split, coalesced response boundaries, header-gated length rejection,
  sticky failure, partial-reader state transfer, and validated move-only short writes. The stream
  owners and installed external-consumer check passed; TLS carrier or remote pair acquisition
  completion is not claimed.
- Raft-observation outbound-mTLS continuation: 3 focused real/portable cases passed for exact
  authenticated acquisition, principal denial before request writing, and sticky exact deadlines.
  TCP connection ownership, inbound serving, fan-out, and pair acquisition are not claimed.
- Raft-observation outbound-TCP continuation: 2 focused cases passed for a real nonblocking
  TCP/mTLS/correlated exchange and route-mismatch plus exact connect-deadline failure. The exact
  cases required approved host execution because workspace sandbox policy forbids loopback bind.
  Inbound serving, multi-address retry, fan-out, and pair acquisition are not claimed.
- Raft-observation inbound-mTLS continuation: 3 focused cases passed for one exact authenticated
  exchange/service invocation, client-principal rejection before service dispatch, and sticky exact
  deadlines. Listener admission, fan-out, and pair acquisition are not claimed.
- Raft-observation TCP-server continuation: 2 focused approved-host cases passed for a real
  TCP/mTLS observation with exact metrics and a one-connection admission cap plus deterministic
  shutdown. Remote multi-address retry, fan-out, and pair acquisition are not claimed.
- Raft-observation retry continuation: 2 focused approved-host cases passed for refused-first,
  live-second finite address rotation with exact attempt/service metrics and for duplicate/node
  route rejection plus active-attempt cancellation. Fan-out and pair acquisition are not claimed.
- Raft-observation pair continuation: 3 focused approved-host cases passed for concurrent
  leader/follower mTLS acquisition with no partial result and for rejection of two individually
  valid but different-term observations, plus survivor cancellation after a leader failure.
  Catalog-wide multi-pair acquisition is not claimed.
- Raft-observation route continuation: 1 focused case passed for committed numeric and DNS endpoint
  resolution with exact port/TLS binding, duplicate-target and missing-TLS rejection, and the hard
  selected-route limit. DNS caching, TTL, IPv6, and catalog-wide follower selection are not claimed.
- Raft-observation batch continuation: 1 focused approved-host case passed for two canonical groups
  over four concurrent mTLS exchanges, exact service/result/terminal metrics, all-pair cancellation,
  and duplicate-group rejection. Automatic placement-backed pair selection is not claimed.
- Raft-observation batch-construction continuation: 1 focused case passed for canonical planned
  group derivation, local/fallback follower selection, unique committed routes, exact correlations,
  and overflow rejection. Packaged acquisition/query composition is not claimed.
- Packaged remote bounded-stale continuation: 1 focused approved-host service case passed for two
  real mTLS observations, exact authority-to-metadata/Manifest binding, transition into a TCP query
  owner, cross-phase metrics, and execution cancellation. A real query response is not claimed.
- Request-local query-worker continuation: 1 focused service case passed for a real Raft-sourced
  temporal CSEG, owning Manifest/schema/placement/group/barrier acquisition, exact filtered terminal
  aggregate state, and fresh group/placement rejection. Remote socket execution is not claimed.
- Owned real-CSEG query-service continuation: 1 focused approved-host case passed for a canonical
  dispatch over real loopback mutual TLS, exact certificate-principal/node authorization,
  request-local proof revalidation, the installed-CSEG terminal aggregate, and ordered shutdown.
- Grouped-exchange continuation: 2 focused cases passed for the frozen nullable-FLOAT64 key/partial
  layout, signed-zero/NaN/NULL canonicalization, exact decoding, integrity/version rejection, and
  hostile checksum-valid noncanonical representations. Grouped planning is not claimed.
- Grouped partial-I/O continuation: 1 focused case passed for all 137 split positions, coalesced
  successor ownership, sticky corruption, exact short-write suffixes, overrun rollback, and
  move-only cursor transfer. That carrier slice did not claim grouped transport or coordination.
- Empty grouped-terminal continuation: 1 focused case passed for the distinct terminal-only layout,
  exact identity/sequence/CRC round trip, and truncation/version/reserved/input rejection. It does
  not fabricate a NULL group; that terminal-codec slice did not claim grouped coordination.
- Grouped-terminal partial-I/O continuation: 1 focused case passed for all 65 split positions,
  coalesced successor ownership, sticky corruption, multiple exact short-write advances, overrun
  rollback, and move-only cursor transfer. That byte-ownership slice did not claim stream
  discrimination, transport, or grouped coordination.
- Grouped-coordinator continuation: 4 focused cases passed for canonical signed-zero/NaN/NULL
  grouping, exact retries and cross-form conflicts, gaps/post-terminal rejection, terminal-only
  empty tablets, bounded history, first-failure stability, completed-worker loss, all-tablet
  closure, and final merge overflow. That coordinator slice did not claim grouped fragments or
  authenticated transport.
- Grouped-fragment-intent continuation: 2 focused cases passed for the distinct outer layout,
  nested Fragment v1 preservation, exact authority/key-index round trip, corruption/version/
  reserved/length/key-bound rejection, and inherited projection limits. Authority/type binding,
  executable grouped dispatch, and worker execution are not claimed.
- Grouped-fragment-binding continuation: 1 new focused case and all 7 binding cases passed against a
  real pinned Manifest v2 snapshot for exact group/query/database/projection ownership, same-column
  FLOAT64 key/aggregate support, and timestamp/out-of-bounds key rejection. Executable grouped
  dispatch and worker execution are not claimed.
- Grouped-fragment-dispatch continuation: 2 new focused cases and all 4 dispatch cases passed for
  distinct magic, exact group/nested-intent round trip, full integrity, bidirectional ungrouped
  confusion rejection, and damage/version/nil-group rejection. Worker execution and authenticated
  grouped transport are not claimed.
- Grouped-worker continuation: the focused real-Manifest-v2/real-CSEG worker case passed for reused
  local group/placement/barrier/schema/snapshot gates, two canonical grouped partials, contiguous
  terminal sequencing, event-filtered terminal-only empty output, and route rejection before loader
  I/O. Authenticated grouped transport and packaged multi-tablet execution are not claimed.
- Packaged grouped-dispatch continuation: the focused real-Manifest-v2 binding case and all 7
  binding cases passed for direct construction of the canonical grouped dispatch from one complete
  authority input, exact group preservation, encodability, and unchanged unsupported-key status.
  The installed-consumer gate references the new public constructor. Authenticated grouped
  transport and packaged multi-tablet execution are not claimed.
- Grouped-query-transport codec continuation: 2 focused cases passed for exact grouped request,
  partial, terminal-only, failure, all-status, correlation, advisory-hint, nested-damage,
  future-version, type-confusion, and payload-kind substitution behavior. The installed-consumer
  gate covers both public codec directions. Authenticated receiver/partial-I/O and packaged
  multi-tablet grouped execution are not claimed.
- Grouped-query partial-I/O continuation: 3 new stream cases and all 5 grouped transport cases passed
  across every request split, every partial/terminal/failure response split, coalesced successors,
  sticky damage, checksum-valid oversized header rejection, and move-only checked short writes. The
  installed-consumer gate covers both readers and the cursor. Authenticated receiver, sender, and
  packaged multi-tablet grouped execution are not claimed.
- Authenticated grouped-receiver continuation: 1 new case and all 6 grouped transport cases passed
  for auth/source/target ordering, complete two-part and terminal-only publication, exact committed
  leader-hint lookup, contiguous-sequence rejection, exception containment, and response-frame
  exhaustion. The installed-consumer gate covers receiver construction. Production real-CSEG
  service adaptation and multi-response TLS/TCP/sender ownership are not claimed.
- Grouped real-CSEG service continuation: the focused production worker case passed with a fresh
  grouped request-local authority acquisition and exact terminal key/sum `2.5` from the installed
  Manifest-v2/CSEG data. The installed-consumer gate covers grouped service construction. Grouped
  TLS/TCP, sender/coordinator, and packaged multi-tablet execution are not claimed.
- Owned grouped real-CSEG receiver continuation: the same focused case passed a canonical
  authenticated grouped request through the move-only packaged worker/receiver owner and
  exact-decoded terminal key/sum `2.5`, with a second fresh authority acquisition. The
  installed-consumer gate covers owner construction. Grouped TCP and sender/coordinator execution
  are not claimed.
- Grouped mutual-TLS continuation: 2 focused cases passed for a real nonblocking mutual-TLS
  socket pair carrying two ordered correlated response frames, exact certificate-fingerprint and
  principal/node authorization, one receiver invocation, terminal-only response publication,
  invalid frame-limit rejection, and a sticky exact handshake deadline. The installed-consumer
  gate covers both public carrier constructors. TCP connection/listener ownership,
  sender/coordinator integration, and packaged multi-tablet execution are not claimed.
- Grouped outbound-TCP continuation: 2 focused cases passed for a real nonblocking loopback connect
  followed by the complete two-frame mutual-TLS stream, both authenticated certificate
  fingerprints, exact route/principal/node binding, invalid frame-limit rejection, and sticky exact
  connect-deadline closure with descriptor release. The installed-consumer gate covers the public
  client constructor. Inbound listener/server ownership, sender/coordinator integration, and
  packaged multi-tablet execution are not claimed.
- Grouped inbound-TCP continuation: 2 focused cases passed for a real grouped-client loopback query
  returning the complete two-frame stream with exact authentication and completion metrics, plus a
  one-slot/two-connection admission gate proving one explicit rejection, bounded active state,
  invalid configuration rejection, and deterministic shutdown. The installed-consumer gate covers
  the public server constructor. Production real-CSEG service composition, sender/coordinator
  integration, and packaged multi-tablet execution are not claimed.
- Owned grouped real-CSEG TCP-service continuation: the focused real-Manifest-v2/CSEG service case
  passed a canonical grouped dispatch through the production loopback TCP/mTLS stack and returned
  exact terminal key/sum `2.5` after a fresh owning authority acquisition. It also proved both
  certificate fingerprints, invalid packaged configuration rejection, one completed connection,
  ordered shutdown, and installed-consumer construction. The same focused service case now transfers
  the exact CSEG through learner-first movement, reopens a distinct target root, and obtains the
  identical result from promoted node 13 over a second production mTLS owner. Multi-process and
  multi-tablet moved-CSEG reads are not claimed.
- Grouped-sender continuation: 2 focused cases passed for exact immutable attempt bytes, complete
  two-part and terminal-only success, payload-level correlation rejection without state mutation,
  advisory leader capture, exact capped exponential backoff, transport failure, terminal status,
  attempt exhaustion, and no result publication across retries. The installed-consumer gate covers
  sender construction. Coordinator delivery, multi-tablet TCP scheduling, and packaged grouped
  execution are not claimed.
- Compatible grouped-snapshot continuation: the focused two-tablet binding case passed for one
  retained Manifest generation, exact plan/group/tablet order, shared FLOAT64 key proof, and direct
  grouped dispatch construction, plus out-of-bounds and TIMESTAMP key rejection. The
  installed-consumer gate covers the public binder. Coordinator execution, TCP scheduling, and
  packaged grouped construction are not claimed.
- Grouped execution-owner continuation: 2 focused cases passed for retained Manifest generation,
  one sender per compatible dispatch, terminal-only closure, no result before all tablets close,
  exact-once coordinator delivery, duplicate/foreign-tablet rejection, nonterminal retry backoff,
  and exact terminal failure propagation. Header self-containment and the installed-consumer gate
  cover the public owner. Multi-tablet TCP scheduling and packaged grouped construction are not
  claimed.
- Grouped TCP-scheduler continuation: 2 focused cases passed for two plan-ordered real-mTLS tablet
  servers, a refused first address and finite candidate rotation, same-key cross-tablet merge,
  exact attempt/retry/transport metrics, zero residual clients, complete route prevalidation,
  no-I/O deadline expiry, and active-client cancellation. Header self-containment and the installed
  consumer cover the public scheduler. Packaged grouped construction and explicit whole-query
  rebinding are not claimed.
- Packaged leader-linearizable grouped-construction continuation: the focused durable-Raft service
  case passed for exact metadata-barrier coverage, group/schema/placement/Manifest binding,
  transfer of the same compatible aggregate owner into a grouped snapshot, committed route
  resolution, and creation of the running grouped TCP lifecycle. The exact group, generation, and
  FLOAT64 key are retained; a TIMESTAMP key rejects before I/O. Header self-containment and the
  installed consumer cover the public constructor. Bounded-stale grouped construction is not
  claimed.
- Packaged bounded-stale grouped-construction continuation: the focused replicated service case
  passed a stable same-term leader/follower pair, metadata-only barrier, committed two-replica
  placement, matching follower Manifest position, and follower TLS route through the packaged
  grouped constructor. The running owner retains serving node 12 and FLOAT64 key input one. Header
  self-containment and the installed consumer cover the public boundary. Remote observation
  acquisition composition is not claimed.
- Packaged remote bounded-stale grouped-lifecycle continuation: the focused real-mTLS service case
  acquired one leader/follower pair from two authenticated observation servers, retained the
  plan/Manifest pin, transitioned directly into the grouped TCP owner, exposed acquisition and
  execution metrics, and made execution-phase cancellation sticky. Header self-containment and the
  installed consumer cover the public lifecycle. A real remote CSEG response and process failover
  are not claimed.
- Explicit grouped whole-query rebinding continuation: the focused real-mTLS cluster case accepted
  one old partial with value 100, failed its peer retryably, rejected a different query identity,
  then replaced the complete execution and returned count two/sum six rather than 106. Four
  cumulative complete transports and one rebind prove that old partials were discarded. Header
  self-containment and installed consumption cover the public boundary; automatic metadata refresh
  and process failover are not claimed.
- Global grouped order/LIMIT continuation: the focused query case merged one key across two tablets,
  ordered negative/NaN/NULL keys with descending NULLS LAST, and applied LIMIT 2 only after global
  merge; LIMIT zero and invalid option rejection also passed. A cluster-owner case carried the same
  options through execution and selected global key 7 over key 5. Header self-containment and the
  installed consumer cover the public API; arbitrary-expression and arbitrary-row ordering are not
  claimed.
- Global grouped aggregate-order continuation: the focused query case merged key 1 across two
  tablets to SUM 6, ordered descending by SUM with LIMIT 2, and used canonical group-key order to
  break its tie with another SUM-6 key. Invalid order-key rejection passed. General expression and
  arbitrary-row ordering are not claimed.
- Distributed vector-result codec continuation: two focused query cases wrapped a canonical
  mixed-type Columnar Batch v1 and a terminal-only empty stream in exact correlated/checksummed
  frames, then rejected truncation, nested corruption, limit excess, and empty nonterminal input.
  At that checkpoint, general vector request fragments, coordination, transport, and execution were
  not claimed.
- Distributed vector partial-I/O continuation: one focused query case enumerated every split of a
  mixed-type variable frame, consumed a coalesced empty terminal through exact reported prefixes,
  retained sticky header corruption and unsupported-version failure, rejected a nested byte limit
  before payload buffering, and proved short-write suffix, overrun rollback, and moved-from cursor
  ownership. Header self-containment and the installed consumer cover the public API. At that
  checkpoint, general vector request fragments, coordination, authenticated transport, and
  execution were not claimed.
- Distributed vector-plan intent continuation: two focused query cases round-tripped row,
  ungrouped-aggregate, and multi-key grouped-aggregate shapes with final ordering and a present zero
  LIMIT, then rejected truncation, an unknown version, noncanonical absent input, lower caller
  bounds, duplicate groups, invalid COUNT(*), and invalid output order. Header self-containment and
  installed consumption cover the public codec. At that checkpoint, authority-bound vector
  fragments, schema/type binding, global coordination, transport, and execution were not claimed.
- Group-scoped vector-fragment continuation: two focused query cases round-tripped bounded-stale
  snapshot/route/group proof, unique projection, event bounds, and the nested multi-key ordered
  plan, then rejected truncation, unknown outer version, independently damaged nested plan, lower
  projection limits, out-of-projection plan indices, lag contradiction, and nil group identity.
  Header self-containment and installed consumption cover the public codec. At that checkpoint,
  committed-authority construction, worker execution, coordination, transport, and process
  integration were not claimed.
- Authority-bound vector-fragment continuation: the focused Manifest-backed binding case carried an
  exact leader barrier, committed placement/group, durable Raft position, recovery schema, unique
  projection, grouped SUM, output ordering, and LIMIT into an encodable owned dispatch. SUM over
  the projected TIMESTAMP and an out-of-projection input rejected before dispatch creation. All
  existing aggregate/grouped/metadata binding cases passed. Installed consumption covers the public
  binder. At that checkpoint, multi-tablet vector ownership, execution, coordination, and transport
  were not claimed.
- Compatible vector-snapshot continuation: the existing focused two-tablet binding case now also
  pinned one Manifest generation behind two plan-ordered group-scoped vector dispatches carrying
  the same grouped SUM/order/LIMIT intent. Reversed authority bindings and a three-ordinal total
  projection budget rejected before ownership publication. Existing aggregate/grouped/metadata
  cases remained green. At that slice, metadata-backed vector batch construction, execution,
  coordination, and transport were not claimed.
- Distributed vector-fragment partial-I/O continuation: one focused case enumerated every split of
  a variable group-scoped dispatch, consumed two coalesced frames only through each reported
  prefix, retained sticky header corruption, rejected lower reader and exact-decoder frame limits
  as resource exhaustion, and proved short-write suffix, overrun rollback, and moved-from cursor
  ownership. Header self-containment and installed consumption cover the public state machines.
  At that slice, metadata-backed vector batch construction was not claimed; execution,
  coordination, authenticated transport, and process integration remain unclaimed.
- Metadata-backed vector-snapshot continuation: the focused two-tablet catalog case now also
  derived plan-ordered vector admissions, active schema, committed placements, immutable groups,
  and exact leader barriers through the shared aggregate/vector authority resolver before binding
  grouped SUM/order/LIMIT dispatches under one Manifest generation. A catalog-derived TIMESTAMP SUM
  rejected before publication; the three existing metadata proof-policy cases remained green.
  Header self-containment and installed consumption cover the public constructor. Group-keyed proof
  acquisition, execution, coordination, authenticated transport, and process integration are not
  claimed.
- Group-keyed vector-proof continuation: the focused two-tablet authority case now also mapped a
  canonical group-sorted vector containing an unrelated metadata group through committed
  tablet-to-group bindings into plan-ordered vector dispatches. A span missing one selected group
  rejected before binding; the shared aggregate reversed-order and metadata/proof-policy cases
  remained green. Header self-containment and installed consumption cover the public constructor.
  At that slice, correlated follower group binding was not claimed; remote acquisition, execution,
  coordination, authenticated transport, and process integration remain unclaimed.
- Correlated follower vector-proof continuation: the focused bounded-stale authority case now also
  derived a row-output vector dispatch from one same-group, same-term stable leader/follower pair,
  preserving the follower serving node and leader commit frontier without a caller scalar. A term
  mismatch rejected both vector and aggregate binding before publication. Header self-containment
  and installed consumption cover the public constructor. Remote acquisition, execution,
  coordination, authenticated transport, and process integration are not claimed.
- Distributed vector-query request continuation: two focused cluster cases wrapped one proof-bound
  grouped SUM/order/LIMIT vector dispatch in a distinct node-routed request with independent header,
  payload, and complete CRCs. They rejected route aliasing, truncation, reserved bytes, a
  checksum-valid future version, nested damage under recomputed outer checksums, and grouped-request
  confusion. Header self-containment and installed consumption cover the public codec. Response
  framing, partial-I/O, authentication, execution, coordination, and process integration are not
  claimed; general worker execution still requires an explicit result-schema identity contract.
- Distributed vector-query response continuation: two additional focused cluster cases round-tripped
  a correlated terminal-only vector exchange, every failure status, and an advisory leader hint,
  then rejected encoder correlation mismatch, unknown payload kind, checksum-valid header/payload
  mismatch, and nested damage under recomputed outer checksums. All four request/response cases,
  header self-containment, and installed consumption cover both exact codec directions. At that
  checkpoint, partial-I/O, authentication, execution, coordination, and process integration were
  not claimed.
- Distributed vector-query partial-I/O continuation: one focused case enumerated every request and
  response split, consumed two coalesced requests only through reported prefixes, retained sticky
  header damage, rejected lower caller frame bounds, and proved short-write suffix, overrun rollback,
  and moved-from cursor ownership. All five vector transport cases, header self-containment, and
  installed consumption cover the public state machines. At that checkpoint, authentication,
  execution, coordination, and process integration were not claimed.
- Distributed vector-coordinator continuation: two focused cases enforced contiguous tablet
  sequences, exact retry/conflict arbitration, post-terminal rejection, incomplete-result
  withholding, terminal-only empty closure, deterministic plan order, message and byte exhaustion,
  first-failure stability, completed-worker loss, and one-shot finish. All five vector-exchange
  cases, header self-containment, and installed consumption cover the public coordinator. It treats
  canonical nested batches as opaque; result-schema authorization, execution, authenticated
  lifecycle, and process integration are not claimed.
- Distributed vector result-schema continuation: two focused cases round-tripped owned duplicate
  names and mixed logical descriptors, rejected damage, a checksum-valid future version, invalid
  UTF-8, and lower caller limits, then proved repeated row-output and grouped COUNT/SUM physical
  shapes while rejecting width/nullability mismatch. Header self-containment and installed
  consumption cover the API. The accepted v1 exchange bytes remain unchanged; schema carriage,
  schema-light result batches, execution, authenticated lifecycle, and process integration are not
  claimed.
- Schema-bound vector-fragment continuation: the focused fragment case wrapped unchanged v1
  authority bytes plus exact result-schema bytes, round-tripped v2, rejected v1/v2 confusion,
  truncation, a lower caller frame, and nested damage under recomputed wrapper checksums. The
  authority-binding case accepted exact grouped key/SUM descriptors and rejected nullability
  mismatch. Header self-containment and installed consumption cover the API. V2 partial-I/O,
  compatible snapshot ownership, cluster transport, result cells, execution, and process
  integration are not claimed.
- Schema-bound vector-result-exchange continuation: focused cluster cases round-tripped the exact
  native schema-light descriptor/cell contract under mandatory Fragment-v2 result-schema binding,
  including duplicate names, fixed/text/NULL cells, and terminal-only closure. They rejected schema
  mismatch, nested damage, truncation, lower bounds, a checksum-valid future version, and v1/v2
  confusion; enumerated every partial-read split and coalesced-frame boundary; proved short-write
  and moved-from cursor behavior; classified owned allocation failures; and added a deterministic
  ASan/UBSan libFuzzer smoke target for exact and fragmented decoding. Header self-containment and
  installed consumption cover the API. Fragment-v2 carrier ownership, schema-bound
  coordination, execution, authenticated lifecycle, and process integration remain incomplete.
- Bounded Fragment-v2 ownership continuation: focused query cases enumerated every wrapper split,
  coalesced successor boundary, sticky damage path, lower nested bound, future-version rejection,
  and short-write state; allocation injection classified encode, exact-decode, and streaming-reader
  ownership; deterministic ASan/UBSan fuzzing exercised exact and fragmented decode. A two-tablet
  compatible owner retained one Manifest pin and one shared result schema, proved it against every
  exact projection, encoded each authorized pairing, and rejected nullability mismatch without
  retaining per-tablet schema copies. Header self-containment and installed consumption cover the
  API. At that boundary, v2 cluster carriage, schema-bound coordination, worker execution,
  authenticated lifecycle, and process integration remained incomplete.
- Schema-bound vector-query-transport-v2 continuation: distinct request and response magics now
  carry exact Fragment-v2 and Result-Exchange-v2 values without changing v1. Focused cluster cases
  round-tripped schema-light success and correlated failure responses; rejected truncation, nested
  damage, schema mismatch, outer correlation mismatch, lower caller bounds, checksum-valid future
  versions, and v1/v2 confusion; enumerated every request/response split and coalesced boundary;
  proved sticky failure, typed short-write, moved-from cursor, and allocation-failure behavior; and
  added deterministic ASan/UBSan fuzzing for exact and fragmented decoding. Header
  self-containment and installed consumption cover the API. Peer-authenticated receiver/TLS
  ownership, retry, schema-bound coordination, worker execution, and process integration remain
  incomplete.
- Authenticated vector-query-receiver-v2 continuation: authentication now precedes decode, claimed
  source authorization and local-target validation precede one worker call, and complete
  schema-bound terminal streams publish atomically under independent frame-count and exact
  encoded-byte ceilings. Focused cases cover two-frame and terminal-only success, unavailable hints,
  malformed sequence/schema/empty streams, exception containment, and both exhaustion paths;
  allocation injection covers receiver-owned publication. Header self-containment and installed
  consumption cover the API. Mutual-TLS/TCP ownership, retry, production worker execution,
  coordination, and process integration remain incomplete.
- Bounded vector-query-v2 mutual-TLS continuation: move-only client/server carriers now map verified
  peer-certificate fingerprints before protocol I/O, transfer the admitted schema into response
  decoding, invoke the receiver once, retain response prefixes until terminal closure, and enforce
  exact response count/byte limits with sticky handshake/exchange deadlines. Real nonblocking TLS
  cases cover a two-frame schema-bound stream and rejection of a 200-byte response under a 199-byte
  budget; focused construction covers invalid bounds, target mismatch, and exact timeout. Header
  self-containment and installed consumption cover the API. TCP acquisition/
  listener ownership, retry, production worker execution, coordination, and process integration
  remain incomplete.
- Deadline-bound vector-query-v2 TCP-client continuation: one move-only composite now validates the
  exact schema-bound attempt and count/byte/deadline limits before opening a descriptor, exact-binds
  the authenticated peer address to the endpoint, proves nonblocking completion through
  `SO_ERROR`, and transfers ownership to TLS while preserving carrier-before-descriptor teardown.
  A real loopback case completes mutual TLS and returns two terminally closed responses; focused
  construction proves exact connect-deadline closure, sticky failure, and invalid byte-bound
  rejection. Header self-containment and installed consumption cover the API. Inbound listener
  ownership, retry, production worker execution, coordination, and process integration remain
  incomplete.
- Bounded vector-query-v2 TCP-server continuation: one move-only listener owner validates TLS and
  exact frame/byte/deadline limits before bind, reserves finite connection/poll storage, bounds
  accepts per poll, keeps every carrier/socket pair at a stable reverse-safe address, and exposes
  saturating admission/completion/failure metrics. Real loopback coverage completes the v2 TCP
  client through mutual TLS; a one-slot case proves explicit excess rejection, invalid
  configuration/poll rejection, and ordered idempotent shutdown. Header self-containment and
  installed consumption cover the API. Retry, production worker construction, schema-bound
  coordination, multi-tablet execution, and process integration remain incomplete.
- Finite vector-query-v2 sender continuation: one move-only policy owner now reconstructs immutable
  Fragment-v2 attempts, canonically revalidates complete responses against the owned schema, exact
  route/query/tablet/sequence/terminal identity, and independent frame/encoded-byte bounds before
  value-owned publication. Focused cases cover two-frame and terminal-only success, schema,
  sequence, frame and byte rejection without mutation, exact capped backoff, transport failure,
  advisory leader capture without target rebinding, and terminal exhaustion; allocation injection
  covers schema-validation and publication copies. Header self-containment and installed
  consumption cover the API. Coordinator delivery, multi-tablet TCP scheduling, production worker
  execution, and process integration remain incomplete.
- Schema-bound vector-result-coordinator-v2 continuation: one move-only owner now retains the
  admitted schema, canonically revalidates direct in-memory messages, arbitrates exact retries and
  gaps per planned tablet, bounds message count and full exchange-frame bytes, owns first failure,
  and transfers schema plus plan-ordered results only after all-tablet terminal closure. Focused
  cases cover schema mismatch, exact retry/conflict, gaps, empty terminals, ordering, count/byte
  exhaustion, completed-worker loss, and one-shot finish; allocation injection covers construction,
  admission rollback, and retryable final publication. Header self-containment and installed
  consumption cover the API. Later continuations now supply sender delivery, TCP scheduling,
  production row execution, and global row result semantics. Aggregate semantics and process
  integration remain incomplete.
- Proof-revalidated vector-row-worker-v2 continuation: a query-layer worker now canonically
  revalidates Fragment v2, repeats current route/placement/barrier/Manifest/schema/part authority,
  resolves real temporal CSEG winners, applies event-time filtering, and emits bounded row chunks
  without consuming global ORDER/LIMIT. A request-local service adapter owns coherent authority,
  encodes exact native result batches, bounds message count and complete exchange-frame bytes, and
  publishes only a complete terminal stream. Focused real-CSEG coverage proves two source-order
  rows survive a descending limit-one global intent, rejects schema mismatch, aggregate mode,
  stale authority, and invalid loader/configuration contracts, and exact-decodes the service output.
  Aggregate merge state and process integration remain incomplete.
- Owned vector-v2 inbound service continuation: a move-only heap-stable owner now constructs the
  proof-revalidating row worker, authenticated schema-bound receiver, and bounded TCP/mTLS server in
  dependency order and destroys them in reverse order. The focused real-CSEG gate moves the public
  owner before use, authenticates both peers, executes one canonical Fragment-v2 request, and
  exact-decodes the two-row terminal native batch with one completed connection and clean shutdown.
  Aggregate merge state and process integration remain incomplete.
- Pinned vector-v2 execution continuation: a move-only portable owner now accepts only the
  compatible schema-bearing snapshot, retains its Manifest pin, drives one finite sender per
  plan-ordered tablet, delivers each complete stream once, and transfers the global plan with the
  schema-bound result only after all tablets terminate. Focused cases prove exact two-tablet
  ordering, withheld partial completion, retry-to-terminal failure, foreign-tablet rejection, and
  coordinator-failure poisoning. Aggregate state and process integration remain.
- Pinned vector-v2 TCP scheduling continuation: one move-only scheduler prevalidates complete
  immutable node routes before acquisition, drives plan-ordered attempts and sender-authorized due
  retries through a fixed poll table, rotates only finite address candidates for the same target,
  and tears down every client on terminal failure, whole-query deadline, or explicit cancellation.
  A two-tablet real-loopback/mTLS case proves one refused first address rotates to the serving
  endpoint and publishes only the all-tablet result; focused cases prove incomplete-route rejection
  before I/O, expired-deadline suppression, and cancellation of active clients. Aggregate
  semantics, authority rebinding, and process integration remain.
- Bounded global vector-row finalization continuation: one consuming final pass independently
  validates row-mode plan/schema shape, tablet-stream closure, native descriptors, and exact
  row/message/input/working/output bounds before decoded-state allocation. It stably orders every
  current scalar type with explicit NULL placement, applies LIMIT only after the global order, and
  emits bounded native batches plus one schema-bearing zero-row batch. Focused cases prove
  cross-tablet ordering, deterministic ties, output rebatching, malformed-stream and bound
  rejection, and allocation-failure classification. Aggregate semantics, authority rebinding, and
  process integration remain.
- Mergeable all-type vector aggregate-state continuation: local ungrouped and grouped operators now
  share one move-only state for checked COUNT, wide exact and floating SUM, AVG sum/count,
  parallel-Welford variance, and all-type extrema. Identically defined partitions merge without
  finalizing intermediate cells; variable winners reserve before copy and preserve the prior state
  on failure. Focused cases prove numeric partition results, exact final overflow, bounded STRING
  extrema, definition rejection, and injected allocation-failure atomicity. Versioned state bytes,
  distributed worker/coordinator execution, authority rebinding, and process integration remain.
- Canonical aggregate-state bytes continuation: a distinct nested v1 frame preserves the exact
  aggregate definition and sufficient COUNT/SUM/AVG/extremum/variance state without finalization.
  Header-first reads prove integrity and caller limits before allocation, complete integrity gates
  variable decode, query credit owns decoded text/binary extrema, and move-only cursors own checked
  short writes. Focused tests cover every logical-type extremum, all sufficient numeric states,
  every split, coalesced suffixes, canonical damage, lower bounds, and injected allocation failure;
  a dedicated deterministic fuzz target covers exact and fragmented parsing. The correlated
  schema-bound exchange and aggregate execution remain.
- Schema-bound ungrouped aggregate-exchange continuation: a distinct outer v1 frame correlates one
  nested mergeable state to exact query/tablet identity and canonical aggregate ordinal/count/
  sequence/terminal position. Every codec and partial-I/O owner requires the Fragment-v2-derived
  definition vector, and exact decode independently checks outer, nested, and complete integrity
  before query-accounted variable state allocation. Focused tests freeze the layout, reject schema
  and canonical damage, enumerate every split, and inject allocation failure; a dedicated
  deterministic fuzz target covers arbitrary and mutated frames. Worker execution, stream
  coordination, global merge/finalization, grouped exchange, and process integration remain.
- Multi-key grouped sufficient-state framing continuation: a distinct grouped v1 frame binds one
  exact canonical scalar key tuple plus zero or more nested all-type aggregate states to query,
  tablet, group ordinal/count/sequence, and terminal identity. A separate empty terminal cannot
  fabricate a NULL-key group. Header-first reads, move-only writes, integrity-before-allocation,
  query-accounted decoded keys, every-split coverage, and allocation injection pass. Worker-side
  grouped accumulation, stream arbitration, authenticated transport, global merge/finalization,
  and shuffle routing remain.
- Shared grouped-state owner continuation: the local operator now composes one public move-only,
  query-accounted table retaining the existing canonical hash/equality and all-type state kernels.
  Accounted input ownership is exact, stable borrowed group spans feed the canonical grouped
  encoder synchronously, and local output still materializes through the same table. Focused
  encode/decode evidence proves COUNT/SUM states without a second grouping oracle. Worker plan
  splitting, stream construction, global merge, transport, and routing remain.
- Grouped partial-state merge continuation: the shared table now accepts exact borrowed scalar keys
  and sufficient states, coalesces them with the physical-row all-type hash/equality rules, and
  finalizes first-seen rows only after entering a sealed output phase. Query-context mismatches fail
  closed; allocation/state errors destroy the table so partial merge is never observable. Every
  frozen type plus signed-zero/NaN differential coverage, COUNT/SUM/AVG cross-tablet results, and
  variable key/extremum allocation injection pass. Canonical all-tablet stream arbitration, worker
  plan splitting, transport, and partition routing remain.
- All-tablet grouped coordinator continuation: one move-only owner now retains canonical grouped
  frames as exact retry identity, enforces gap-free per-tablet group streams and explicit empty
  terminals, requires every planned tablet to close, and merges in plan-tablet/group-ordinal order
  before any query-accounted row is visible. Identical retries are idempotent, conflicts and shape
  drift fail closed, first incomplete-worker failure is sticky, and finish-time allocation is
  retryable from retained bytes. Full allocation injection includes variable keys/extrema and
  output publication. Worker plan splitting, authenticated transport/read authority, final grouped
  projection/order/limit, and partition routing remain.
- Grouped sufficient-state worker continuation: the proof-revalidated Fragment-v2 real-CSEG worker
  now derives exact direct-input multi-key/all-type authority, reuses committed temporal winner and
  event-time gates, accumulates the shared query-accounted grouped table, and returns bounded owned
  Grouped Exchange v1 frames or the distinct empty terminal. Retained configuration and aggregate
  encoded bytes have independent hard bounds. Computed pre-group expressions, compatible
  all-tablet dispatch ownership, transport, and final SQL integration remain.
- Cross-tablet aggregate-definition ownership continuation: the compatible Fragment-v2 snapshot
  derives ungrouped definitions independently under every tablet's projected destination schema,
  rejects any exact mismatch, and retains the shared vector once with the Manifest pin and result
  schema. This prevents COUNT/AVG/variance output descriptors from erasing input-type authority.
  Row and grouped plans expose no ungrouped definitions; worker execution and coordination remain.
- Proof-revalidated vector aggregate worker continuation: the row worker's exact local authority and
  real-CSEG winner gates now feed a distinct ungrouped all-type state path. It materializes the
  fragment projection, applies event-time filtering, returns one canonical correlated sufficient
  state per definition, and leaves final ORDER BY/LIMIT/finalization untouched. Real-CSEG focused
  coverage proves COUNT/SUM/AVG/MAX and fail-closed loader/placement/width behavior; an empty-tablet
  allocation matrix proves complete publication or resource exhaustion. Service/transport,
  grouped exchange, and global result materialization remain.
- Bounded vector aggregate coordinator continuation: one single-threaded owner now validates the
  definition/result-schema authority, canonicalizes direct in-memory admissions, retains exact
  bounded frames for retry identity, requires a complete state vector from every planned tablet,
  and merges in deterministic tablet order before globally finalizing each scalar once. Focused
  coverage proves AVG sufficient-state semantics, gaps/retries/conflicts, first-failure ownership,
  post-terminal loss, limits, and retryable allocation failure. Authenticated transport/process
  ownership remains.
- Native vector aggregate finalization continuation: the exact definition authority now survives
  coordinator finish and is revalidated with the ungrouped plan, result descriptors, scalar types,
  nullability, and finite output limits. One canonical Native Protocol payload covers all 18
  logical types; global LIMIT zero emits a schema-bearing zero-row payload. Focused all-type,
  negative, decode, and heap-string allocation-failure cases pass. Authenticated aggregate
  transport and process orchestration remain.

The C++ files changed by the grouped-, vector-exchange-, Fragment-v2-, and vector-transport-v2
continuations pass the repository-pinned clang-format 18 check. A full-tree check was also run and
still reports pre-existing violations in the subscription protocol, subscription, multi-tablet
checkpoint implementation, and focused subscription test files; these slices do not rewrite those
unrelated files or claim a full-tree formatting pass. The full serialized 1,618-test developer
suite, focused ASan/UBSan cases, and deterministic 10,000-run transport-v2, aggregate-state, and
aggregate-exchange fuzz campaigns pass.
Apple's sanitizer runtime does not support LeakSanitizer, so those sanitizer runs explicitly
disabled leak detection. Broader cross-compiler/Linux parity, benchmark, profile, and chaos checks
were deliberately not run.

## Known risks and limitations

### Correctness

- The feature graph is not completely process-integrated. A three-daemon Linux gate now covers the
  packaged mutable row, global aggregate, expression/predicate, and row-backed grouped SQL surfaces
  before and after common-leader loss. A canonical authenticated remote read-authority codec and
  receiver, bounded stream owners, mutual-TLS sessions, deadline/admission-bound TCP endpoints, and
  finite immutable-route retry now feed the packaged daemon service and all-group Native attempt
  coordination. Focused in-process coverage proves local-metadata/remote-tablet SQL; Linux
  multi-daemon split-leader qualification, multi-process real-CSEG execution, and partition
  schedules remain absent.
- Temporal corrections have durable WAL/CSEG v2/Manifest v2 composition; direct vector winner
  lowering and mixed WAL/Raft-source composition remain incomplete.
- Distributed Native execution covers bounded row-backed multi-key/all-type grouping, global
  ordering and LIMIT in addition to row and global aggregate plans. The scalable sufficient-state
  path now has multi-key/all-type group-state bytes but does not yet feed them from workers or merge
  them globally; arbitrary relational plans, shuffle/skew handling, and fragment-level durable
  retries remain absent.
- Movement now composes deterministic actions with joint Raft membership and durable checkpoints;
  automatic placement-driven orchestration remains external.
- Cold upload independently performs exact schema/source-bound CSEG validation before remote
  mutation, but broader corruption, allocation-failure, and fuzz evidence remains deferred.
- Raft now prevalidates malformed higher-term messages and divergent matching-term entries, but
  snapshot boundaries, response-state combinations, and membership still need broader model evidence.

### Concurrency

- Live/materialized-view, metadata-application, movement, and tiering owners are intentionally
  single-thread-affine. Multi-Raft has a bounded dedicated worker; committed tablet and metadata
  application now have concrete worker-affine owners, and a bounded flat extension set can host
  both. Packaged transport/service composition remains incomplete.
- BoundedExchange and MemoryObjectStore use mutexes but have no TSan evidence in this pass.
- io_uring protocol cancellation, forced in-flight shutdown, and close/completion races lack broad
  Linux and TSan evidence beyond the focused clean-shutdown lifecycle.
- Tiering upload/catalog restoration remains single-owner, while quiesced catalog reads and the
  bounded LRU support concurrent query access. TSan evidence and production worker scheduling remain
  deferred.

### Durability

- Materialized-view/subscription checkpoints, temporal mutation/CSEG v2/Manifest v2 state, segmented
  Raft persistence, metadata commands/snapshots, movement checkpoints/receipts, and cold-location
  authority are durable and checksummed. The full-object cache intentionally has no durable index;
  its routing catalog is restored from selected authority and bytes rebuild on verified demand.
- Shared Raft-log and application-snapshot reclamation are caller-triggered and lack syscall/crash
  fault matrices; the ordering protocols are implemented and focused restart tests pass.
- QUORUM_SYNC receipts, Protocol 2.0 negotiation/acknowledgement bytes, metadata-derived local
  routing, bounded execution, queue-facing backpressure/drain composition, database-root reopen,
  and explicit packaged daemon advertisement exist. Authenticated multi-node peer transport is now
  packaged. Replicated query snapshots pin one committed binder catalog, fail closed for partial
  table residency, and now require every metadata/tablet publication to cover an exactly correlated
  current-term quorum read barrier before bounded native SELECT dispatch. Bounded Linux evidence now
  covers three authenticated daemon processes, quorum ingest, common-leader loss, a higher-term
  matching retry, mutable row/global-aggregate/expression/predicate/multi-key grouped SQL before and
  after failover, and identical retained-root recovery. This is a controlled common-leader topology;
  a globally atomic cross-group instant, independently led group coordination, and broader
  distributed relational execution remain absent. Strict native
  endpoint/certificate-principal configuration, secure TLS-route/context ownership, redirect
  selection, exact QUORUM_SYNC body/session replay, the deadline-bound authenticated TCP/TLS
  reconnect carrier, single-operation poll scheduling, and the packaged `chronosctl quorum-sync`
  composition now have bounded, fail-closed owners. Finite queries now also have exact SQL/session
  replay with bounded terminal-only result ownership, an authenticated deadline-bound TCP/TLS
  redirect carrier, whole-operation-aware poll execution, and an explicit packaged single-group
  `chronosctl routed-sql` command. The packaged service now emits an authoritative whole-query
  redirect for an exact single table-group route only when fresh ordered observations show one
  common stable remote leader across its complete read gate. A distinct checksummed mutable vector
  fragment now binds and locally executes one exact committed/applied TabletState publication with
  complete route, placement, barrier, schema, projection, and result-shape revalidation. Its
  distinct bounded request carrier now adds authenticated source authorization, exact response
  schema validation, terminal publication, leader hints, and finite retries. A nonblocking
  mutual-TLS client/server carrier now authenticates and node-authorizes certificates before
  request bytes or worker execution. Dedicated TCP owners now add prevalidated nonblocking connect,
  a separate exact connect deadline, bounded listener admission and polling, stable carrier/socket
  lifetimes, metrics, and idempotent shutdown. A packaged request-local mutable worker now
  reacquires and pins the exact TabletSnapshot/schema/placement/group/barrier authority, emits only
  a complete bounded Native result stream, and is composed with the receiver/TCP server in
  reverse-safe lifetime order. A portable multi-tablet mutable execution owner now validates one
  common plan/schema authority, retains finite senders, exposes fresh-authority hints without
  rewriting fragments, and publishes only a complete plan-ordered result. Its TCP scheduler now
  drives one bounded mutual-TLS client per tablet, rotates finite same-node addresses, enforces
  whole-query deadlines, and releases all clients on terminal failure. Explicit bounded rebinding
  now accepts only a freshly constructed execution with identical logical query and tablet/group
  identity while permitting new leader/position/placement/barrier authority. The replicated query
  snapshot now retains the committed metadata publication and database identity and performs the
  complete plan-ordered join from correlated leader barriers/observations to resident immutable
  publications, placements, group bindings, projection, plan, and result schema. Those mutable
  fragments now use the shared committed-node/TLS resolver, and one snapshot call returns the
  complete owning fragment set plus deduplicated bounded routes from the same metadata
  publication. Bound row SQL now lowers direct columns and bounded source-independent scalar
  outputs to the exact schema identity, unique real-source projection,
  normalized event-time comparisons and inclusive `BETWEEN`, global row order/limit intent, and
  result descriptors required by that fragment path. Unselected direct order columns travel as
  bounded hidden worker outputs under Plan Intent minor 1 and are removed only after global
  sort/limit. Coordinator-owned canonical constants are injected only after complete sort/limit,
  with a real event-time anchor preserving all-constant row cardinality; row-dependent computed and
  relational semantics fail closed. A separate checked global-aggregate
  lowering now emits direct aggregate sufficient-state intent, unique projection, event-time truth,
  LIMIT, and exact result descriptors. A bounded transitional finalizer now consumes complete
  mutable all-tablet row streams through the shared aggregate kernel and emits one all-or-none
  Native payload. The replicated Native service and packaged daemon now route aggregate inputs
  through the existing proof-revalidated local/remote mutable query plane with the same all-group
  retry, deadline, and cancellation owner; worker-side state pushdown remains deferred.
  The retained snapshot now constructs the canonical tablet plan from the row SQL semantics
  plus correlated current-leader authorities and returns the all-or-none bound/routed package. A
  move-only request owner now
  composes bounded mutual-TLS scheduling with exact-once all-tablet result transfer and global
  Native row finalization. The replicated Native service now joins a real request to correlated
  read authorities, snapshot binding/lowering, fragment/route preparation, finite TCP execution,
  and terminal response routing. The replicated database now supplies the production worker
  context through an atomic required-term group observation plus one pinned committed
  metadata/tablet snapshot and exact fragment matching. Self-led fragments now execute through
  that same production worker while remote fragments retain mutual-TLS scheduling, and one
  all-tablet coordinator withholds global finalization and Native output until both subsets close.
  Remote read-authority transport now also has bounded mutual-TLS/TCP owners, finite immutable-route
  retry, and concurrent all-or-nothing group fan-out. A production service adapter issues one exact
  group through the durable replicated barrier owner on a required non-poll thread. One bounded shared
  private endpoint can authenticate before routing either existing mutable or authority protocol.
  The packaged daemon now owns both production receivers on that one committed endpoint with the
  replicated barrier and listener thread in explicit deadlock-safe lifetime order; Native outbound
  authority acquisition now observes every group, combines locally led barriers with an
  all-or-nothing batch to observed authenticated remote leaders, and re-observes the whole attempt
  under one deadline. A focused local-metadata/remote-tablet SQL gate passes; Linux multi-daemon
  split-leader qualification remains absent.
  The packaged daemon now binds the committed local private query endpoint, reuses the immutable
  authenticated peer bundle for inbound/outbound query TLS, polls synchronous workers separately
  from Raft, and extends its three-process gate through remote SELECT before and after tablet-leader
  failover. Reactor-visible exact cancellation now uses one joined query slot, cooperatively tears
  down remote work, and suppresses the complete response. Retryable local or remote failure now
  discards the whole attempt and installs only a freshly barrier-covered, logically identical
  all-group fragment/route set under the original deadline.
- Production S3 semantics are implemented through the libcurl SigV4 backend but still require
  object-store fault and deployment qualification.

### Performance

- Full-state Raft persistence is intentionally simple and may amplify writes.
- Ordered maps/multisets and copied snapshots favor correctness over throughput.
- Live fan-out, temporal history, exchange, cache, and movement have no scale measurements.
- The io_uring socket backend may be slower than epoll; no comparison was run.
- Beyond the timestamp range kernel, no SIMD/NUMA optimization, allocation profile, flame graph, or
  tail-latency campaign ran.

## Deferred validation and recommended Phase 18 order

The exact subsystem/category ledger is
[`deferred-validation.md`](../development/deferred-validation.md). Recommended order:

1. Extend distributed SQL beyond the direct-column row subset while preserving the applied read-
   barrier data-plane sequence.
2. Specify database namespaces/catalog tombstones and placement-driven membership orchestration
   without changing Metadata Command v1 or Metadata Application Snapshot 1.0 bytes in place.
3. Finish direct vector temporal winner lowering, mixed WAL/Raft recovery, durable retention
   integration, and broader distributed grouping/order/top-N/LIMIT plan coverage.
4. Run full compiler/Debug/Release/install suites, then ASan/UBSan/TSan, fuzz/corruption/crash,
   deterministic/chaos campaigns, SQL differential tests, and only afterward benchmarks/profiling/
   epoll-io_uring/SIMD/NUMA comparison and final tuning.
