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

- Resume Token v1 with fixed versioned bytes, HMAC-SHA256, constant-time MAC comparison, and bound
  database/subscription/plan/schema/tablet/WAL/sequence identity;
- single-source register-before-boundary handoff, snapshot-phase buffering, live transition,
  at-least-once poll, acknowledgment checkpoints, retained-suffix resume, cancellation, and bounded
  fail-closed overflow that never rejects an already committed source change;
- removable count, sum, min, max, VWAP, OHLC, and Welford population/sample variance state; and
- tumbling/sliding window materialized state, watermark finalization, corrections/tombstones,
  revisions, consecutive committed progress, and finite row/window bounds;
- plan-bound single-tablet snapshot execution, schema-bound `SUBSCRIBE SELECT` identity, and a
  durable exact-SQL registry that reprepares and verifies executable fingerprints after restart;
- canonical multi-tablet delivery order, exact logical checkpoint/restore, and frozen checksummed
  checkpoint-generation bytes; and
- lock-owning, exact-next-generation filesystem installation, fail-closed latest selection, and a
  durable coordinator owner that publishes retention frontiers only after synchronized install;
  plus exact multi-tablet historical execution through one global physical pipeline, started
  directly from a durably recovered executable without mutable-manager escape. Snapshot teardown
  abandons state without token allocation, while client cancellation still returns a safe token;
  and committed schema incompatibility has a distinct terminal phase, precise Protocol 1.1 reason,
  invalidated resume state, and durable checkpoint/reopen representation; and a bounded reactor-
  facing service owns SQL validation, snapshot/READY/live/ack/cancel/resume transitions, exact
  response-ring backpressure retry, disconnect cleanup, and resumable shutdown drain.

The source-retention boundary now component-wise intersects storage/Raft safety with every durable
plan frontier and validates committed placement epochs and local replica membership before invoking
a source-specific batch reclaimer. Logical subscription positions are not fabricated into physical
WAL offsets.

Node-wide Raft physical prefix reclamation is now implemented after a complete all-group checkpoint.
Mapping a particular durable subscription frontier into that node-wide scheduling policy and dynamic
plan-owner retirement remain production composition work.

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
execution gates to stable same-term leader/follower observation pairs. General vector-plan
grouping/order/top-N/LIMIT remains incomplete. A distinct canonical frame now carries one nullable
FLOAT64 group key and mergeable partial with SQL-equivalent signed-zero/NaN canonicalization. An
authenticated mutual-TLS carrier owns its bounded ordered response stream, and a deadline-bound
outbound TCP composite owns one validated connection attempt. Inbound TCP server ownership,
sender/coordinator integration, packaged grouped execution, and multi-key/non-FLOAT64 state remain
incomplete. A distinct
canonical observation protocol, authenticated receiver, mTLS clients/servers, finite multi-address
acquisition, correlated
leader/follower pairs, canonical all-group batches, placement-backed construction, and packaged
query lifecycle now provide remote follower authority acquisition. Committed numeric or
lowercase-DNS routes acquire a fresh
bounded ordered unique IPv4 candidate set before polling, and finite sender retries rotate
candidates without changing node/proof/TLS authority. Live DNS churn/latency/cache policy, IPv6, a
packaged multi-process runtime, remote CSEG execution in the movement gate, and broad
failure/measurement evidence remain incomplete; the Phase 16 exit gate is not claimed.

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
loopback applied write and matching retry after daemon restart. The requested real
three-process/data-plane workflow does not exist. A later focused
gate uses real mutual-TLS query sockets around the complete movement state machine, but simulates the
externally committed promotion/removal milestones and deterministic worker aggregates. A separate
one-process service gate now queries an installed CSEG through the production mTLS worker stack. No
gate starts three server processes, executes SQL through the native protocol, kills a process,
applies a Raft command to mutable/CSEG storage, or queries a moved CSEG on another process. Those
remain high-priority integration and hardening tasks, not passed checks.

## Public APIs and formats

Important new public targets are `chronos::live`, `chronos::runtime`, `chronos::raft`, and
`chronos::tiering`; `chronos::query` gained temporal/distributed APIs and `chronos::network` gained
explicit backend selection.

Important APIs include `SubscriptionManager`, `WindowedMaterializedView`,
`IncrementalAggregateSet`, `TemporalSnapshotProvider`, `RaftNode`, `MultiRaftRuntime`,
`MetadataStateMachine`, `TabletMovement`, `BoundedExchange`, `DistributedAggregateCoordinator`,
`ObjectStore`, `TieredPartManager`, `Reactor`, and `apply_current_thread_placement`.

Accepted formats added during and after the pass include authenticated Resume Token v1,
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

The C++ files changed by the grouped-exchange continuations pass the repository-pinned clang-format
18 check. A full-tree check was also run and still reports pre-existing violations in the
subscription protocol, subscription, multi-tablet checkpoint implementation, and focused
subscription test files; this grouped slice does not rewrite those unrelated files or claim a
full-tree formatting pass. Full-suite, sanitizer, fuzz, broader cross-compiler/Linux parity,
benchmark, profile, and chaos checks were deliberately not run.

## Known risks and limitations

### Correctness

- The feature graph is not completely process-integrated. Replicated distributed aggregate
  construction now preserves committed catalog and correlated Raft proof preconditions through the
  TCP lifecycle, but remote observation acquisition and the native client/process entry point are
  still absent.
- Temporal corrections have durable WAL/CSEG v2/Manifest v2 composition; direct vector winner
  lowering and mixed WAL/Raft-source composition remain incomplete.
- The distributed implementation covers numeric global aggregate state with finite whole-query
  retry/rebinding, not arbitrary plans, grouping, order, top-N, limits, or fragment-level durable
  retries.
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
  current-term quorum read barrier before bounded native SELECT dispatch. A globally atomic
  cross-group instant, remote fragments/client leader routing, and real three-process failover
  evidence are still absent.
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

1. Add remote query fragments and client leader routing, then run the real three-process data-plane
   smoke path over the already composed worker-affine applications, authenticated transport,
   QUORUM_SYNC ingest, and applied read-barrier native SELECT.
2. Specify database namespaces/catalog tombstones and placement-driven membership orchestration
   without changing Metadata Command v1 or Metadata Application Snapshot 1.0 bytes in place.
3. Finish direct vector temporal winner lowering, mixed WAL/Raft recovery, durable retention
   integration, and broader distributed grouping/order/top-N/LIMIT plan coverage.
4. Run full compiler/Debug/Release/install suites, then ASan/UBSan/TSan, fuzz/corruption/crash,
   deterministic/chaos campaigns, SQL differential tests, and only afterward benchmarks/profiling/
   epoll-io_uring/SIMD/NUMA comparison and final tuning.
