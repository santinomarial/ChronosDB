# Architecture Decision Records

Architecture Decision Records (ADRs) preserve the context and reasoning behind consequential project choices. An ADR is required when a decision changes a durable or network contract, an invariant, component ownership, concurrency or memory-ordering strategy, recovery/consistency semantics, security boundary, production dependency, supported platform, compatibility policy, or a choice that would be expensive to reverse. Routine local implementation details that follow accepted specifications do not need an ADR.

## Numbering and filenames

Use a four-digit, monotonically increasing repository-wide number followed by a short lowercase hyphenated title:

```text
0013-short-decision-title.md
0014-another-decision-title.md
```

Reserve the number when opening the ADR. Numbers are never reused, even when a proposal is rejected. The index below lists records in number order.

## Statuses

- **proposed:** under review and not an implementation authority.
- **accepted:** approved and authoritative for work in its scope.
- **superseded:** replaced by one or more named ADRs; retained for history.
- **rejected:** considered and deliberately not selected; retained with rationale.
- **deprecated:** once accepted, but no longer recommended or valid for new work; migration may still be underway.

## Process

1. Copy [the template](template.md), allocate the next number, and fill every section. Use `None` with a reason rather than deleting a section.
2. Link the affected [invariants](../architecture/invariants.md), specifications, benchmarks, prototypes, and issue discussions that actually exist. Do not invent references.
3. Review alternatives, failure modes, compatibility, operations, and validation evidence before changing status to `accepted`.
4. Record implementation follow-ups outside the ADR while keeping links back to the decision.
5. When the decision changes, add a new ADR and update both records' statuses and cross-references.

An accepted ADR is not silently rewritten after implementation begins. Correct typographical errors or add clearly labeled retrospective notes without changing the original decision. A semantic change, new tradeoff, or reversed decision requires a new ADR so the project's history remains reviewable.

## Index

| ADR | Decision | Status |
| --- | --- | --- |
| [0001](0001-project-scope-and-workloads.md) | Project scope and workloads | accepted |
| [0002](0002-language-platform-and-portability.md) | Language, platform, and portability | accepted |
| [0003](0003-single-node-first-development-order.md) | Single-node-first development order | accepted |
| [0004](0004-thread-ownership-and-ingress-concurrency.md) | Thread ownership and ingress concurrency | accepted |
| [0005](0005-columnar-heads-and-immutable-cseg-parts.md) | Columnar heads and immutable CSEG parts | accepted |
| [0006](0006-wal-durability-and-group-commit.md) | WAL durability and group commit | accepted |
| [0007](0007-event-time-system-time-and-row-versioning.md) | Event time, system time, and row versioning | accepted |
| [0008](0008-custom-sql-and-vectorized-execution.md) | Custom SQL and vectorized execution | accepted |
| [0009](0009-network-reactor-strategy.md) | Network reactor strategy | accepted |
| [0010](0010-tablets-raft-and-multiplexed-log-storage.md) | Tablets, Raft, and multiplexed log storage | accepted |
| [0011](0011-dependency-and-build-versus-buy-policy.md) | Dependency and build-versus-buy policy | accepted |
| [0012](0012-correctness-testing-and-performance-evidence.md) | Correctness testing and performance evidence | accepted |
| [0013](0013-wal-v1-format-and-recovery.md) | WAL v1 format and recovery | accepted |
| [0014](0014-logical-types-schema-identity-and-evolution.md) | Logical types, schema identity, and evolution | accepted |
| [0015](0015-columnar-batch-v1-and-wal-append-command.md) | Columnar batch v1 and WAL append command | accepted |
| [0016](0016-cseg-v1-layout-integrity-and-compression.md) | CSEG v1 layout, integrity, and compression | accepted |
| [0017](0017-manifest-generations-installation-and-checkpoints.md) | Manifest generations, part installation, and checkpoints | accepted |
| [0018](0018-append-only-cseg-compaction-and-manifest-replacement.md) | Append-only CSEG compaction and Manifest replacement | accepted |
| [0019](0019-rebuildable-pruning-delta-planning-and-part-reclamation.md) | Rebuildable pruning, delta planning, and part reclamation | accepted |
| [0020](0020-bounded-vector-chunk-representation.md) | Bounded vector chunk representation | accepted |
| [0021](0021-query-resource-accounting-and-cooperative-cancellation.md) | Query resource accounting and cooperative cancellation | accepted |
| [0022](0022-pull-based-physical-operator-lifecycle.md) | Pull-based physical operator lifecycle | accepted |
| [0023](0023-bounded-physical-pipeline-plan.md) | Bounded physical pipeline plan | accepted |
| [0024](0024-lifetime-pinned-vector-chunk-backing.md) | Lifetime-pinned vector chunk backing | accepted |
| [0025](0025-allocation-free-cseg-projected-read-planning.md) | Allocation-free CSEG projected read planning | accepted |
| [0026](0026-pinned-in-memory-cseg-scan-source.md) | Pinned in-memory CSEG scan source | accepted |
| [0027](0027-snapshot-bound-cseg-images-and-part-lifetime-pins.md) | Snapshot-bound CSEG images and part-lifetime pins | accepted |
| [0028](0028-pruned-multi-part-snapshot-cseg-scan.md) | Pruned multi-part snapshot CSEG scan | accepted |
| [0029](0029-query-accounted-mutable-head-scan-source.md) | Query-accounted mutable-head scan source | accepted |
| [0030](0030-exact-timestamp-range-vector-filtering.md) | Exact timestamp-range vector filtering | accepted |
| [0031](0031-exact-prune-then-filter-snapshot-cseg-scans.md) | Exact prune-then-filter snapshot CSEG scans | accepted |
| [0032](0032-exact-event-time-mutable-head-scans.md) | Exact event-time mutable-head scans | accepted |
| [0033](0033-accounted-source-column-output-materialization.md) | Accounted source-column output materialization | accepted |
| [0034](0034-accounted-typed-constant-vector-outputs.md) | Accounted typed-constant vector outputs | accepted |
| [0035](0035-bounded-checked-vector-expression-programs.md) | Bounded checked vector expression programs | accepted |
| [0036](0036-bound-select-to-physical-pipeline-lowering.md) | Bound SELECT to physical pipeline lowering | accepted |
| [0037](0037-fixed-width-vector-casts-and-scalar-functions.md) | Fixed-width vector casts and scalar functions | accepted |
| [0038](0038-borrowed-variable-width-vector-materialization.md) | Borrowed variable-width vector materialization | accepted |
| [0039](0039-borrowed-text-predicate-vector-kernels.md) | Borrowed text-predicate vector kernels | accepted |
| [0040](0040-streaming-ungrouped-vector-aggregates.md) | Streaming ungrouped vector aggregates | accepted |
| [0041](0041-bound-global-aggregate-physical-lowering.md) | Bound global aggregate physical lowering | accepted |
| [0042](0042-query-accounted-bounded-grouped-aggregates.md) | Query-accounted bounded grouped aggregates | accepted |
| [0043](0043-bound-grouped-aggregate-physical-lowering.md) | Bound grouped aggregate physical lowering | accepted |
| [0044](0044-query-accounted-bounded-physical-sort.md) | Query-accounted bounded physical sort | accepted |
| [0045](0045-shared-vector-row-version-suffix.md) | Shared vector row-version suffix | accepted |
| [0046](0046-exact-bounded-sql-order-by-lowering.md) | Exact bounded SQL ORDER BY lowering | accepted |
| [0047](0047-exact-append-only-snapshot-tablet-scan.md) | Exact append-only snapshot tablet scan | accepted |
| [0048](0048-snapshot-tablet-physical-pipeline-instantiation.md) | Snapshot tablet physical pipeline instantiation | accepted |
| [0049](0049-query-accounted-variable-width-extrema.md) | Query-accounted variable-width extrema | accepted |
| [0050](0050-canonical-query-accounted-group-hashing.md) | Canonical query-accounted group hashing | accepted |
| [0051](0051-exact-bounded-latest-by-physical-lowering.md) | Exact bounded LATEST BY physical lowering | accepted |
| [0052](0052-query-accounted-bounded-asof-join.md) | Query-accounted bounded ASOF join | accepted |
| [0053](0053-checked-left-deep-asof-physical-plan.md) | Checked left-deep ASOF physical plan | accepted |
| [0054](0054-bound-asof-select-physical-lowering.md) | Bound ASOF SELECT physical lowering | accepted |
| [0055](0055-snapshot-bound-multi-source-asof-instantiation.md) | Snapshot-bound multi-source ASOF instantiation | accepted |
| [0056](0056-shared-query-credit-and-bounded-parallel-scheduling.md) | Shared query credit and bounded parallel scheduling | accepted |
| [0057](0057-bounded-checksummed-external-sort.md) | Bounded checksummed external sort | accepted |
| [0058](0058-shared-snapshot-publication-query-credit.md) | Shared snapshot publication query credit | accepted |
| [0059](0059-bounded-physical-strategy-selection.md) | Bounded physical strategy selection | accepted |
| [0060](0060-native-protocol-v1-framing.md) | Native Protocol v1 framing | accepted |
| [0061](0061-native-protocol-handshake-and-request-lifecycle.md) | Native protocol handshake and request lifecycle | accepted |
| [0062](0062-bounded-connection-buffer-ownership.md) | Bounded connection buffer ownership | accepted |
| [0063](0063-bounded-reactor-shard-spsc-routing.md) | Bounded reactor-to-shard SPSC routing | accepted |
| [0064](0064-bounded-linux-epoll-reactor.md) | Bounded Linux epoll reactor ownership and overload | accepted |
| [0065](0065-self-describing-query-result-batches.md) | Self-describing Protocol v1 query result batches | accepted |
| [0066](0066-authentication-and-tls-integration-boundary.md) | Authentication and TLS integration boundary | accepted |
| [0067](0067-bounded-native-client-session.md) | Bounded native client session | accepted |
| [0068](0068-live-handoff-and-resume-token-v1.md) | Bounded live handoff and Resume Token v1 | accepted |
| [0069](0069-deterministic-raft-and-multiplexed-state-record.md) | Deterministic Raft and multiplexed state records | accepted |
| [0070](0070-feature-pass-logical-boundaries.md) | Temporal, distributed, cold-tier, and runtime feature-pass boundaries | accepted |
| [0071](0071-segmented-multi-raft-persistence.md) | Segmented Multi-Raft persistence and recovery | accepted |
| [0072](0072-explicit-wal-and-raft-commit-identities.md) | Explicit WAL and Raft commit identities | accepted |
| [0073](0073-committed-raft-tablet-application.md) | Committed Raft tablet application and retained-log recovery | accepted |
| [0074](0074-quorum-sync-proof-boundary.md) | Raft quorum-synchronization proof boundary | accepted |
| [0075](0075-durable-metadata-raft-commands.md) | Durable metadata Raft commands and recovery | accepted |
| [0076](0076-joint-consensus-raft-membership.md) | Joint-consensus Raft membership | accepted |
| [0077](0077-snapshot-membership-checkpoints.md) | Raft snapshot membership checkpoints | accepted |
| [0078](0078-two-stage-raft-snapshot-installation.md) | Two-stage Raft snapshot installation | accepted |
| [0079](0079-temporal-mutation-command-v1.md) | Temporal Mutation Command v1 | accepted |
| [0080](0080-cseg-v2-temporal-system-columns.md) | CSEG v2 temporal system columns | accepted |
| [0081](0081-cseg-v2-temporal-snapshot-resolution.md) | CSEG v2 temporal snapshot resolution | accepted |
| [0082](0082-source-neutral-manifest-v2-layout.md) | Source-neutral Manifest v2 layout | accepted |
| [0083](0083-manifest-v2-temporal-wal-recovery.md) | Manifest v2 temporal WAL recovery composition | accepted |
| [0084](0084-verified-temporal-checkpoint-overlap.md) | Verified temporal checkpoint overlap | accepted |
| [0085](0085-raft-tablet-application-snapshot-v1.md) | Raft Tablet Application Snapshot v1 | accepted |
| [0086](0086-durable-raft-tablet-snapshot-installation.md) | Durable Raft tablet application-snapshot installation | accepted |
| [0087](0087-raft-tablet-snapshot-recovery-composition.md) | Raft tablet application-snapshot recovery composition | accepted |
| [0088](0088-owned-raft-tablet-snapshot-compaction.md) | Owned Raft tablet snapshot creation and compaction | accepted |
| [0089](0089-exact-logical-materialized-view-checkpoints.md) | Exact logical materialized-view checkpoints | accepted |
| [0090](0090-materialized-view-checkpoint-v1.md) | Materialized View Checkpoint v1 | accepted |
| [0091](0091-durable-materialized-view-checkpoint-storage.md) | Durable materialized-view checkpoint storage | accepted |
| [0092](0092-materialized-view-checkpoint-generations.md) | Materialized-view checkpoint generations | accepted |
| [0093](0093-durable-windowed-materialized-view-owner.md) | Durable windowed materialized-view owner | accepted |
| [0094](0094-native-protocol-1-1-subscriptions.md) | Native Protocol 1.1 subscriptions | accepted |
| [0095](0095-multi-tablet-subscription-delivery-order.md) | Multi-tablet subscription delivery order | accepted |
| [0096](0096-plan-bound-subscription-snapshot-execution.md) | Plan-bound subscription snapshot execution | accepted |
| [0097](0097-schema-bound-subscription-plan-identity.md) | Schema-bound subscription plan identity | accepted |
| [0098](0098-exact-multi-tablet-subscription-checkpoints.md) | Exact multi-tablet subscription checkpoints | accepted |
| [0099](0099-multi-tablet-subscription-checkpoint-v1.md) | Multi-tablet Subscription Checkpoint v1 | accepted |
| [0100](0100-durable-subscription-checkpoint-generations.md) | Durable multi-tablet subscription checkpoint generations | accepted |
| [0101](0101-durable-multi-tablet-subscription-owner.md) | Durable multi-tablet subscription owner | accepted |
| [0102](0102-exact-multi-tablet-subscription-snapshot.md) | Exact multi-tablet subscription snapshots | accepted |
| [0103](0103-durable-subscription-plan-registry.md) | Durable subscription plan registry | accepted |
| [0104](0104-schema-change-subscription-boundary.md) | Schema-change subscription terminal boundary | accepted |
| [0105](0105-bounded-subscription-service-lifecycle.md) | Bounded reactor-facing subscription service lifecycle | accepted |
| [0106](0106-topology-bound-subscription-retention.md) | Topology-bound subscription retention authority | accepted |
| [0107](0107-bounded-io-uring-socket-reactor.md) | Bounded io_uring socket reactor ownership | accepted |
| [0108](0108-query-worker-placement-startup-gate.md) | Query-worker placement startup gate | accepted |
| [0109](0109-runtime-dispatched-timestamp-filter-kernel.md) | Runtime-dispatched timestamp filter kernel | accepted |
| [0110](0110-multi-tablet-temporal-wal-recovery.md) | Multi-tablet temporal WAL recovery routing | accepted |
| [0111](0111-query-accounted-temporal-vector-source.md) | Query-accounted temporal vector source | accepted |
| [0112](0112-monotonic-temporal-retention-frontier.md) | Monotonic temporal retention frontier | accepted |
| [0113](0113-linearizable-raft-read-barrier.md) | Linearizable Raft read barrier | accepted |
| [0114](0114-bounded-asynchronous-multi-raft-owner.md) | Bounded asynchronous Multi-Raft owner | accepted |
| [0115](0115-proof-bound-distributed-read-admission.md) | Proof-bound distributed read admission | accepted |
| [0116](0116-raft-metadata-tablet-reconfiguration.md) | Raft and metadata tablet reconfiguration | accepted |
| [0117](0117-tablet-movement-checkpoint-v1.md) | Tablet Movement Checkpoint v1 | accepted |
| [0118](0118-durable-tablet-movement-checkpoint-generations.md) | Durable tablet movement checkpoint generations | accepted |
| [0119](0119-deterministic-tablet-reconfiguration-action-identities.md) | Deterministic tablet reconfiguration action identities | accepted |
| [0120](0120-tablet-reconfiguration-action-v1.md) | Tablet Reconfiguration Action v1 | accepted |
| [0121](0121-durable-tablet-reconfiguration-action-ledger.md) | Durable tablet reconfiguration action ledger | accepted |
| [0122](0122-tablet-movement-snapshot-chunk-v1.md) | Tablet Movement Snapshot Chunk v1 | accepted |
| [0123](0123-durable-tablet-movement-snapshot-chunks.md) | Durable tablet movement snapshot chunks | accepted |
| [0124](0124-tablet-movement-external-prefix-reference-v1.md) | Tablet movement external-prefix reference v1 | accepted |
| [0125](0125-tablet-movement-reference-generation-v1.md) | Tablet movement reference generation v1 | accepted |
| [0126](0126-mixed-tablet-movement-checkpoint-generations.md) | Mixed tablet movement checkpoint generations | accepted |
| [0127](0127-composed-tablet-movement-checkpoint-recovery.md) | Composed tablet movement checkpoint recovery | accepted |
| [0128](0128-tablet-movement-rtas-handoff.md) | Tablet movement RTAS handoff | accepted |
| [0129](0129-tablet-movement-raft-snapshot-completion.md) | Tablet movement Raft snapshot completion | accepted |
| [0130](0130-durable-tablet-movement-ready-reconciliation.md) | Durable tablet movement ready reconciliation | accepted |
| [0131](0131-restartable-tablet-reconfiguration-phases.md) | Restartable tablet reconfiguration phases | accepted |
| [0132](0132-durable-tablet-reconfiguration-phase-checkpoints.md) | Durable tablet reconfiguration phase checkpoints | accepted |
| [0133](0133-prepared-tablet-reconfiguration-dispatch.md) | Prepared tablet reconfiguration dispatch boundary | accepted |
| [0134](0134-sealed-local-tablet-reconfiguration-execution.md) | Sealed local tablet reconfiguration execution | accepted |
| [0135](0135-bounded-asynchronous-prepared-reconfiguration-admission.md) | Bounded asynchronous prepared reconfiguration admission | accepted |
| [0136](0136-idempotent-retained-reconfiguration-action-replay.md) | Idempotent retained reconfiguration action replay | accepted |
| [0137](0137-current-term-raft-progress-noop.md) | Current-term Raft progress no-op | accepted |
| [0138](0138-fifo-ordered-raft-group-observation.md) | FIFO-ordered Raft group observation | accepted |
| [0139](0139-observation-driven-tablet-reconfiguration-reconciliation.md) | Observation-driven tablet reconfiguration reconciliation | accepted |
| [0140](0140-atomic-current-leader-term-admission.md) | Atomic current-leader term admission | accepted |
| [0141](0141-authenticated-remote-reconfiguration-receiver.md) | Authenticated remote reconfiguration receiver | accepted |
| [0142](0142-bounded-remote-reconfiguration-retry.md) | Bounded remote reconfiguration response and retry | accepted |
| [0143](0143-nonblocking-reconfiguration-response-publication.md) | Nonblocking reconfiguration response publication | accepted |
| [0144](0144-maintained-mutual-tls-socket-carrier.md) | Maintained mutual-TLS socket carrier | accepted |
| [0145](0145-bounded-epoll-mutual-tls-admission.md) | Bounded epoll mutual-TLS admission | accepted |
| [0146](0146-raft-tablet-physical-snapshot-projection.md) | Raft tablet physical snapshot projection | accepted |
| [0147](0147-tablet-physical-part-chunk-v1.md) | Tablet physical part chunk v1 | accepted |
| [0148](0148-durable-physical-part-chunk-receipt.md) | Durable physical-part chunk receipt | accepted |
| [0149](0149-idempotent-final-temporal-part-adoption.md) | Idempotent final temporal-part adoption | accepted |
| [0150](0150-verified-physical-part-destination-installation.md) | Verified physical-part destination installation | accepted |
| [0151](0151-raft-tablet-destination-manifest-composition.md) | Raft tablet destination Manifest composition | accepted |
| [0152](0152-atomic-temporal-manifest-publication.md) | Atomic temporal Manifest publication | accepted |
| [0153](0153-restartable-tablet-physical-ownership-publication.md) | Restartable tablet physical ownership publication | accepted |
| [0154](0154-physical-ownership-gated-tablet-movement-readiness.md) | Physical-ownership-gated tablet movement readiness | accepted |
| [0155](0155-durable-physical-part-receipt-reclamation.md) | Durable physical-part receipt reclamation | accepted |
| [0156](0156-authorized-raft-tablet-source-retirement-manifest.md) | Authorized Raft-tablet source-retirement Manifest | accepted |
| [0157](0157-durable-raft-tablet-source-retirement-installation.md) | Durable Raft-tablet source-retirement installation | accepted |
| [0158](0158-reader-pinned-raft-tablet-source-retirement-publication.md) | Reader-pinned Raft-tablet source-retirement publication | accepted |
| [0159](0159-reader-pinned-temporal-source-part-reclamation.md) | Reader-pinned temporal source-part reclamation | accepted |
| [0160](0160-authority-bound-temporal-retirement-recovery.md) | Authority-bound temporal retirement recovery | accepted |
| [0161](0161-canonical-distributed-aggregate-exchange-frame.md) | Canonical distributed aggregate exchange frame | accepted |
| [0162](0162-bounded-distributed-exchange-partial-io.md) | Bounded distributed exchange partial I/O | accepted |
| [0163](0163-bounded-distributed-fragment-sequencing.md) | Bounded distributed fragment sequencing | accepted |
| [0164](0164-snapshot-bound-distributed-aggregate-fragment-v1.md) | Snapshot-bound distributed aggregate fragment v1 | accepted |
| [0165](0165-group-scoped-distributed-fragment-dispatch.md) | Group-scoped distributed fragment dispatch | accepted |
| [0166](0166-authority-bound-distributed-fragment-construction.md) | Authority-bound distributed fragment construction | accepted |
| [0167](0167-proof-revalidated-distributed-aggregate-worker.md) | Proof-revalidated distributed aggregate worker | accepted |
| [0168](0168-authenticated-distributed-query-transport.md) | Authenticated distributed query transport | accepted |
| [0169](0169-bounded-distributed-query-carrier-lifecycle.md) | Bounded distributed query carrier lifecycle | accepted |
| [0170](0170-compatible-multi-tablet-manifest-snapshot-binding.md) | Compatible multi-tablet Manifest snapshot binding | accepted |
| [0171](0171-fail-closed-distributed-query-execution-owner.md) | Fail-closed distributed query execution owner | accepted |
| [0172](0172-maintained-mutual-tls-client-socket.md) | Maintained mutual-TLS client socket | accepted |
| [0173](0173-bounded-outbound-distributed-query-tls-carrier.md) | Bounded outbound distributed-query TLS carrier | accepted |
| [0174](0174-bounded-inbound-distributed-query-tls-carrier.md) | Bounded inbound distributed-query TLS carrier | accepted |
| [0175](0175-nonblocking-ipv4-tcp-descriptor-ownership.md) | Nonblocking IPv4 TCP descriptor ownership | accepted |
| [0176](0176-bounded-distributed-query-tcp-server.md) | Bounded distributed-query TCP server | accepted |
| [0177](0177-deadline-bound-distributed-query-tcp-client.md) | Deadline-bound distributed-query TCP client | accepted |
| [0178](0178-pinned-multi-tablet-tcp-query-scheduling.md) | Pinned multi-tablet TCP query scheduling | accepted |
| [0179](0179-whole-query-tcp-cancellation-deadline.md) | Whole-query TCP cancellation and deadline | accepted |
| [0180](0180-explicit-whole-query-authority-rebinding.md) | Explicit whole-query authority rebinding | accepted |
| [0181](0181-authenticated-distributed-leader-hint-publication.md) | Authenticated distributed leader-hint publication | accepted |
| [0182](0182-libcurl-sigv4-s3-object-store.md) | libcurl SigV4 S3 object-store backend | accepted |
| [0183](0183-separate-cold-location-manifest.md) | Separate cold-location manifest authority | accepted |
| [0184](0184-durable-cold-location-generations.md) | Durable cold-location generation installation and recovery | accepted |
| [0185](0185-atomic-tiered-storage-publication.md) | Atomic Manifest v2 and cold-location publication | accepted |
| [0186](0186-durable-tiered-pair-commit.md) | Durable Manifest v2/cold pair commit and recovery | accepted |
| [0187](0187-manifest-bound-tiered-cseg-loading.md) | Manifest-bound local and remote CSEG loading | accepted |
| [0188](0188-proof-gated-tiered-distributed-query-loading.md) | Proof-gated tiered distributed-query loading | accepted |
| [0189](0189-tier-aware-pair-recovery.md) | Tier-aware pair recovery for remote-only CSEGs | accepted |
| [0190](0190-reader-pinned-tiered-local-reclamation.md) | Reader-pinned tiered local CSEG reclamation | accepted |
| [0191](0191-manifest-retirement-bound-cold-route-removal.md) | Manifest-retirement-bound cold route removal | accepted |
| [0192](0192-exact-conditional-object-deletion.md) | Exact conditional object deletion | accepted |
| [0193](0193-reader-pinned-remote-object-reclamation.md) | Reader-pinned remote object reclamation | accepted |
| [0194](0194-optional-arrow-parquet-interoperability.md) | Optional Apache Arrow and Parquet interoperability | accepted |
| [0195](0195-durable-cold-history-remote-garbage-discovery.md) | Durable cold-history remote garbage discovery | accepted |
| [0196](0196-bounded-s3-retry-and-credential-refresh.md) | Bounded S3 retry and credential refresh | accepted |
| [0197](0197-conditional-s3-multipart-upload.md) | Conditional S3 multipart upload | accepted |
| [0198](0198-schema-source-bound-cseg-upload-admission.md) | Schema/source-bound CSEG upload admission | accepted |
| [0199](0199-explicit-s3-environment-credential-provider.md) | Explicit S3 environment credential provider | accepted |
| [0200](0200-concurrent-bounded-tiered-part-cache.md) | Concurrent bounded tiered-part cache | accepted |
| [0201](0201-authority-restored-volatile-tiered-cache.md) | Authority-restored volatile tiered cache | accepted |
| [0202](0202-source-general-tiered-local-reclamation.md) | Source-general tiered local reclamation | accepted |
| [0203](0203-strict-s3-multipart-completion-result.md) | Strict S3 multipart completion result | accepted |
| [0204](0204-bounded-s3-retry-after-hints.md) | Bounded S3 Retry-After hints | accepted |
| [0205](0205-explicit-s3-proxy-policy.md) | Explicit S3 proxy policy | accepted |
| [0206](0206-explicit-s3-server-side-encryption.md) | Explicit S3 server-side encryption | accepted |
| [0207](0207-bounded-parallel-s3-multipart-parts.md) | Bounded parallel S3 multipart parts | accepted |
| [0208](0208-strict-http-date-retry-after.md) | Strict HTTP-date Retry-After parsing | accepted |
| [0209](0209-bounded-s3-retry-jitter.md) | Bounded S3 retry jitter | accepted |
| [0210](0210-pinned-ordered-s3-credential-chain.md) | Pinned ordered S3 credential chain | accepted |
| [0211](0211-explicit-s3-container-credential-provider.md) | Explicit S3 container credential provider | accepted |
| [0212](0212-imdsv2-only-s3-instance-credentials.md) | IMDSv2-only S3 instance credentials | accepted |
| [0213](0213-packaged-native-daemon-lifecycle.md) | Packaged native daemon lifecycle | accepted |
| [0214](0214-durable-complete-schema-definitions.md) | Durable complete schema definitions | accepted |
| [0215](0215-complete-table-policy-metadata.md) | Complete table policy metadata | accepted |
| [0216](0216-durable-database-root-bootstrap.md) | Durable database-root bootstrap | accepted |
| [0217](0217-vectorized-tablet-state-query-source.md) | Vectorized tablet-state query source | accepted |
| [0218](0218-recoverable-single-node-database-owner.md) | Recoverable single-node database owner | accepted |
| [0219](0219-restartable-single-node-table-creation.md) | Restartable single-node table creation | accepted |
| [0220](0220-native-protocol-ingest-service-adapter.md) | Native protocol ingest service adapter | accepted |
| [0221](0221-global-multi-tablet-vector-source.md) | Global multi-tablet vector source | accepted |
| [0222](0222-bounded-native-vector-query-results.md) | Bounded native vector query results | accepted |
| [0223](0223-native-create-table-dispatch.md) | Native CREATE TABLE dispatch | accepted |
| [0224](0224-configured-single-node-chronosd.md) | Configured single-node chronosd | accepted |
| [0225](0225-sql-insert-columnar-materialization.md) | SQL INSERT columnar materialization | accepted |
| [0226](0226-native-sql-insert-dispatch.md) | Native SQL INSERT dispatch | accepted |
| [0227](0227-empty-manifest-namespace-initialization.md) | Empty Manifest namespace initialization | accepted |
| [0228](0228-single-node-manifest-root-ownership.md) | Single-node Manifest root ownership | accepted |
| [0229](0229-manifest-aware-single-node-startup.md) | Manifest-aware single-node startup | accepted |
| [0230](0230-live-single-node-sealed-head-flush.md) | Live single-node sealed-head flush | accepted |
| [0231](0231-manifest-snapshot-native-query-source.md) | Manifest-snapshot native query source | accepted |
| [0232](0232-shutdown-wal-checkpoint-publication.md) | Shutdown WAL checkpoint publication | accepted |
| [0233](0233-native-whole-table-asof-query-composition.md) | Native whole-table ASOF query composition | accepted |
| [0234](0234-fail-closed-native-historical-query-admission.md) | Fail-closed native historical query admission | accepted |
| [0235](0235-query-accounted-columnar-batch-source.md) | Query-accounted columnar batch source | accepted |
| [0236](0236-committed-append-subscription-result-changes.md) | Committed append subscription result changes | accepted |
| [0237](0237-single-node-applied-append-observation.md) | Single-node applied append observation | accepted |
| [0238](0238-fail-closed-subscription-continuity-loss.md) | Fail-closed subscription continuity loss | accepted |
| [0239](0239-bounded-single-node-live-append-fanout.md) | Bounded single-node live append fan-out | accepted |
| [0240](0240-write-synchronous-live-checkpoint-gate.md) | Write-synchronous live checkpoint gate | accepted |
| [0241](0241-single-node-subscription-runtime-composition.md) | Single-node subscription runtime composition | accepted |
| [0242](0242-configured-chronosd-subscription-lifecycle.md) | Configured chronosd subscription lifecycle | accepted |
| [0243](0243-canonical-raft-transport-envelope.md) | Canonical group-scoped Raft transport envelope | accepted |
| [0244](0244-pre-observation-raft-message-validation.md) | Pre-observation Raft message validation | accepted |
| [0245](0245-bounded-raft-transport-partial-io.md) | Bounded Raft transport partial-I/O ownership | accepted |
| [0246](0246-authenticated-raft-transport-receiver.md) | Authenticated Raft transport receiver | accepted |
| [0247](0247-persistent-inbound-raft-mtls-carrier.md) | Persistent inbound Raft mutual-TLS carrier | accepted |
| [0248](0248-persistent-outbound-raft-mtls-carrier.md) | Persistent outbound Raft mutual-TLS carrier | accepted |
| [0249](0249-generation-tagged-raft-runtime-timers.md) | Generation-tagged Raft runtime timers | accepted |
| [0250](0250-async-durable-raft-timer-driver.md) | Asynchronous durable Raft timer driver | accepted |
| [0251](0251-bounded-raft-peer-carrier-pool.md) | Bounded Raft peer carrier pool | accepted |
| [0252](0252-replayable-deterministic-raft-fault-simulator.md) | Replayable deterministic Raft fault simulator | accepted |
| [0253](0253-ownership-safe-raft-tcp-connect-attempt.md) | Ownership-safe Raft TCP connect attempt | accepted |
| [0254](0254-capped-raft-peer-reconnect-policy.md) | Capped Raft peer reconnect policy | accepted |
| [0255](0255-bounded-raft-outbound-peer-manager.md) | Bounded Raft outbound peer manager | accepted |
| [0256](0256-bounded-inbound-raft-tcp-server.md) | Bounded inbound Raft TCP server | accepted |
| [0257](0257-ordered-inbound-raft-observation.md) | Ordered inbound Raft observation | accepted |
| [0258](0258-portable-durable-raft-completion-wakeup.md) | Portable durable Raft completion wakeup | accepted |
| [0259](0259-exact-raft-runtime-deadline-introspection.md) | Exact Raft runtime deadline introspection | accepted |
| [0260](0260-embedding-owned-inbound-raft-readiness.md) | Embedding-owned inbound Raft readiness | accepted |
| [0261](0261-fifo-identified-raft-completions.md) | FIFO-identified Raft completions | accepted |
| [0262](0262-retain-admitted-raft-results-after-disconnect.md) | Retain admitted Raft results after disconnect | accepted |
| [0263](0263-immediate-outbound-raft-terminal-reconnect.md) | Immediate outbound Raft terminal reconnect | accepted |
| [0264](0264-client-initiated-raft-tls-handshake.md) | Client-initiated Raft TLS handshake | accepted |
| [0265](0265-unified-raft-transport-runtime.md) | Unified Raft transport runtime | accepted |
| [0266](0266-metadata-application-snapshot-v1.md) | Metadata Application Snapshot v1 | accepted |
| [0267](0267-durable-metadata-snapshot-installation.md) | Durable metadata snapshot installation | accepted |
| [0268](0268-owned-metadata-snapshot-compaction.md) | Owned metadata snapshot compaction and recovery | accepted |
| [0269](0269-node-wide-raft-log-reclamation.md) | Node-wide checkpointed Raft log reclamation | accepted |
| [0270](0270-raft-authoritative-application-snapshot-reclamation.md) | Raft-authoritative application snapshot reclamation | accepted |
| [0271](0271-native-protocol-v2-quorum-sync-negotiation.md) | Native Protocol 2.0 QUORUM_SYNC negotiation | accepted |
| [0272](0272-worker-affine-raft-application-extension.md) | Worker-affine durable Raft application extension | accepted |
| [0273](0273-bounded-term-bound-applied-quorum-completions.md) | Bounded term-bound applied-quorum completions | accepted |
| [0274](0274-nonblocking-replicated-ingest-operation.md) | Nonblocking replicated ingest operation | accepted |
| [0275](0275-negotiated-network-task-context.md) | Preserve negotiated context in network tasks | accepted |
| [0276](0276-bounded-replicated-ingest-coordinator.md) | Bounded replicated ingest coordinator | accepted |
| [0277](0277-bounded-worker-extension-composition.md) | Bounded durable Raft worker-extension composition | accepted |
| [0278](0278-worker-affine-metadata-application.md) | Worker-affine asynchronous metadata application | accepted |
| [0279](0279-authoritative-tablet-group-binding.md) | Authoritative tablet-to-Raft-group binding | accepted |
| [0280](0280-authoritative-replicated-ingest-routing.md) | Authoritative replicated-ingest routing | accepted |
| [0281](0281-committed-schema-replicated-ingest-admission.md) | Committed-schema replicated-ingest admission | accepted |
| [0282](0282-owning-replicated-ingest-runtime.md) | Owning replicated-ingest runtime composition | accepted |
| [0283](0283-bounded-reactor-facing-replicated-ingest-service.md) | Bounded reactor-facing replicated-ingest service | accepted |
| [0284](0284-committed-metadata-replicated-database-recovery.md) | Committed-metadata replicated database recovery | accepted |
| [0285](0285-strict-replicated-group-deployment-config.md) | Strict replicated-group deployment configuration | accepted |
| [0286](0286-explicit-replicated-native-daemon-mode.md) | Explicit replicated native-daemon mode | accepted |
| [0287](0287-strict-authenticated-raft-peer-config.md) | Strict authenticated Raft peer deployment configuration | accepted |
| [0288](0288-exact-raft-certificate-node-authority.md) | Exact Raft certificate-to-node authority | accepted |
| [0289](0289-owning-authenticated-raft-transport-runtime.md) | Owning authenticated Raft transport runtime | accepted |
| [0290](0290-packaged-authenticated-raft-peer-transport.md) | Packaged authenticated Raft peer transport | accepted |
| [0291](0291-stable-local-applied-replicated-query-snapshot.md) | Stable local-applied replicated query snapshot | accepted |
| [0292](0292-packaged-replicated-native-select.md) | Packaged replicated native SELECT | accepted |
| [0293](0293-ordered-application-raft-transport-submissions.md) | Ordered application Raft transport submissions | accepted |
| [0294](0294-applied-replicated-read-barrier-vector.md) | Applied replicated read-barrier vector | accepted |
| [0295](0295-negotiated-native-leader-redirect.md) | Negotiated native leader redirect | accepted |
| [0296](0296-authoritative-replicated-ingest-leader-redirect.md) | Authoritative replicated-ingest leader redirect | accepted |
| [0297](0297-metadata-backed-distributed-query-authority.md) | Metadata-backed distributed query authority | accepted |
| [0298](0298-committed-distributed-query-route-resolution.md) | Committed distributed query route resolution | accepted |
| [0299](0299-correlated-replicated-read-authority.md) | Correlated replicated read authority | accepted |
| [0300](0300-group-keyed-distributed-query-proof-binding.md) | Group-keyed distributed query proof binding | accepted |
| [0301](0301-bound-snapshot-distributed-query-execution.md) | Bound-snapshot distributed query execution | accepted |
| [0302](0302-packaged-replicated-distributed-query-construction.md) | Packaged replicated distributed query construction | accepted |
| [0303](0303-correlated-follower-read-proof-binding.md) | Correlated follower read proof binding | accepted |
| [0304](0304-packaged-bounded-stale-query-construction.md) | Packaged bounded-stale query construction | accepted |
| [0305](0305-bounded-dns-multi-address-query-routing.md) | Bounded DNS and multi-address distributed query routing | accepted |
| [0306](0306-authenticated-raft-observation-transport.md) | Authenticated Raft observation transport | accepted |
| [0307](0307-bounded-raft-observation-partial-io.md) | Bounded Raft observation partial-I/O ownership | accepted |
| [0308](0308-outbound-raft-observation-mtls-acquisition.md) | Outbound Raft observation mTLS acquisition | accepted |
| [0309](0309-deadline-bound-raft-observation-tcp-client.md) | Deadline-bound Raft observation TCP client | accepted |
| [0310](0310-bounded-inbound-raft-observation-mtls-session.md) | Bounded inbound Raft observation mTLS session | accepted |
| [0311](0311-bounded-raft-observation-tcp-server.md) | Bounded Raft observation TCP server | accepted |
| [0312](0312-finite-multi-address-raft-observation-acquisition.md) | Finite multi-address Raft observation acquisition | accepted |
| [0313](0313-correlated-raft-observation-pair-fan-out.md) | Correlated Raft observation pair fan-out | accepted |
| [0314](0314-catalog-backed-raft-observation-route-resolution.md) | Catalog-backed Raft observation route resolution | accepted |
| [0315](0315-canonical-multi-pair-raft-observation-acquisition.md) | Canonical multi-pair Raft observation acquisition | accepted |
| [0316](0316-placement-backed-raft-observation-batch-construction.md) | Placement-backed Raft observation batch construction | accepted |
| [0317](0317-packaged-remote-bounded-stale-query-lifecycle.md) | Packaged remote bounded-stale query lifecycle | accepted |
| [0318](0318-request-local-real-cseg-query-worker-service.md) | Request-local real-CSEG query worker service | accepted |
| [0319](0319-owned-real-cseg-distributed-query-tcp-service.md) | Owned real-CSEG distributed-query TCP service | accepted |
| [0320](0320-canonical-nullable-float64-grouped-exchange.md) | Canonical nullable-FLOAT64 grouped exchange | accepted |
| [0321](0321-bounded-grouped-exchange-partial-io.md) | Bounded grouped-exchange partial I/O | accepted |
| [0322](0322-distinct-empty-grouped-stream-terminal.md) | Distinct empty grouped-stream terminal | accepted |
| [0323](0323-bounded-grouped-terminal-partial-io.md) | Bounded grouped-terminal partial I/O | accepted |
| [0324](0324-bounded-grouped-float64-coordinator.md) | Bounded grouped FLOAT64 coordinator | accepted |
| [0325](0325-distinct-grouped-float64-fragment-intent.md) | Distinct grouped FLOAT64 fragment intent | accepted |
| [0326](0326-authority-bound-grouped-float64-fragment.md) | Authority-bound grouped FLOAT64 fragment | accepted |
| [0327](0327-group-scoped-grouped-float64-dispatch.md) | Group-scoped grouped FLOAT64 dispatch | accepted |
| [0328](0328-proof-revalidated-grouped-float64-worker.md) | Proof-revalidated grouped FLOAT64 worker | accepted |
| [0329](0329-packaged-authority-bound-grouped-dispatch.md) | Packaged authority-bound grouped dispatch | accepted |
| [0330](0330-distinct-grouped-float64-query-transport.md) | Distinct grouped FLOAT64 query transport | accepted |
| [0331](0331-bounded-grouped-query-partial-io.md) | Bounded grouped query partial I/O | accepted |
| [0332](0332-authenticated-grouped-query-receiver.md) | Authenticated grouped query receiver | accepted |
| [0333](0333-request-local-real-cseg-grouped-worker-service.md) | Request-local real-CSEG grouped worker service | accepted |
| [0334](0334-owned-real-cseg-grouped-query-receiver.md) | Owned real-CSEG grouped query receiver | accepted |
| [0335](0335-bounded-grouped-query-mutual-tls.md) | Bounded grouped-query mutual TLS | accepted |
| [0336](0336-deadline-bound-grouped-query-tcp-client.md) | Deadline-bound grouped-query TCP client | accepted |
| [0337](0337-bounded-grouped-query-tcp-server.md) | Bounded grouped-query TCP server | accepted |
| [0338](0338-owned-real-cseg-grouped-query-tcp-service.md) | Owned real-CSEG grouped-query TCP service | accepted |
| [0339](0339-finite-grouped-query-sender.md) | Finite grouped-query sender | accepted |
| [0340](0340-compatible-grouped-float64-snapshot-binding.md) | Compatible grouped FLOAT64 snapshot binding | accepted |
| [0341](0341-fail-closed-grouped-query-execution-owner.md) | Fail-closed grouped query execution owner | accepted |
| [0342](0342-pinned-grouped-query-tcp-scheduling.md) | Pinned grouped-query TCP scheduling | accepted |
| [0343](0343-packaged-leader-linearizable-grouped-query-construction.md) | Packaged leader-linearizable grouped-query construction | accepted |
| [0344](0344-packaged-bounded-stale-grouped-query-construction.md) | Packaged bounded-stale grouped-query construction | accepted |
| [0345](0345-packaged-remote-bounded-stale-grouped-query-lifecycle.md) | Packaged remote bounded-stale grouped-query lifecycle | accepted |
| [0346](0346-explicit-grouped-whole-query-rebinding.md) | Explicit grouped whole-query rebinding | accepted |
| [0347](0347-real-cseg-tablet-movement-query-gate.md) | Real-CSEG tablet-movement query gate | accepted |
| [0348](0348-global-grouped-order-and-limit.md) | Global grouped order and limit | accepted |
| [0349](0349-global-grouped-aggregate-ordering.md) | Global grouped aggregate ordering | accepted |
| [0350](0350-canonical-distributed-vector-batch-exchange.md) | Canonical distributed vector-batch exchange | accepted |
| [0351](0351-bounded-distributed-vector-partial-io.md) | Bounded distributed vector partial I/O | accepted |
| [0352](0352-canonical-distributed-vector-plan-intent.md) | Canonical distributed vector plan intent | accepted |
| [0353](0353-group-scoped-distributed-vector-fragment.md) | Group-scoped distributed vector fragment | accepted |
| [0354](0354-authority-bound-distributed-vector-fragment.md) | Authority-bound distributed vector fragment | accepted |
| [0355](0355-compatible-multi-tablet-vector-snapshot.md) | Compatible multi-tablet vector snapshot | accepted |
| [0356](0356-bounded-distributed-vector-fragment-partial-io.md) | Bounded distributed vector fragment partial I/O | accepted |
| [0357](0357-metadata-backed-distributed-vector-snapshot.md) | Metadata-backed distributed vector snapshot | accepted |
| [0358](0358-group-keyed-distributed-vector-proof-binding.md) | Group-keyed distributed vector proof binding | accepted |
| [0359](0359-correlated-follower-vector-proof-binding.md) | Correlated follower vector proof binding | accepted |
| [0360](0360-distinct-distributed-vector-query-request.md) | Distinct distributed vector query request | accepted |
| [0361](0361-correlated-distributed-vector-query-response.md) | Correlated distributed vector query response | accepted |
| [0362](0362-bounded-distributed-vector-query-partial-io.md) | Bounded distributed vector query partial I/O | accepted |
| [0363](0363-bounded-distributed-vector-coordinator.md) | Bounded distributed vector coordinator | accepted |
| [0364](0364-schema-light-distributed-vector-results.md) | Schema-light distributed vector results | accepted |
| [0365](0365-schema-bound-distributed-vector-fragment.md) | Schema-bound distributed vector fragment | accepted |
| [0366](0366-schema-bound-distributed-vector-result-exchange.md) | Schema-bound distributed vector result exchange | accepted |
| [0367](0367-bounded-distributed-vector-fragment-v2-ownership.md) | Bounded distributed vector Fragment v2 ownership | accepted |
| [0368](0368-schema-bound-distributed-vector-query-transport-v2.md) | Schema-bound distributed vector query transport v2 | accepted |
| [0369](0369-authenticated-schema-bound-vector-query-receiver-v2.md) | Authenticated schema-bound vector query receiver v2 | accepted |
| [0370](0370-bounded-schema-bound-vector-query-v2-mutual-tls.md) | Bounded schema-bound vector query v2 mutual TLS | accepted |
| [0371](0371-deadline-bound-schema-bound-vector-query-v2-tcp-client.md) | Deadline-bound schema-bound vector query v2 TCP client | accepted |
| [0372](0372-bounded-schema-bound-vector-query-v2-tcp-server.md) | Bounded schema-bound vector query v2 TCP server | accepted |
| [0373](0373-finite-schema-bound-vector-query-v2-sender.md) | Finite schema-bound vector query v2 sender | accepted |
| [0374](0374-bounded-schema-bound-vector-result-coordinator-v2.md) | Bounded schema-bound vector result coordinator v2 | accepted |
| [0375](0375-proof-revalidated-schema-bound-vector-row-worker-v2.md) | Proof-revalidated schema-bound vector row worker v2 | accepted |
| [0376](0376-owned-schema-bound-vector-query-v2-tcp-service.md) | Owned schema-bound vector query v2 TCP service | accepted |
| [0377](0377-pinned-schema-bound-vector-query-v2-execution-owner.md) | Pinned schema-bound vector query v2 execution owner | accepted |
| [0378](0378-pinned-multi-tablet-vector-query-v2-tcp-scheduling.md) | Pinned multi-tablet vector query v2 TCP scheduling | accepted |
| [0379](0379-bounded-global-vector-row-finalization-v2.md) | Bounded global vector row finalization v2 | accepted |
| [0380](0380-mergeable-all-type-vector-aggregate-state.md) | Mergeable all-type vector aggregate state | accepted |
| [0381](0381-canonical-mergeable-vector-aggregate-state-bytes.md) | Canonical mergeable vector aggregate state bytes | accepted |
| [0382](0382-schema-bound-ungrouped-vector-aggregate-exchange.md) | Schema-bound ungrouped vector aggregate exchange | accepted |
| [0383](0383-owned-cross-tablet-vector-aggregate-definitions.md) | Owned cross-tablet vector aggregate definitions | accepted |
| [0384](0384-proof-revalidated-vector-aggregate-worker-v2.md) | Proof-revalidated vector aggregate worker v2 | accepted |
| [0385](0385-bounded-vector-aggregate-coordinator-v2.md) | Bounded vector aggregate coordinator v2 | accepted |
| [0386](0386-native-vector-aggregate-result-finalization-v2.md) | Native vector aggregate result finalization v2 | accepted |
| [0387](0387-definition-bound-vector-aggregate-query-response-v2.md) | Definition-bound vector aggregate query response v2 | accepted |
| [0388](0388-authenticated-vector-aggregate-query-receiver-v2.md) | Authenticated vector aggregate query receiver v2 | accepted |
| [0389](0389-request-local-real-cseg-vector-aggregate-worker-service-v2.md) | Request-local real-CSEG vector aggregate worker service v2 | accepted |
| [0390](0390-finite-definition-bound-vector-aggregate-query-sender-v2.md) | Finite definition-bound vector aggregate query sender v2 | accepted |
| [0391](0391-bounded-definition-bound-vector-aggregate-query-v2-mutual-tls.md) | Bounded definition-bound vector aggregate query v2 mutual TLS | accepted |
| [0392](0392-deadline-bound-definition-bound-vector-aggregate-query-v2-tcp-client.md) | Deadline-bound definition-bound vector aggregate query v2 TCP client | accepted |
| [0393](0393-bounded-definition-bound-vector-aggregate-query-v2-tcp-server.md) | Bounded definition-bound vector aggregate query v2 TCP server | accepted |
| [0394](0394-owned-definition-bound-vector-aggregate-query-v2-tcp-service.md) | Owned definition-bound vector aggregate query v2 TCP service | accepted |
| [0395](0395-pinned-definition-bound-vector-aggregate-query-v2-execution-owner.md) | Pinned definition-bound vector aggregate query v2 execution owner | accepted |
| [0396](0396-pinned-vector-aggregate-query-v2-tcp-scheduling-and-finalization.md) | Pinned vector aggregate query v2 TCP scheduling and finalization | accepted |
| [0397](0397-metadata-backed-schema-bound-vector-v2-snapshots.md) | Metadata-backed schema-bound vector v2 snapshots | accepted |
| [0398](0398-committed-vector-v2-query-route-resolution.md) | Committed vector v2 query route resolution | accepted |
| [0399](0399-packaged-leader-linearizable-vector-aggregate-v2-query.md) | Packaged leader-linearizable vector aggregate v2 query | accepted |
| [0400](0400-packaged-bounded-stale-vector-aggregate-v2-query.md) | Packaged bounded-stale vector aggregate v2 query | accepted |
| [0401](0401-placement-backed-vector-raft-observation-batch-construction.md) | Placement-backed vector Raft observation batch construction | accepted |
| [0402](0402-complete-remote-follower-vector-aggregate-v2-lifecycle.md) | Complete remote follower vector aggregate v2 lifecycle | accepted |
| [0403](0403-injectable-common-time-source.md) | Injectable common time source | accepted |
| [0404](0404-injectable-system-uuid-entropy.md) | Injectable system UUID entropy | accepted |
| [0405](0405-verified-logical-to-physical-wal-prefix-resolution.md) | Verified logical-to-physical WAL prefix resolution | accepted |
| [0406](0406-prevalidated-subscription-wal-prefix-reclamation.md) | Prevalidated subscription WAL prefix reclamation | accepted |
| [0407](0407-source-tagged-resume-token-v2.md) | Source-tagged Resume Token v2 | accepted |
| [0408](0408-source-tagged-subscription-checkpoint-v2.md) | Source-tagged Subscription Checkpoint v2 | accepted |
| [0409](0409-source-tagged-native-subscription-changes.md) | Source-tagged Native Subscription Changes | accepted |
| [0410](0410-raft-subscription-snapshot-and-prefix-reclamation.md) | Raft subscription snapshot and prefix reclamation | accepted |
| [0411](0411-mixed-wal-raft-subscription-snapshot.md) | Mixed WAL/Raft subscription snapshot | accepted |
| [0412](0412-dynamic-subscription-plan-retention-owners.md) | Dynamic subscription-plan retention owners | accepted |
| [0413](0413-bounded-native-leader-redirect-routing.md) | Bounded native leader-redirect routing | accepted |
| [0414](0414-exact-native-quorum-ingest-redirect-replay.md) | Exact native QUORUM_SYNC redirect replay | accepted |
| [0415](0415-deadline-bound-native-quorum-ingest-tcp-client.md) | Deadline-bound native QUORUM_SYNC TCP client | accepted |
| [0416](0416-bounded-native-quorum-ingest-tcp-execution.md) | Bounded native QUORUM_SYNC TCP execution | accepted |
| [0417](0417-strict-native-client-route-configuration.md) | Strict native-client route configuration | accepted |
| [0418](0418-owning-native-client-tls-routes.md) | Owning native-client TLS routes | accepted |
| [0419](0419-packaged-native-quorum-sync-client.md) | Packaged native QUORUM_SYNC client command | accepted |
| [0420](0420-packaged-native-mutual-tls-server.md) | Packaged native mutual-TLS server | accepted |
| [0421](0421-descriptor-bound-in-memory-tls-credentials.md) | Descriptor-bound in-memory TLS credentials | accepted |
| [0422](0422-transactional-native-tls-security-reload.md) | Transactional native TLS security reload | accepted |
| [0423](0423-packaged-loopback-sql-client.md) | Packaged loopback SQL client command | accepted |
| [0424](0424-exact-native-query-redirect-replay.md) | Exact native finite-query redirect replay | accepted |
| [0425](0425-deadline-bound-native-query-tcp-client.md) | Deadline-bound native finite-query TCP client | accepted |
| [0426](0426-bounded-native-query-tcp-execution.md) | Bounded native finite-query TCP execution | accepted |
| [0427](0427-packaged-single-group-routed-sql-client.md) | Packaged single-group routed SQL client | accepted |
| [0428](0428-authoritative-co-located-native-query-redirect.md) | Authoritative co-located native query redirect | accepted |
| [0429](0429-distinct-proof-bound-mutable-vector-fragment.md) | Distinct proof-bound mutable vector fragment | accepted |
| [0430](0430-distinct-mutable-vector-query-transport.md) | Distinct mutable vector query transport | accepted |
| [0431](0431-mutual-tls-mutable-vector-query-carrier.md) | Mutual-TLS mutable vector query carrier | accepted |
| [0432](0432-bounded-mutable-vector-query-tcp-ownership.md) | Bounded mutable vector query TCP ownership | accepted |
| [0433](0433-request-local-mutable-vector-query-service.md) | Request-local mutable vector query service | accepted |
| [0434](0434-proof-bound-mutable-vector-query-execution.md) | Proof-bound mutable vector query execution | accepted |
| [0435](0435-bounded-mutable-vector-query-tcp-scheduling.md) | Bounded mutable vector query TCP scheduling | accepted |
| [0436](0436-compatible-mutable-query-authority-rebinding.md) | Compatible mutable query authority rebinding | accepted |
| [0437](0437-correlated-replicated-mutable-fragment-binding.md) | Correlated replicated mutable fragment binding | accepted |
| [0438](0438-committed-mutable-query-route-composition.md) | Committed mutable query route composition | accepted |
| [0439](0439-schema-bound-distributed-row-sql-lowering.md) | Schema-bound distributed row SQL lowering | accepted |
| [0440](0440-correlated-sql-mutable-query-preparation.md) | Correlated SQL mutable query preparation | accepted |
| [0441](0441-bounded-native-mutable-row-query-ownership.md) | Bounded Native mutable row query ownership | accepted |
| [0442](0442-bounded-native-distributed-row-request-wiring.md) | Bounded Native distributed row request wiring | accepted |
| [0443](0443-database-owned-mutable-worker-context.md) | Database-owned mutable worker context | accepted |
| [0444](0444-proof-revalidated-local-and-remote-native-row-merge.md) | Proof-revalidated local and remote Native row merge | accepted |
| [0445](0445-committed-daemon-mutable-query-plane.md) | Committed daemon mutable query plane | accepted |
| [0446](0446-reactor-visible-native-query-cancellation.md) | Reactor-visible Native query cancellation | accepted |
| [0447](0447-fresh-all-group-native-query-rebinding.md) | Fresh all-group Native query rebinding | accepted |
| [0448](0448-distributed-event-time-between-lowering.md) | Distributed event-time BETWEEN lowering | accepted |
| [0449](0449-hidden-distributed-row-order-outputs.md) | Hidden distributed row order outputs | accepted |
| [0450](0450-schema-bound-distributed-global-aggregate-sql-lowering.md) | Schema-bound distributed global aggregate SQL lowering | accepted |
| [0451](0451-bounded-mutable-row-global-aggregate-finalization.md) | Bounded mutable-row global aggregate finalization | accepted |
| [0452](0452-replicated-native-global-aggregate-execution.md) | Replicated Native global aggregate execution | accepted |
| [0453](0453-coordinator-owned-source-independent-row-outputs.md) | Coordinator-owned source-independent row outputs | accepted |
| [0454](0454-bounded-coordinator-row-expression-execution.md) | Bounded coordinator row-expression execution | accepted |
| [0455](0455-bounded-coordinator-row-predicate-execution.md) | Bounded coordinator row-predicate execution | accepted |
| [0456](0456-bounded-coordinator-global-aggregate-predicates.md) | Bounded coordinator global-aggregate predicates | accepted |
| [0457](0457-bounded-coordinator-computed-row-ordering.md) | Bounded coordinator computed row ordering | accepted |
| [0458](0458-bounded-coordinator-global-aggregate-output-expressions.md) | Bounded coordinator global-aggregate output expressions | accepted |
| [0459](0459-bounded-row-backed-distributed-grouped-sql.md) | Bounded row-backed distributed grouped SQL | accepted |
| [0460](0460-three-daemon-mutable-sql-failover-qualification.md) | Three-daemon mutable SQL failover qualification | accepted |
| [0461](0461-authenticated-remote-raft-read-authority.md) | Authenticated remote Raft read authority | accepted |
| [0462](0462-bounded-raft-read-authority-partial-io-and-mtls.md) | Bounded Raft read-authority partial I/O and mutual TLS | accepted |
| [0463](0463-deadline-bound-raft-read-authority-tcp.md) | Deadline-bound Raft read-authority TCP endpoints | accepted |
| [0464](0464-finite-multi-address-raft-read-authority-acquisition.md) | Finite multi-address Raft read-authority acquisition | accepted |
| [0465](0465-all-or-nothing-raft-read-authority-fanout.md) | All-or-nothing Raft read-authority fan-out | accepted |
