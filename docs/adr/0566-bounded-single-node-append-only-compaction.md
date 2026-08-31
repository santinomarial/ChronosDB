# ADR 0566: Bounded Single-Node Append-Only Compaction

- **Status:** accepted
- **Date:** 2026-08-31
- **Owners:** ChronosDB single-node service and storage maintainers

## Context

ADR 0018 and the Phase 7 implementation already provide deterministic append-only overlap
planning, complete CSEG v1 row-equivalence proof, output/Manifest installation, atomic aggregate
publication, exact durable-successor resumption, and pin-aware reclamation. The reusable
`AppendOnlyCompactionCoordinator` deliberately requires a caller to provide the exact sorted input
set, fresh output `PartId`, and both installation nonces.

The recoverable `SingleNodeDatabase` owned live Manifest-v1 flush but exposed no product operation
that planned and invoked compaction. No product owner therefore supplied output identities from the
same locked namespace used by installation. A naive caller could reuse a selected or orphan final
part, collide with a recognized temporary, retry forever, or discard a coordinator's fail-closed
state after a Manifest became durable but publication failed.

## Decision

`SingleNodeDatabase::compact_append_only_parts()` synchronously plans and executes at most one
append-only compaction. It acquires one aggregate storage publication and passes its complete part
descriptors to `plan_append_only_compaction()` under caller-selectable finite planner limits. No
candidate returns a successful empty optional and consumes no generated identity.

For one plan, the owner rebuilds exact retained schema bindings for every durable tablet. It then
scans the Manifest storage namespace under its lifetime writer lock and requires the publication's
generation to equal the highest final generation. Before invoking the coordinator it allocates:

1. one output `PartId` that is nonnil and absent from every final part, including orphans, and from
   the `PartId` component of every recognized temporary part;
2. one nonnil part-install nonce distinct from the output identity and from its exact temporary
   candidate name; and
3. one nonnil next-generation Manifest nonce distinct from both earlier values and from its exact
   temporary candidate name.

Each identity uses the database's configured nonzero `maximum_storage_identity_attempts`. Generator
failure propagates immediately. Nil or collision candidates consume attempts; exhaustion returns
`RESOURCE_EXHAUSTED`. All identities are resolved before the coordinator merges rows, acquires
output work, or mutates the filesystem.

The method creates one coordinator for the call and passes the exact owned plan, namespace-derived
identities, schema bindings, compression policy, and resource limits. If the coordinator becomes
unusable after a durable successor, the database copies its poison status into lifetime state and
rejects every later in-process compaction. Restart recovery is the only authority that may select
and publish that durable truth. A pre-Manifest failure leaves the database compaction path retryable
with fresh identities, while installed orphan outputs remain collision-visible.

The operation does not automatically reclaim retained inputs, schedule background work, infer a
throttle, compact temporal CSEG v2, or change query/row semantics. Existing pin-aware reclamation
remains a separate explicit authority.

## Detailed rationale

Planning from one aggregate publication ensures the candidate describes query-visible durable
parts, while the immediately following locked namespace scan supplies the complete installation
collision set. Requiring those generations to agree prevents allocation against a stale
publication. Separating planning from identity generation makes the no-work path deterministic and
entropy-free.

The output identity must consider unreferenced finals because a crash may leave a valid orphan that
ordinary recovery conservatively retains. It must also consider every temporary's embedded part
identity because temporary cleanup has not run during the live process. Exact nonce-name checks
prevent exclusive-create collisions from becoming the normal control path. A finite bound makes a
broken or adversarial injected source observable without touching durable state.

Retaining coordinator poison outside the local coordinator object preserves its recovery contract.
Discarding that bit after a durable-but-unpublished successor would allow a later operation to act
on an ambiguous live epoch rather than restart from the highest durable Manifest.

## Alternatives considered

- **Expose only the reusable coordinator:** keeps the service small but leaves product callers to
  invent namespace, identity, retry, and poison policy; this was the incomplete state.
- **Generate identities inside `ManifestStorage`:** storage can see names but does not own planner,
  publication, schema-lineage, or product entropy policy, and would conflate installation with work
  selection.
- **Allocate before planning:** wastes entropy on the common no-candidate path and may reserve an
  identity for no exact operation.
- **Treat an existing name as collision retry:** occurs after output work/filesystem mutation and
  cannot distinguish stale publication, orphan finals, and candidate-name conflict as cleanly as
  preflight.
- **Run automatic background compaction now:** requires accepted scheduling, throttling, shutdown,
  and foreground-interference policy. The synchronous bounded primitive is independently useful and
  testable without inventing those policies.

## Consequences

The single-node product can now reduce eligible Manifest-v1 overlap through the existing complete
equivalence and crash-safe publication path. No-work, generator failure, and collision exhaustion
are mutation-free. Output identities cannot reuse any live final or temporary namespace value, and
post-durability ambiguity remains fail-closed until restart.

The call is blocking and may perform substantial decode, merge, compression, synchronization, and
publication work. Operators must choose resource bounds and invoke it deliberately. It produces at
most one output because the accepted reference merger does; large partitioned outputs and temporal
compaction remain future work.

## Affected invariants

- [Invariant 2](../architecture/invariants.md#2-manifests-reference-only-completely-installed-durable-parts):
  the product path still installs the proven output before its replacement Manifest.
- [Invariant 3](../architecture/invariants.md#3-immutable-parts-are-never-modified-in-place): fresh
  allocation rejects selected, orphan, and temporary part identities before installation.
- [Invariant 6](../architecture/invariants.md#6-queries-observe-stable-snapshots) and
  [Invariant 7](../architecture/invariants.md#7-compaction-preserves-visible-logical-rows): the
  existing full-row oracle and atomic aggregate publication remain mandatory.
- [Invariant 8](../architecture/invariants.md#8-recovery-is-idempotent): durable-successor ambiguity
  poisons live compaction and delegates selection to restart recovery.
- [Invariant 11](../architecture/invariants.md#11-referenced-storage-is-not-reclaimed): input finals
  remain retained; this operation grants no unlink authority.
- [Invariant 18](../architecture/invariants.md#18-optimization-cannot-weaken-guarantees): planning and
  identity policy cannot bypass equivalence, installation, or publication proofs.

## Validation plan

Product-path tests create two overlapping flushed parts, first prove that no candidate consumes no
identity, then inject nil, selected-final, retained-temporary, same-operation, and exact Manifest
candidate collisions before accepting fresh values. They require one equivalent output generation,
retained input finals, unchanged row visibility through the aggregate native query path, and the
same output/query result after shutdown and recovery.

A separate repeating-collision case exhausts the exact finite output-attempt bound and requires the
selected generation, selected part set, final-file count, and temporary-file count to remain
unchanged. The same database then succeeds with fresh output/nonces, proving that exhaustion did not
poison or consume compaction work. Existing coordinator unit, subprocess crash, equivalence,
reclamation, sanitizer, and full repository suites remain required.

## Migration or rollback considerations

No durable or network bytes change. Existing CSEG v1 parts and Manifest v1 generations reopen
unchanged. Older binaries that already support Phase 7 replacement generations can recover outputs
created by this method; a Phase 6-only writer still must not extend a history after replacement, as
recorded by ADR 0018.

Removing the product method leaves already compacted histories valid. Changing collision or poison
authority requires a new decision because it affects immutable naming and recovery ownership.

## Unresolved questions

- Background scheduling, per-table/tablet prioritization, throughput throttling, cancellation, and
  shutdown coordination remain deferred pending workload measurements.
- Multi-output partitioning and temporal CSEG v2 compaction require their own accepted equivalence
  and publication contracts.
- Automatic invocation of the existing pin-aware retired-input reclaimer remains separate from
  compaction success.

## References

- [ADR 0018](0018-append-only-cseg-compaction-and-manifest-replacement.md)
- [ADR 0019](0019-rebuildable-pruning-delta-planning-and-part-reclamation.md)
- [ADR 0564](0564-authoritative-bounded-live-flush-identity-allocation.md)
- [Manifest v1](../formats/manifest-v1.md)
- [Manifest installation and checkpointing](../architecture/manifest-installation-and-checkpointing.md)
- [Append-only compaction](../learning/append-only-compaction.md)
- [Single-node database owner](../learning/single-node-database-owner.md)
