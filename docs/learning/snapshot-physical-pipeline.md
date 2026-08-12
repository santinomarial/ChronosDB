# Snapshot Physical Pipeline Instantiation

## Purpose and public interface

`instantiate_snapshot_tablet_pipeline` connects a reusable `PhysicalPipelinePlan` to the complete
append-only tablet source selected by one `DatabaseStorageSnapshot`. It is the first production
storage adapter for the supported vector SQL subset; parse, bind, and physical lowering remain
separate so their source-spanned SQL diagnostics survive unchanged.

The single-node native SELECT adapter now acquires this source through its database owner for the
canonical local tablet vector. A tablet absent from the durable Manifest but present in aggregate
head publication receives an empty bounded CSEG plan, so the same path covers head-only and mixed
CSEG/head epochs. Native writes refresh aggregate head publication before later queries acquire it.

The call takes query resources, Manifest storage, the held aggregate snapshot, tablet and schema
lineage identities, the physical plan, and `SnapshotTabletPipelineLimits`. It returns one
thread-affine pull operator or an explicit status. The optimized overload accepts an
`OptimizedPhysicalPipelinePlan`, requires its exact one-source shape, and consumes runtime spill
targets for any selected external sort.

## Shape and stage invariants

The plan input is authoritative. Its prefix must exactly equal every destination-schema column in
ordinal order, including logical type and nullability. The only accepted extension is the shared
non-null suffix:

1. WAL ID (`UUID`);
2. record sequence (`UINT64`);
3. row ordinal (`UINT32`); and
4. operation (`UINT8`).

An exact schema-width input selects omit mode. An exact schema-plus-four input selects append mode.
No caller flag can disagree with this decision. This matters because base-row ORDER BY lowering
uses the suffix after its DEDUP logical identity, while aggregate ORDER BY uses encoded group-key
identity and therefore accepts ordinary schema input.

The connector does not reinterpret stages. The lowerer's established order remains WHERE, scalar
preparation, aggregation where present, ORDER BY, hidden-column removal, and LIMIT. All schema
columns are currently sourced, and WHERE executes exactly in the pipeline. These conservative
choices preserve truth until checked predicate and projection pushdown contracts exist.

## Construction and ownership

Construction performs the following sequence:

1. find and validate the destination schema;
2. validate the complete plan input and infer suffix mode;
3. build a bounded durable-part plan for the target tablet;
4. load its authenticated snapshot-bound images;
5. allocate the full schema-ordinal projection;
6. compose durable, sealed-head, and active-head children with one suffix mode; and
7. instantiate the physical pipeline's runtime source-shape boundary and stages.

Every source and output remains governed by the shared `QueryResourceContext`. Any failure destroys
images and partially constructed children through RAII. Cancellation is shared across all stages;
returned chunks may outlive the pipeline because they carry their own pins and reservations.

## Failure behavior and complexity

Missing schema/tablet identities, mismatched user columns, and malformed suffixes are invalid
arguments. Finite planning, head-count, configuration, decode, and query-memory bounds report
resource exhaustion. Storage validation and I/O errors remain unchanged. New `bad_alloc` and
container-length failures are converted to resource exhaustion.

For `C` schema columns, `P` selected parts, and `H` heads, connector setup is `O(C + P + H)` plus
synchronous validation/I/O for selected parts. Execution complexity is the sum of the chosen source
and pipeline stages; the connector adds no per-row work.

## Evidence and tradeoffs

End-to-end tests lower and execute a filtered descending LIMIT query, proving automatic suffix
selection and client-visible helper removal. A global aggregate test proves suffix-free source
selection. Hostile shapes and limits fail before leaking credit, and allocation injection sweeps all
new construction allocations. Authenticated fuzzing varies shape, suffix validity, cancellation,
and limits. `instantiate_and_execute_snapshot_pipeline` measures connector construction plus a
bounded full-head execution.

The eager full-column design is deliberately simple and exact. It leaves CPU/I/O opportunities on
the table, but predicate/projection pushdown needs explicit equivalence and required-column
contracts. One aggregate-publication charge is now shared across the source graph. The optimized
connector can select external spill for SQL ORDER BY, but does not split one tablet into parallel
tasks; that requires an accepted morsel and ordering contract.

## Review questions

**Why infer suffix mode from input shape?** The lowerer already encodes its semantic dependency in
the checked source shape. A second caller-controlled flag can only duplicate or contradict it.

**Why not infer it from a sort stage?** Aggregate sorts use group identity, while base sorts use row
identity. Stage inspection cannot distinguish the source requirement safely.

**Why is SQL lowering not part of this function?** SQL errors carry spans and diagnostic categories;
storage and physical construction errors do not. Separate steps keep both contracts precise.
