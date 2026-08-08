# Bounded Physical Strategy Selection

## Purpose and boundary

`OptimizedPhysicalPipelinePlan` connects the accepted in-memory sort, external sort, serial source
composition, and bounded parallel scheduler without changing the checked stage order. It owns the
exact `PhysicalPipelinePlan` it selected for and instantiates one pull pipeline from a declared
finite set of same-shape sources.

This is a capacity-and-cost strategy selector, not a relational rewrite optimizer. It does not
derive statistics, push predicates, reorder joins or aggregates, invent partial plans, or infer
that scheduling order is semantically irrelevant.

## Inputs, decisions, and runtime targets

`PhysicalExecutionStatistics` names the number of source tasks, a source row upper bound, abstract
work units, the source merge requirement, and one ordered estimate per physical sort. A sort
estimate includes maximum total rows, maximum input-chunk rows, logical/retained output bytes,
serialized spill bytes, and one serialized record.

`PhysicalExecutionCapabilities` names bounded workers/scheduler limits and stage-indexed spill
limits. `PhysicalOptimizerPolicy` bounds all retained selection state and supplies in-memory and
parallel thresholds. The result exposes source merge strategy, selected workers, per-sort
strategies, deterministic cost units, and retained configuration bytes.

External directory descriptors are query-runtime capabilities and cannot live in a reusable plan.
`ExternalSortExecutionTarget` supplies one open descriptor-relative directory and exclusive prefix
for each external decision. `PhysicalPipelinePlan` then replaces only those exact `SortStage`
instances with `SpillSortOperator`; every key comes from the checked plan.

## Selection rules

An in-memory sort is admissible only when its authoritative maximum fits:

- the policy row/retained thresholds;
- the stage row and output-row bounds; and
- the stage logical and retained output-byte bounds.

If it fits, it is selected. Otherwise, an external capability must cover total rows, the largest
whole input chunk, disk bytes, record bytes, and key limits. Failure to find either finite path is
resource exhaustion, not a silently weakened sort.

For `R` rows and `K` keys, reported in-memory comparison work is
`R * ceil(log2(R)) * K`, saturating on overflow. External work reports bounded run-sort work plus
the current baseline merge scan `R * runs * K`; I/O is twice the spill-byte upper bound. The units
are for deterministic comparison and profiling, not wall-clock predictions.

Source tasks stay serial unless the caller declares the whole downstream pipeline
`kOrderIndependent`. With enough tasks, rows, work, and workers, parallel work is estimated as
`ceil(work/workers) + workers*overhead`; it is selected only when lower than serial work. A plan
with order-sensitive accumulation, LIMIT, LATEST, ASOF, or incomplete ties must retain the default
ordered requirement.

## Ownership, failure, and cancellation

The optimized plan is move-only and owns its checked pipeline plus bounded decision vector. Serial
merge reserves query credit for its source owners/container before construction and drains complete
tasks in ordinal order. Parallel selection delegates to `ParallelMergeOperator`, including worker
thread affinity, bounded publication, deterministic failure arbitration, cancellation, and joins.

External instantiation consumes its directory handles into spill operators. Missing, duplicate,
reordered, closed, or wrong-stage targets fail before execution. Runtime operator limits still
check every chunk and byte even when a planner supplied smaller estimates. Any error uses ordinary
RAII to destroy sources, join workers, remove spill files, and return credit.

The snapshot adapter builds one complete append-only tablet source with the optimized plan's exact
input shape, then instantiates it as the plan's sole source. This supports optimizer-selected
external SQL ORDER BY while retaining one aggregate snapshot publication and the original hidden
identity contract.

## Complexity and evidence

Selection is `O(stages + spill capabilities)` and owns `O(sort stages)` decisions. Serial merge is
`O(tasks + chunks)` with constant work per pull transition. Parallel and external complexity remain
those of their delegated operators.

Tests cover threshold equalities, incompatible bounds, exact external execution and cleanup,
parallel selection followed by an authoritative full sort, ordered fallback, hostile targets, SQL
snapshot ORDER BY/LIMIT, and allocation failure. `chronos_physical_optimizer_fuzz` varies policy,
task, estimate, capability, and order-proof shapes. Benchmarks measure selection at 1/8/64 sorts and
selected source-merge instantiation at 1/4 tasks; raw timing depends on the declared build and host.

## Review questions

**Why must the result own the plan?** A row estimate for stage 3 cannot safely select an operator for
an unrelated stage 3. Ownership makes that misuse structurally impossible.

**Why prefer in-memory whenever it fits?** The current external baseline necessarily writes and
rereads checked rows. It exists for bounded capacity, not as an unmeasured speed claim.

**Why is order independence explicit?** The scheduler publishes completion order. The optimizer
cannot infer from a generic column shape whether that changes floating aggregation, LIMIT, winners,
or hidden ties.

**What if an estimate is wrong?** Runtime limits reject the excess. Estimates influence strategy,
never permission to exceed memory, disk, record, or chunk admission.
