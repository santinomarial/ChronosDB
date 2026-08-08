# Snapshot-Bound ASOF Execution

## Purpose and public interface

`instantiate_snapshot_asof_plan` is the storage bridge for a lowered `PhysicalAsofPlan`. It takes
one query resource context, Manifest storage, one held aggregate database snapshot, source bindings
in SQL order, and the reusable plan. It returns one thread-affine pull operator.

Each `SnapshotTabletSourceBinding` identifies a tablet, borrows its retained schema lineage for the
duration of construction, selects a destination schema, and carries the existing bounded planning,
validation, CSEG, head, and composition limits.

## Shape, epoch, and ordering invariants

The plan supplies each source's exact physical input shape. Source zero uses the first join's left
preparation input; source `i + 1` uses join `i`'s right preparation input. Every shape must be all
destination-schema columns followed by either nothing or the complete four-column row-version
suffix. The connector infers and applies that suffix mode uniformly to every durable and mutable
component of that source.

All sources are planned and loaded from the same `DatabaseStorageSnapshot`. This is stronger than
receiving arbitrary source operators: a caller cannot accidentally join independently acquired
epochs. SQL source order is explicit even when aliases repeat the same table or tablet.

The connector does not add order. Exact ASOF winner selection and final SQL ORDER BY ties remain
encoded in the checked plan. Physical scan arrival order is never a SQL tie-break.

## Ownership and failure behavior

Bindings and lineages are borrowed only while the call runs. Snapshot images, mutable-head
snapshots, physical operators, pins, and query reservations are owned by the constructed source
chain. Construction is eager and serial; if source `i` fails, the local owner vector destroys
sources `0..i-1` before returning the original status. Allocation and container-limit exceptions at
the connector boundary become resource exhaustion.

Returned chunks retain their own pins and reservations and may outlive the root operator. Shared
cancellation reaches both ASOF inputs through the existing operator contract.

## Complexity and tradeoffs

For `S` sources with `C_i` schema columns, `P_i` selected parts, and `H_i` heads, connector setup is
`O(sum(C_i + P_i + H_i))` plus synchronous authenticated part I/O. ASOF execution retains the
bounded right-side state specified by each join. Repeated aliases may conservatively repeat pin
charges; shared publication accounting is deliberately deferred.

## Evidence

A three-source self-join executes from one epoch with descending ORDER BY, LIMIT, and no visible
identity helpers. Hostile tests cover source counts, schema identities, and a later source failing
its head limit after an earlier source exists. Allocation injection sweeps the complete connector,
the authenticated snapshot fuzzer varies count/schema/limit/cancellation paths, and a benchmark
measures source construction plus bounded ASOF execution.

## Review questions

**Why pass one snapshot instead of one per source?** One object is a structural proof that every
source belongs to the same aggregate publication epoch.

**Why are bindings in SQL order?** The left-deep plan assigns different preparation and identity
roles to each alias; table identity alone cannot recover those roles.

**Does the returned plan borrow the lineage?** No. Lineage is needed while scan factories validate
and construct their owned runtime state, matching the unary snapshot connector contract.
