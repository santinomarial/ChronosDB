# Multi-tablet subscription order

`MultiTabletSubscriptionManager` coordinates one fixed, plan-bound source set. It sorts tablet/WAL
identities into a canonical vector, captures that vector for historical execution, and accepts only
the next committed sequence independently for each tablet. One owner thread invokes publication,
so the cross-tablet invocation order becomes a recorded delivery order.

That distinction matters. Tablet A sequence 100 and tablet B sequence 2 cannot be compared as a
database commit coordinate. Sorting them would either reorder a later arrival or wait indefinitely
for an idle tablet. The coordinator instead says only: “these already-committed changes were admitted
for this subscription in this order.” Delivery sequence is the replay identity for that retained
history; each change still carries its authoritative tablet position.

Registration copies all latest components before another owner call can publish. During snapshot,
later changes enter the same bounded per-subscriber buffer used by the single-source path. An
acknowledgement removes one delivery prefix and advances only the components represented in that
prefix. The Resume Token therefore combines one safe delivery ordinal with an exact source vector.

Retained changes remain in coordinator admission order. Resume exact-checks database, plan, schema,
tablet membership, WAL lineage, and every component against current committed state. It skips a
retained change only when that change is already covered by its own source component. If retention
has evicted a required change from any one tablet, the whole resume fails; silently delivering a
partial suffix would violate the snapshot-to-stream contract.

The manager owns no threads and is not internally synchronized. Returned delivery records share
immutable change ownership. Memory is bounded globally for retention and independently per
subscriber. A slow subscriber overflows without rejecting a committed source change. A topology or
source-lineage change requires a newly bound coordinator and snapshot.

Work is linear in active subscribers per publish and in the retained suffix per resume. Useful
review questions are: why are tablet record sequences incomparable, what exactly makes replay order
authoritative, why does acknowledgement update a vector rather than one scalar, and why must expiry
of one component fail the complete resume?
