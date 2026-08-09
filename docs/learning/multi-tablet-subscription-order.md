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

The logical checkpoint copies each source's latest and expiry frontier plus that retained deque in
the same admission order. Restore does not attempt to infer an interleaving: it proves every
per-source retained suffix is consecutive through the declared latest vector, then reinstalls the
recorded order. Subscriber buffers are reconstructed only from an authenticated safe Resume Token.

`DurableMultiTabletSubscription` couples that logical state to a lock-owning immutable-generation
store. Reopen treats the latest installed checkpoint vector as the authoritative replay boundary;
committed log entries after it must be fed back through `publish_committed` in each source's exact
sequence. The owner exposes checkpoint expiry components to a future retention manager only after
the generation file and directory have synchronized. If installation fails, the prior durable
frontier remains unchanged even though newer state is still live in memory.

For the historical half, `MultiTabletSnapshotSubscription` validates every registered vector
component against one aggregate storage publication. Raw tablet scans are concatenated before the
physical plan is instantiated, so a global aggregate, sort, latest, or limit observes the complete
source set rather than one independently finalized result per tablet. READY is impossible until
that global pipeline has ended and END_STREAM has been emitted.

The manager owns no threads and is not internally synchronized. Returned delivery records share
immutable change ownership. Memory is bounded globally for retention and independently per
subscriber. A slow subscriber overflows without rejecting a committed source change. A topology or
source-lineage change requires a newly bound coordinator and snapshot.

Work is linear in active subscribers per publish, in the retained suffix per resume, and in retained
state size per checkpoint. Useful review questions are: why are tablet record sequences
incomparable, what exactly makes replay order authoritative, why does acknowledgement update a
vector rather than one scalar, why must expiry of one component fail the complete resume, and why
can retention advance only after checkpoint installation?
