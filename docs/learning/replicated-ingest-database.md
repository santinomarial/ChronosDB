# Replicated-Ingest Database Recovery

`ReplicatedIngestDatabase` bridges the durable database root and the address-stable replicated
runtime. Deployment configuration says which Raft groups reside on this node and their voter
configuration. Committed metadata says which tablet each data group owns, its table, active and
retained schemas, placement, and retry policy. Keeping those roles separate prevents a daemon flag
from silently overriding consensus state.

Startup has two log-owner stages. A temporary synchronous durable runtime recovers only the metadata
catalog and then closes. That owning projection is used to build bounded tablet/retry owners. The
final asynchronous runtime reopens the same log, recovers metadata and every selected tablet on its
worker, and only then exposes the coordinator. The two log owners never overlap.

When durable Raft state has compacted a prefix, both stages require the exact group-owned
application-snapshot storage named by configuration. The temporary metadata stage rebuilds its
catalog from the metadata snapshot plus retained suffix; projected tablet owners then receive their
own snapshot locks for asynchronous recovery. Missing snapshot ownership fails before a partial
runtime is exposed, and neither stage selects a merely newer orphan snapshot over Raft's exact
authority.

```text
Database root lock
  -> [kRootOwnerReady]
  -> temporary Raft-log owner -> committed metadata projection -> close
  -> [kCatalogRecovered] -> build resident tablet owners -> [kTabletOwnersPrepared]
  -> asynchronous ReplicatedIngestRuntime -> [kRuntimeReady] -> native service
```

The optional startup observer borrows caller-owned state only for `open_existing`. Its four
callbacks run synchronously in the order above on the opening thread, are non-throwing, and expose
no partially constructed database. This makes phase timing observable and gives crash tests exact
ownership boundaries; a blocking observer also blocks startup and is therefore an embedding-owned
policy choice.

The committed catalog is cluster-global, so remote bindings are skipped when their placement does
not include this node and their group is absent from the resident configuration. A locally placed
group cannot be omitted. A configured data group cannot lack one committed binding. Placement and
current membership are deliberately not forced equal during startup because a durable
reconfiguration can be in progress; write admission performs the stable-membership proof.

Recovery ownership is per binding rather than per table. When two resident groups own different
tablets of one table, replay rebuilds two tablet states and two retry directories without merging
their Raft indexes or client identities. A later query snapshot may concatenate both immutable
tablet publications beneath one global pipeline, but that query composition does not weaken the
independent durability boundaries.

Projection is linear in catalog definitions, bindings, and configured groups with straightforward
searches favored for correctness at the present scale. Recovery cost is dominated by reading the
Raft log/application snapshots twice and replaying tablet suffixes. A later profile can justify a
single-pass discovery format or indexed catalog projection without weakening authority.

Failure closes temporary owners through RAII and never exposes a partial runtime. Shutdown drains
the asynchronous owner before releasing the root lock. Likely review questions include why group
membership remains external, why the log is reopened, why remote catalog entries are not local
owners, and why placement equality is checked at admission rather than recovery.

A process crash does not run that shutdown sequence, so correctness depends on already synchronized
Raft state and kernel release of advisory locks and file descriptors. The bounded subprocess test
kills a packaged owner only after a committed publication is recovered and observable. The next
owner must reacquire the root and Raft locks, rebuild the same rows and retry directory, accept an
exact retry without duplication, and survive a second reopen. The same lifecycle also runs with
authoritative metadata and tablet application snapshots plus committed retained suffixes, proving
that all four lock domains are process-scoped and that Raft's exact snapshot boundary—not directory
recency—still selects recovery state. This evidence does not provide deterministic cuts inside
persistence syscalls, application hooks, or snapshot installation/compaction. A separate four-case
matrix stops the owner at the coordinator's correlated route-validated, proposal-admitted,
application-proved, and response-ready boundaries. Route validation is before proposal submission
and must recover the exact pre-write publication. Proposal admission allows only pre-write or fully
committed state. Application proof and response readiness require the committed rows and retry
identity. The next owner uses the same request identity to return one application or a matching
retry without duplicate rows, and a second reopen preserves the resulting frontier.

The deterministic startup matrix pauses separately at all four stages. `kRootOwnerReady` holds only
the validated bootstrap/root owner; `kCatalogRecovered` has completed and closed the temporary Raft
owner; `kTabletOwnersPrepared` retains the projected tablet owners; and `kRuntimeReady` holds the
live asynchronous Raft/application graph before the final database result is returned. Killing each
process proves release of its exact partial ownership set. In every case the next packaged owner
reconstructs the exact rows and retry identity, advances only the application frontier for an exact
retry, and survives another reopen. Syscall-level cuts inside those stages remain separate work.

Shutdown exposes a separate observer borrowed only for the synchronous call.
`kCoordinatorReleased` means request/result ownership is gone while the durable worker remains;
`kAcceptedWorkDrained`, `kApplicationsStopped`, and `kLogClosed` expose the worker-owned teardown
order; `kRuntimeStopped` follows join and runtime reset while the root owner remains; and
`kRootReleased` follows the root close. A six-case `SIGKILL` matrix proves exact reopen and retry.
The first four cases admit a real proposal before coordinator destruction. Recovery at the first
may select only the atomic pre-write or committed state, while every case at or after drain requires
the committed mutation and retry identity; exact retry cannot duplicate either outcome. Cuts inside
worker drain, extension hooks, physical close, and root-close syscalls remain separate work.

## Query snapshot boundary

`acquire_query_snapshot` pins one immutable applied metadata projection and reconstructs each active
schema lineage from its retained definitions. It then resolves the projection's tablet bindings and
pins the immutable publications available from the resident tablet application. The resulting
`ReplicatedQuerySnapshot` owns all of those objects, so binding and vector execution use the same
catalog generation and execution can outlive the database owner.

The no-argument overload remains a stable local-applied vector. The barrier overload accepts exactly
one leader-confirmed read index for the metadata group and every resident data group, then fails
unless the immutable catalog and matching tablet publications cover those indexes. The packaged
native service uses this stronger overload. Separate groups can still contribute different applied
positions, so this is not a globally atomic cross-group instant. If any placement is nonresident,
the table remains visible to the binder but execution fails `UNAVAILABLE`; a local subset is never
presented as a whole-table result. The physical source concatenates all pinned tablet generations
beneath one pipeline, so global SQL operators run once rather than independently per shard.

For negotiated query routing, the local-applied snapshot also owns an optional table route. It is
present only when every table placement has one identical group, placement epoch, and replica set.
This preliminary snapshot is used only to bind and select authority; it never supplies returned
rows. The database enqueues ordered observations for the complete packaged barrier vector, then
reacquires metadata and exact-compares the route and each resident group's placement membership.
All-local leadership proceeds to the normal barrier snapshot. Only a common stable remote leader
across the entire vector becomes a Protocol 2 redirect for the table group. Split, candidate,
reconfiguring, or changed authority fails closed, leaving remote fragments as the required design
for independently led groups.

For split-leader fragment construction, `ReplicatedQuerySnapshot` also retains the exact committed
metadata publication and durable database identity used to build its catalog and tablet pins. Its
leader-linearizable mutable binder consumes the correlated authority vector returned by
`ReplicatedReadBarrier::await_authority`. For every plan-ordered tablet it joins the resident
immutable publication to committed placement and group binding, requires stable observed voters
equal to placement, proves the publication covers that group's barrier, and exact-matches the
planner's leader/applied/commit copies before delegating to the proof-bound fragment binder. The
call returns every owning fragment or none. A target worker still reacquires and exact-matches its
current publication, so ordinary Raft progress can make a fragment unavailable but cannot make it
read a different boundary; the coordinator must then use whole-query authority rebinding.
