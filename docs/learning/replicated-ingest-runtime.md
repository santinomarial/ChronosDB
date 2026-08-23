# Owning Replicated-Ingest Runtime

`ReplicatedIngestRuntime` is the address-stable outer owner for the replicated write path. It
creates the tablet and metadata application extensions, hosts both on one asynchronous durable
Multi-Raft worker, and constructs the authoritative coordinator only after that runtime reaches its
final owned address.

The configured Raft group set must be exactly the metadata group plus every local tablet group.
This ensures no resident group can commit application entries without a matching state-machine
owner. Each application still enforces its own tablet, retry, schema, snapshot, and resource bounds.

Ownership is nested in this order:

```text
ReplicatedIngestRuntime
  -> ReplicatedIngestCoordinator
  -> AsyncDurableMultiRaftRuntime
       -> AsyncDurableRaftWorkerExtensionSet
            -> AsyncRaftTabletApplication
            -> AsyncRaftMetadataApplication
```

The arrows describe lifetime, not callback order. Worker callbacks initialize, prepare, and
complete tablet then metadata application; shutdown runs metadata then tablet. The outer owner
destroys the coordinator before asking the runtime to drain, so no new observation/proposal can be
admitted while extensions are being torn down.

Destroying the coordinator drops pending response owners, not work already accepted by the
asynchronous runtime. A production-lifecycle test advances one request through its ordered route
observation until the tablet proposal is admitted, then calls shutdown without polling that result.
The worker drains the proposal through tablet and metadata callbacks before reverse extension and
physical-log teardown. Reopen recovers the exact metadata route and applied rows, and resubmitting
the same identity in a new leader term returns a matching retry rather than adding rows.

Shutdown can borrow an observer for the synchronous call. `kCoordinatorReleased` is emitted after
the coordinator is destroyed but before worker drain begins; `kWorkerStopped` follows reverse
application shutdown and physical-log close. The packaged database maps those stages into its own
observer before releasing the root. A real-process crash at the first boundary with an admitted
proposal proves that losing response ownership cannot create partial rows or separate a mutation
from its retry identity.

The asynchronous runtime object must not move after coordinator construction because the
coordinator borrows it directly. Startup therefore allocates the final implementation and moves the
runtime there first. Application objects are shared heap owners, so moving their `shared_ptr`
handles does not change the borrowed object addresses.

Create/open cost is linear in configured groups plus retained-log and application recovery work.
Steady-state cost remains in the coordinator and underlying Raft/application owners; this outer
object adds no per-request synchronization. It intentionally does not own elections, transport,
metadata provisioning, or native-protocol advertisement.

The database-root composition reconstructs each tablet from committed catalog authority. It creates
the fresh tablet at the retained lineage root, registers every consecutive successor with a
schema-specific mutable-head capacity, and only then replays retained Raft commands. A restart after
catalog activation can therefore rebuild predecessor rows and retries before a later successor
command rotates the active generation. A second reopen must reproduce both schema-bound generations
and keep an exact successor retry row-neutral.

Startup also treats external group voters as the base configuration, not as permission to overwrite
retained consensus history. A committed joint entry is re-derived before application admission. The
owner can therefore reopen with both old and new majorities active, preserve tablet publication,
finish the final membership entry after a new joint election, align committed placement, and reopen
again under the derived stable voters.

The separate `ReplicatedIngestService` borrows the coordinator and queue pair. It must be destroyed
or drained before this owner shuts down; the eventual packaged lifecycle owns that ordering.

Likely review questions: why must the runtime address stabilize before coordinator creation, why
must every resident group have an application owner, why is coordinator destruction first, and why
does this owner not automatically elect a group during recovery?
