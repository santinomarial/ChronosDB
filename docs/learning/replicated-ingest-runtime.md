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

The asynchronous runtime object must not move after coordinator construction because the
coordinator borrows it directly. Startup therefore allocates the final implementation and moves the
runtime there first. Application objects are shared heap owners, so moving their `shared_ptr`
handles does not change the borrowed object addresses.

Create/open cost is linear in configured groups plus retained-log and application recovery work.
Steady-state cost remains in the coordinator and underlying Raft/application owners; this outer
object adds no per-request synchronization. It intentionally does not own elections, transport,
metadata provisioning, or native-protocol advertisement.

The separate `ReplicatedIngestService` borrows the coordinator and queue pair. It must be destroyed
or drained before this owner shuts down; the eventual packaged lifecycle owns that ordering.

Likely review questions: why must the runtime address stabilize before coordinator creation, why
must every resident group have an application owner, why is coordinator destruction first, and why
does this owner not automatically elect a group during recovery?
