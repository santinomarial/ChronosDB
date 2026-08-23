# ADR 0282: Owning replicated-ingest runtime composition

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB service, Raft, metadata, and ingest maintainers

## Context

The asynchronous durable runtime, flat worker-extension set, tablet application, metadata
application, and authoritative ingest coordinator could be assembled by tests, but no production
owner fixed their construction addresses or shutdown order. In particular, constructing a
coordinator against a temporary asynchronous runtime and then moving that runtime leaves its raw
borrow dangling even though every individual type remains move-safe.

The packaged daemon needs one object whose successful construction proves all of these components
share the exact worker and whose destruction cannot release an application before worker shutdown.

## Decision

`ReplicatedIngestRuntime` is a move-only outer lifecycle owner. Its configuration contains one local
node, one physical Raft log, the complete resident group set, one metadata application definition,
the complete local tablet application definitions, and the existing bounded limits.

Preflight requires the resident group set to equal exactly one metadata group plus every unique
tablet group. Tablet groups cannot alias metadata. This rejects incomplete or extra application
ownership before a log or worker is created.

Startup creates tablet and metadata applications, composes them in that order in one flat extension
set, and creates or reopens the asynchronous durable runtime. The runtime is then moved into its
final heap-stable implementation object. Only after that address is fixed is the coordinator
constructed against it. Accessors return borrowed pointers only while the owner is running.

Shutdown first destroys the coordinator, then drains and shuts down the asynchronous runtime. The
worker invokes reverse extension shutdown before closing the physical log. Repeated shutdown
returns the first result. Destruction performs the same sequence best-effort.

## Consequences and validation

Production embeddings no longer need to recreate a fragile ownership graph. Tablet and metadata
recovery finish before the coordinator is exposed, and coordinator borrows remain address-stable.
The owner deliberately does not elect groups, configure transport, publish metadata, or advertise
Protocol 2; those are higher lifecycle stages.

No durable or network bytes change. Focused tests cover pre-worker cross-group rejection, a complete
new-runtime write and acknowledgement, idempotent ordered shutdown, exact log reopen, restored
metadata/tablet publications, a new-term matching retry, and accessor invalidation after shutdown.
A production-lifecycle case advances a request through authoritative routing until its tablet
proposal is admitted, then shuts down without coordinator result pickup. Coordinator destruction
drops only the response owner; the durable worker drains application, closes in lifecycle order,
and reopens the exact catalog, rows, and retry identity. Worker-start and allocation fault
injection, partial shutdown faults, TSan, and multi-node transport remain hardening work. A packaged
steady-state subprocess test now kills the fully recovered outer database owner and proves the next
owner reacquires every lock and rebuilds exact rows, retry identity, and application frontier. A
snapshot-backed variant repeats that proof while metadata and tablet application-snapshot locks and
retained suffixes are all live. A coordinator variant kills the owner after the observation and
write proposal are admitted but before any response is observed; reopen accepts either atomic
pre-write or committed state, and an exact protocol retry converges without duplicate rows or retry
identity. Startup, additional write-stage, snapshot-operation, and shutdown crash cuts remain
deferred.

## Affected invariants

Invariants 1, 4–6, 8, 9, 11, 14, 15, and 18 apply.

## References

- [ADR 0277](0277-bounded-worker-extension-composition.md)
- [ADR 0278](0278-worker-affine-metadata-application.md)
- [ADR 0280](0280-authoritative-replicated-ingest-routing.md)
