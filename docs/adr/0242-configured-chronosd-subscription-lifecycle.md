# ADR 0242: Configured chronosd Subscription Lifecycle

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB service, live-query, networking, recovery, and operations maintainers

## Context

Protocol 1.1 subscription messages, durable plans and coordinators, applied-append fan-out, and the
single-node subscription runtime existed but `chronosd` still returned an explicit unconfigured
error. Composing them must preserve the reactor queue's one-producer/one-consumer contract, keep
resume-token authentication stable across restart, and recover safely when committed database
storage is ahead of the latest coordinator checkpoint.

## Decision

`chronosd` accepts subscriptions only when both `--subscription-sql SQL` and
`--subscription-key-file PATH` accompany `--data-dir`. The key file must be an exact 32-byte,
nonzero, final-component-nonsymlink regular file with no group or other permission bits. The daemon
does not create, rotate, print, or copy this deployment secret.

After database recovery and before reactor admission, the daemon creates or reopens
`subscriptions/plans` and `subscriptions/checkpoints` below the locked database root. It installs
the exact SQL through `SubscriptionPlanStorage`, reloads the executable fingerprint, discovers the
current local WAL-backed tablet vector, and creates or opens that plan's durable coordinator. A new
or dirty coordinator is checkpointed before service. If recovered database storage is ahead, the
coordinator fail-closed rebases replay expiry through the exact current vector and checkpoints it;
old tokens fail instead of skipping an unrepresented suffix. Backward positions, source-set drift,
or non-WAL sources fail startup.

One stable committed-append router is passed into the database before open and is bound to the
configured runtime before the reactor starts. Subscription tasks use separate bounded internal
SPSC queues. The data-plane worker remains the sole reactor-request consumer and reactor-response
producer; on its same thread it routes subscription start, acknowledgement, and owned cancellation,
polls snapshot/live progress, and forwards complete responses. Ordinary query cancellation remains
separate.

On signal shutdown, the worker begins subscription shutdown and continues while the main thread
polls the reactor. Active sessions receive the existing resumable `SERVER_SHUTDOWN` termination
before the worker joins. The reactor then closes, the runtime unbinds, and database shutdown drains
WAL and Raft ownership.

This release configures one immutable row-preserving plan per daemon. Different new-query SQL is
rejected by the plan fingerprint check. Stateful aggregate/window plans remain outside the
committed-batch evaluator until the retained incremental engine is routed into fan-out.

## Consequences

The single-node daemon now serves the complete historical-to-live, acknowledgement, shutdown, and
restart-resume lifecycle using existing Protocol 1.1 bytes. The explicit operator key is required
on every restart; losing or changing it invalidates prior tokens. The SQL must reference a table
already present in the recovered catalog, so configuring a newly created table requires a restart
with the subscription options.

Coordinator checkpoints are synchronous with online writes. Startup replay drift sacrifices old
resume tokens for a fresh snapshot rather than reconstructing append boundaries from current heads.
Multi-plan dynamic registration, stateful incremental plan routing, source-log replay, TLS CLI
configuration, and distributed leader routing remain later integrations.

## Validation

macOS builds validate the daemon and all portable service/live tests; the Linux-only process test
is syntax-checked here and is registered to create a table, configure a plan/key, negotiate 1.1,
stream snapshot/READY, apply SQL INSERT, observe and acknowledge the live result, restart, and
resume from the token. Linux execution remains part of the deferred platform matrix.

## References

- [ADR 0103](0103-durable-subscription-plan-registry.md)
- [ADR 0105](0105-bounded-subscription-service-lifecycle.md)
- [ADR 0224](0224-configured-single-node-chronosd.md)
- [ADR 0240](0240-write-synchronous-live-checkpoint-gate.md)
- [ADR 0241](0241-single-node-subscription-runtime-composition.md)
