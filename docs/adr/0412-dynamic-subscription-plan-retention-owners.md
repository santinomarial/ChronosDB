# ADR 0412: Dynamic subscription-plan retention owners

- **Status:** accepted
- **Date:** 2026-08-20
- **Owners:** ChronosDB live-query and storage-retention maintainers
- **Extends:** [ADR 0106](0106-topology-bound-subscription-retention.md)

## Context

The topology-bound retention authority previously borrowed one fixed vector of durable plan owners
for its entire lifetime. A process that activates or retires subscription plans dynamically would
therefore need to rebuild the authority, while silently changing the borrowed vector could omit a
resume promise or leave a dangling owner pointer. Registration after physical reclamation also has
a second hazard: a new plan configured behind the already-authorized frontier could promise a
suffix that no longer exists.

## Accepted decision

`SubscriptionRetentionCoordinator` owns a bounded thread-affine registry of borrowed durable plan
owners. Initial and dynamic registrations exact-validate database, table, canonical source lineage,
unique plan fingerprint, and owner identity. Dynamic registration must occur before any service may
admit or resume through that owner. Every configured source component must be at or beyond the
coordinator's already-authorized frontier. An owner without a durable checkpoint may register, but
retention remains blocked until its first checkpoint is durably installed.

Retirement is an explicit lifecycle declaration: every admission and resume path for the plan must
already be stopped and drained, the owner remains alive through the call, and it may be destroyed
only after successful removal. Retirement does not advance physical authority. A later `advance`
recomputes the component-wise minimum across the remaining registry, validates topology again, and
invokes the source reclaimer only if the complete candidate vector increases.

The configured owner limit applies to dynamic registration. Duplicate owner pointers and duplicate
plan fingerprints are rejected before the capacity check, so retries receive a stable identity
error. Removing an unknown owner fails without changing the registry.

## Consequences and alternatives

Plan activation can now bind retention before exposing a service, and plan retirement can release
its constraint without reconstructing topology authority. Registration never reduces an authorized
frontier, and removal alone cannot delete bytes. The higher-level lifecycle remains responsible for
serializing service drain with the thread-affine retention owner; this API does not manufacture a
cross-thread service registry.

Unvalidated vector replacement was rejected because it permits dangling pointers and retroactive
resume promises. Automatically dropping an owner whose checkpoint store fails was rejected because
storage failure is not plan retirement. Rebuilding the full authority for every plan change was
rejected because it loses its monotonic in-process reclamation record and needlessly couples plan
churn to fixed source topology.

## Affected invariants and validation

Invariants 8, 11, 12, 15, and 17 apply. Focused tests first authorize reclamation without plans,
reject a plan whose source begins behind that frontier, register a current owner, enforce duplicate
and capacity bounds, prove its absent checkpoint blocks reclamation, retire it, reject repeated
retirement, and advance only on the subsequent topology-validated attempt. Header self-containment,
full suites, sanitizers, formatting, and static analysis remain required by the task verification
record.
