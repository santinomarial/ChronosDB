# ADR 0154: Physical-ownership-gated tablet movement readiness

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB cluster, manifest, ingest, and distributed-systems maintainers
- **Extends:** [ADR 0130](0130-durable-tablet-movement-ready-reconciliation.md) and
  [ADR 0153](0153-restartable-tablet-physical-ownership-publication.md)

## Context

The application catch-up reconciler proves that the exact RTAS is durable and that the target Raft
group has applied the same full snapshot metadata. That proof alone does not establish query-visible
physical ownership: the destination's atomically published Manifest may still omit the tablet or
name a different CSEG part set. Advancing the movement checkpoint to `ready` in that state could
allow metadata promotion before the destination can serve the transferred history.

## Decision

Production physical movement readiness is composed by
`checkpoint_tablet_physical_movement_readiness()`. Before delegating to the existing durable
application catch-up reconciler, it:

1. installs or exact-verifies the movement-owned RTAS;
2. exact-decodes one acquired destination publication epoch;
3. projects the target Raft tablet at the RTAS applied boundary; and
4. requires the projected canonical part-set checksum to equal the full Raft snapshot metadata.

Only after both the application and physical proofs succeed may the existing reconciler install the
next `ready` checkpoint and mutate the live recovered movement. The RTAS installation may complete
before physical proof fails because it is independently durable and idempotent; the authoritative
checkpoint and live movement remain `catching-up`.

The lower-level `checkpoint_recovered_tablet_movement_catch_up()` remains available as an
application-only primitive. A production path that transfers physical CSEG state must use the
cluster composition. External-prefix generations continue to revalidate and retain their exact
chunk reference when the ready successor is installed.

## Consequences and validation

The gate deliberately recomputes a canonical projection from the published destination instead of
trusting a caller-supplied completion flag. It requires exact applied-boundary equality; a newer or
older published tablet is not inferred to be equivalent. The publication snapshot remains owned for
the duration of validation, so concurrent publication cannot change the proof underfoot.

A real-filesystem integration test first completes RTAS and Raft installation while supplying an old
published destination epoch. It verifies that no ready checkpoint is installed. The same recovered
movement then succeeds against the newly published physical owner and installs exactly the next
generation. Cluster tests, installed-consumer checks, and focused ASan/UBSan cover the composition.

Invariants 1–6, 8, 10, 11, 14, 18, and 20 apply.

## Migration and rollback

No durable format changes. Existing self-contained and external-prefix catching-up checkpoints
remain readable. Rollback can leave an installed RTAS or published physical successor in place;
recovery exact-verifies both and repeats readiness without advancing twice.

## References

- [Tablet movement checkpoint](../learning/tablet-movement-checkpoint.md)
- [Raft tablet physical snapshot projection](../formats/raft-tablet-physical-snapshot-v1.md)
- [Manifest installation and checkpointing](../architecture/manifest-installation-and-checkpointing.md)
