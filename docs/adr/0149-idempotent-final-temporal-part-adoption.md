# ADR 0149: Idempotent final temporal-part adoption

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB manifest and storage maintainers
- **Extends:** Manifest v2 installation

## Context

Temporal CSEG installation synchronizes a temporary file, publishes it with a no-replace rename,
and synchronizes the parts directory before a successor Manifest can own the part. A process may
stop after the final file becomes durable but before Manifest publication. Rejecting every existing
final file makes the operation impossible to resume, while accepting identity alone could adopt
different or corrupt bytes.

## Decision

After complete request prevalidation, temporal-part installation opens an existing canonical final
name before creating a temporary. It accepts that final as an idempotent result only when its exact
length and every byte equal the canonical encoded request and a second complete descriptor, owner,
schema, checksum, and semantic validation succeeds. The retry performs no write, rename, sync, or
installed-part metric increment.

A different length or any different byte for the same part identity is corruption. Other open/read
failures retain their underlying status. Resource exhaustion while materializing the bounded
verification buffer fails explicitly. The storage owner is not poisoned because these failures
occur before this attempt changes durable state.

## Consequences and validation

The retry temporarily holds one additional part-sized buffer. This is accepted at the Manifest
installation boundary, whose existing validation already materializes and reads back the complete
part; the physical transfer receiver remains independently streaming. A future streaming installer
may reduce this cost without weakening exact equality or complete semantic validation.

Real-filesystem tests cover exact retry with a new nonce, absence of retry temporaries and duplicate
metrics, and rejection of mutated durable final bytes. Existing fault-injection tests continue to
cover the rename and directory-synchronization boundary.

Invariants 1–5, 8, 10, 11, and 18 apply.

## Migration and rollback

No durable format changes. Rollback restores rejection of orphan finals and can strand a valid
unreferenced part after a crash, so operators must not delete such a file without proving that no
Manifest generation references it.

## References

- [Manifest v2](../formats/manifest-v2.md)
- [Manifest installation and checkpointing](../architecture/manifest-installation-and-checkpointing.md)
