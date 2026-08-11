# ADR 0191: Manifest-retirement-bound cold route removal

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB manifest, tiering, recovery, and object-lifecycle maintainers
- **Extends:** [ADR 0184](0184-durable-cold-location-generations.md), [ADR 0185](0185-atomic-tiered-storage-publication.md)

## Context

Cold Location Manifest successors were strictly add-only. That was the correct initial boundary for
local reclamation, but it makes remote reclamation impossible: even after a newer Manifest v2
generation removes a logical part, every later cold generation must continue naming its object.
Removing a route merely because a newer cold file omits it would be unsafe while the same logical
Manifest still references the part or while a reader holds the predecessor aggregate epoch.

## Decision

Cold Location Manifest v1 bytes remain unchanged. Successor validation now receives the exact
decoded Manifest v2 value to which the candidate cold generation binds. Generation and deployment
identities remain exact and consecutive, and base authority may not move backward.

At the same base Manifest generation, cold authority remains add-only. When the base Manifest
generation advances, a predecessor route must still be present byte-for-byte whenever its part ID
remains in the successor Manifest. A route may be omitted only when that part ID is absent from the
successor logical authority. Existing keys may never be rewritten, including when their part is
retired; omission is the only retirement representation. Newly added routes must continue to bind
their part ID, length, and SHA-256 exactly to the successor Manifest.

Both durable installation and in-memory aggregate publication apply the same transition validator.
This decision makes a route *eligible* for a later remote-reclamation proof; it does not delete an
object. The pair commit must first select the successor and every live predecessor reader that can
name the route must drain.

## Consequences and validation

No durable format or protocol version changes. Transition validation performs a bounded scan of
predecessor routes against the successor route table and logical part table. The current
implementation uses linear logical-part lookup per retired candidate; this is metadata-path work
and has no performance claim.

Focused storage tests prove that same-base omission fails, a newer base can omit an actually
retired part, a still-logical route cannot disappear, and a route cannot be rewritten. Existing
generation, binding, exact-readback, synchronization, and no-fallback tests remain applicable.

Invariants 2, 3, 6, 8, 10, 11, 14, and 18 apply.

## Alternatives considered

- **Keep routes forever:** rejected because it prevents bounded remote storage.
- **Allow arbitrary omission after any base advance:** rejected because unrelated logical parts
  may still depend on their remote copies.
- **Encode tombstones in Cold Location Manifest v1:** rejected because full-generation omission is
  sufficient and changing the frozen format has no current need.
- **Delete during transition validation:** rejected because publication, pair durability, and
  reader lifetime are separate authorities.

## Migration and rollback

Older cold generations remain immutable and readable. New binaries accept an omission only under
the successor Manifest proof above. Before actual remote deletion, rollback can republish an exact
route in a later cold generation if the logical part is also restored through its proper Manifest
transition. After deletion, restore must first recreate and verify the exact immutable object.

## References

- [Cold Location Manifest v1](../formats/cold-location-manifest-v1.md)
- [Reader-pinned local reclamation](0190-reader-pinned-tiered-local-reclamation.md)
- [Architecture invariants](../architecture/invariants.md)
