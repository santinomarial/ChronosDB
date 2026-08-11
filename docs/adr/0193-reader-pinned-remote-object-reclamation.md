# ADR 0193: Reader-pinned remote object reclamation

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB tiering, query, recovery, and operations maintainers
- **Extends:** [ADR 0191](0191-manifest-retirement-bound-cold-route-removal.md), [ADR 0192](0192-exact-conditional-object-deletion.md)

## Context

A route omitted by a newer cold generation and an exact-delete backend are both necessary but not
sufficient for remote reclamation. The successor Manifest/cold pair must be durable, the logical
part and route must both be absent from current authority, and every reader that acquired an older
aggregate epoch containing that route must finish. Deleting before those facts hold can make a
valid pinned query fail after it chooses remote fallback.

## Decision

The single-writer `TieredDatabaseStoragePublisher` authorizes a strictly sorted bounded set of part
IDs against the exact current pair record. Each candidate must be absent from the current Manifest
and cold generation. The publisher searches its weak aggregate-epoch history for the unique exact
old route, requires its part length/SHA-256 to match the old logical descriptor, rejects route/key
reuse or rewriting, copies the retired location into a move-only proof, and captures weak pins for
every live epoch that can still expose that route. The proof retains the current aggregate epoch but
does not retain any predecessor it waits upon.

Reclamation returns pending without object-store I/O while any captured route reader remains. Once
unpinned it reloads the highest pair marker without fallback and requires exact equality with the
proof. It rechecks that no current logical part, cold route, or key refers to a candidate, then stats
all objects before the first delete. Every present object must match key, length, and SHA-256. Only
after complete preflight does it invoke `remove_if_exact` for each candidate. Already absent objects
make retry idempotent. A remote failure after one deletion may leave a safe partial set; retrying the
same proof converges because current authority already omits every candidate.

The live publisher history is the authorization source for this first implementation. A process
crash destroys all readers, so an interrupted deletion cannot cause a stale-reader failure, but the
remaining unreachable objects may leak until a future durable garbage-discovery pass compares old
cold generations with the selected pair. Bucket listing remains non-authoritative and is not used.

## Consequences and validation

Authorization performs metadata-path scans over bounded live epochs and route tables. Reclamation
performs one stat preflight plus the backend's exact-delete work per present candidate. Both are
single-writer control-plane operations and make no hot-path performance claim.

The integration test commits a routed predecessor pair, holds an old reader, installs and publishes
an exact source-retirement Manifest plus route-free cold successor, commits that pair, and observes
pending with zero mutation. After reader release it rejects wrong metadata, removes the exact
object, retries absent successfully, and rejects the proof after pair advancement. Existing S3
tests cover the conditional provider request itself.

Invariants 2, 3, 6, 8, 10, 11, 14, and 18 apply.

## Alternatives considered

- **Delete when the route is omitted:** rejected because old aggregate readers retain their own
  valid cold authority.
- **Wait for all readers:** rejected because epochs that never expose the retired route cannot use
  the object.
- **Use bucket listing to find garbage:** rejected because listing is neither pair authority nor a
  complete transactional reference graph.
- **Treat partial remote deletion as storage poisoning:** rejected because all candidates are
  already unreachable from selected authority and idempotent retry safely converges.

## Migration and rollback

No durable format changes. Before deletion, a later authorized generation may restore a logical
part and exact route after verifying/recreating the object. After deletion, rollback must first
restore exact bytes under the immutable key. Deployments upgrading from older binaries can leave
reclamation disabled while preserving all remote objects.

## References

- [Exact conditional object deletion](0192-exact-conditional-object-deletion.md)
- [Tiered CSEG loading](../learning/tiered-cseg-loading.md)
- [Architecture invariants](../architecture/invariants.md)
