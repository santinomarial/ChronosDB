# ADR 0192: Exact conditional object deletion

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB tiering, storage, security, and operations maintainers
- **Extends:** [ADR 0182](0182-libcurl-sigv4-s3-object-store.md), [ADR 0191](0191-manifest-retirement-bound-cold-route-removal.md)

## Context

A higher-level retirement proof cannot safely call an unconditional object delete. The key may be
absent on an idempotent retry or may contain bytes different from the retired part because of
misconfiguration or out-of-band mutation. A separate `stat` followed by an unconditional delete
also leaves a race between verification and removal.

## Decision

`ObjectStore::remove_if_exact` accepts one key and its expected immutable length and SHA-256. It
returns whether it removed the object or found it already absent. A present object with different
metadata fails without deletion.

`MemoryObjectStore` performs lookup, exact comparison, and erase under its existing mutex. The S3
backend performs a signed `HEAD`, requires exact Chronos SHA-256 metadata and length plus one bounded
ETag, then issues a signed `DELETE` with `If-Match` set to that ETag. HTTP 404 is idempotent absence;
HTTP 412 reports that the key changed and does not claim success. Redirects, TLS defaults, finite
timeouts, response bounds, and independent per-call libcurl handles remain unchanged.

The ETag is only a conditional request validator, not content identity; Chronos still uses its own
SHA-256 metadata for identity. Deployment must preserve the immutable-key contract and prevent
unmanaged delete/recreate races. Higher layers remain responsible for proving that no logical,
cold-authority, recovery, or reader reference can still use the object before invoking this method.

## Consequences and validation

The S3 operation costs one HEAD plus one conditional DELETE for a present exact object, and one HEAD
for an absent or mismatched object. It is synchronous and must remain off latency-sensitive shard
owners. Required bucket permissions now include conditional `DeleteObject` for the configured
prefix.

Focused memory tests cover mismatch, successful removal, and absent retry. The loopback signed-S3
test requires ETag capture, checks the emitted `If-Match`, deletes once, and treats the second call
as already absent. Live provider/versioning and external-writer races remain in the Phase 18 ledger.

Invariants 2, 3, 6, 10, 11, 14, and 18 apply.

## Alternatives considered

- **Unconditional DELETE:** rejected because a mismatched current object could be destroyed.
- **Trust ETag as content SHA-256:** rejected because ETag semantics vary and multipart ETags are
  not full-object SHA-256.
- **Persist provider version IDs in Cold Location Manifest v1:** deferred because exact conditional
  deletion composes with the existing frozen format and versioning requirements vary by provider.
- **Make deletion automatic inside cold-manifest installation:** rejected because durable pair
  selection and reader pins are separate authorities.

## Migration and rollback

This changes no durable bytes. Existing custom `ObjectStore` implementations must implement the new
exact deletion operation before upgrading. Deployments may withhold `DeleteObject`; reclamation then
fails without changing metadata or claiming removal. Rollback leaves already absent objects absent
and requires ordinary exact restore before an older authority can reference them again.

## References

- [S3 object-store learning guide](../learning/s3-object-store.md)
- [Manifest-retirement-bound cold route removal](0191-manifest-retirement-bound-cold-route-removal.md)
- [Architecture invariants](../architecture/invariants.md)
