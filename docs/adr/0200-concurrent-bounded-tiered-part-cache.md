# ADR 0200: Concurrent bounded tiered-part cache

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB storage, query, and runtime maintainers
- **Extends:** [ADR 0187](0187-manifest-bound-tiered-cseg-loading.md)

## Context

`TieredPartManager` maintained an in-memory full-object cache with an LRU list, byte counter, and
entry map, but exposed `read_range` without synchronization. Concurrent remote readers could race
while finding, touching, inserting, or evicting entries, invalidating iterators and violating cache
bounds. Serializing complete remote downloads would avoid the data race but would tie unrelated
query latency to the slowest object-store request.

The manager also owns an upload catalog and manifest-install callback. Making that mutation
concurrent would require a broader transaction and callback-reentrancy design that is not needed by
the current publication lifecycle.

## Decision

Upload and catalog mutation remain single-owner and may not overlap reads. Once installation
quiesces, any number of callers may invoke `read_range`, `cached_bytes`, and `cached_entries` while
the manager remains alive.

One mutex protects only the cache map, LRU list, byte counter, and their iterator relationships. A
cache hit touches and copies the requested bytes while holding that mutex so eviction cannot
invalidate its source. A miss releases the mutex before remote I/O and whole-object SHA-256
validation. It then reacquires the mutex and rechecks the key: the first completed contender inserts
after bounded eviction, while later contenders discard their redundant download and copy from the
installed entry. The entry map and LRU list are updated as one critical section, and insertion
failure rolls back the provisional list node.

Large uncached range reads retain their independent expected-range-checksum behavior and do not
touch the cache mutex. No atomic publication is introduced: mutex unlock/relock supplies the
happens-before relation for every cache field. Manager move/destruction require ordinary external
lifetime exclusion.

## Consequences and validation

Unrelated cache misses and remote reads remain concurrent. Multiple simultaneous misses for the
same object may perform redundant bounded downloads, deliberately trading transfer work for a
small, nonblocking synchronization boundary. Cache hits copy under the mutex; this keeps entry
lifetime simple and is acceptable until measurement justifies a pinned-entry design.

A focused eight-reader test repeatedly cycles across three exact CSEGs with a two-entry capacity,
validates every returned image, and checks the final entry and byte bounds. Existing hit, eviction,
authenticated-range, and upload-admission tests remain in the focused gate. ThreadSanitizer and
adversarial object-store latency scheduling remain in Phase 18.

Invariants 3, 6, 10, 11, and 18 apply.

## Alternatives considered

- **Declare the entire manager single-threaded:** rejected because remote query reads naturally
  execute concurrently and the S3 backend already supports them.
- **Hold one lock across object-store I/O:** rejected because unrelated cache misses would serialize
  on network latency.
- **Publish raw pointers to entries:** rejected because eviction would need an additional lifetime
  and reclamation protocol.
- **Use atomics for counters only:** rejected because map/list iterators and compound eviction state
  require one linearized update, not independent numeric visibility.

## Migration and rollback

No durable, network, CSEG, Manifest, or object format changes. The public metric methods are no
longer declared `noexcept` because mutex acquisition may report a platform synchronization failure.
Rollback restores the former single-thread-only read boundary and must not claim concurrent cache
safety.

## References

- [Tiered CSEG loading learning guide](../learning/tiered-cseg-loading.md)
- [Architecture invariants](../architecture/invariants.md)
