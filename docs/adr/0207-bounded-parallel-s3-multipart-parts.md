# ADR 0207: Bounded parallel S3 multipart parts

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB object-storage, concurrency, runtime, and operations maintainers
- **Extends:** [ADR 0197](0197-conditional-s3-multipart-upload.md)

## Context

Multipart upload was correct but sequential: each part completed signing, transfer, and retry before
the next began. Large cold-object uploads therefore left network capacity unused. The object-store
implementation already gives each HTTP attempt an independent libcurl handle, credentials snapshot,
signed headers, and response storage, so part requests have no required transport serialization.

Parallelism must not make completion order nondeterministic, leak worker lifetime past borrowed
object bytes, race failure state, or allow completion/abort while a part still uses the upload ID.

## Decision

`multipart_maximum_concurrency` bounds one to 64 active workers per object and defaults to four.
After initiation, the caller allocates fixed result and failure slots for every part. Workers claim
unique ascending indices through one atomic counter, hash and upload the corresponding borrowed
span, then publish the ETag or failure only into that index's exclusive slot. The first failure sets
an atomic stop flag; already-claimed work may finish, but workers claim no further parts after they
observe it.

The worker vector is scoped so every `std::jthread` joins before result inspection. Thus all slot
writes happen-before the owner reads them through thread-join synchronization. The counter and stop
flag use C++'s default sequentially consistent ordering, which is stronger than needed but makes
claim/stop visibility explicit and carries negligible cost relative to HTTP transfer. Slots need no
locks because exactly one claimed worker owns each index and the owner reads only after all joins.
Completion XML still iterates the fixed ETag vector in ascending index order regardless of request
arrival order.

On any part or worker failure, all started workers join before the existing scope guard attempts
abort. Thread creation failure also unwinds and joins already-created workers before abort. The
caller-owned object span and store therefore outlive every worker.

## Consequences and validation

Memory remains bounded by the borrowed object plus `O(part count)` ETags/failures and at most the
configured number of HTTP handles. Credentials providers must already support concurrent calls as
required by their public contract. A failed worker can allow up to the configured number of
already-claimed parts to finish; none become query-visible without conditional completion and exact
HEAD verification.

A focused provider barrier proves two part workers overlap. The loopback server may observe part
requests in either order, while the completion document is asserted to retain part 1 before part 2.
The existing failed-part test proves all workers join before exact abort and that no final object is
published. Invalid zero concurrency is rejected before network access. TSan, high-part-count stress,
provider rate-limit tuning, and live throughput evidence remain deferred.

Invariants 2, 3, 8, 10, 14, 16, and 18 apply.

## Alternatives considered

- **Keep sequential uploads:** rejected because the independent carrier design can safely support a
  bounded improvement now.
- **One thread per part:** rejected because S3 permits 10,000 parts and unbounded threads violate
  resource ownership.
- **Append ETags in arrival order:** rejected because multipart completion must identify parts in
  ascending part-number order.
- **Detach workers on failure:** rejected because they borrow caller bytes and the upload ID and
  would race abort/destruction.
- **Use a process-global pool:** rejected because it creates scheduling and shutdown policy outside
  the current object-store boundary.

## Migration and rollback

Existing callers receive a maximum of four concurrent part requests per uploaded object. Operators
can set one to preserve sequential behavior or lower provider pressure. No durable or network
format changes; rollback restores sequential scheduling without changing uploaded object identity.

## References

- [Conditional S3 multipart upload](0197-conditional-s3-multipart-upload.md)
- [S3 object-store learning guide](../learning/s3-object-store.md)
- [Architecture invariants](../architecture/invariants.md)
