# ADR 0196: Bounded S3 retry and credential refresh

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB storage, security, runtime, and operations maintainers
- **Extends:** [ADR 0182](0182-libcurl-sigv4-s3-object-store.md), [ADR 0192](0192-exact-conditional-object-deletion.md)

## Context

The first libcurl S3 carrier returned retryable transport and HTTP failures to its caller and held
one static credential set for the store lifetime. Production object stores routinely return bounded
transient failures, while session and workload credentials expire. Blind retry is unsafe unless
each operation is idempotent and each attempt is freshly signed. Refresh also needs an explicit
ownership and concurrency contract; the object store must not read process environment or contact
an instance-metadata service behind an undocumented global policy.

## Decision

`S3ObjectStoreConfig` selects exactly one credential mode:

- static access key, secret, and optional session token owned by the store; or
- one shared `S3CredentialProvider` supplied by the embedding runtime.

The provider's `acquire` method may be called concurrently and owns synchronization of its cache and
any environment, workload identity, instance role, or ordered-chain policy. Every HTTP attempt
acquires and validates a complete credential value before constructing a fresh easy handle and
SigV4 signature. HTTP 401/403 causes exactly the next provider request to carry `kRefresh`; a
provider must not report successful refresh while knowingly returning its rejected cached value.
Static credentials are not pointlessly retried on authorization rejection.

Every object-store operation has a configured attempt count from one through 32 and capped
exponential backoff. Zero backoff is permitted for tests and embeddings that provide their own
scheduling policy. Transport failures classified `UNAVAILABLE` and HTTP 409, 425, 429, and 5xx are
retryable. Other transport errors and statuses return immediately. Provider `UNAVAILABLE` is also
retried within the same budget; invalid provider values fail `UNAUTHENTICATED`.

All current operations are safe to replay:

- HEAD and exact range GET are read-only;
- PUT always carries `If-None-Match: *`, so an ambiguous first success becomes 412 followed by exact
  length/SHA-256 verification; and
- DELETE follows exact HEAD validation and carries `If-Match`, so an ambiguous first success becomes
  an idempotent 404 on retry.

[ADR 0197](0197-conditional-s3-multipart-upload.md) later adds multipart request kinds. UploadPart,
Complete, and Abort retain operation-specific replay/reconciliation rules, while Create is
deliberately not transport-retried because a successful attempt allocates a new upload ID.

Each attempt retains the original request body and conditional validator only for the synchronous
call. Retry does not weaken response bounds, TLS verification, redirect rejection, checksum
validation, or final status classification.

## Consequences and validation

Worst-case synchronous latency is bounded by the configured attempt count, per-attempt request
timeout, and capped sleeps. [ADR 0204](0204-bounded-s3-retry-after-hints.md) and
[ADR 0209](0209-bounded-s3-retry-jitter.md) subsequently add bounded provider hints and per-store
jitter without weakening that ceiling.
The public provider seam supports refresh and ordered chains without embedding provider-specific
network clients or secret discovery in the core library.

The local S3-compatible test proves a three-attempt sequence across rejected stale credentials,
forced refresh, transient service failure, and successful conditional upload. A separate test
proves that repeated 503 responses stop at the exact configured attempt limit. Existing tests retain
conditional PUT/retry verification, signed HEAD/range reads, and conditional deletion coverage.

Invariants 2, 3, 8, 10, 14, and 18 apply.

## Alternatives considered

- **Retry at every caller:** rejected because classification, signing, and ambiguous-write handling
  would diverge across upload, cache, recovery, and reclamation paths.
- **Retry every failure:** rejected because corruption, invalid ranges, conflicting immutable
  content, and TLS identity failures require intervention rather than replay.
- **Read a fixed environment-variable chain inside the library:** rejected because workload
  identity and instance metadata require deployment-specific security, timeout, and network policy.
- **Share one mutable libcurl easy handle:** rejected because it would serialize callers and entangle
  credential generation with connection state.

## Migration and rollback

No durable or network format changes. Existing static configurations retain their fields and now
receive bounded transient retries by default. Deployments that require the previous single-attempt
behavior set `maximum_attempts` to one. Provider configurations must leave all static credential
fields empty. Rollback requires returning to static credentials and caller-managed retries.

## References

- [libcurl SigV4 S3 backend](0182-libcurl-sigv4-s3-object-store.md)
- [Exact conditional object deletion](0192-exact-conditional-object-deletion.md)
- [S3 object-store learning guide](../learning/s3-object-store.md)
- [Architecture invariants](../architecture/invariants.md)
