# ADR 0197: Conditional S3 multipart upload

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB storage, security, runtime, and operations maintainers
- **Extends:** [ADR 0182](0182-libcurl-sigv4-s3-object-store.md), [ADR 0196](0196-bounded-s3-retry-and-credential-refresh.md)

## Context

A single signed PUT is simple and immutable, but large CSEG objects need a multipart transfer path
with finite request bodies and independently retryable parts. Multipart upload introduces an upload
ID, opaque part ETags, a completion document, and abandoned remote state. It must not weaken
`put_if_absent`: no partial upload may become query authority, an existing different object must
never be overwritten, and an ambiguous completion must be reconciled against exact metadata.

AWS S3 permits `If-None-Match: *` on `CompleteMultipartUpload`. It returns 412 when the key already
exists, while a concurrent 409 requires a newly initiated multipart session rather than replaying
the same completion. S3 can also return an embedded error inside HTTP 200, so status alone is not
success.

## Decision

`S3ObjectStoreConfig` selects a multipart threshold and part size. Both are finite; part size is at
least S3's 5 MiB non-final minimum, and one upload may contain at most 10,000 parts. Objects below
the threshold retain the single conditional PUT path. The provider-neutral `ObjectStore` interface
and its whole-object SHA-256 identity do not change.

For a multipart object, `put_if_absent`:

1. checks an existing key by exact HEAD, returning idempotent success or immutable conflict;
2. initiates one signed multipart upload and stores the full Chronos SHA-256 as object metadata;
3. strict-decodes one bounded XML upload ID and percent-encodes it in subsequent query parameters;
4. uploads sequential numbered borrowed spans, signing each exact part payload and retaining one
   bounded opaque ETag per part;
5. builds a bounded escaped completion document in ascending part order;
6. completes with `If-None-Match: *`, requiring a non-error completion result; and
7. exact-HEAD verifies final length and Chronos SHA-256 before returning success.

UploadPart is replay-safe because the same upload ID, part number, and bytes replace only that
session part. Complete is replayed for transport/5xx ambiguity; if an earlier attempt completed, a
later 404 is reconciled by exact final HEAD. A 409 is not replayed against the same upload ID. Create
is not automatically replayed after transport ambiguity because each success allocates a new upload
ID. Every request, including abort, acquires fresh credentials and is independently signed.

After an upload ID is decoded, a scope guard attempts exact abort on every non-success exit,
including allocation failure. Abort is idempotent and bounded. A lost create response or failed
abort can still leave an invisible billed multipart session, so production buckets must configure
an incomplete-multipart lifecycle rule. Such parts are never installed in a Cold Location Manifest
and therefore are never query-visible.

## Consequences and validation

The current implementation is synchronous and uploads parts sequentially. It retains no second
copy of part bytes, but keeps the caller's whole object borrowed until return plus `O(part count)`
ETag/completion metadata. Each part receives the existing request retry budget. A caller retry after
409 begins with a new HEAD and, if still absent, a new multipart session.

The local S3-compatible success test transfers a 5 MiB part and a short final part, checks encoded
upload-ID queries and conditional completion, assembles exact bytes, and verifies the final HEAD.
The failure test rejects part two, observes an abort for the exact upload ID, and proves no object
was published. Live providers, embedded-200 errors, completion races, abort failure, parallel parts,
and lifecycle-rule qualification remain deferred.

Invariants 2, 3, 8, 10, 14, and 18 apply.

## Alternatives considered

- **Use multipart completion without a condition:** rejected because it could overwrite a
  conflicting immutable key.
- **Treat HTTP 200 as complete:** rejected because S3 may embed an error after sending 200 headers.
- **Retry multipart creation transparently:** rejected because an ambiguous success allocates an
  unknown upload ID and repeated creation amplifies leaked billed parts.
- **Trust multipart ETag as the object digest:** rejected because ETags are opaque and not a stable
  full-object SHA-256 contract.
- **Expose multipart state through `ObjectStore`:** rejected because part/session mechanics are a
  provider transfer concern; logical authority remains one exact immutable object.

## Migration and rollback

No durable or Chronos network format changes. Raising the threshold above the configured maximum
object size disables multipart use. Rollback to a binary without multipart support can still read,
verify, and conditionally delete completed objects because their key, length, and Chronos SHA-256
metadata are identical to single-PUT objects.

## References

- [CompleteMultipartUpload API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html)
- [CreateMultipartUpload API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html)
- [UploadPart API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html)
- [AbortMultipartUpload API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html)
- [S3 object-store learning guide](../learning/s3-object-store.md)
- [Architecture invariants](../architecture/invariants.md)
