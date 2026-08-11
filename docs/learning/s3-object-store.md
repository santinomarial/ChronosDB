# S3 Object Store

## Purpose and interfaces

`ObjectStore` is ChronosDB's provider-neutral immutable-object boundary. `S3ObjectStore` supplies
the production HTTP implementation; `MemoryObjectStore` remains a deterministic reference backend
and does not claim remote durability. The API intentionally contains only conditional upload,
authoritative per-key metadata, and exact range read. It has no listing operation because an
eventually consistent or incomplete bucket listing must never determine query-visible state.

`S3ObjectStore::create` owns its configuration and credentials. `put_if_absent` borrows upload
bytes only until the synchronous call returns. `stat` and `get_range` are const and each call owns a
fresh libcurl easy handle, header list, and bounded response buffer. The store itself is therefore
safe for concurrent calls, while destruction still requires ordinary external lifetime exclusion.

## Invariants and failure behavior

An immutable key is created with `If-None-Match: *`. The request carries the exact SHA-256 as the
SigV4 content digest, an S3 checksum, and Chronos-owned metadata. If the key already exists, a HEAD
request must prove equal length and digest before the retry is successful. A conflicting object is
never overwritten or accepted.

HTTPS and certificate/hostname verification are the default. Plain HTTP must be selected
explicitly and is appropriate only where the deployment accepts exposure of credentials and object
bytes. Redirects are rejected so credentials are not silently replayed to another authority.
Timeout/connectivity/server-overload failures are retryable `UNAVAILABLE`; authentication and TLS
identity failures are `UNAUTHENTICATED`; conflicting immutable content is `ALREADY_EXISTS`; damaged
or incomplete metadata and wrong-length ranges are `CORRUPTION`.

Range reads require HTTP 206, one exact `Content-Range`, and the requested body length. The tier
manager separately verifies a whole-object digest when it populates the full-object cache, or an
expected range digest when one is available. CSEG's internal checksums remain responsible for page
framing and interpretation.

## Complexity, tradeoffs, and operations

Upload hashing and transfer are `O(object bytes)` and retain no second upload copy. HEAD uses
constant response storage. A range read is `O(requested bytes)` and retains at most the configured
bound. Every operation currently creates one easy handle and is synchronous; connection pooling,
the libcurl multi interface, multipart upload, retries/backoff, and rotating credential providers
are deferred until their ownership and cancellation contracts are defined.

Operators should grant only `PutObject`, `HeadObject`/`GetObject`, and ranged `GetObject` access for
the configured prefix, enforce TLS, keep conditional-write bucket policy compatible with
`If-None-Match`, and rotate credentials outside the current store lifetime. ChronosDB records the
object key and SHA-256 in future cold-manifest authority; bucket listings and ETags are not a
replacement.

Likely review questions include why conditional PUT precedes HEAD, why ETag is insufficient, why
redirects are disabled, what verifies a range, and why the memory backend is not a durability
implementation.
