# S3 Object Store

## Purpose and interfaces

`ObjectStore` is ChronosDB's provider-neutral immutable-object boundary. `S3ObjectStore` supplies
the production HTTP implementation; `MemoryObjectStore` remains a deterministic reference backend
and does not claim remote durability. The API contains conditional upload, authoritative per-key
metadata, exact range read, and exact conditional deletion. It has no listing operation because an
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

Operators should grant only `PutObject`, `HeadObject`/`GetObject`, ranged `GetObject`, and
conditional `DeleteObject` access for the configured prefix, enforce TLS, keep bucket policy
compatible with
`If-None-Match`, and rotate credentials outside the current store lifetime. Cold Location Manifest
v1 records the object key, store identity, and exact Manifest v2 part SHA-256; its durable
installation, pair selection, and publication path is separate from the object backend. Bucket
listings and ETags are not a replacement.

## Cold location authority

Cold Location Manifest v1 is a separate immutable full-generation registry because Manifest v2's
fixed reserved bytes remain frozen zero. It binds one database and exact base generation to a
deployment object-store UUID, then maps a sorted subset of part IDs to exact length, SHA-256, and
bounded object key. The strict codec owns parsed keys, validates the header before trusting lengths,
checks every descriptor and the complete file, and rejects trailing bytes. Binding constructs an
ordered part index in `O(base parts log base parts)` time and checks locations in
`O(cold parts log base parts)` time with `O(base parts)` temporary pointers.

The codec is not a deletion receipt. Production publication must acquire a compatible Manifest-v2/
cold pair, and local reclamation must wait for every older reader pin. The object backend can now
delete only after exact length/SHA-256 verification and, for S3, an ETag `If-Match`.
`TieredRemoteObjectReclamationCoordinator` supplies the separate current-pair and historical-reader
proof before invoking it. After a crash, `TieredRestartRemoteGarbageCoordinator` uses consecutive
local cold-generation history—not a bucket listing—to rediscover unreachable routes before reader
admission, rebinds each generation to its exact historical Manifest, preflights all metadata, and
retries already-absent objects safely.

Likely review questions include why conditional PUT precedes HEAD, why ETag is insufficient, why
redirects are disabled, what verifies a range, and why the memory backend is not a durability
implementation.
