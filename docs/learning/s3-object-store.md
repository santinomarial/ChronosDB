# S3 Object Store

## Purpose and interfaces

`ObjectStore` is ChronosDB's provider-neutral immutable-object boundary. `S3ObjectStore` supplies
the production HTTP implementation; `MemoryObjectStore` remains a deterministic reference backend
and does not claim remote durability. The API contains conditional upload, authoritative per-key
metadata, exact range read, and exact conditional deletion. It has no listing operation because an
eventually consistent or incomplete bucket listing must never determine query-visible state.

`S3ObjectStore::create` owns its configuration and either owns static credentials or shares one
caller-supplied credential provider. The provider may be called concurrently and owns its cache
synchronization. `S3EnvironmentCredentialProvider` is one explicit built-in option: it snapshots
the standard AWS access key, secret, and optional session-token variables once, rejects incomplete
values, and stays immutable. `put_if_absent` borrows upload bytes only until the synchronous call
returns. `S3CredentialProviderChain` composes an explicit ordered provider list: only `NOT_FOUND`
advances initial selection, and the first successful identity remains pinned for current and refresh
requests so authorization rejection cannot downgrade credentials.
`stat` and `get_range` are const and each attempt owns a fresh libcurl easy handle, credential
value, signature, header list, and bounded response buffer. The store itself is therefore safe for
concurrent calls, while destruction still requires ordinary external lifetime exclusion.

## Invariants and failure behavior

An immutable key is created with `If-None-Match: *`. The request carries the exact SHA-256 as the
SigV4 content digest, an S3 checksum, and Chronos-owned metadata. If the key already exists, a HEAD
request must prove equal length and digest before the retry is successful. A conflicting object is
never overwritten or accepted. Concurrent independent clients therefore converge to the same
identity or elect one complete winner; a losing different identity receives `ALREADY_EXISTS`.

HTTPS and certificate/hostname verification are the default. Plain HTTP must be selected
explicitly and is appropriate only where the deployment accepts exposure of credentials and object
bytes. Redirects are rejected so credentials are not silently replayed to another authority.
Process proxy variables are disabled. One bounded credential-free HTTP(S) proxy may be configured
explicitly; endpoint TLS verification remains active and ambient `no_proxy` cannot override it.
Operators may require SSE-S3 or SSE-KMS. The selected headers are signed on a single PUT or
multipart initiation, and an authoritative HEAD must return the exact algorithm before ChronosDB
accepts the object. SSE-KMS additionally requires an exact canonical key identifier. Encryption
headers are never sent on HEAD or GET. With no explicit setting, bucket-default encryption remains
unverified and ChronosDB makes no at-rest-encryption assertion.
Timeout/connectivity/server-overload failures are retryable `UNAVAILABLE`; authentication and TLS
identity failures are `UNAUTHENTICATED`; conflicting immutable content is `ALREADY_EXISTS`; damaged
or incomplete metadata and wrong-length ranges are `CORRUPTION`.

Range reads require HTTP 206, one exact `Content-Range`, and the requested body length. The tier
manager separately verifies a whole-object digest when it populates the full-object cache, or an
expected range digest when one is available. CSEG's internal checksums remain responsible for page
framing and interpretation.

Transport `UNAVAILABLE` and HTTP 409/425/429/5xx outcomes retry within a configured one-to-32
attempt budget using capped exponential backoff. Valid delta-seconds or any of the three RFC HTTP-date
`Retry-After` forms may raise the next delay but never above the configured ceiling;
malformed/repeated hints are ignored. Bounded nonnegative jitter then spreads concurrent retries
within the same hard ceiling. HEAD/range GET are read-only; conditional PUT
turns an ambiguous success into exact 412 verification; exact conditional DELETE turns it into an
already-absent retry. Other failures do not retry. A provider-backed 401/403 requests one forced
refresh on the next attempt, whereas rejected static credentials fail immediately. Provider errors
and refresh attempts share the same finite budget.

Objects at or above the configured multipart threshold use a bounded set of signed parallel part
workers with parts of at least 5 MiB except for the final part. Every worker owns its HTTP state and
exclusive ETag slot; all workers join before completion or abort. Initiation records the whole-object
Chronos SHA-256 metadata; each
part retains its opaque ETag; completion submits ascending part numbers under `If-None-Match: *`;
and exact HEAD revalidates final length and SHA-256. A bounded XML parser accepts one upload ID and
percent-encodes it for every part/complete/abort query. HTTP 200 completion is not sufficient: the
bounded body must have one top-level completion-result root, no error element, and no non-whitespace
trailing content, after which exact HEAD remains the final authority.

UploadPart retries the same session/number/bytes. Complete reconciles ambiguous outcomes with exact
HEAD, but a 409 is returned so the next caller attempt creates a fresh session. Create is not
transport-retried because its success produces a new upload ID. After an ID is known, every failure
path makes a bounded best-effort abort, including allocation unwinding. Operators must still apply
an incomplete-multipart lifecycle rule for lost create responses and failed aborts; incomplete parts
are never cold-manifest authority.

## Complexity, tradeoffs, and operations

Upload hashing and transfer are `O(object bytes)` and retain no second upload copy. HEAD uses
constant response storage. A range read is `O(requested bytes)` and retains at most the configured
bound. Multipart retains borrowed object bytes plus `O(part count)` ETags and completion XML, with a
bounded number of simultaneous handles. Every attempt currently creates one easy handle and the
public call is synchronous; connection pooling and the libcurl multi interface remain deferred.
Workload,
instance-metadata and shared-file providers remain deferred.

Operators should grant only `PutObject`, multipart create/upload/complete/abort,
`HeadObject`/`GetObject`, ranged `GetObject`, and conditional `DeleteObject` access for the configured
prefix, enforce TLS, keep bucket policy compatible with `If-None-Match`, and configure incomplete
multipart expiry. A refreshable provider can rotate credentials during the store lifetime;
deployments using static fields or the immutable environment snapshot must recreate the store to
rotate them. Forced refresh of the environment provider fails closed rather than returning the
server-rejected snapshot. Cold Location Manifest v1
records the object key, store identity, and exact Manifest v2 part SHA-256; its durable
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
redirects are disabled, what makes every retried method replay-safe, who synchronizes a credential
provider, what verifies a range, and why the memory backend is not a durability implementation.
