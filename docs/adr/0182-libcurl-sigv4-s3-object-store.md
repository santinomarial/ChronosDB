# ADR 0182: libcurl SigV4 S3 object-store backend

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB storage, security, and operations maintainers
- **Extends:** [ADR 0070](0070-feature-pass-logical-boundaries.md)

## Context

The Phase 17 object-store boundary had only a deterministic memory implementation. Production
tiering needs HTTPS, server-certificate verification, AWS Signature Version 4, conditional object
creation, finite connection/request bounds, and exact range responses. Implementing HTTP, TLS,
proxies, DNS, and SigV4 inside ChronosDB would duplicate security-sensitive protocol machinery.
Importing an AWS-specific SDK would add a much broader provider/runtime surface than the existing
S3-compatible `put_if_absent`/`stat`/`get_range` contract requires.

## Decision

ChronosDB depends on libcurl 7.75 or newer for the production `S3ObjectStore`. Version 7.75 is the
first libcurl release with `CURLOPT_AWS_SIGV4`. The backend uses path-style endpoint URLs and signs
each request with service `s3` and the configured region. Each operation owns an independent easy
handle, header list, response buffer, and error state, so calls may execute concurrently without
sharing mutable handles.

Configuration owns one endpoint, bucket, region, access-key pair, optional session token, optional
CA bundle, finite connect/request timeouts, and a finite response bound. HTTPS is mandatory by
default and always uses peer and hostname verification. Plain HTTP requires an explicit switch and
exposes credentials and bytes to that network. Redirects are never followed, response content
decoding is disabled, and credentials or remote response bodies are never placed in returned error
messages.

`put_if_absent` hashes the supplied body locally, sends `If-None-Match: *`, signs the exact payload
SHA-256, requests S3 SHA-256 validation, and stores the same digest in
`x-amz-meta-chronos-sha256`. A successful conditional create returns the requested immutable
identity. HTTP 412 triggers a signed `HEAD`; only an exact size and Chronos SHA-256 metadata match
is idempotent success. A different existing object returns `ALREADY_EXISTS`, and disappearance
during verification returns retryable `UNAVAILABLE`.

`stat` accepts only a successful `HEAD` with addressable content length and exactly one valid
Chronos SHA-256 metadata value. `get_range` sends one exact inclusive HTTP byte range, requires
status 206, one matching `Content-Range`, and exactly the requested number of bytes, and rejects
servers that silently return a whole object. Zero-length ranges use `HEAD` to validate the requested
boundary. Provider listings remain outside the API and are never metadata authority.

The AWS documentation defines `If-None-Match: *` conditional object creation and its 412/409
behavior. The libcurl documentation defines `CURLOPT_AWS_SIGV4`, including the credential and
payload-hash inputs used here:

- <https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html>
- <https://curl.se/libcurl/c/CURLOPT_AWS_SIGV4.html>

## Consequences and validation

libcurl becomes a required exported package dependency. The synchronous `ObjectStore` interface
still ties one caller thread to one HTTP request; higher-level scheduling must keep it off latency-
sensitive shard owners. Static configuration does not refresh expiring credentials. This first
production carrier deliberately excludes automatic retries/backoff, multipart upload, redirects,
proxy policy, and a credential-provider chain. [ADR 0192](0192-exact-conditional-object-deletion.md)
adds the exact conditional delete primitive, while its invocation still requires a separate
reader/authority lifecycle owner.

[ADR 0196](0196-bounded-s3-retry-and-credential-refresh.md) subsequently adds bounded retry/backoff
and a concurrent caller-supplied credential provider with explicit forced refresh after 401/403.
Multipart upload, redirect/proxy policy, and built-in environment/workload/instance provider
implementations remain outside this original carrier decision.

A local S3-compatible HTTP integration test checks TLS-default rejection of plaintext, SigV4 and
session-token headers, conditional creation, exact retry verification, percent-encoded keys, HEAD
metadata, and a two-byte range response. Live AWS/MinIO/LocalStack, credential-expiry, proxy, TLS-
failure, timeout, partial-response, concurrent-writer, and object-store fault matrices remain in the
deferred-validation ledger.

Invariants 2, 3, 10, 14, and 18 apply.

## Alternatives considered

- **Custom HTTP/TLS/SigV4:** rejected because it would create an unnecessary security and protocol
  maintenance surface.
- **AWS SDK for C++:** rejected for this boundary because its provider-specific dependency graph is
  substantially larger than the three required S3 operations.
- **Unconditional PUT followed by HEAD:** rejected because a conflicting key could be overwritten
  before verification.
- **Treat ETag as SHA-256:** rejected because S3 ETags are not a stable full-object SHA-256 contract,
  especially for multipart uploads.

## Migration and rollback

This adds no ChronosDB durable or network format. Existing `MemoryObjectStore` users are unchanged.
Rollback may remove the production backend and libcurl dependency, but production tiering must then
fail configuration rather than silently use the nondurable memory backend.

## References

- [Feature-pass logical boundaries](0070-feature-pass-logical-boundaries.md)
- [Object-store learning guide](../learning/s3-object-store.md)
- [Architecture invariants](../architecture/invariants.md)
