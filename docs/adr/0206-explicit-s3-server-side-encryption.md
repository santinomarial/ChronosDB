# ADR 0206: Explicit S3 server-side encryption

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB security, object-storage, runtime, and operations maintainers
- **Extends:** [ADR 0182](0182-libcurl-sigv4-s3-object-store.md)

## Context

An S3 bucket default can encrypt new objects, but an implicit external default is not an
application-verifiable storage policy. ChronosDB previously sent no encryption request headers and
did not inspect encryption metadata after immutable upload. A missing or changed bucket policy
could therefore violate an operator's intended at-rest-encryption boundary without failing cold
publication.

Amazon S3 accepts server-side-encryption headers on `PutObject` and
`CreateMultipartUpload`. `HeadObject` returns the stored algorithm and KMS key ID, but rejects those
encryption headers as request headers for SSE-S3 and SSE-KMS objects.

## Decision

`S3ObjectStoreConfig` may explicitly require either SSE-S3 (`AES256`) or SSE-KMS (`aws:kms`). SSE-KMS
also requires one bounded KMS key identifier; the caller supplies the canonical value expected in
the provider's HEAD response, normally a key ARN. A KMS identifier without SSE-KMS, SSE-KMS without
an identifier, unknown enum values, controls, spaces, and oversized identifiers are rejected before
network access.

Single-part PUT and multipart initiation send the selected algorithm; SSE-KMS also sends the key
identifier. Part upload, completion, HEAD, GET, and DELETE do not send encryption request headers.
After an encrypted single-part upload and after multipart completion, exact HEAD must report the
configured algorithm. SSE-KMS must also report the exact configured key identifier; SSE-S3 must not
report a KMS identifier. Missing, repeated, conflicting, or mismatched metadata is corruption and
the object is not accepted as immutable ChronosDB content.

The configuration remains optional for S3-compatible deployments that intentionally delegate to a
bucket default. In that mode ChronosDB makes no at-rest-encryption claim. SSE-C, DSSE-KMS, KMS
encryption context, and S3 Bucket Keys are not supported by this boundary.

## Consequences and validation

Explicitly configured deployments now make stored encryption part of exact object acceptance, not
only a write preference. An ambiguous encrypted PUT receives the same authoritative HEAD
reconciliation as conditional retry. A successful provider write with mismatched HEAD metadata may
leave an unreachable object at the immutable key; publication fails closed and operator repair is
required rather than silently accepting weaker or wrong-key storage.

Focused loopback tests prove SSE-S3 request and HEAD verification, SSE-KMS multipart initiation and
final verification, invalid configuration rejection, absence of encryption request headers on
HEAD, and fail-closed behavior when the stored algorithm differs. Live AWS KMS permissions, aliases
versus returned canonical identifiers, bucket-policy enforcement, and third-party provider
qualification remain deployment validation.

Invariants 2, 3, 8, 10, 14, and 18 apply.

## Alternatives considered

- **Trust bucket defaults:** retained only as an explicit unverified mode; it cannot establish the
  configured application boundary.
- **Verify only the algorithm:** rejected for SSE-KMS because silently using another key changes
  recovery access and security ownership.
- **Send encryption headers on HEAD:** rejected because AWS documents that as an invalid request for
  SSE-S3 and SSE-KMS.
- **Support SSE-C immediately:** rejected because customer key transport, redaction, rotation, and
  read-path headers require a separate secret-lifecycle design.

## Migration and rollback

Existing configurations retain bucket-default behavior. Operators can opt in by selecting SSE-S3
or SSE-KMS and, for KMS, supplying the canonical expected key identifier. No durable ChronosDB
format changes. Removing the option returns to unverified bucket-default behavior and weakens the
runtime storage assertion.

## References

- [Amazon S3 PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html)
- [Amazon S3 CreateMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html)
- [Amazon S3 HeadObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html)
- [S3 object-store learning guide](../learning/s3-object-store.md)
- [Architecture invariants](../architecture/invariants.md)
