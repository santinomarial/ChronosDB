# ADR 0203: Strict S3 multipart completion result

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB object-storage, security, and runtime maintainers
- **Extends:** [ADR 0197](0197-conditional-s3-multipart-upload.md)

## Context

S3 may return HTTP 200 for `CompleteMultipartUpload` and continue streaming whitespace before an
embedded XML error. Treating status alone—or merely searching the response for a success substring—
could misclassify an error or malformed body. Exact final HEAD prevents a nonexistent object from
being installed, but the completion response must still have a bounded, deterministic success
language and must not accept mixed success/error documents.

## Decision

An HTTP-200 completion body is a candidate success only when, after bounded capture and leading
whitespace, it contains an optional complete XML declaration followed by one top-level
`CompleteMultipartUploadResult` element. The root name must end at a legal tag boundary, exactly one
closing root must exist, no `Error` element may occur, and only whitespace may follow the closing
root. Empty, truncated, duplicated, mixed, or unrelated bodies are not completion success.

Even a structurally valid result is not authoritative. ChronosDB performs exact HEAD verification
of the immutable key's length and Chronos SHA-256 metadata before releasing the abort guard or
returning success. An invalid or embedded-error response also performs exact HEAD reconciliation in
case completion succeeded ambiguously; absent exact content returns corruption and best-effort
aborts the session.

The parser does not expose provider messages or XML bodies in returned errors.

## Consequences and validation

The recognition pass is allocation-free and linear in the already bounded response body. It is not
a general XML parser; it recognizes only enough outer structure to distinguish the provider's
document kinds while exact object metadata remains the final proof.

The local S3 fixture returns HTTP 200 with an XML declaration and top-level `Error`. The test proves
the operation returns `CORRUPTION`, exact HEAD finds no object, the precise upload session is
aborted, and a later stat remains `NOT_FOUND`. Existing successful multipart and part-failure tests
remain in the focused gate.

Invariants 2, 3, 8, 10, 14, and 18 apply.

## Alternatives considered

- **Trust HTTP 200:** rejected because the provider explicitly permits embedded completion errors.
- **Search for the success root substring:** rejected because a malformed or mixed document can
  contain that text without being a success document.
- **Skip HEAD after a valid result:** rejected because immutable object identity, not response XML,
  is the durable cross-layer contract.
- **Return the provider XML error:** rejected because response bodies may contain deployment details
  and are not needed for the stable status contract.

## Migration and rollback

No durable, network, object-metadata, or provider configuration format changes. Rollback weakens
response classification and should retain exact final HEAD at minimum.

## References

- [CompleteMultipartUpload API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html)
- [S3 object-store learning guide](../learning/s3-object-store.md)
- [Architecture invariants](../architecture/invariants.md)
