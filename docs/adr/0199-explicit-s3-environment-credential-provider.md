# ADR 0199: Explicit S3 environment credential provider

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB security, runtime, and object-storage maintainers
- **Extends:** [ADR 0196](0196-bounded-s3-retry-and-credential-refresh.md)

## Context

The S3 carrier accepted static credentials or a caller-supplied refreshable provider, but a common
deployment still had to implement its own adapter for standard AWS environment credentials. Reading
process environment implicitly inside `S3ObjectStore::create` would hide credential precedence and
global-state access. Re-reading a process environment while other threads mutate it is also not a
safe rotation mechanism.

AWS standardizes `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optional `AWS_SESSION_TOKEN` for
environment-provided credentials. The session token is required for temporary credentials.

## Decision

`S3EnvironmentCredentialProvider::create` is an explicit opt-in factory. It reads exactly those
three variables once, copies them into provider-owned storage, validates the complete value with the
same bounds and character policy used by the S3 carrier, and rejects missing, partial, empty, or
malformed credentials without returning secret contents in an error.

The provider is immutable after construction, so concurrent `kCurrent` acquisition needs no lock
and never accesses process environment. A `kRefresh` request fails `UNAUTHENTICATED`; it never
returns credentials already rejected by the server. Operators rotate environment credentials by
constructing a new provider and S3 store at a controlled configuration boundary. Workload identity,
instance metadata, shared files, and automatic ordered-chain precedence remain separate policies.
The embedding runtime must exclude environment mutation while the factory takes its bounded copy.

`S3ObjectStore` does not automatically create or prefer this provider. Existing static and custom
provider modes remain mutually exclusive and unchanged.

## Consequences and validation

The provider offers a small dependency-free environment integration and preserves explicit
deployment policy. Its strings remain in normal process memory for the provider lifetime, matching
the existing static-credential ownership boundary; errors and tests never print them.

Focused tests snapshot all three variables, acquire the exact value, sign a request accepted by the
local S3 fixture, and prove forced refresh fails closed. A second test supplies an incomplete
environment and verifies `UNAUTHENTICATED` without the access-key value appearing in the error. The
installed external-consumer check references the public factory.

Invariants 2, 3, 8, 10, 14, and 18 apply.

## Alternatives considered

- **Read environment automatically in `S3ObjectStore::create`:** rejected because credential
  precedence and global-state access would become implicit.
- **Re-read environment on every attempt:** rejected because process-environment mutation is not a
  safe concurrent refresh protocol.
- **Return the same snapshot on refresh:** rejected because it could replay credentials the remote
  authority just rejected.
- **Embed a complete AWS credential chain now:** rejected because workload and instance providers
  require additional HTTP, token-file, timeout, endpoint, and SSRF policy.

## Migration and rollback

There is no durable or network migration. Existing configurations are source-compatible. Removing
the provider later requires embedding runtimes that select it to return to static fields or supply
an equivalent custom provider.

## References

- [AWS environment variables](https://docs.aws.amazon.com/sdkref/latest/guide/environment-variables.html)
- [AWS standardized credential providers](https://docs.aws.amazon.com/sdkref/latest/guide/standardized-credentials.html)
- [S3 object-store learning guide](../learning/s3-object-store.md)
- [Architecture invariants](../architecture/invariants.md)
