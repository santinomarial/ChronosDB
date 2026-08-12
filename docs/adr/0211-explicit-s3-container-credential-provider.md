# ADR 0211: Explicit S3 container credential provider

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB security, runtime, object-storage, and operations maintainers
- **Extends:** [ADR 0196](0196-bounded-s3-retry-and-credential-refresh.md), [ADR 0210](0210-pinned-ordered-s3-credential-chain.md)

## Context

ECS task roles and EKS Pod Identity expose temporary credentials from a local agent through an HTTP
GET returning access key, secret, token, and expiration. ChronosDB had a refreshable provider seam
but no built-in workload implementation. Automatically reading the standard endpoint/token
environment variables would make network authority, credential precedence, token-file access, and
SSRF policy implicit.

## Decision

`S3ContainerCredentialProvider::create` requires an explicit endpoint and optionally accepts one
in-memory Authorization value, CA bundle, connect/request timeouts, expiration refresh window, and
response bound. It never reads environment variables or token files. HTTPS is required by default;
plain HTTP requires an explicit switch for a reviewed loopback/link-local container agent.

The endpoint is a bounded HTTP(S) URL without userinfo, fragments, controls, or spaces. Every fetch
uses a fresh libcurl easy handle, disables ambient proxies and response decoding, verifies HTTPS
peer/hostname, follows no redirect, accepts only the configured protocol, and bounds the response
to at most 1 MiB. Returned errors never include the Authorization token, credential values, URL, or
response body.

HTTP 200 must contain exactly one plain JSON string each for `AccessKeyId`, `SecretAccessKey`,
`Token`, and `Expiration`. Duplicate/missing fields, escapes, controls, oversized values, invalid
credentials, malformed UTC `YYYY-MM-DDTHH:MM:SSZ`, and expired values fail `UNAUTHENTICATED`.
401/403/404 are rejected identity; other non-200 responses and transport failures are unavailable.

One mutex serializes fetch and cache publication. `kCurrent` returns the cached value only while it
is strictly outside the configured refresh window (bounded to seven days); otherwise it fetches.
`kRefresh` always fetches and never falls back to a previously rejected cached value. A failed fetch
does not replace the last cache, but that cache is not returned for the failed request. Credential
copies translate allocation failure into bounded status.

## Consequences and validation

The provider supports explicit ECS/EKS-compatible agents without embedding a cloud SDK or automatic
metadata access. Operators or startup code may deliberately construct it from reviewed environment
and token-file configuration, then place it in the pinned ordered chain. File opening, symlink
policy, and environment precedence remain outside this provider.

Focused loopback tests prove Authorization delivery, exact temporary credential parsing, cache reuse,
forced refresh, successful SigV4/session-token S3 use, TLS-default rejection of plaintext, malformed
response rejection, and secret-redacted invalid configuration. The external consumer gate references
the public factory. Live ECS/EKS qualification, token-file rotation integration, TLS-agent fixtures,
clock-step testing, and concurrent stampede/latency evidence remain deferred.

Invariants 2, 3, 8, 10, 14, 16, and 18 apply.

## Alternatives considered

- **Read standard environment automatically:** rejected because endpoint/token precedence and global
  state would be implicit.
- **Accept arbitrary redirects or proxies:** rejected because credential-agent tokens must not be
  replayed to another authority.
- **Return stale credentials on refresh failure:** rejected because the S3 authority may have just
  rejected them.
- **Use a general JSON dependency:** rejected because four bounded strings do not justify a new
  production dependency; the strict subset also rejects ambiguous duplicate/escaped inputs.
- **Implement token-file reading here:** rejected because file ownership, symlinks, permissions, and
  rotation need an embedding/runtime policy.

## Migration and rollback

Existing credential modes are unchanged. Workload deployments may explicitly configure the new
provider or compose it in `S3CredentialProviderChain`. No durable, object, credential, or network
format changes.

## References

- [AWS container credential provider](https://docs.aws.amazon.com/sdkref/latest/guide/feature-container-credentials.html)
- [Amazon EKS Pod Identity agent](https://docs.aws.amazon.com/eks/latest/userguide/pod-id-how-it-works.html)
- [Pinned ordered S3 credential chain](0210-pinned-ordered-s3-credential-chain.md)
- [S3 object-store learning guide](../learning/s3-object-store.md)
- [Architecture invariants](../architecture/invariants.md)
