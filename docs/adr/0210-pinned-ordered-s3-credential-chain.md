# ADR 0210: Pinned ordered S3 credential chain

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB security, runtime, object-storage, and operations maintainers
- **Extends:** [ADR 0196](0196-bounded-s3-retry-and-credential-refresh.md), [ADR 0199](0199-explicit-s3-environment-credential-provider.md)

## Context

ChronosDB exposes an explicit credential-provider seam and one environment provider, but embedding
runtimes still had to implement stable precedence across environment, workload, instance, or custom
sources. Generic fallback is security-sensitive: an unavailable high-priority identity must not be
confused with an absent identity, and authorization rejection must not silently switch the process
to a lower-priority principal with different permissions.

Concurrent initial requests also need one linearizable choice rather than selecting different
providers based on timing.

## Decision

`S3CredentialProviderChain::create` accepts one to 32 nonnull caller-owned shared providers in
explicit highest-to-lowest precedence. The chain never discovers or inserts providers itself.

Before selection, a `kCurrent` request calls providers in order. Only `NOT_FOUND` means that a
provider is not configured and advances to the next. Any other failure—including `UNAVAILABLE`,
`UNAUTHENTICATED`, malformed data reported by a provider, or internal failure—stops selection and is
returned unchanged. The first successful value pins that provider index for the chain lifetime.
If all providers return `NOT_FOUND`, the chain returns `UNAUTHENTICATED` without provider secrets.

After selection, every `kCurrent` and `kRefresh` request is delegated only to the pinned provider.
Refresh failure is returned; the chain never falls through. `kRefresh` before initial selection
fails closed because no server-rejected identity is known.

One chain mutex serializes selection and delegated calls. This makes the first winner linearizable
and remains compatible with providers whose public contract permits concurrency but does not
require it. The mutex is never shared with the object-store carrier, and provider callbacks must not
recursively call the same chain. The chain owns shared references and no secret copies.

## Consequences and validation

Embedding policy is now composable without implicit environment/metadata access. A temporarily
unavailable workload provider blocks fallback, which is deliberate: operators must decide whether
two identities are interchangeable rather than receiving an automatic privilege change.
Serialization can reduce credential-acquisition concurrency, but provider calls should be cached
and small relative to object transfer; evidence is required before weakening the selection lock.

Focused tests prove `NOT_FOUND` advancement, stable first-success precedence, repeated current calls,
refresh pinning, no fallback after `UNAVAILABLE`, and empty/null composition rejection. The external
consumer gate references the public factory. Workload and instance HTTP/token providers remain
separate integrations.

Invariants 2, 3, 8, 10, 14, 16, and 18 apply.

## Alternatives considered

- **Fallback on every error:** rejected because outage or malformed credentials are not absence.
- **Re-evaluate precedence on every request:** rejected because callers could alternate principals
  and authorization rejection could downgrade identity.
- **Fallback after refresh failure:** rejected because it silently changes security authority.
- **Build a fixed automatic AWS chain:** rejected because provider inclusion, endpoints, token
  files, and metadata access are deployment policy.
- **Lock-free first-success racing:** rejected because it makes precedence timing-dependent.

## Migration and rollback

Existing static, environment, and custom-provider configurations are unchanged. Embeddings may
explicitly wrap providers in the new chain. No durable, object, credential, or network format
changes.

## References

- [Bounded S3 retry and credential refresh](0196-bounded-s3-retry-and-credential-refresh.md)
- [Explicit S3 environment credential provider](0199-explicit-s3-environment-credential-provider.md)
- [S3 object-store learning guide](../learning/s3-object-store.md)
- [Architecture invariants](../architecture/invariants.md)
