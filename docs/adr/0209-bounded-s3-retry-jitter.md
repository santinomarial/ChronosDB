# ADR 0209: Bounded S3 retry jitter

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB object-storage, reliability, concurrency, and operations maintainers
- **Extends:** [ADR 0196](0196-bounded-s3-retry-and-credential-refresh.md), [ADR 0204](0204-bounded-s3-retry-after-hints.md)

## Context

Identically configured nodes seeing the same provider outage compute identical exponential sleeps.
This can create synchronized retry waves even though every individual request respects its finite
attempt and timeout budget. Provider `Retry-After` guidance supplies a floor but does not spread a
fleet that receives the same response.

Jitter must preserve the operator's hard maximum, remain safe under concurrent object-store calls,
and avoid introducing a blocking entropy dependency on the request path.

## Decision

`maximum_retry_jitter` is a nonnegative millisecond bound and defaults to 50 ms. After computing the
capped exponential delay and applying any valid provider hint as a floor, each retry adds a uniform
integer delay from zero through the smaller of the configured jitter bound and remaining room below
`maximum_retry_backoff`. Thus jitter never weakens `Retry-After`, adds attempts, or exceeds the
existing hard ceiling. Credential-refresh retries use the same spreading policy but still ignore a
provider hint on the rejected authorization response.

Each store owns an atomic 64-bit sequence initialized by mixing steady time, wall time, store
address, and a process-wide atomic sequence. Every concurrent retry claims a distinct sequence
value and runs a SplitMix64 mixing step before bounded reduction. This is scheduling noise, not a
security random number generator. An optional fixed `retry_jitter_seed` exists for deterministic
tests and reproducible embeddings; ordinary production configurations leave it absent.

The atomic sequence uses default sequentially consistent ordering. Only uniqueness within a store
is required, so relaxed ordering would suffice, but the stronger default keeps the concurrency
argument simple and its cost is negligible beside a network retry.

## Consequences and validation

Worst-case latency is unchanged because `maximum_retry_backoff` remains the hard per-sleep ceiling.
A maximum of zero preserves exact deterministic exponential/provider behavior. Very small remaining
room naturally narrows the jitter distribution.

A focused loopback test uses a fixed seed, a 20 ms exponential floor, 80 ms jitter budget, and 100
ms hard ceiling. It proves a material deterministic delay above the floor and bounded completion;
the existing Retry-After tests disable jitter and continue to prove exact provider-floor behavior.
Negative jitter is rejected before network access. Statistical distribution tests and fleet-scale
recovery simulation remain deferred.

Invariants 2, 3, 8, 10, 16, and 18 apply.

## Alternatives considered

- **No jitter:** rejected because synchronized fleets can amplify overload.
- **Full jitter from zero to the exponential value:** rejected because it can retry earlier than the
  existing exponential or provider floor.
- **Add jitter beyond the maximum:** rejected because it weakens the operator's latency contract.
- **Use `std::random_device` on every retry:** rejected because implementations can block or expose
  platform-dependent cost for a non-security requirement.
- **Use a mutable non-atomic generator:** rejected because object-store calls and multipart workers
  are concurrent.

## Migration and rollback

Existing configurations gain at most 50 ms of spreading within their existing maximum, never above
it. Set `maximum_retry_jitter` to zero to preserve exact prior sleeps. No durable, object, or network
format changes.

## References

- [Bounded S3 retry and credential refresh](0196-bounded-s3-retry-and-credential-refresh.md)
- [Bounded S3 Retry-After hints](0204-bounded-s3-retry-after-hints.md)
- [S3 object-store learning guide](../learning/s3-object-store.md)
- [Architecture invariants](../architecture/invariants.md)
