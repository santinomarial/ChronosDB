# ADR 0204: Bounded S3 Retry-After hints

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB object-storage, runtime, and operations maintainers
- **Extends:** [ADR 0196](0196-bounded-s3-retry-and-credential-refresh.md)

## Context

The S3 carrier retries replay-safe transient responses with capped exponential backoff, but ignored
provider `Retry-After` guidance. Retrying earlier than requested can amplify throttling. Conversely,
an untrusted endpoint must not use an enormous, malformed, or duplicated header to exceed the
operator's configured retry-delay bound.

`Retry-After` also supports an HTTP-date form, subsequently implemented by
[ADR 0208](0208-strict-http-date-retry-after.md).

## Decision

The response-header callback recognizes at most one case-insensitive `Retry-After` field in the
standard nonnegative delta-seconds form. Parsing is allocation-free into `uint64_t`. Empty,
negative, date-form, malformed, overflowing, or repeated values invalidate the hint and are ignored.

For a replayable transient HTTP response, the next sleep is the larger of the local exponential
delay and the parsed provider hint, capped by `maximum_retry_backoff`. Conversion to milliseconds
checks overflow before multiplication. The hint never changes the one-to-32 attempt budget,
per-attempt timeout, replay classification, credential refresh policy, or the special multipart
create/complete rules. Authorization rejection uses the credential-refresh schedule rather than a
provider delay header.

## Consequences and validation

Operators retain one hard maximum-delay control while cooperating with short provider throttling
hints. A provider request longer than that ceiling is deliberately shortened; production operators
should configure a ceiling suitable for their service quota and synchronous latency budget. HTTP
ADR 0208 and [ADR 0209](0209-bounded-s3-retry-jitter.md) subsequently add strict HTTP-date support
and bounded per-store jitter.

The local S3 fixture returns one transient 503 with `Retry-After: 1`. A focused test configures zero
local delay and a 20 ms ceiling, proves the request succeeds on its second exact signed attempt,
observes a material bounded delay, and rules out an unbounded one-second sleep. Existing exhaustion,
credential refresh, and multipart retry tests remain in the gate.

Invariants 2, 3, 8, 10, 14, and 18 apply.

## Alternatives considered

- **Ignore the header:** rejected because immediate synchronized retry can worsen provider overload.
- **Sleep the raw requested duration:** rejected because endpoint input would override the finite
  operator latency bound.
- **Accept HTTP dates now:** deferred because wall-clock skew, parsing, and test policy are not
  needed for the duration-based carrier.
- **Let Retry-After add attempts:** rejected because it would weaken the explicit finite budget.

## Migration and rollback

No durable, network, object, or configuration format changes. Existing configurations retain the
same hard `maximum_retry_backoff`; rollback merely ignores server hints again.

## References

- [HTTP Retry-After](https://www.rfc-editor.org/rfc/rfc9110#name-retry-after)
- [Bounded S3 retry and credential refresh](0196-bounded-s3-retry-and-credential-refresh.md)
- [S3 object-store learning guide](../learning/s3-object-store.md)
- [Architecture invariants](../architecture/invariants.md)
