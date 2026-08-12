# ADR 0208: Strict HTTP-date Retry-After parsing

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB object-storage, HTTP, reliability, and operations maintainers
- **Extends:** [ADR 0204](0204-bounded-s3-retry-after-hints.md)

## Context

`Retry-After` permits either delta seconds or an HTTP date. ChronosDB handled only the numeric form,
so a standards-compliant service using an absolute date could not raise the next delay within the
operator's configured ceiling. RFC 9110 requires recipients parsing HTTP dates to accept IMF-fixdate
and the two obsolete compatibility forms.

General-purpose date parsers commonly accept formats and invalid calendar values beyond the HTTP
grammar. Retry input is remote and should not broaden the carrier's policy or create an unbounded
sleep.

## Decision

The response callback captures one trimmed `Retry-After` value of at most 64 bytes. Empty, repeated,
control-bearing, or oversized fields are invalid. Immediately before a replay-safe transient retry,
the client first parses an exact unsigned decimal. Otherwise it recognizes exactly:

- IMF-fixdate: `Sun, 06 Nov 1994 08:49:37 GMT`;
- obsolete RFC 850: `Sunday, 06-Nov-94 08:49:37 GMT`; and
- ANSI C asctime: `Sun Nov  6 08:49:37 1994`.

English weekday/month names, separators, widths, GMT, real Gregorian dates, time ranges, and the
weekday/date correspondence must all match. RFC 850 two-digit years more than 50 years in the future
are interpreted as the previous century. The parsed UTC instant is compared to the current system
clock immediately before sleeping; a past date contributes zero seconds. Parsing failure discards
the hint.

The resulting delay retains ADR 0204's policy: the hint may raise exponential backoff but never
exceed `maximum_retry_backoff`, never creates another retry, and never affects authentication
refresh or non-replayable requests.

## Consequences and validation

The parser is allocation-free after bounded header capture and independent of locale/time zone.
System-clock skew can shorten or lengthen the requested absolute delay, but the configured ceiling
remains authoritative. Operators remain responsible for maintaining a synchronized clock, which is
already required by SigV4.

Focused loopback tests send future dates in all three RFC forms and prove each raises a zero base
delay to the same 20 ms ceiling without exceeding the finite request budget. The existing numeric
test remains. Broader clock-step simulation and live provider qualification remain deferred.

Invariants 2, 3, 8, 10, and 18 apply.

## Alternatives considered

- **Continue ignoring dates:** rejected because it discards a required interoperable form.
- **Use `curl_getdate`:** rejected because it intentionally recognizes many loose date forms and
  documents incomplete invalid-date detection.
- **Sleep until the absolute date without a ceiling:** rejected because remote input cannot override
  the operator's latency bound.
- **Use the server `Date` header as the reference clock:** rejected because it is another remote
  input and can turn skew into an attacker-controlled delay.

## Migration and rollback

No configuration, durable, or network format changes. Services returning HTTP dates now receive the
same bounded cooperation as numeric hints. Rollback ignores those dates and returns to exponential
backoff.

## References

- [RFC 9110 HTTP Semantics, Retry-After](https://www.rfc-editor.org/rfc/rfc9110.html#name-retry-after)
- [RFC 9110 HTTP-date](https://www.rfc-editor.org/rfc/rfc9110.html#name-date-time-formats)
- [libcurl curl_getdate](https://curl.se/libcurl/c/curl_getdate.html)
- [Bounded S3 Retry-After hints](0204-bounded-s3-retry-after-hints.md)
- [S3 object-store learning guide](../learning/s3-object-store.md)
