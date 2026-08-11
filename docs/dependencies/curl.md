# curl / libcurl

## Purpose and boundary

`chronos_tiering` uses libcurl only as the maintained HTTP(S), DNS, TLS-integration, proxy, and AWS
Signature Version 4 carrier behind `S3ObjectStore`. ChronosDB owns immutable-key semantics, object
and range bounds, SHA-256 identity, error classification, manifest authority, cache policy, and
tiering lifecycle. No curl type appears in a public ChronosDB header, and libcurl does not define a
durable ChronosDB representation.

## Version source and compatibility

CMake requires the system `CURL::libcurl` target at version 7.75 or newer and below 9.0. Version
7.75 introduced `CURLOPT_AWS_SIGV4`. Ubuntu 24.04 resolves the supported baseline through
`libcurl4-openssl-dev`; macOS uses the Command Line Tools SDK libcurl. Installed static ChronosDB
consumers repeat the dependency lookup. The S3 HTTP contract and Chronos checksum metadata, not
libcurl-owned bytes, define compatibility.

## License and transitive graph

curl uses the curl license, an MIT/X-style permissive license. The selected system target may depend
on a TLS backend, resolver, compression libraries, HTTP/2 implementation, Kerberos/GSSAPI, and proxy
support according to the platform package. ChronosDB explicitly configures the narrow request
features it uses and retains OpenSSL as its direct cryptography/TLS dependency elsewhere.

## Ownership, security, and updates

The tiering owner maintains the wrapper, finite timeouts and response buffers, TLS-default policy,
redirect prohibition, SigV4 inputs, conditional-write behavior, metadata parsing, and exact range
validation. Distribution package channels supply security updates. A minor upgrade must run the
local S3-compatible test, live supported-provider tests when configured, TLS/timeout/partial-I/O
faults, sanitizer checks, and installed-consumer validation. A major version requires an ADR review
before the upper bound changes. Advisories affecting URL parsing, SigV4, credential handling, HTTP
headers, redirects, TLS verification, or response framing require prompt review.

## Rejected alternatives

- A custom HTTP/TLS/SigV4 stack would expand protocol and security risk without adding core database
  value.
- The AWS SDK for C++ has a much broader provider-specific dependency and runtime surface than the
  three S3-compatible operations required here.
- Vendoring curl would duplicate supported-platform security update ownership and enlarge the source
  tree; reproducibly constrained system packages are sufficient for the current platform matrix.
- Shelling out to the `curl` executable would make credential, cancellation, response framing, and
  process ownership less explicit.

