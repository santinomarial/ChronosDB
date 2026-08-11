# OpenSSL

## Purpose and boundary

`chronos_ingest` uses OpenSSL's maintained EVP provider API to compute SHA-256 request digests.
`chronos_network` uses the maintained TLS record, certificate-loading, chain-verification,
DNS/IP identity-verification, SNI, and EVP certificate-digest APIs behind an OpenSSL-free public
PIMPL boundary. OpenSSL does not define a
durable representation, database command, application principal, or authorization policy.

## Version source and compatibility

CMake requires the system-provided `OpenSSL::Crypto` and `OpenSSL::SSL` targets at version 3.0 or
newer and below the next incompatible major version once CMake's package range support is applied. CI installs
`libssl-dev` on Ubuntu 24.04 and `openssl@3` through Homebrew on macOS. The installed ChronosDB
package repeats this dependency lookup for static-library consumers.

## License and transitive graph

OpenSSL 3 is Apache License 2.0. `chronos_ingest` links `libcrypto`; `chronos_network` links
`libcrypto` and `libssl`. No OpenSSL public type appears in a ChronosDB header. The host OpenSSL
provider/module and trust-store behavior are the transitive runtime surfaces.

## Ownership, security, and updates

The ingestion and network owners maintain their wrappers and compatibility tests. Supported-platform
security updates come from the operating-system or Homebrew package channel. Dependency updates
must run SHA-256 vectors, mutual-TLS socket tests, network tests, fuzz targets, sanitizers,
installed-consumer tests, and applicable benchmarks. Advisories affecting SHA-256, EVP/provider
initialization, certificate or server-identity verification, TLS, SNI, or library loading require
prompt review.

## Rejected alternatives

- A custom SHA-256 implementation was rejected because cryptographic primitives are commodity
  security-sensitive code and a local implementation would expand audit and maintenance risk.
- Deprecated low-level OpenSSL SHA APIs were rejected in favor of the OpenSSL 3 EVP provider API.
- Command-byte concatenation before hashing was rejected because EVP supports streaming the exact
  fragments without an additional batch-sized allocation.
- A custom TLS implementation was rejected because protocol and certificate validation are
  security-sensitive commodity functionality.
