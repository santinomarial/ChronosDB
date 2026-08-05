# OpenSSL

## Purpose and boundary

`chronos_ingest` uses OpenSSL's maintained EVP provider API solely to compute SHA-256 request
digests. OpenSSL does not define the preimage, durable representation, command parser, identity
semantics, or any database subsystem. ChronosDB owns those contracts and verifies them with
independent golden fixtures and published SHA-256 vectors.

## Version source and compatibility

CMake requires the system-provided `OpenSSL::Crypto` target at version 3.0 or newer and below the
next incompatible major version once CMake's package range support is applied. CI installs
`libssl-dev` on Ubuntu 24.04 and `openssl@3` through Homebrew on macOS. The installed ChronosDB
package repeats this dependency lookup for static-library consumers.

## License and transitive graph

OpenSSL 3 is Apache License 2.0. `chronos_ingest` links `libcrypto`; no OpenSSL public type appears
in a ChronosDB header. The runtime provider/module configuration used by the host OpenSSL build is
the only transitive runtime surface introduced here.

## Ownership, security, and updates

The ingestion owner maintains the wrapper and compatibility vectors. Supported-platform security
updates come from the operating-system or Homebrew package channel. Dependency updates must run
the SHA-256 vectors, command golden fixture, corruption/property suite, fuzz target, sanitizers,
installed-consumer test, and digest/codec benchmarks. Security advisories affecting SHA-256,
EVP/provider initialization, or `libcrypto` loading require prompt review.

## Rejected alternatives

- A custom SHA-256 implementation was rejected because cryptographic primitives are commodity
  security-sensitive code and a local implementation would expand audit and maintenance risk.
- Deprecated low-level OpenSSL SHA APIs were rejected in favor of the OpenSSL 3 EVP provider API.
- Command-byte concatenation before hashing was rejected because EVP supports streaming the exact
  fragments without an additional batch-sized allocation.
