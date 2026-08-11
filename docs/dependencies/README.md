# Production dependency records

This directory records every production dependency required by ADR-0011. Each record identifies
the narrow ownership boundary, version source, license, transitive surface, security and update
policy, and rejected alternatives. Test-only dependencies remain governed by the reproducible
CMake dependency configuration.

- [OpenSSL](openssl.md) — SHA-256 provider and maintained mutual-TLS socket carrier.
- [Zstandard](zstd.md) — bounded general-purpose compression provider for CSEG v1 pages.
- [curl / libcurl](curl.md) — bounded HTTPS and SigV4 carrier for S3-compatible object storage.
