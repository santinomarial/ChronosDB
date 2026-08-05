# Production dependency records

This directory records every production dependency required by ADR-0011. Each record identifies
the narrow ownership boundary, version source, license, transitive surface, security and update
policy, and rejected alternatives. Test-only dependencies remain governed by the reproducible
CMake dependency configuration.

- [OpenSSL](openssl.md) — SHA-256 provider for the in-memory ingest command layer.
