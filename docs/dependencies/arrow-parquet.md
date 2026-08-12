# Apache Arrow and Parquet

## Purpose and boundary

The optional `chronos_interop` library uses Apache Arrow C++ for Arrow IPC files and the maintained
Arrow/Parquet bridge for Parquet files. ChronosDB owns exact target-schema admission, logical-type
mapping, resource limits, canonical column validation, error classification, and atomic file
publication. No Arrow or Parquet type appears in a public ChronosDB header, and neither format is a
ChronosDB durable source of truth.

## Version source and compatibility

CMake accepts system package configurations from Apache Arrow and Parquet 25.x when
`CHRONOS_ENABLE_ARROW_INTEROP=ON`. The current macOS qualification uses Homebrew Apache Arrow
25.0.1. Installed consumers repeat both dependency lookups only when the installed ChronosDB build
contains `chronos::interop`. Major upgrades require fixture, mapping, security, and transitive-graph
review before the upper bound changes.

## License and transitive graph

Apache Arrow and Parquet use Apache License 2.0. The resolved binary may include compression,
Unicode, Thrift, TLS/HTTP, cloud SDK, allocator, and platform support libraries depending on the
distribution build. Shared Arrow and Parquet targets keep that graph outside ChronosDB's public C++
API, but deployment SBOMs must include the actually resolved runtime libraries.

## Ownership, security, and updates

The interoperability owner maintains mappings, input limits, exact schema checks, temporary-file
publication, and corruption/round-trip fixtures. Distribution channels supply security updates.
Advisories affecting Arrow IPC or Parquet parsing, decompression, buffer/offset validation,
filesystem output, or dependency loading require prompt review. Updates must run every-type IPC and
Parquet round trips, hostile/corrupt inputs, sanitizers, installed-consumer checks, and supported
third-party fixtures. Operators must impose a process memory ceiling for untrusted Parquet because
decoded data may exceed the compressed file limit.

## Rejected alternatives

- A local format implementation would duplicate a mature hostile-input parsing surface.
- Making Parquet the primary store would surrender CSEG's accepted integrity, temporal, and
  installation contracts.
- Unconditional linkage would impose a broad dependency graph on deployments that do not exchange
  these formats.
- Exposing third-party types publicly would make their ABI and lifetime contracts part of the
  ChronosDB API.
