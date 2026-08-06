# Zstandard

## Purpose and boundary

`chronos_cseg` uses the maintained Zstandard library solely for the general-purpose compression
stage assigned code 2 by CSEG v1. ChronosDB owns page boundaries, canonical settings, fallback to
raw storage, checksums, frame-property requirements, resource limits, durable codes, and error
classification. No Zstandard type appears in a ChronosDB public header.

## Version source and compatibility

CMake requires a system `zstd` package configuration at version 1.5.5 or newer and below 2.0, and
links its `zstd::libzstd` target. Ubuntu 24.04 supplies the supported baseline through `libzstd-dev`; macOS CI
uses Homebrew `zstd`. Installed static ChronosDB consumers repeat the configuration lookup. CSEG
fixtures, not a particular compressed byte sequence from a future provider release, define decoded
compatibility; new writes remain deterministic for one provider version and canonical settings.

## License and transitive graph

Zstandard is dual-licensed BSD-3-Clause and GPL-2.0; ChronosDB uses it under BSD-3-Clause. The
selected library target may use the provider package's thread dependency, although CSEG v1 forces
single-threaded compression. It introduces no database, storage engine, or format implementation.

## Ownership, security, and updates

The CSEG owner maintains the wrapper and validates content size, dictionary absence, frame checksum,
window size, exact single-frame consumption, output size, and provider errors. Distribution package
channels supply security updates. An upgrade must run raw/Zstandard fixtures, malformed and
decompression-limit tests, the full part codec/fuzzer once present, sanitizers, package-consumer
tests, and compression benchmarks. Changes to experimental frame-header inspection require focused
source/ABI review because that provider API is privately wrapped rather than exposed.

## Rejected alternatives

- A custom compressor violates the project's explicit non-goal and expands compatibility and
  security maintenance.
- Vendoring Zstandard would duplicate supported-platform update ownership and enlarge the source
  tree; reproducible system package requirements are sufficient for the current supported matrix.
- Omitting frame-property validation would allow dictionaries, missing checksums, oversized windows,
  or concatenated frames to bypass the frozen CSEG v1 contract.
- Always retaining compressed output was rejected because canonical v1 stores raw bytes when the
  complete frame is not smaller.
