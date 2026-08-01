# ADR 0002: Language, Platform, and Portability

- **Status:** accepted
- **Date:** 2026-08-01
- **Owners:** ChronosDB core-platform maintainers

## Context

ChronosDB requires precise memory ownership, predictable data layout, direct access to Linux storage and networking facilities, portable durable codecs, and room for architecture-specific acceleration. Development may also occur on Apple Silicon, so “Linux-first” cannot mean that portable storage and query logic silently accumulates Linux or x86 assumptions.

Expected operational failures such as malformed input, I/O failure, cancellation, or an unavailable resource are normal control flow at subsystem boundaries. Their representation must not depend on whether a build globally enables or disables language runtime features.

## Accepted decision

C++23 is the implementation language for core engine and server components. The project requires a compiler and standard library with the C++23 language/library capabilities actually used by the codebase, supported sanitizer/tooling integration, and correct x86-64 and ARM64 code generation; this ADR does not pin a compiler minor release.

Linux is the production and performance reference platform, and x86-64 is the initial performance reference architecture. Storage formats, the SQL front end, the columnar engine, and common libraries should remain buildable and correct on modern macOS where practical. ARM64 correctness support should be maintained where practical, particularly for Apple Silicon development; performance parity is not implied.

Linux-specific networking and I/O are isolated behind interfaces. `epoll`, `io_uring`, file-descriptor, and other Linux-native types must not leak into portable core interfaces.

Portable scalar implementations precede architecture-specific SIMD or accelerated CRC paths. Optimized variants must preserve identical results and have runtime or build-time dispatch with a correct portable fallback.

Durable and network formats use fixed-width integers, explicit byte order, and field-by-field encoding. C++ object representations, padding, vtables, and native struct layouts are never serialized directly.

Exceptions and RTTI are not globally disabled. Public subsystem interfaces use explicit status/result values for expected operational failures. Exceptions may remain available for language/library behavior and narrowly documented boundaries; exception policy below that level is a later coding specification.

## Detailed rationale

C++ provides control over allocation, layout, atomics, SIMD, syscalls, and zero-copy interfaces while retaining mature compilers, profilers, sanitizers, and libraries. C++23 gives the project a modern baseline without requiring every novelty to be used. Separating portable semantics from Linux backends allows macOS development and deterministic tests to exercise substantial engine logic without pretending macOS is a performance-reference server platform.

Scalar-first implementation creates a correctness oracle for optimized paths and avoids making a CPU feature part of a durable contract. Explicit results keep frequent failures visible in function signatures and avoid cross-module exception ambiguity.

## Alternatives considered

- **Rust:** offers strong memory-safety properties and capable systems tooling, but changing the mandated language would redirect the project's C++ systems-learning and implementation goals and does not remove the need to design durable/concurrent semantics.
- **Java:** provides portability and mature tooling, but managed layout, garbage collection, and JNI for Linux-specific paths conflict with the intended ownership and low-level storage work.
- **Go:** simplifies concurrency and deployment, but garbage collection and less direct control over layout/SIMD do not fit the planned core engine.
- **C++20:** is workable but would freeze the baseline below the chosen C++23 project contract without a concrete compatibility need.
- **Linux-only compilation for every module:** would allow accidental coupling between core logic and syscalls, reduce Apple Silicon development coverage, and make deterministic simulation harder.
- **Globally disabling exceptions and RTTI:** may reduce some binary/runtime concerns but imposes ecosystem and diagnostic costs. No measurements or complete boundary design justify making that irreversible choice now.

## Consequences

- Platform-specific code must live behind narrow interfaces and have test doubles or portable implementations where appropriate.
- Performance reports target Linux/x86-64 unless they explicitly name another platform.
- CI and local tooling should eventually cover Linux x86-64 and practical ARM64/macOS correctness configurations.
- SIMD and CRC acceleration add dispatch, cross-architecture fixtures, and differential-test obligations.
- Expected failures appear in explicit result types, increasing visible handling but improving auditability.

## Affected invariants

This decision supports invariants [10 and 14](../architecture/invariants.md) through endian-explicit, versioned, field-wise codecs; invariant 16 through auditable publication and ownership; and invariant 18 by requiring accelerated paths to preserve portable semantics.

## Validation plan

- Compile portable modules with at least the supported Linux toolchain configuration and a modern macOS configuration where practical.
- Run golden format fixtures on x86-64 and ARM64 and compare byte-for-byte encodings.
- Differentially test scalar and accelerated CRC/SIMD implementations, including forced fallback.
- Use sanitizers and warnings to detect alignment, lifetime, object-representation, and error-handling mistakes.
- Review public portable headers for Linux-native types.

## Deferred decisions

Minimum compiler versions, standard-library vendors, warning policy, exception use inside implementation code, symbol visibility, ABI stability, CPU dispatch mechanism, and the supported macOS version range remain Phase 1 decisions.

## Migration or reversal implications

There is no source or durable data to migrate. Changing the core language or globally disabling runtime features after public interfaces exist would require a superseding ADR and an explicit API/toolchain migration. Platform backends can be added without reversal if portable interfaces remain free of their native types.

## References

- [Agent C++ rules](../../AGENTS.md)
- [Architecture overview](../architecture/overview.md)
- [Roadmap phases 1 and 12](../roadmap.md)
