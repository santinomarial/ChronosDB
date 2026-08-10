# ADR 0109: Runtime-dispatched timestamp filter kernel

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB query maintainers

## Context

Phase 12 calls for SIMD-ready vector kernels only where integration is bounded and obvious. The
exact `TIMESTAMP_NS` range filter is such a boundary: an identity selection over a validated,
non-null fixed-width column is a contiguous array of canonical signed 64-bit values, and the output
is the already-owned index vector. The generic per-cell path remains necessary for sparse selections
and nullable columns. BOOL is not a suitable byte kernel because ChronosDB stores it as a packed
bitmap.

Any acceleration must preserve signed comparisons, inclusive/exclusive endpoints, unbounded and
empty ranges, `INT64_MIN`/`INT64_MAX`, stable row order, and the scalar fallback on every supported
host. CSEG/query bytes remain canonical little-endian and cannot be reinterpreted on a big-endian
host without conversion.

## Accepted decision

Add a private `timestamp_filter_kernel` boundary with scalar, AVX2, and ARM NEON implementations.
`VectorSelection::where_timestamp_in_range` selects it only when:

- the selection is the complete identity domain;
- the validated timestamp column has zero NULLs; and
- the predicate is not already known empty.

All other shapes retain the existing cell-validated scalar path. The kernel stable-compacts the
existing index allocation and performs no allocation. Its scalar implementation decodes canonical
little-endian bytes explicitly. Accelerated implementations are available only on little-endian
hosts: AVX2 uses a compiler target function plus runtime CPU-feature detection, and AArch64 NEON uses
the baseline architecture feature. An explicitly requested unavailable kernel falls back to scalar.

AVX2 processes four signed timestamps at a time; NEON processes two. Lower and upper comparisons are
constructed directly from greater/less and equality-inclusive vector predicates—endpoints are never
incremented or decremented. Lane masks are emitted in ascending lane order, preserving the selection
ordering invariant. Scalar tails use `TimestampRangePredicate::matches`.

This is a semantic kernel architecture and a bounded implementation, not evidence of a speedup.
Runtime dispatch chooses an available specialization, but epoll/io_uring and SIMD benchmarking,
threshold tuning, AVX-512, and additional expression kernels remain deferred.

## Consequences

- Dense, non-null timestamp filters have one clean scalar/AVX2/NEON dispatch point.
- Sparse and nullable filtering remain on the auditable generic path; no gather or validity-mask
  complexity is introduced speculatively.
- No public API, durable format, network format, selection allocation, or query memory accounting
  changes.
- Big-endian and unsupported CPU configurations always retain exact scalar decoding.

## Alternatives considered

- **Byte-wise BOOL SIMD:** incorrect for the canonical packed bitmap representation.
- **Vectorize the full expression interpreter:** too broad without profiles and would couple many
  SQL types and failure modes to architecture-specific code.
- **Require global `-mavx2`:** would make the binary illegal on older x86 CPUs and remove the portable
  fallback.
- **Adjust exclusive endpoints:** overflows at the signed domain limits and changes frozen semantics.
- **SIMD sparse gathers and nullable masks now:** substantially more complexity without current
  evidence.

## Affected invariants

Invariants 6 and 18 apply: snapshot rows and exact predicate truth cannot change with CPU features,
and acceleration cannot weaken semantics. No concurrency or memory-ordering rule is introduced; the
kernel is synchronous and mutates only its caller-owned selection prefix.

## Validation

Differential tests force scalar and every available specialization across vector/tail boundaries,
open/closed/unbounded/equal/reversed ranges, generated signed values, and both domain extrema. They
also verify unavailable forced kernels fall back to scalar and exercise the integrated identity
selection. The AArch64 NEON path passed on the development host; the AVX2 source passed an x86_64
warnings-as-errors compile-only check. x86 runtime differential testing, AVX-512, sanitizer/cross-
compiler matrices, and comparative benchmarks remain in the Phase 18 ledger.
