# Phase 18 Local Hardening Audit

## Decision

The complete locally available build, static-analysis, Debug, Release, ASan/UBSan, and TSan
matrices pass at the reviewed revision. This closes one local validation slice of Phase 18; it does
not satisfy the Phase 18 exit gate and is not a production-readiness declaration.

The first full TSan run found a shutdown race in the test-only S3 HTTP server. The main thread
closed and rewrote the listener descriptor while the worker could still read it. Commit `03fc940`
made shutdown ownership explicit: signal stop, wake the socket, join the sole concurrent reader,
then close and invalidate the descriptor. The entire object-store TSan family and the complete TSan
suite subsequently passed.

## Reviewed revision and host

- **Revision:** `03fc940cd8e02d607b8a86b02ea66dc95bbc6de0`
  (`test: synchronize object store server shutdown`).
- **Run date:** 2026-08-15.
- **Host:** macOS 26.6.1 build 25G76, Darwin 25.6.0, arm64.
- **Build compiler:** Apple clang 21.0.0 through `/usr/bin/c++`.
- **Static-analysis tool:** Homebrew clang-tidy 18.1.8.
- **Build generator:** CMake 4.2.3.
- **Configurations:** Debug, Release (`-O3 -DNDEBUG`), Debug with ASan plus non-recovering UBSan,
  and isolated Debug with TSan.

This is a single Apple-silicon macOS observation. The Linux CI reference and the remaining platform
matrix are outside this audit.

## Executed checks

| Boundary | Command | Result |
| --- | --- | --- |
| Repository static analysis | `CLANG_TIDY=/opt/homebrew/opt/llvm@18/bin/clang-tidy scripts/lint.sh dev` | Passed; the configured lint build completed. |
| Debug build | `cmake --build build -j2` | Passed. |
| Debug suite | `ctest --test-dir build --output-on-failure -j2` | Passed 1,675/1,675 in 20.35 seconds. |
| Release build | `cmake --build build/release -j2` | Passed; the regenerated tree completed 958 build steps. |
| Release suite | `ctest --test-dir build/release --output-on-failure -j2` | Passed 1,675/1,675 in 9.01 seconds. |
| ASan/UBSan build | `cmake --build build/asan-ubsan -j2` | Passed. |
| ASan/UBSan suite | `ASAN_OPTIONS=detect_leaks=0 ctest --test-dir build/asan-ubsan --output-on-failure -j2` | Passed 1,676/1,676 in 86.07 seconds, including the non-recovering UBSan configuration assertion. |
| TSan build | `cmake --build build/tsan -j2` | Passed. |
| Focused TSan regression | `TSAN_OPTIONS=halt_on_error=1 ctest --test-dir build/tsan --output-on-failure -j2 -R '^(MemoryObjectStore\|S3)'` | Passed 25/25 in 26.22 seconds. |
| Complete TSan suite | `TSAN_OPTIONS=halt_on_error=1 ctest --test-dir build/tsan --output-on-failure -j2` | Passed 1,675/1,675 in 382.96 seconds. |

The ASan/UBSan matrix contains one configuration-only test that is absent from the other matrices,
which accounts for its 1,676-test total. Leak detection was disabled because this macOS sanitizer
runtime is not the project's Linux leak-detection reference. This run therefore provides no
LeakSanitizer evidence.

Elapsed times are execution records, not benchmark results. They were not collected under the
benchmark contract and must not be used for performance comparison.

## Evidence established

- Every test registered in the four local configurations completed successfully at one reviewed
  source revision.
- The Release build compiles and exercises the repository with assertions removed and optimization
  enabled, rather than relying only on the Debug configuration.
- The local ASan/UBSan suite completed without an address or undefined-behavior report under the
  stated options.
- The local TSan suite completed with fail-fast reporting enabled after repairing the race it first
  exposed.
- The configured repository clang-tidy build completed with the pinned local clang-tidy 18 tool.

## Evidence not established

- Linux, GCC, upstream Clang build coverage; x86-64 coverage; cross-compiler durable-byte golden
  comparison; or the complete supported platform matrix.
- LeakSanitizer coverage, because `detect_leaks=0` was required for this macOS run.
- Sustained fuzzing, exhaustive property campaigns, long-duration soak, chaos, packet-shaping,
  clock-change, disk-full, or physical power-loss qualification.
- Production multi-process and multi-node deployment, failover, rolling upgrade, mixed-version,
  DNS, certificate-rotation, or cloud-provider qualification.
- Security review, dependency vulnerability triage, release signing, provenance, packaging, or
  operator runbook validation.
- Performance, tail latency, throughput, allocation, CPU, I/O, recovery-time, failover-time, NUMA,
  epoll/io_uring, or scalar/SIMD evidence under the benchmark contract.
- Closure of the explicit Phase 11–17 feature and integration gaps in the roadmap and deferred
  validation ledger.

## Phase 18 disposition

Phase 18 remains in progress. This audit advances the local full-build, full-suite, static-analysis,
and sanitizer entries in the [deferred validation ledger](../development/deferred-validation.md).
The remaining exit evidence must be produced on its named reference platforms and workloads before
any production-readiness or release claim.
