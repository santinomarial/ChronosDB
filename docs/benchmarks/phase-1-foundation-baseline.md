# Phase 1 Foundation Build and Harness Baseline

This is the first reproducible local record for the narrow Phase 1 measurement gate: one common
benchmark target's clean build time, selected artifact file sizes, and the benchmark framework's
iteration-plus-optimization-barrier proxy. It is not a database-performance result, regression
budget, release-build-size claim, cross-toolchain comparison, or value to subtract from other
microbenchmarks.

## Source and environment

- ChronosDB commit: `fe9d2b4b363ea937ea93f40d1e4a0c79ebc5294b`, with a clean worktree before,
  during, and after measurement.
- Run time: 2026-08-15 22:31 UTC.
- Host: Apple M4, 10 physical and 10 logical CPUs, 17,179,869,184 bytes of memory.
- Operating system: macOS 26.6.1 build 25G76; Darwin 25.6.0 arm64.
- Compiler: Apple clang 21.0.0, target `arm64-apple-darwin25.6.0`, Apple libc++.
- Build tools: CMake 4.2.3 and Ninja 1.13.2; Google Benchmark 1.8.3.
- Build configuration: Release, `-O3 -DNDEBUG`, C++23, static ChronosDB and Google Benchmark
  archives, two compile jobs, no tests, sanitizers, static analysis, Arrow interoperability, or
  `io_uring`. The benchmark preset did not enable warnings-as-errors for this run.

The host was not isolated, pinned, or tuned. There was ordinary concurrent desktop load. CPU
frequency, turbo/power state, cache state, memory topology, process affinity, and temperature were
not controlled. Google Benchmark could not read `hw.cpufrequency`, printed an invalid 24 MHz
estimate, and could not set thread affinity; that frequency metadata is ignored. This local Apple
arm64 observation does not supply the Linux x86-64 reference-platform evidence.

## Clean target build

Each repetition ran sequentially from the same already-configured benchmark tree:

```sh
/usr/bin/time -p cmake --build build/benchmark --clean-first \
  --target chronos_common_benchmarks -j2
```

`--clean-first` removed all compiled artifacts in that tree before every repetition. Configuration,
dependency download, and FetchContent source checkout were excluded. Each timed target build
compiled 19 Google Benchmark translation units, eight `chronos_common` translation units, two
ChronosDB benchmark translation units, `benchmark_main`, and their static archives/executable—35
reported Ninja actions. Operating-system filesystem caches were neither flushed nor preconditioned.

| Repetition | Real seconds | User seconds | System seconds |
| ---: | ---: | ---: | ---: |
| 1 | 4.58 | 7.86 | 0.98 |
| 2 | 4.57 | 7.98 | 0.95 |
| 3 | 4.59 | 8.04 | 0.95 |

The median real time is 4.58 seconds and the observed real-time range is 0.02 seconds. Three runs on
one unisolated host are a reproducibility record, not a statistically justified regression
threshold. Parallel user CPU time can exceed real time because the command used two compile jobs.

## Artifact file sizes

After repetition 3, macOS `stat -f '%N %z bytes'` reported logical file length:

| Artifact | Bytes | Scope |
| --- | ---: | --- |
| `build/benchmark/libchronos_common.a` | 75,096 | Static common-library archive |
| `build/benchmark/chronos_common_benchmarks` | 356,856 | arm64 Mach-O benchmark executable |
| `build/benchmark/_deps/google_benchmark-build/src/libbenchmark.a` | 589,824 | Static benchmark framework archive |
| `build/benchmark/_deps/google_benchmark-build/src/libbenchmark_main.a` | 1,920 | Static benchmark entry-point archive |

These are filesystem lengths, not text/data/RSS, installed-package size, stripped distribution
size, or a sum that should be interpreted as runtime memory. Static archive members may not all be
linked into the executable.

## Harness-iteration proxy

The exact invocation was:

```sh
build/benchmark/chronos_common_benchmarks \
  --benchmark_filter='^benchmark_harness_iteration$' \
  --benchmark_min_time=0.1s \
  --benchmark_repetitions=10 \
  --benchmark_report_aggregates_only=false
```

There was no separate warm-up or cooldown and no outlier removal. Google Benchmark selected
616,115,830 iterations for each repetition. The run began at load averages 2.73/3.20/3.46. Raw
reported wall/CPU nanoseconds per iteration were:

| Repetition | Wall ns | CPU ns |
| ---: | ---: | ---: |
| 1 | 0.227 | 0.227 |
| 2 | 0.227 | 0.227 |
| 3 | 0.227 | 0.227 |
| 4 | 0.226 | 0.226 |
| 5 | 0.226 | 0.226 |
| 6 | 0.227 | 0.227 |
| 7 | 0.226 | 0.226 |
| 8 | 0.226 | 0.226 |
| 9 | 0.226 | 0.226 |
| 10 | 0.227 | 0.227 |

Google Benchmark reported 0.226 ns mean/median at the displayed precision and 0.15% coefficient of
variation. The case contains only the framework range-for iteration and `DoNotOptimize` barrier.
It is a lower-bound proxy affected by compiler/framework implementation and display rounding; it is
not an estimate of timer calls, benchmark calibration, fixture setup, reporting, or executable
startup, and it must not be subtracted from another case.

## Applicability

No table, row, durable write, query, socket, storage device, dataset, random seed, cache policy,
replication mode, consistency mode, or durability mode participates in this baseline. Those
database-workload fields are inapplicable. No correctness failure occurred: every build returned
zero and the benchmark completed all ten requested repetitions. The broader Phase 1 correctness,
compiler/platform, utility-surface, and production-qualification gates remain governed by the
[roadmap](../roadmap.md); this document closes only the named local measurement artifact.
