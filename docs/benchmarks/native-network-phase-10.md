# Phase 10 Native Network Baseline

This is a reproducible microbenchmark baseline for the bounded native-protocol and Linux epoll
paths. It is not a service-level result, regression threshold, production capacity claim, or
comparison with another system. All three repetitions are retained, including visible host-noise
outliers.

## Source and environment

- ChronosDB commit: `f11da3fdd2efad8e1731f4e3d885ccb314bca710`, clean worktree at run start.
- Run time: 2026-08-08 06:08 UTC.
- Container: `ubuntu:24.04`, image
  `sha256:4fbb8e6a8395de5a7550b33509421a2bafbc0aab6c06ba2cef9ebffbc7092d90`.
- Kernel: LinuxKit 6.12.76, aarch64. Docker Desktop 29.6.1 allocated 10 logical CPUs and
  8,321,515,520 bytes of memory.
- Host: Apple arm64 model `Mac16,1`, 17,179,869,184 bytes of memory. LinuxKit does not expose the
  CPU model, physical-core topology, frequency controls, NUMA topology, memory channels, or host
  power policy. Google Benchmark's reported 48 MHz is therefore ignored.
- Compiler: Ubuntu GCC/G++ 13.3.0 with libstdc++; CMake 3.28.3; Ninja; C++23 Release build with
  `-O3 -DNDEBUG` and the project's warnings-as-errors flags. No sanitizer, LTO, profiling,
  coverage, CPU affinity, privilege change, or host tuning was used. Libraries are statically
  linked except normal system dependencies.
- The host was not isolated and had concurrent load. The run began at load averages
  0.41/0.42/0.34. Large variance in two codec cases below is retained and makes this baseline
  unsuitable for setting thresholds.

The exact build was:

```sh
cmake -S /src -B /tmp/chronos-network-benchmark -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCHRONOS_BUILD_TESTS=OFF \
  -DCHRONOS_BUILD_BENCHMARKS=ON \
  -DCHRONOS_WARNINGS_AS_ERRORS=ON
cmake --build /tmp/chronos-network-benchmark \
  --target chronos_network_benchmarks -j2
```

The executable invocation was:

```sh
/tmp/chronos-network-benchmark/chronos_network_benchmarks \
  --benchmark_filter='^(frame_round_trip/(64|4096|1048576)|query_result_round_trip/(16|4096)|fragmented_connection_receive/(4096/1|4096/64|65536/1500)|spsc_push_pop/64|spsc_saturation_cycle/(1|64|1024)|epoll_connection_churn|epoll_equal_connection_round/(1|8|32))$' \
  --benchmark_min_time=0.02s \
  --benchmark_repetitions=3
```

Google Benchmark calibrated an iteration count to at least 20 ms per repetition. Cases ran in the
listed deterministic order with no separate warm-up or cooldown and no outlier removal. The tables
retain each repetition's aggregate CPU time per benchmark iteration; the displayed median
throughput is Google Benchmark's corresponding median counter. Wall and CPU time were effectively
equal. These are closed-loop service-time microbenchmarks, so they do not provide arrival-rate
corrected latency percentiles.

## Portable codec, buffer, and queue results

Frame byte throughput counts the payload once for encode and once for checked decode. Query-result
throughput uses two `INT64` columns, one nullable with every eighth value NULL. Allocation counters
are regular current-thread `new`/`new[]` calls and requested bytes for one complete encode/decode;
they are not RSS or allocator-retained bytes.

| Case | Raw CPU ns/iteration | Median throughput | Allocations / requested bytes |
|---|---:|---:|---:|
| frame, 64-byte payload | 409, 360, 366 | 333.2 MiB/s | 2 / 168 B |
| frame, 4,096-byte payload | 17,528, 17,899, 16,909 | 445.7 MiB/s | 2 / 8,232 B |
| frame, 1,048,576-byte payload | 4,714,535, 4,634,479, 9,214,771 | 424.2 MiB/s | 2 / 2,097,192 B |
| result batch, 16 rows | 2,929, 1,906, 918 | 8.39 M rows/s | 3 / 1,247 B |
| result batch, 4,096 rows | 328,773, 184,699, 176,085 | 22.18 M rows/s | 3 / 290,927 B |

The declining first-repetition times and the 1 MiB third-repetition outlier demonstrate host and
frequency noise; no optimization conclusion is drawn from those cases. What is stable by
construction and observation is the finite allocation count: frame size changes requested bytes,
not allocation calls, and result row count likewise remains at three calls.

Fragmented receive includes bounded append, header validation, CRC, frame extraction, and inbound
compaction. Buffer construction is outside timing.

| Payload / fragment bytes | Fragments | Raw CPU ns/iteration | Median throughput |
|---|---:|---:|---:|
| 4,096 / 1 | 4,136 | 387,399, 390,881, 388,365 | 10.16 MiB/s |
| 4,096 / 64 | 65 | 14,750, 14,688, 14,946 | 267.4 MiB/s |
| 65,536 / 1,500 | 44 | 141,239, 139,840, 140,552 | 444.9 MiB/s |

The SPSC cases run producer and consumer operations on one benchmark thread to isolate ring cost;
the separate 100,000-item TSan test covers the two-thread memory-ordering contract. Push/pop has no
steady-state allocation. Every saturation cycle fills exactly to its declared capacity, observes
one rejected push, and drains every accepted task.

| Queue case | Raw CPU ns/iteration | Median accepted items/s | Observed overload |
|---|---:|---:|---:|
| push/pop, capacity 64 | 12.2, 12.0, 12.3 | 81.75 M | not saturated |
| saturation, capacity 1 | 12.1, 12.0, 12.4 | 82.68 M | 1 rejection/cycle |
| saturation, capacity 64 | 737, 737, 739 | 86.88 M | 1 rejection/cycle |
| saturation, capacity 1,024 | 12,048, 11,890, 11,957 | 85.64 M | 1 rejection/cycle |

## Linux loopback results

Connection churn times blocking loopback `connect`, bounded epoll admission, peer close, and
descriptor detach. Every repetition accepted and closed exactly 3,042 connections; no errors or
retries were excluded.

| Case | Raw CPU ns/connection | Median connections/s | Errors |
|---|---:|---:|---:|
| connect/admit/detach | 8,709, 8,954, 10,465 | 111,685 | 0 |

An equal-connection round sends one `SELECT 1` from every established client, transfers each owned
request through the request ring, returns a checked zero-row one-column result plus `QUERY_END`, and
ends only after every client has consumed its terminal frame. Setup and handshake are excluded.
Queue capacity is `2 * connections + 1`; event capacity is at least `2 * connections`. The test is
a fairness barrier—each client completes exactly one request per iteration—not a per-client tail
distribution.

| Connections | Queue capacity | Raw CPU ns/round | Median aggregate queries/s | Wire bytes/query |
|---:|---:|---:|---:|---:|
| 1 | 3 | 7,083, 7,224, 7,056 | 141,190 | 173.034 |
| 8 | 17 | 50,508, 51,218, 51,281 | 156,194 | 173.249 |
| 32 | 65 | 210,647, 214,208, 217,091 | 149,387 | 173.962 |

The wire counter includes handshake bytes amortized over completed requests, which explains its
small connection-count variation. A diagnostic run before commit `80c8ac1` exposed a roughly
40 ms terminal-frame delay caused by Nagle/delayed ACK interaction between separately owned result
and terminal writes. The accepted `TCP_NODELAY` decision removed it; that diagnostic was not a
clean-commit benchmark and is not presented as a comparative performance result.

## Applicability and omitted metrics

These cases have no table data, durability mode, replication, cache preload, flush, compaction,
storage, filesystem, remote network, subscriber, query-engine execution, random generator, or
random seed. Those benchmark-contract fields are inapplicable rather than silently defaulted.
Protocol payloads and query rows are deterministic. Default protocol and buffer ceilings remain
16 MiB inbound payload, 32 MiB outbound bytes, and 128 outbound frames; real loopback cases use the
benchmark's explicitly reported queue/event/connection bounds and default 16 I/O operations per
event.

The harness checks every result and reports failures instead of timing incomplete work. This run
had no errors, timeouts, dropped results, or unexpected overload. RSS, peak memory, CPU user/system
split, performance counters, p50/p95/p99/p99.9, and per-request raw latency samples were not
collected; claiming them from aggregate Google Benchmark iterations would be misleading. Phase 12
may establish isolated hardware, longer intervals, machine-readable artifacts, profiles, tail
distributions, and regression thresholds after baseline variance is known.
