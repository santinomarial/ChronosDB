# Raft Simulator Rate Harness

The `chronos_raft_benchmarks` target measures deterministic simulator action throughput in explicit
three- and five-voter topologies. It is a local microbenchmark harness, not a published baseline,
service-level result, regression threshold, or claim that the Phase 14 measurement exit gate is
complete.

## Workloads

Both cases use seed `20260821`, 1,024 actions per benchmark iteration, the default bounded virtual
network and Raft limits, and an all-voter topology with node IDs beginning at one.

- `seeded_simulation` times seeded action selection, action execution, and the safety oracle.
- `replay_simulation` first generates one fixed trace outside timing, then times action copying,
  execution, and the same safety oracle.

Simulator construction and destruction are excluded so the reported item rate isolates executed
simulation actions. Each iteration checks the returned status and exact retained action count;
invalid or incomplete work is reported as a benchmark error. Counters expose the seed, node count,
actions, and safety checks per iteration. This harness does not measure production network, disk,
fsync, commit, catch-up, or snapshot-transfer performance.

## Local invocation

```sh
cmake --preset benchmark
cmake --build --preset benchmark --target chronos_raft_benchmarks -j 4
build/benchmark/chronos_raft_benchmarks \
  --benchmark_min_time=0.02s \
  --benchmark_repetitions=3 \
  --benchmark_report_aggregates_only=false
```

Google Benchmark reports closed-loop CPU and wall time plus actions per second. A publishable result
still requires the complete source, environment, procedure, raw samples, and manifest fields in the
[benchmark publication contract](benchmark-contract.md). Repetitions from an unisolated developer
host are smoke evidence only and must not establish a performance threshold.
