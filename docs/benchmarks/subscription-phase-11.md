# Phase 11 Subscription Microbenchmarks

This optional Release-mode suite supplies reproducible microbenchmark cases for the Phase 11
measurement gate. It publishes no achieved number, regression threshold, production capacity, or
end-to-end service claim. Any published run must follow the
[benchmark publication contract](benchmark-contract.md) and retain its raw output and environment.

## Build and run

```sh
cmake --preset benchmark
cmake --build build/benchmark --target chronos_live_benchmarks -j2
build/benchmark/chronos_live_benchmarks \
  --benchmark_min_time=0.02s \
  --benchmark_repetitions=3
```

The cases are deterministic and use fixed nonzero database, table, tablet, schema, WAL, plan, MAC,
and subscription identities. No source data generator or random seed is involved.

## Measured boundaries

- `subscription_handoff` measures registration, authenticated initial token encoding, one committed
  change admitted while the historical snapshot is open, snapshot completion, and the exact first
  live poll. Manager construction and destruction are excluded.
- `subscription_publish_fanout/{1,8,64}` measures one already-committed logical change copied into
  bounded live subscriber buffers. Subscriber registration and destruction are excluded.
- `subscription_slow_consumer_overflow/{1,8,64}` begins with full one-record subscriber buffers and
  measures the next committed change forcing every stalled subscriber to its bounded overflow
  state. The source publish must still succeed.
- `subscription_resume_replay/{1,128,4096}` measures authenticated token decoding, retained suffix
  reconstruction, refreshed token encoding, and a bounded poll of the complete suffix. Retained
  source creation and between-iteration cancellation are excluded.
- `incremental_correction_update/{64,4096}` measures replacement of one aggregate row while
  alternating an in-order and accepted late event-time position, including snapshot materialization.
- `incremental_checkpoint_restore/{64,4096}` measures an exact logical aggregate checkpoint copy and
  complete reconstruction of row and ordered aggregate indexes.

Google Benchmark reports aggregate closed-loop wall/CPU time and calibrated throughput counters.
It does not provide request latency percentiles, RSS, allocator-retained bytes, CPU utilization,
ingest/storage I/O, durability cost, remote networking, or arrival-rate correction. Setup excluded
from timing still consumes host resources and can affect caches. The suite covers bounded logical
owners, not Protocol 1.1 sockets, historical query execution, durable filesystem checkpoint
installation, Raft replication, or dynamic topology.

## Interpretation limits

Fan-out item throughput counts subscriber buffer admissions, not source commits. Resume throughput
counts reconstructed retained records. Aggregate restore throughput counts restored rows. Slow
consumer cases intentionally exercise overload containment; they are not successful delivery
throughput. Results from Debug or sanitizer builds are diagnostic only and cannot be compared with
Release measurements.

The suite materially covers handoff delay, live update/fan-out scaling, retained replay cost,
bounded slow-consumer impact, late correction cost, aggregate state size scaling, and logical
recovery cost. It does not complete the Phase 11 measurement gate: durable checkpoint installation,
socket delivery distributions, concurrent ingest impact, and a publication-contract-complete clean
baseline remain open.
