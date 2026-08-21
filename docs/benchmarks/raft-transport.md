# Raft Transport Codec Harness

The transport cases in `chronos_raft_benchmarks` measure canonical Raft envelope encoding and
checked owned decoding. They are local codec microbenchmarks, not a published baseline, network or
fsync result, regression threshold, or claim that the Phase 14 measurement exit gate is complete.

## Workloads

Every case uses one nonnil group, source node one, destination node two, term four, default codec
limits, and the complete v1 header, payload, payload CRC32C, header CRC32C, and frame CRC32C.

- A granted vote response represents a fixed-size control message.
- AppendEntries covers an empty heartbeat, one 128-byte entry, and 32 entries with 4,096 payload
  bytes each. Entries have consecutive indexes and deterministic payload bytes.
- InstallSnapshot metadata covers ascending three- and five-voter configurations. Application
  snapshot bytes remain outside this envelope and are not measured here.

Encode and decode are separate cases. Setup first encodes, decodes, and exact-compares every
workload outside timing. Each timed encode checks success and stable frame size; each timed decode
performs the complete bounded integrity and canonical-value validation and owns variable entry
payloads or voter lists. Counters expose exact frame bytes and the case-specific entry or voter
shape. TLS, sockets, routing, persistence, fsync, retransmission, and snapshot-byte transfer are not
part of these measurements.

## Local invocation

```sh
cmake --preset benchmark
cmake --build --preset benchmark --target chronos_raft_benchmarks -j 4
build/benchmark/chronos_raft_benchmarks \
  --benchmark_filter='^(vote_response|append_entries|snapshot_request)_(encode|decode)' \
  --benchmark_min_time=0.02s \
  --benchmark_repetitions=3 \
  --benchmark_report_aggregates_only=false
```

Google Benchmark reports closed-loop CPU and wall time, messages per second, and processed frame
bytes per second. This harness does not instrument allocation or resource consumption; deterministic
failure sweeps separately prove allocation behavior. A publishable result still requires the full
[benchmark publication contract](benchmark-contract.md), including a clean commit, host manifest,
raw repetitions, resource metrics, and a predeclared analysis policy.
