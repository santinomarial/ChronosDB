# Benchmark Publication Contract

> **Status: contract implemented by the focused WAL harness; no result is published here.** The
> [WAL benchmark harness](wal-benchmarks.md) emits reviewable local WAL measurement artifacts. The
> broader ChronosDB engine and ChronosBench suite remain unimplemented. This contract governs every
> measurement under [ADR 0012](../adr/0012-correctness-testing-and-performance-evidence.md).

## Required run manifest

Every published result artifact must contain the following machine-readable and human-readable fields. “Unknown” is allowed only with an explanation; omission is not.

### Source and invocation

- ChronosDB Git commit and whether the working tree was clean, including a diff artifact if dirty;
- exact benchmark command, environment variables, privilege/tuning steps, and scenario version;
- dataset generator name/version, exact command, and random seed;
- complete schema and data-model policy clauses;
- row count, byte count, cardinalities, skew/hot-key distribution, event-time range;
- out-of-order distribution and reorder horizon;
- duplicate, correction, and tombstone rates; and
- batch size distribution, not only its mean.

### Database workload state

- requested/effective durability mode and group-commit configuration;
- replication mode/factor and read consistency mode, or `single-node`;
- subscriber count and subscriber consumption-rate distribution;
- concurrent query workload and arrival/concurrency model;
- active flush/compaction state, backlog, and throttling;
- all ChronosDB configuration, including defaults expanded to concrete values; and
- cache state, preload, retention, history, lateness, and data placement.

### Hardware and software

- CPU model, sockets, physical core count, logical CPU policy, frequency/turbo/power settings when known;
- NUMA topology and process/memory affinity;
- memory capacity, channel/speed details when available, and huge-page/swap settings;
- storage model, count/topology, firmware when available, cache/power-loss-protection claims as reported by the device, mount options, and filesystem;
- network interface/link/topology for networked or distributed runs;
- operating system and kernel;
- compiler and exact version, standard library and version;
- build type, assertions/sanitizers/instrumentation, link mode, and relevant compiler/linker flags; and
- container, hypervisor, cloud instance, or orchestration details.

### Experimental procedure

- warm-up policy and why it is sufficient;
- measured interval or operation count;
- run count, run ordering/randomization, cooldown, and outlier policy fixed before observing results;
- raw per-operation or interval samples at sufficient resolution;
- percentile calculation method, interpolation convention, units, and sample population; and
- known errors, retries, rejected operations, data loss, and incomplete runs.

## Required reported metrics

Report counts and distributions where meaningful, not only averages:

- latency p50, p95, p99, and p99.9 when the sample size supports a meaningful p99.9;
- throughput in operations, logical rows, and logical bytes per second as applicable;
- CPU utilization and CPU time, including user/system split when available;
- peak and steady-state RSS;
- disk bytes read/written and I/O operations;
- network bytes sent/received;
- compression ratio with exact numerator/denominator definitions;
- write amplification, read amplification, and space amplification with formulas and observation interval;
- recovery time for restart/recovery scenarios;
- failover time for future replicated scenarios; and
- acknowledged-write loss count, including the method used to reconcile sent, acknowledged, committed, and recovered identities.

Errors, timeouts, overload rejections, retries, compaction debt, and result-correctness failures accompany performance metrics. A run with incorrect output is a correctness failure, not a fast result.

## Comparison rules

- Target numbers, budgets, and regression thresholds are not achieved results and must be labeled as targets.
- `ASYNC` results cannot be presented as equivalent to `LOCAL_SYNC` or `QUORUM_SYNC`; modes appear in chart titles, legends, tables, and prose.
- Debug, assertion-heavy, coverage, and sanitizer builds cannot be compared with optimized release builds without explicit disclosure and are never used to claim release performance.
- Containerized, Docker, virtualized, and cloud results are labeled with their isolation/topology limitations.
- One unusually good run cannot represent the benchmark. Publish every valid repetition and the predeclared aggregation method.
- Comparisons use the same schema, generated data, correctness checks, query results, cache state, compaction state, durability, replication, and resource limits unless the changed factor is the subject of the comparison.
- A system that omits checksums, history, synchronization, corrections, or result validation is not equivalent; the difference must be central to the report.
- Benchmark code, generator version, scenario manifest, raw stdout/stderr, raw samples, system inventory, and analysis scripts are retained with the result artifact.

## Statistical and regression policy

The report distinguishes service-time latency from coordinated-omission-corrected end-to-end latency and states the arrival model. Percentiles are calculated over the declared population, never averaged across percentiles from unequal runs. Confidence intervals or repeated-run dispersion accompany comparative claims.

Regression thresholds are set from stable baseline variance and user impact before candidate results are reviewed. A breach triggers investigation and repeat measurement; it is not automatically waived as noise. Failed or interrupted runs remain visible in the artifact even when excluded under the predeclared policy.

## Review gate

README prose may cite a number only when it links to a complete result artifact satisfying this contract and a reviewer has verified workload equivalence and correctness. The [ChronosBench suite](chronosbench.md) defines planned scenarios; neither document contains achieved numbers.
