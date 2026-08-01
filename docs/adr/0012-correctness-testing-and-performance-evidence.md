# ADR 0012: Correctness Testing and Performance Evidence

- **Status:** accepted
- **Date:** 2026-08-01
- **Owners:** ChronosDB maintainers and subsystem reviewers

## Context

Storage and distributed failures emerge from byte corruption, crash boundaries, retries, reordering, and rare concurrency schedules that ordinary unit examples do not cover. Analytical optimizations can return plausible but wrong results. Performance numbers are likewise easy to make irreproducible by omitting hardware, durability, data skew, build mode, or raw observations.

ChronosDB needs an evidence policy that treats correctness and performance as linked but never interchangeable.

## Accepted decision

Unit tests are necessary but insufficient for storage, query, concurrency, and distributed correctness.

- Codecs receive round-trip and generative property tests across valid values and boundary cases.
- Durable and network parsers receive coverage-guided fuzzing with hostile lengths, truncation, corruption, unknown versions, and structured corpus fixtures.
- WAL, manifest, part installation, flush, compaction, checkpoint, and reclamation operations receive failpoint-based crash-consistency tests at every durable state transition.
- Query execution receives differential and metamorphic testing against the scalar reference engine and, for a compatible SQL intersection, external reference engines. The vectorized engine is continuously compared with the scalar path across varied chunk and plan boundaries.
- Raft is tested first in deterministic simulation using virtual time, controlled disks, controlled networks, reproducible seeds, and shrinkable traces before production integration.
- AddressSanitizer and UndefinedBehaviorSanitizer are part of regular development. ThreadSanitizer covers concurrency code where feasible; exceptions and platform limitations are recorded rather than represented as passing.

Benchmarks must record exact hardware, CPU configuration where material, kernel and filesystem, compiler, build mode/options, configuration, workload/dataset generator and seed, batch size, query concurrency, durability mode, run duration/warmup/repetitions, and Git commit. Distributed runs additionally record topology, replica count, consistency mode, and fault conditions.

Performance claims require reproducible commands and stored raw results. No benchmark result may appear in README prose without an accompanying result artifact that provides the run manifest and raw observations. Correctness checks, checksums, or durability may not be silently disabled for a favorable number; deliberately weaker modes are labeled and compared only under equivalent contracts.

Microbenchmarks isolate mechanisms such as codecs, queues, checksums, or operators. Mixed-workload benchmarks exercise ingest, flush/compaction, historical scans, and live subscribers together. Both are required where a claim crosses subsystem boundaries.

Statistical reporting includes distributions or relevant percentiles, sample count, repetitions, variance/noise treatment, and resource consumption. Regression thresholds are defined from repeated baseline variance and practical impact, not selected after seeing one result. A threshold breach triggers investigation; noise is neither automatically a regression nor an excuse to discard inconvenient runs.

## Detailed rationale

Property testing explores combinatorial value spaces, fuzzing hardens untrusted byte boundaries, and crash testing verifies ordering that cannot be inferred from successful runs. Deterministic simulation turns distributed races into replayable traces and makes virtual fault schedules routine. Differential execution separates semantic truth from optimized implementation.

Complete run manifests make performance changes attributable and prevent comparing `ASYNC` with synchronized durability or a hot cache with a cold one. Raw results support later reanalysis and expose variation hidden by a headline number.

## Alternatives considered

- **Unit tests only:** provide fast local feedback but miss large input spaces, torn state, and adversarial schedules.
- **End-to-end tests only:** exercise integration but make failures difficult to localize and leave codec/operator state spaces sparse.
- **Random distributed tests using wall time:** can find issues, but failures are hard to reproduce and scheduler/network control is weak.
- **Formal verification alone:** could prove selected algorithms but does not cover filesystem, compiler, integration, and operational contracts; it may complement executable evidence.
- **Publish only best throughput:** obscures tails, variance, correctness settings, and resource tradeoffs.
- **Fixed universal regression percentage:** is simple but ignores benchmark noise and subsystem/user impact.

## Consequences

- Subsystems need test seams, failpoints, reference models, corpus management, deterministic clocks, and injectable storage/network interfaces.
- CI and scheduled testing will have multiple cost tiers; not every fuzz, sanitizer, crash, simulation, and benchmark campaign runs on each edit.
- Result artifacts and run manifests are versioned engineering outputs, not prose claims.
- Performance work may wait for noise reduction or correctness parity before acceptance.
- Unsupported sanitizer/platform combinations are explicitly reported.

## Affected invariants

This ADR supplies the evidence strategy for all [18 architecture invariants](../architecture/invariants.md). It is the enforcement mechanism for invariant 18 and maps directly to the eventual-test obligation recorded under every invariant.

## Validation plan

- Each subsystem specification maps its invariant obligations to named automated suites and negative cases.
- Phase reviews include executed commands, artifacts, failures/waivers, and scope-diff inspection; compilation cannot satisfy a gate.
- Seeded property/fuzz/simulation failures retain minimal reproducer inputs or traces.
- Crash suites prove both old/new complete-state recovery and repeat recovery to demonstrate idempotence.
- Benchmark harnesses validate required metadata fields and retain raw samples; independent clean reruns must reproduce conclusions within declared noise bounds.
- Optimization reviews show reference/optimized correctness parity before considering performance evidence.

## Deferred decisions

Specific test, fuzz, benchmark, and simulation frameworks; corpus storage; CI tier cadence; coverage targets; failpoint API; raw-result schema/location; statistical method; regression thresholds; reference-engine versions; and hardware fleet remain deferred to Phase 1 and subsystem ADRs.

## Migration or reversal implications

There are no existing tests or results to migrate. Future result schemas and corpora require versioning so prior evidence remains interpretable. Weakening the required evidence or publishing unbacked README numbers requires a superseding ADR; strengthening test coverage does not.

## References

- [Engineering verification rules](../../AGENTS.md)
- [Architecture invariants](../architecture/invariants.md)
- [Product benchmark policy](../product/vision.md)
- [Roadmap phase gates](../roadmap.md)
