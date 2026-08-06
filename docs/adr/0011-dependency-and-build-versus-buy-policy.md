# ADR 0011: Dependency and Build-Versus-Buy Policy

- **Status:** accepted
- **Date:** 2026-08-01
- **Owners:** ChronosDB maintainers

## Context

External libraries can reduce risk for commodity functionality, but they can also quietly replace the storage, query, concurrency, or distributed systems work that defines ChronosDB. A dependency policy must distinguish leverage from architectural substitution and account for licenses, supply-chain maintenance, compatibility, and reproducibility.

## Accepted decision

The following components must be designed, implemented, validated, and maintained by ChronosDB rather than delegated to an existing database or execution engine:

- WAL framing, segmented persistence, and recovery;
- the CSEG format and codecs;
- manifests, flush, checkpoints, and installation recovery;
- mutable columnar heads;
- compaction and row-version resolution;
- sparse indexing and pruning metadata;
- SQL tokenizer/parser, binder, logical/physical planning, and optimizer;
- scalar and vectorized execution;
- concurrency queues used by the core ownership design;
- reactor abstraction and native protocol framing/state machine;
- historical-to-live and incremental live-query state management;
- deterministic Raft state machine;
- Multi-Raft runtime and multiplexed-log coordination; and
- distributed query coordination.

Carefully selected external libraries may be used for commodity or interoperability concerns, including:

- test and benchmark frameworks;
- general-purpose compression such as LZ4 or Zstandard;
- TLS and cryptography;
- LLVM if JIT is later justified;
- `liburing` for an optional `io_uring` backend;
- object-store HTTP transport; and
- Arrow and Parquet interoperability.

Every production dependency requires pinned or reproducibly resolved versions, license compatibility, a documented rationale and ownership boundary, maintenance/security review, and an upgrade policy. An ADR is required when the dependency affects a public/durable contract, core architecture, security boundary, or is expensive to replace. No dependency may be added merely to avoid implementing the subsystem that demonstrates the project's core engineering value.

Test references such as DuckDB or PostgreSQL may validate a supported semantic intersection but never execute production queries or define ChronosDB-specific semantics.

## Detailed rationale

Owning the core components is necessary to reason end-to-end about acknowledgment, recovery, snapshots, temporal versions, pruning, live handoff, and consensus application. A generic library boundary cannot compensate for semantics hidden inside an embedded engine.

Conversely, building cryptography, TLS, general-purpose compressors, test harnesses, or HTTP stacks would create avoidable security and maintenance risk without advancing ChronosDB's differentiating contracts. Explicit classification keeps “custom engine” from becoming “build every utility.”

## Alternatives considered

- **No external dependencies:** maximizes control but needlessly recreates security-sensitive and commodity functionality and increases audit burden.
- **Use best-of-breed libraries for every subsystem:** accelerates assembly but turns the project into integration glue and fragments core semantics across opaque engines.
- **Embed RocksDB and build columnar/query layers above it:** outsources WAL, recovery, compaction, and manifests, contradicting the accepted storage architecture.
- **Embed DuckDB for execution:** provides SQL breadth but removes ownership of planning, vectorization, storage pushdown, and live/system-time semantics.
- **Use an existing Raft implementation:** may shorten distribution work but prevents the required deterministic state-machine/runtime learning and tight validation of multiplexed log behavior.
- **Unpinned rolling dependencies:** reduce update effort initially but make builds, fixtures, benchmarks, and security response irreproducible.

## Consequences

- Core milestones require more engineering time and dedicated learning documentation.
- The dependency graph should remain comparatively small and boundaries explicit.
- Selected libraries need license/SBOM tracking, update ownership, and compatibility tests.
- Interoperability libraries do not make their format the ChronosDB primary store.
- A useful library can still be rejected if it imposes hidden global state, exceptions/ABI constraints, unsafe parsing, or an incompatible license.

## Affected invariants

The policy supports invariants [1–18](../architecture/invariants.md) indirectly by keeping ownership of invariant-critical code explicit. It is especially material to invariants 10, 14, and 18: integrity/version contracts cannot be outsourced opaquely, and performance convenience cannot weaken semantics. External crypto/TLS is chosen to reduce rather than expand correctness and security risk.

## Validation plan

- Require a dependency record with purpose, version source, license, owner, transitive graph, security/update policy, and rejected alternatives.
- Reproduce dependency resolution from a clean environment and archive format/protocol fixtures independent of library internals.
- Test library error, corruption, cancellation, and upgrade behavior at the ChronosDB boundary.
- Audit the production link/runtime graph to ensure testing/reference engines are absent.
- Review each major upgrade for output compatibility, performance changes, and newly introduced transitive code.

## Deferred decisions

Build system, package-resolution mechanism, initial test/benchmark frameworks, checksum/compression/TLS libraries, dependency pin-file format, vulnerability scanning, SBOM tooling, and stable third-party ABI policy remain deferred to Phase 1 or the consuming subsystem.

## Migration or reversal implications

OpenSSL 3 is the first production dependency, narrowly used through EVP as the maintained SHA-256
provider for `chronos_ingest`; its required dependency record is under `docs/dependencies/`.
Zstandard 1.5.5 or newer is the second, narrowly used as the maintained CSEG v1 page-compression
provider under [ADR 0016](0016-cseg-v1-layout-integrity-and-compression.md); its dependency record
is maintained beside OpenSSL's. Permitted libraries otherwise remain options, not commitments.
Replacing a commodity dependency requires compatibility tests and, when it affects stored/wire
bytes, versioned migration.
Reclassifying a core subsystem for external implementation reverses this policy and requires a
superseding ADR.

## References

- [Agent scope discipline](../../AGENTS.md)
- [Architecture non-goals](../architecture/non-goals.md)
- [Roadmap](../roadmap.md)
- [Custom SQL ADR](0008-custom-sql-and-vectorized-execution.md)
