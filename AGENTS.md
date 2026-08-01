# ChronosDB Agent Guide

ChronosDB is a pre-alpha, architecture-phase systems project. Planned capabilities are not implemented unless the repository and current documentation explicitly say otherwise. Detailed project documentation starts at [`docs/README.md`](docs/README.md).

## Required reading

Before modifying a subsystem, read:

- this file;
- [`docs/architecture/invariants.md`](docs/architecture/invariants.md);
- the relevant [ADRs](docs/adr/README.md);
- the relevant format or protocol specification; and
- the nearest subsystem documentation.

## Engineering priority

1. Correctness
2. Recoverability
3. Observability and debuggability
4. Performance
5. Feature breadth

## Scope discipline

- Implement only the requested phase; do not silently implement future roadmap phases.
- Do not create speculative abstractions without a current use.
- Do not add a production dependency without an ADR or an explicit existing dependency policy allowing it.
- Do not replace custom ChronosDB core subsystems with RocksDB, SQLite, DuckDB, DataFusion, an existing Raft implementation, or another hidden database engine.
- Do not leave fake implementations, placeholder success responses, empty TODO bodies, or commented-out alternatives.
- Do not fabricate benchmark numbers, test results, or platform support, and do not describe planned functionality as implemented.

## Correctness and C++ rules

- Version durable formats; checksum durable records and pages.
- Parse untrusted or corrupted bytes safely, without undefined behavior.
- Make recovery idempotent; never expose partially installed durable state.
- Document ownership, lifetime, thread affinity, and synchronization assumptions.
- Require a benchmark, profile, or other evidence for optimizations. State the memory-ordering argument for every concurrency algorithm.
- Name the applicable durability mode for every acknowledged-write guarantee.
- Use C++23, avoiding poorly supported novelty without concrete benefit. Prefer RAII, explicit ownership, `std::span`, and `std::byte`; avoid raw owning pointers, unsafe type punning, and unaligned `reinterpret_cast` loads.
- Keep public headers self-contained and implementation details private. Avoid per-row heap allocation on data paths.
- Do not disable exceptions or RTTI globally without an accepted ADR.
- Use fixed-width integers in durable and network formats. Never serialize native structs by dumping their object representation.

## Verification

Every implementation task must add or update tests; run relevant build and test commands, configured formatting and static analysis, and applicable sanitizers; report commands actually executed; distinguish passing checks from checks that could not run; and review the final diff for accidental scope expansion.

## Learning documentation

Every major subsystem must eventually have a document under `docs/learning/` explaining its purpose, public interfaces, data structures, invariants, ownership and lifetime, failure behavior, complexity, tradeoffs, benchmark methodology, and likely interview questions. Create such documents only when the relevant subsystem work calls for them.
