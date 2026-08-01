# Non-Goals

These boundaries protect the correctness and systems depth of the planned engine. “Deferred” means the capability requires an explicit later decision; it does not imply a commitment.

## General-purpose OLTP

ChronosDB targets append-heavy analytical event data, not small-row transactional applications with many point mutations, constraints, and short serializable transactions. Optimizing for general OLTP would conflict with columnar heads, immutable parts, and scan-oriented execution.

## Arbitrary in-place row updates

Corrections and logical replacements will create ordered versions. Durable CSEG parts are immutable. Arbitrary byte or row mutation in place would complicate snapshots, crash safety, compression, history, and reader concurrency.

## Full SQL-standard compatibility

The project will define and test a coherent analytical SQL subset driven by representative workloads. Claiming full compatibility would consume effort better spent specifying temporal, streaming, storage, and recovery semantics. Unsupported syntax must fail clearly.

## General cross-table distributed transactions

Per-tablet ordered commits and analytical snapshots are the distributed foundation. General atomic transactions spanning arbitrary tables and tablets introduce coordination, recovery, and isolation scope that is not required for the initial workloads. Narrower metadata or ingestion atomicity may be designed separately.

## Custom cryptography or TLS

ChronosDB will not design cryptographic primitives or a TLS implementation. Those require specialist review and continual security maintenance. A future secure transport will integrate maintained libraries through a versioned, testable boundary.

## Custom replacements for general-purpose compressors

CSEG may define encodings specialized for column values, but it will use established compressors such as Zstandard where a general-purpose compression stage is needed. Inventing a replacement is outside the database's differentiating scope and would add compatibility and security risk.

## GPU execution for novelty

GPU execution is not a roadmap milestone. It may be considered only for a measured workload where transfer, scheduling, memory, operational complexity, and fallback behavior show a compelling end-to-end benefit.

## A web dashboard as a substitute for engine depth

Operational surfaces will eventually be needed, but a polished dashboard cannot substitute for durable recovery, query correctness, explainability, metrics, or profiling interfaces. UI work must follow engine contracts and a real user need.

## Thread-per-connection networking

The server is Linux-first and event-driven with epoll. A thread for every connection creates avoidable scheduling and memory overhead and makes backpressure difficult to control under high connection counts.

## Indiscriminate lock-free structures

Lock-free code is not a goal by itself. It increases proof and reclamation complexity. Single-writer ownership and bounded SPSC queues serve identified hot paths; ordinary locks are preferred in cold paths where they are clearer. Every concurrency algorithm needs a memory-ordering argument and evidence of need.

## Unqualified exactly-once delivery claims

A resume token can provide a deterministic committed replay position, and input identities can make retries idempotent. ChronosDB cannot guarantee that an external sink applies an event exactly once unless the database also controls or coordinates the sink transaction and acknowledgment. The subscription contract will state its actual delivery and duplication behavior.

## Equal performance-sensitive support for every operating system

Linux is the first and authoritative server platform because epoll, storage behavior, observability, and reproducible performance testing are platform-specific. Portable libraries and tools are welcome where they do not dilute correctness, but other server operating systems will not receive equal performance claims without dedicated implementations and evidence.

## Hidden database-engine delegation

RocksDB, SQLite, DuckDB, DataFusion, an existing Raft package, or another database engine will not implement ChronosDB's core storage, query, or consensus path behind an adapter. Such substitution would prevent the project from owning and validating its defining contracts. Small external libraries may still be adopted under the dependency and ADR policy for non-core functions.
