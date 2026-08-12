# ADR 0217: Vectorized Tablet-State Query Source

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB query, mutable-head, and ingestion maintainers

## Context

The physical SQL engine could instantiate queries over an aggregate Manifest snapshot, but the
recoverable single-node ingestion owner exposes current data as one `TabletSnapshot`: zero or more
pinned sealed mutable heads plus one active head. Sending daemon queries through the scalar oracle
would bypass the implemented vectorized product path. Instantiating the physical plan separately for
each generation would produce incorrect global aggregate, sort, latest, and limit semantics.

## Decision

`instantiate_tablet_state_pipeline` validates the physical plan's exact destination-schema input
shape using the same public helper as Manifest/CSEG snapshot pipelines. It configures every head
scan with the same optional row-version suffix, builds one bounded serial source over sealed
generations followed by the active generation, and instantiates the checked physical pipeline once
above that source.

The `TabletSnapshot` value pins all generations for the construction call; each resulting
`HeadScanOperator` retains its own pinned generation. The serial source reserves query memory for
its configuration, checks cancellation and query ownership on every pull, releases completed child
sources eagerly, and drops all children on failure.

## Rationale and alternatives

This reuses the production vector expression, aggregate, sort, latest, and output operators without
manufacturing a Manifest or CSEG. The shared shape helper prevents mutable-only and durable-source
interpretations from diverging. Scalar execution remains an oracle and is not the daemon product
path. Per-generation physical execution was rejected because it changes SQL semantics; flattening
heads into an intermediate owned batch was rejected because it duplicates all current data and
breaks pull-based memory accounting.

## Consequences

The single-node service can query current recovered WAL state before immutable-part flush is wired
into its owner. The source intentionally covers only the `TabletState` generations supplied by that
owner; combining those generations with Manifest-selected CSEGs requires the existing aggregate
snapshot publication path. Multiple tablets still need one higher-level source composition.

## Invariants and validation

This decision supports stable snapshot and reference-lifetime invariants by pinning one published
tablet epoch and its immutable generations. Focused tests rotate a tablet, execute one global count
across sealed and active heads, verify exact output, release all query credit, and reject a foreign
physical shape. Allocation fault injection, concurrent publication schedules, multi-tablet merging,
and full SQL differential coverage remain deferred.

## References

- [ADR 0022](0022-pull-based-vector-operator-memory-contract.md)
- [ADR 0041](0041-vectorized-tablet-snapshot-pipeline.md)
- [Mutable-head publication](../architecture/mutable-head-publication.md)
- [Architecture invariants](../architecture/invariants.md)
