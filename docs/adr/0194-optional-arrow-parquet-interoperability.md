# ADR 0194: Optional Apache Arrow and Parquet interoperability

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB columnar, interoperability, build, and security maintainers
- **Extends:** [ADR 0011](0011-dependency-and-build-versus-buy-policy.md)

## Context

Phase 17 requires a documented ecosystem import/export format while CSEG remains ChronosDB's
primary immutable storage format. Reimplementing Arrow IPC or Parquet would create a large,
security-sensitive parser and compatibility surface unrelated to ChronosDB's core storage
contracts. External files also cannot authoritatively supply ChronosDB table, schema, column, or
role identities.

## Decision

The optional `CHRONOS_ENABLE_ARROW_INTEROP` build selects Apache Arrow C++ and Parquet 25.x and
exports a separate `chronos::interop` target. The option is off by default so the
core engine and existing supported builders do not acquire this large transitive dependency.
Public ChronosDB headers expose only file paths, canonical batches, target schemas, and bounded
import limits; no Arrow type crosses the public boundary.

Arrow IPC file and Parquet providers map all current logical types exactly: fixed-width signed and
unsigned integers and floats retain their width; decimal uses Decimal128 precision/scale; timestamp
uses nanoseconds without a timezone; date uses Date32; string and symbol use UTF-8; binary remains
binary; UUID uses fixed-size binary(16); and nullability and field order must match. Symbol and UUID
exports include advisory field metadata, but imports rely on the caller's exact target schema.

Imports require an existing `TableSchema`. Field count, order, names, Arrow types, and nullability
must match it before canonical `OwnedColumnarBatch` construction. Table/schema/column identities,
roles, lineage, and durable meaning are never inferred from external metadata. File bytes and final
canonical row/column/buffer retention are bounded. The current implementation supports the
project's little-endian Linux and macOS hosts and fails compilation on a big-endian host.

Exports write to a same-directory uniquely named temporary file, close the external writer and
stream, then rename to the requested path. A failed write cleans up its temporary file rather than
publishing partial output. These interchange files are not Manifest members and do not change any
ChronosDB durable format.

## Consequences and validation

Apache Arrow and Parquet become reviewed optional production dependencies. Their parsers own
external framing, encoding, compression, and compatibility; ChronosDB owns schema admission,
resource limits, canonical validation, status mapping, and final publication. Compressed Parquet
decoding can transiently allocate more than its input file size inside Arrow, so callers must apply
process/container memory controls in addition to `ImportLimits` for untrusted large files.

Tests round-trip every current logical type and exact canonical buffer through Arrow IPC and
Parquet, and reject schema mismatch, corruption, and oversized inputs. CI enables the provider on
macOS. Independent third-party fixtures, multi-version compatibility, allocation-failure injection,
hostile compression-ratio tests, and Linux package qualification remain in the deferred-validation
ledger.

Invariants 10, 14, and 18 apply. External format support cannot weaken canonical batch validation or
make Parquet an internal source of truth.

## Alternatives considered

- **Implement Arrow IPC/Parquet locally:** rejected because mature commodity formats and hostile
  parsers are not a differentiating database subsystem.
- **Use Parquet as the primary immutable part:** rejected by the accepted CSEG layout and integrity
  contract.
- **Infer a `TableSchema` from field names:** rejected because roles, stable identities, lineage,
  and event-time ownership would be ambiguous.
- **Expose Arrow objects in the public API:** rejected to keep ABI, allocation, and dependency
  ownership behind one narrow optional target.
- **Enable the dependency unconditionally:** rejected because Arrow's broad transitive graph is not
  required for the core database.

## Migration and rollback

Enabling or disabling the provider changes no database bytes. Rollback disables the option and
removes the `chronos::interop` target; existing Arrow/Parquet exports remain ordinary external
files. A future version outside the reviewed range or a mapping change requires compatibility
fixtures and an ADR review before the accepted range or contract changes.

## References

- [Dependency record](../dependencies/arrow-parquet.md)
- [Learning guide](../learning/arrow-parquet-interoperability.md)
- [CSEG v1 decision](0016-cseg-v1-layout-integrity-and-compression.md)
- [Architecture invariants](../architecture/invariants.md)
