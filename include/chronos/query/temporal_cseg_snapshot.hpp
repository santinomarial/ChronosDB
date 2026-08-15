#ifndef CHRONOS_QUERY_TEMPORAL_CSEG_SNAPSHOT_HPP_
#define CHRONOS_QUERY_TEMPORAL_CSEG_SNAPSHOT_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/cseg/projected_reader.hpp"
#include "chronos/cseg/temporal_format.hpp"
#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/query/snapshot.hpp"
#include "chronos/query/temporal_snapshot.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/schema/table_schema.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>

namespace chronos::query {

struct TemporalCsegSourceLineage {
  cseg::temporal_format::CommitSource source{cseg::temporal_format::CommitSource::kWal};
  common::Uuid source_id;
};

struct TemporalCsegResolutionLimits {
  std::size_t maximum_versions{1U << 22U};
  std::size_t maximum_output_rows{1U << 20U};
  std::size_t maximum_identity_bytes{cseg::temporal_format::kMaximumLogicalIdentityBytes};
};

struct TemporalManifestCsegPartView {
  const manifest::TemporalPartDescriptor* descriptor{};
  common::ByteView bytes;
};

struct TemporalManifestCsegResolutionLimits {
  std::size_t maximum_parts{1U << 16U};
  std::size_t maximum_granules{1U << 20U};
  std::uint64_t maximum_decoded_buffer_bytes{1ULL << 30U};
  cseg::CsegProjectedReaderLimits reader;
  TemporalCsegResolutionLimits resolution;
};

// Resolves current or system-time-visible winners from already schema-bound projected CSEG v2
// granules. Every input row must belong to the one authoritative source lineage supplied here;
// unrelated WAL/Raft sources are rejected rather than assigned an invented order. The caller must
// project every schema user column, in schema order, and keep all granules alive through the call.
[[nodiscard]] common::Result<std::shared_ptr<const ScalarTableSnapshot>>
resolve_cseg_v2_temporal_snapshot(const std::shared_ptr<const schema::TableSchema>& schema,
                                  std::span<const cseg::ProjectedCsegGranule* const> granules,
                                  TemporalCsegSourceLineage lineage,
                                  std::optional<std::int64_t> as_of_system_time_ns,
                                  TemporalCsegResolutionLimits limits = {});

// Composes Manifest v2 part descriptors and exact CSEG images into one scalar current/as-of tablet
// snapshot. Descriptor system-time minima conservatively prune future-only parts; every retained
// candidate is schema-projected and page-validated before the exact row-version resolver decides
// winners. Image owners must outlive this call (generation-pinned storage images satisfy that).
[[nodiscard]] common::Result<std::shared_ptr<const ScalarTableSnapshot>>
resolve_manifest_v2_temporal_tablet_snapshot(
    const std::shared_ptr<const schema::TableSchema>& schema, const schema::SchemaLineage& lineage,
    const manifest::TemporalTabletDescriptor& tablet,
    std::span<const TemporalManifestCsegPartView> parts, TemporalCsegSourceLineage source,
    std::optional<std::int64_t> as_of_system_time_ns,
    TemporalManifestCsegResolutionLimits limits = {});

// Reconstructs one fresh provider from the complete retained history in generation-pinned v2 part
// images. retained_system_time_ns is a caller-proven tablet-wide boundary. Cross-part rows are
// canonicalized by source position/row ordinal and atomically installed only after exact decoding,
// lineage/schema validation, and retained mutation-transition validation all succeed.
[[nodiscard]] common::Result<std::unique_ptr<TemporalSnapshotProvider>>
restore_manifest_v2_temporal_tablet_history(
    const std::shared_ptr<const schema::TableSchema>& schema, const schema::SchemaLineage& lineage,
    const manifest::TemporalTabletDescriptor& tablet,
    std::span<const TemporalManifestCsegPartView> parts, TemporalCsegSourceLineage source,
    std::int64_t retained_system_time_ns, TemporalStoreLimits store_limits = {},
    TemporalManifestCsegResolutionLimits limits = {});

} // namespace chronos::query

#endif // CHRONOS_QUERY_TEMPORAL_CSEG_SNAPSHOT_HPP_
