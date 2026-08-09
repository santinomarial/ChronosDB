#ifndef CHRONOS_QUERY_TEMPORAL_CSEG_SNAPSHOT_HPP_
#define CHRONOS_QUERY_TEMPORAL_CSEG_SNAPSHOT_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/cseg/projected_reader.hpp"
#include "chronos/cseg/temporal_format.hpp"
#include "chronos/query/snapshot.hpp"
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

// Resolves current or system-time-visible winners from already schema-bound projected CSEG v2
// granules. Every input row must belong to the one authoritative source lineage supplied here;
// unrelated WAL/Raft sources are rejected rather than assigned an invented order. The caller must
// project every schema user column, in schema order, and keep all granules alive through the call.
[[nodiscard]] common::Result<std::shared_ptr<const ScalarTableSnapshot>>
resolve_cseg_v2_temporal_snapshot(std::shared_ptr<const schema::TableSchema> schema,
                                  std::span<const cseg::ProjectedCsegGranule* const> granules,
                                  TemporalCsegSourceLineage lineage,
                                  std::optional<std::int64_t> as_of_system_time_ns,
                                  TemporalCsegResolutionLimits limits = {});

} // namespace chronos::query

#endif // CHRONOS_QUERY_TEMPORAL_CSEG_SNAPSHOT_HPP_
