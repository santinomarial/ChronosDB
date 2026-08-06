#ifndef CHRONOS_MANIFEST_COMPACTION_PLANNER_HPP_
#define CHRONOS_MANIFEST_COMPACTION_PLANNER_HPP_

#include "chronos/common/result.hpp"
#include "chronos/cseg/types.hpp"
#include "chronos/manifest/types.hpp"
#include "chronos/schema/identity.hpp"

#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <vector>

namespace chronos::manifest {

namespace detail {
class CompactionPlannerBuilder;
}

enum class AppendOnlyPartRole : std::uint8_t {
  kBase,
  kDelta,
};

struct AppendOnlyPartClassification {
  cseg::PartId part_id;
  AppendOnlyPartRole role;

  friend bool operator==(const AppendOnlyPartClassification&,
                         const AppendOnlyPartClassification&) = default;
};

struct AppendOnlyCompactionPlannerLimits {
  std::uint32_t minimum_input_parts{2U};
  std::uint32_t maximum_input_parts{8U};
  std::uint32_t maximum_manifest_parts{1U << 20U};
  std::uint64_t maximum_input_bytes{8ULL * 1024ULL * 1024ULL * 1024ULL};
  std::uint64_t maximum_input_rows{std::numeric_limits<std::uint64_t>::max()};
};

class PlannedAppendOnlyCompaction {
public:
  PlannedAppendOnlyCompaction() = delete;

  [[nodiscard]] const schema::TableId& table_id() const noexcept;
  [[nodiscard]] const schema::TabletId& tablet_id() const noexcept;
  [[nodiscard]] const schema::SchemaId& schema_id() const noexcept;
  [[nodiscard]] schema::SchemaVersion schema_version() const noexcept;
  [[nodiscard]] std::span<const cseg::PartId> input_part_ids() const noexcept;
  [[nodiscard]] std::uint64_t input_bytes() const noexcept;
  [[nodiscard]] std::uint64_t input_rows() const noexcept;
  [[nodiscard]] std::int64_t minimum_event_time() const noexcept;
  [[nodiscard]] std::int64_t maximum_event_time() const noexcept;
  [[nodiscard]] std::uint32_t delta_input_parts() const noexcept;

private:
  PlannedAppendOnlyCompaction(schema::TableId table_id, schema::TabletId tablet_id,
                              schema::SchemaId schema_id, schema::SchemaVersion schema_version,
                              std::vector<cseg::PartId> input_part_ids, std::uint64_t input_bytes,
                              std::uint64_t input_rows, std::int64_t minimum_event_time,
                              std::int64_t maximum_event_time,
                              std::uint32_t delta_input_parts) noexcept;

  schema::TableId table_id_;
  schema::TabletId tablet_id_;
  schema::SchemaId schema_id_;
  schema::SchemaVersion schema_version_;
  std::vector<cseg::PartId> input_part_ids_;
  std::uint64_t input_bytes_{};
  std::uint64_t input_rows_{};
  std::int64_t minimum_event_time_{};
  std::int64_t maximum_event_time_{};
  std::uint32_t delta_input_parts_{};

  friend common::Result<std::optional<PlannedAppendOnlyCompaction>>
      plan_append_only_compaction(std::span<const PartDescriptor>,
                                  AppendOnlyCompactionPlannerLimits);
  friend class detail::CompactionPlannerBuilder;
};

// Returns one classification per input descriptor in caller order. Roles are rebuilt separately
// for every table/tablet/schema group from record-sequence arrival order and never affect truth.
[[nodiscard]] common::Result<std::vector<AppendOnlyPartClassification>>
classify_append_only_parts(std::span<const PartDescriptor> parts,
                           std::uint32_t maximum_manifest_parts = 1U << 20U);

// Deterministically returns the first bounded delta-overlap candidate, then the first ordinary
// overlap component. No candidate is a successful empty optional; scanning remains authoritative.
[[nodiscard]] common::Result<std::optional<PlannedAppendOnlyCompaction>>
plan_append_only_compaction(std::span<const PartDescriptor> parts,
                            AppendOnlyCompactionPlannerLimits limits = {});

} // namespace chronos::manifest

#endif // CHRONOS_MANIFEST_COMPACTION_PLANNER_HPP_
