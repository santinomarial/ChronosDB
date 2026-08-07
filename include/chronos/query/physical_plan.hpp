#ifndef CHRONOS_QUERY_PHYSICAL_PLAN_HPP_
#define CHRONOS_QUERY_PHYSICAL_PLAN_HPP_

#include "chronos/common/result.hpp"
#include "chronos/query/column_output.hpp"
#include "chronos/query/physical_operator.hpp"
#include "chronos/schema/logical_type.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <variant>
#include <vector>

namespace chronos::query {

inline constexpr std::size_t kDefaultPhysicalPipelineStageLimit = 256U;
inline constexpr std::size_t kDefaultPhysicalPipelineConfigurationByteLimit =
    std::size_t{2U} * 1024U * 1024U;

struct PhysicalColumnShape {
  schema::LogicalType type;
  bool nullable;

  friend constexpr bool operator==(const PhysicalColumnShape&,
                                   const PhysicalColumnShape&) = default;
};

struct BooleanFilterStage {
  std::size_t predicate_column;
};

struct TimestampRangeFilterStage {
  std::size_t timestamp_column;
  TimestampRangePredicate predicate;
};

struct ColumnSubsetStage {
  std::vector<std::size_t> column_ordinals;
};

struct SourceColumnOutputStage {
  std::vector<std::size_t> input_column_ordinals;
  VectorChunkLimits output_limits{};
};

struct ColumnOutputStage {
  std::vector<ColumnOutputPosition> positions;
  VectorChunkLimits output_limits{};
};

struct LimitStage {
  std::uint64_t maximum_rows;
};

using PhysicalPipelineStage =
    std::variant<BooleanFilterStage, TimestampRangeFilterStage, ColumnSubsetStage,
                 SourceColumnOutputStage, ColumnOutputStage, LimitStage>;

struct PhysicalPipelinePlanLimits {
  std::size_t maximum_input_columns{kDefaultVectorChunkColumnLimit};
  std::size_t maximum_stages{kDefaultPhysicalPipelineStageLimit};
  std::size_t maximum_retained_configuration_bytes{kDefaultPhysicalPipelineConfigurationByteLimit};
};

// An immutable, reusable plan for the currently supported unary vector stages. Creation validates
// and propagates exact physical shapes; instantiate() adds a source-shape boundary before wrapping
// the source in stage order. Plan objects are safe for concurrent const access. Each instantiated
// operator pipeline remains thread-affine under ADR 0022.
class PhysicalPipelinePlan {
public:
  PhysicalPipelinePlan() = delete;
  PhysicalPipelinePlan(const PhysicalPipelinePlan&) = delete;
  PhysicalPipelinePlan& operator=(const PhysicalPipelinePlan&) = delete;
  PhysicalPipelinePlan(PhysicalPipelinePlan&&) noexcept = default;
  PhysicalPipelinePlan& operator=(PhysicalPipelinePlan&&) noexcept = default;

  [[nodiscard]] static common::Result<PhysicalPipelinePlan>
  create(std::vector<PhysicalColumnShape> input_columns, std::vector<PhysicalPipelineStage> stages,
         PhysicalPipelinePlanLimits limits = {});

  [[nodiscard]] std::span<const PhysicalColumnShape> input_columns() const noexcept;
  [[nodiscard]] std::span<const PhysicalColumnShape> output_columns() const noexcept;
  [[nodiscard]] std::span<const PhysicalPipelineStage> stages() const noexcept;
  [[nodiscard]] std::size_t retained_configuration_bytes() const noexcept;

  [[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>>
  instantiate(std::unique_ptr<PhysicalOperator> source) const;

private:
  PhysicalPipelinePlan(std::vector<PhysicalColumnShape> input_columns,
                       std::vector<PhysicalColumnShape> output_columns,
                       std::vector<PhysicalPipelineStage> stages,
                       std::size_t retained_configuration_bytes) noexcept;

  std::vector<PhysicalColumnShape> input_columns_;
  std::vector<PhysicalColumnShape> output_columns_;
  std::vector<PhysicalPipelineStage> stages_;
  std::size_t retained_configuration_bytes_;
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_PHYSICAL_PLAN_HPP_
