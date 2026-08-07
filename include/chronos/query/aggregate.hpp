#ifndef CHRONOS_QUERY_AGGREGATE_HPP_
#define CHRONOS_QUERY_AGGREGATE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/query/physical_operator.hpp"
#include "chronos/schema/logical_type.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::query {

inline constexpr std::size_t kMaximumUngroupedAggregateWidth = kDefaultVectorChunkColumnLimit;
inline constexpr std::size_t kDefaultUngroupedAggregateConfigurationByteLimit =
    std::size_t{2U} * 1024U * 1024U;
inline constexpr std::size_t kMaximumGroupedAggregateKeys = kDefaultVectorChunkColumnLimit;
inline constexpr std::size_t kMaximumGroupedAggregateWidth = kDefaultVectorChunkColumnLimit;
inline constexpr std::size_t kMaximumGroupedAggregateGroups = 4096U;
inline constexpr std::size_t kDefaultGroupedAggregateConfigurationByteLimit =
    std::size_t{2U} * 1024U * 1024U;
inline constexpr std::size_t kDefaultGroupedAggregateKeyByteLimit = std::size_t{1U} * 1024U * 1024U;
inline constexpr std::size_t kDefaultAggregateExtremumByteLimit = std::size_t{1U} * 1024U * 1024U;

enum class VectorAggregateOperation : std::uint8_t {
  kCountStar,
  kCount,
  kSum,
  kAverage,
  kMinimum,
  kMaximum,
  kVariancePopulation,
  kVarianceSample,
};

struct VectorAggregateInput {
  std::size_t column_ordinal;
  schema::LogicalType type;
  bool nullable;
};

struct VectorAggregateDefinition {
  VectorAggregateOperation operation;
  std::optional<VectorAggregateInput> input;
};

struct VectorAggregateOutputShape {
  schema::LogicalType type;
  bool nullable;

  friend constexpr bool operator==(const VectorAggregateOutputShape&,
                                   const VectorAggregateOutputShape&) = default;
};

struct UngroupedAggregateLimits {
  std::size_t maximum_aggregates{kMaximumUngroupedAggregateWidth};
  std::size_t maximum_variable_extremum_bytes{kDefaultAggregateExtremumByteLimit};
  std::size_t maximum_retained_configuration_bytes{
      kDefaultUngroupedAggregateConfigurationByteLimit};
  VectorChunkLimits output_limits{};
};

struct VectorGroupKeyDefinition {
  std::size_t column_ordinal;
  schema::LogicalType type;
  bool nullable;
};

struct GroupedAggregateLimits {
  std::size_t maximum_groups{kMaximumGroupedAggregateGroups};
  std::size_t maximum_group_keys{kMaximumGroupedAggregateKeys};
  std::size_t maximum_aggregates{kMaximumGroupedAggregateWidth};
  std::size_t maximum_key_bytes_per_group{kDefaultGroupedAggregateKeyByteLimit};
  std::size_t maximum_variable_extremum_bytes{kDefaultAggregateExtremumByteLimit};
  std::size_t maximum_retained_configuration_bytes{kDefaultGroupedAggregateConfigurationByteLimit};
  VectorChunkLimits output_limits{};
};

// Validates one aggregate definition and returns its exact result type/nullability. The current
// version admits COUNT over every physical type, numeric SUM/AVG/variance, and all-type MIN/MAX.
[[nodiscard]] common::Result<VectorAggregateOutputShape>
vector_aggregate_output_shape(const VectorAggregateDefinition& definition);

// Consumes its complete input stream without retaining chunks, accumulates one global group, and
// emits exactly one canonical one-row chunk. Empty input therefore produces COUNT zero and NULL for
// every other aggregate. The operator and every instantiated pipeline are uniquely owned and
// thread-affine; cancellation and failures unwind input/output memory through RAII.
class UngroupedAggregateOperator final : public PhysicalOperator {
public:
  ~UngroupedAggregateOperator() override;

  [[nodiscard]] static common::Result<std::unique_ptr<PhysicalOperator>>
  create(std::unique_ptr<PhysicalOperator> input,
         const std::vector<VectorAggregateDefinition>& definitions,
         UngroupedAggregateLimits limits = {});

  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next(const QueryResourceContext& resources) override;

private:
  class Impl;

  UngroupedAggregateOperator(std::unique_ptr<PhysicalOperator> input, std::unique_ptr<Impl> impl,
                             VectorChunkLimits output_limits) noexcept;

  std::unique_ptr<PhysicalOperator> input_;
  std::unique_ptr<Impl> impl_;
  VectorChunkLimits output_limits_;
  bool emitted_{};
};

// Consumes its complete input stream into a finite query-accounted set of groups and then emits one
// canonical accounted row per pull. Key order in each row is caller order, followed by aggregate
// order. Empty input emits no groups. NULL key cells compare equal for grouping. Every retained
// variable-width extremum is independently bounded and query-accounted.
class GroupedAggregateOperator final : public PhysicalOperator {
public:
  ~GroupedAggregateOperator() override;

  [[nodiscard]] static common::Result<std::unique_ptr<PhysicalOperator>>
  create(std::unique_ptr<PhysicalOperator> input, const std::vector<VectorGroupKeyDefinition>& keys,
         const std::vector<VectorAggregateDefinition>& definitions,
         GroupedAggregateLimits limits = {});

  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next(const QueryResourceContext& resources) override;

private:
  class Impl;

  GroupedAggregateOperator(std::unique_ptr<PhysicalOperator> input, std::unique_ptr<Impl> impl,
                           VectorChunkLimits output_limits) noexcept;

  std::unique_ptr<PhysicalOperator> input_;
  std::unique_ptr<Impl> impl_;
  VectorChunkLimits output_limits_;
  std::size_t output_group_{};
  bool input_consumed_{};
  bool ended_{};
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_AGGREGATE_HPP_
