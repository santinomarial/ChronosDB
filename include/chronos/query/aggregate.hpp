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
  std::size_t maximum_retained_configuration_bytes{
      kDefaultUngroupedAggregateConfigurationByteLimit};
  VectorChunkLimits output_limits{};
};

// Validates one aggregate definition and returns its exact result type/nullability. The current
// version admits COUNT over every physical type and fixed-width inputs for all other operations.
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

} // namespace chronos::query

#endif // CHRONOS_QUERY_AGGREGATE_HPP_
