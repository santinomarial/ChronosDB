#include "chronos/query/relational_plan.hpp"

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

namespace {

[[nodiscard]] chronos::schema::LogicalType type(const chronos::schema::LogicalTypeKind kind) {
  return chronos::schema::LogicalType::create(kind).value();
}

[[nodiscard]] std::vector<chronos::query::PhysicalColumnShape> input_shape() {
  using chronos::schema::LogicalTypeKind;
  return {{.type = type(LogicalTypeKind::kInt64), .nullable = false},
          {.type = type(LogicalTypeKind::kTimestampNs), .nullable = false},
          {.type = type(LogicalTypeKind::kUuid), .nullable = false},
          {.type = type(LogicalTypeKind::kUInt64), .nullable = false},
          {.type = type(LogicalTypeKind::kUInt32), .nullable = false},
          {.type = type(LogicalTypeKind::kUInt8), .nullable = false}};
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  if (data == nullptr || size < 8U)
    return 0;
  using namespace chronos::query;
  std::vector<VectorAsofColumnShape> left;
  std::vector<VectorAsofColumnShape> right;
  for (const PhysicalColumnShape& column : input_shape()) {
    left.push_back({.type = column.type, .nullable = column.nullable});
    right.push_back({.type = column.type, .nullable = column.nullable});
  }
  VectorAsofJoinDefinition definition{.left_input_columns = std::move(left),
                                      .right_input_columns = std::move(right),
                                      .equality_keys = {{.left_column_ordinal = data[0] % 8U,
                                                         .right_column_ordinal = data[1] % 8U}},
                                      .left_timestamp_column_ordinal = data[2] % 8U,
                                      .right_timestamp_column_ordinal = data[3] % 8U,
                                      .right_physical_ordering_key_ordinals = {data[4] % 8U},
                                      .right_row_version_first_column_ordinal = data[5] % 8U,
                                      .left_output_column_ordinals = {data[6] % 8U},
                                      .right_output_column_ordinals = {data[7] % 8U},
                                      .left_outer = (data[0] & 1U) != 0U};
  AsofJoinLimits limits;
  if (size > 8U && (data[8] & 1U) != 0U)
    limits.maximum_state_bytes = 1U;
  auto output = vector_asof_join_output_shape(definition, limits);
  if (!output.has_value())
    return 0;
  std::vector<PhysicalColumnShape> joined;
  for (const VectorAsofColumnShape& column : *output)
    joined.push_back({.type = column.type, .nullable = column.nullable});
  std::vector<PhysicalAsofPlanJoin> joins;
  joins.push_back({.left_preparation = PhysicalPipelinePlan::create(input_shape(), {}).value(),
                   .right_preparation = PhysicalPipelinePlan::create(input_shape(), {}).value(),
                   .definition = std::move(definition),
                   .limits = limits});
  static_cast<void>(PhysicalAsofPlan::create(
      std::move(joins), PhysicalPipelinePlan::create(std::move(joined), {}).value(),
      {.maximum_joins = (data[1] & 1U) != 0U ? 1U : 2U,
       .maximum_retained_configuration_bytes =
           size > 9U && (data[9] & 1U) != 0U ? 1U : 8U * 1024U * 1024U}));
  return 0;
}
