#include "chronos/query/relational_plan.hpp"

#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

class EmptySource final : public PhysicalOperator {
public:
  common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    return PhysicalOperatorStep::end();
  }
};

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

[[nodiscard]] std::vector<PhysicalColumnShape> source_shape() {
  return {{.type = type(schema::LogicalTypeKind::kInt64), .nullable = false},
          {.type = type(schema::LogicalTypeKind::kTimestampNs), .nullable = false},
          {.type = type(schema::LogicalTypeKind::kUuid), .nullable = false},
          {.type = type(schema::LogicalTypeKind::kUInt64), .nullable = false},
          {.type = type(schema::LogicalTypeKind::kUInt32), .nullable = false},
          {.type = type(schema::LogicalTypeKind::kUInt8), .nullable = false}};
}

[[nodiscard]] PhysicalAsofPlan make_plan() {
  std::vector<VectorAsofColumnShape> left;
  std::vector<VectorAsofColumnShape> right;
  for (const PhysicalColumnShape& column : source_shape()) {
    left.push_back({.type = column.type, .nullable = column.nullable});
    right.push_back({.type = column.type, .nullable = column.nullable});
  }
  VectorAsofJoinDefinition definition{
      .left_input_columns = std::move(left),
      .right_input_columns = std::move(right),
      .equality_keys = {{.left_column_ordinal = 0U, .right_column_ordinal = 0U}},
      .left_timestamp_column_ordinal = 1U,
      .right_timestamp_column_ordinal = 1U,
      .right_physical_ordering_key_ordinals = {0U},
      .right_row_version_first_column_ordinal = 2U,
      .left_output_column_ordinals = {0U, 1U, 2U, 3U, 4U, 5U},
      .right_output_column_ordinals = {0U, 1U, 2U, 3U, 4U, 5U},
      .left_outer = true};
  std::vector<PhysicalColumnShape> output;
  for (const VectorAsofColumnShape& column : vector_asof_join_output_shape(definition).value())
    output.push_back({.type = column.type, .nullable = column.nullable});
  std::vector<PhysicalAsofPlanJoin> joins;
  joins.push_back({.left_preparation = PhysicalPipelinePlan::create(source_shape(), {}).value(),
                   .right_preparation = PhysicalPipelinePlan::create(source_shape(), {}).value(),
                   .definition = std::move(definition)});
  return PhysicalAsofPlan::create(std::move(joins),
                                  PhysicalPipelinePlan::create(std::move(output), {}).value())
      .value();
}

void benchmark_asof_plan_instantiation(benchmark::State& state) {
  PhysicalAsofPlan plan = make_plan();
  for ([[maybe_unused]] auto iteration : state) {
    std::vector<std::unique_ptr<PhysicalOperator>> sources;
    sources.reserve(2U);
    sources.push_back(std::make_unique<EmptySource>());
    sources.push_back(std::make_unique<EmptySource>());
    auto pipeline = plan.instantiate(std::move(sources));
    benchmark::DoNotOptimize(pipeline);
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()));
}

BENCHMARK(benchmark_asof_plan_instantiation);

} // namespace
} // namespace chronos::query
