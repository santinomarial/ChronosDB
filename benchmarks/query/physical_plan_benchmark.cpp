#include "chronos/query/physical_plan.hpp"
#include "chronos/schema/logical_type.hpp"

#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

class EmptySource final : public PhysicalOperator {
public:
  [[nodiscard]] common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    return PhysicalOperatorStep::end();
  }
};

[[nodiscard]] std::vector<PhysicalColumnShape> bool_shape() {
  return {{.type = schema::LogicalType::create(schema::LogicalTypeKind::kBool).value(),
           .nullable = true}};
}

[[nodiscard]] std::vector<PhysicalPipelineStage> make_stages(const std::size_t count) {
  std::vector<PhysicalPipelineStage> stages;
  stages.reserve(count);
  for (std::size_t index = 0U; index < count; ++index) {
    if ((index & 1U) == 0U)
      stages.emplace_back(BooleanFilterStage{0U});
    else
      stages.emplace_back(LimitStage{std::numeric_limits<std::uint64_t>::max()});
  }
  return stages;
}

void validate_physical_pipeline_plan(benchmark::State& state) {
  const auto stage_count = static_cast<std::size_t>(state.range(0));
  const std::vector<PhysicalColumnShape> shape = bool_shape();
  const std::vector<PhysicalPipelineStage> stages = make_stages(stage_count);
  for (auto _ : state) {
    static_cast<void>(_);
    auto plan = PhysicalPipelinePlan::create(shape, stages);
    benchmark::DoNotOptimize(plan);
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(stage_count));
  state.counters["stages"] = static_cast<double>(stage_count);
}

void instantiate_physical_pipeline_plan(benchmark::State& state) {
  const auto stage_count = static_cast<std::size_t>(state.range(0));
  const PhysicalPipelinePlan plan =
      PhysicalPipelinePlan::create(bool_shape(), make_stages(stage_count)).value();
  for (auto _ : state) {
    static_cast<void>(_);
    auto pipeline = plan.instantiate(std::make_unique<EmptySource>());
    benchmark::DoNotOptimize(pipeline);
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(stage_count));
  state.counters["stages"] = static_cast<double>(stage_count);
}

BENCHMARK(validate_physical_pipeline_plan)->Arg(1)->Arg(8)->Arg(64)->Arg(256);
BENCHMARK(instantiate_physical_pipeline_plan)->Arg(1)->Arg(8)->Arg(64)->Arg(256);

} // namespace
} // namespace chronos::query
