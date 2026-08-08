#include "chronos/common/status.hpp"
#include "chronos/query/physical_optimizer.hpp"
#include "chronos/schema/logical_type.hpp"

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-optimizer-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }

  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

[[nodiscard]] schema::LogicalType int64_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
}

[[nodiscard]] columnar::OwnedPhysicalColumn
int64_column(const std::span<const std::int64_t> values) {
  columnar::ColumnVectorBuffers buffers;
  buffers.values.resize(values.size() * sizeof(std::int64_t));
  for (std::size_t row = 0U; row < values.size(); ++row) {
    const std::uint64_t bits = std::bit_cast<std::uint64_t>(values[row]);
    for (std::size_t byte = 0U; byte < sizeof(bits); ++byte) {
      buffers.values[row * sizeof(bits) + byte] =
          static_cast<std::byte>((bits >> (byte * 8U)) & 0xffU);
    }
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = int64_type(),
              .nullable = false,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = 0U},
             std::move(buffers))
      .value();
}

[[nodiscard]] AccountedVectorChunk accounted_chunk(const QueryResourceContext& resources,
                                                   const std::span<const std::int64_t> values) {
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(int64_column(values));
  VectorChunk chunk =
      VectorChunk::create(std::move(columns),
                          VectorSelection::all(static_cast<std::uint32_t>(values.size())).value())
          .value();
  QueryMemoryReservation reservation = resources.reserve(chunk.retained_buffer_bytes()).value();
  return AccountedVectorChunk::create(std::move(chunk), std::move(reservation), resources).value();
}

class ChunkSource final : public PhysicalOperator {
public:
  explicit ChunkSource(AccountedVectorChunk chunk) : chunk_(std::move(chunk)) {}

  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next(const QueryResourceContext& resources) override {
    const common::Result<void> active = resources.check_cancelled();
    if (!active.has_value())
      return common::make_unexpected(active.error());
    if (!chunk_.has_value())
      return PhysicalOperatorStep::end();
    AccountedVectorChunk result = std::move(*chunk_);
    chunk_.reset();
    return PhysicalOperatorStep::chunk(std::move(result));
  }

private:
  std::optional<AccountedVectorChunk> chunk_;
};

[[nodiscard]] std::int64_t value_at(const VectorChunk& chunk, const std::size_t row) {
  const common::ByteView bytes =
      chunk.cell({.column_ordinal = 0U, .selected_row = row}).value().bytes().value();
  std::uint64_t bits = 0U;
  for (std::size_t byte = 0U; byte < bytes.size(); ++byte)
    bits |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(bytes[byte])) << (byte * 8U);
  return std::bit_cast<std::int64_t>(bits);
}

[[nodiscard]] std::vector<std::int64_t> drain(PhysicalOperator& input,
                                              const QueryResourceContext& resources) {
  std::vector<std::int64_t> values;
  for (;;) {
    common::Result<PhysicalOperatorStep> step = input.next(resources);
    EXPECT_TRUE(step.has_value()) << step.error().to_string();
    if (!step.has_value() || step->kind() == PhysicalOperatorStepKind::kEnd)
      break;
    AccountedVectorChunk chunk = std::move(*step).take_chunk().value();
    for (std::size_t row = 0U; row < chunk.chunk().selected_row_count(); ++row)
      values.push_back(value_at(chunk.chunk(), row));
  }
  return values;
}

[[nodiscard]] PhysicalPipelinePlan sort_plan(const std::uint32_t maximum_rows) {
  SortLimits limits;
  limits.maximum_rows = maximum_rows;
  limits.output_limits.maximum_rows = maximum_rows;
  std::vector<PhysicalPipelineStage> stages;
  stages.emplace_back(SortStage{.keys = {{.column_ordinal = 0U,
                                          .direction = PhysicalSortDirection::kAscending,
                                          .null_placement = ScalarNullPlacement::kLast}},
                                .limits = limits});
  return PhysicalPipelinePlan::create({{.type = int64_type(), .nullable = false}},
                                      std::move(stages))
      .value();
}

[[nodiscard]] PhysicalSortStageEstimate estimate(const std::uint64_t rows) {
  return {.stage_index = 0U,
          .maximum_rows = rows,
          .maximum_input_chunk_rows = 2U,
          .maximum_output_logical_bytes = rows * sizeof(std::int64_t),
          .maximum_output_retained_bytes = static_cast<std::size_t>(rows * 32U + 1'024U),
          .maximum_spill_bytes = rows * 64U + 512U,
          .maximum_serialized_record_bytes = 64U};
}

[[nodiscard]] SpillSortLimits spill_limits() {
  SpillSortLimits limits;
  limits.maximum_rows = 64U;
  limits.maximum_runs = 32U;
  limits.maximum_spill_bytes = 1U << 20U;
  limits.maximum_serialized_record_bytes = 1U << 10U;
  limits.maximum_configuration_bytes = 1U << 20U;
  limits.run_sort_limits.maximum_rows = 2U;
  limits.run_sort_limits.output_limits.maximum_rows = 2U;
  limits.merge_output_limits.maximum_rows = 2U;
  limits.merge_output_limits.output_limits.maximum_rows = 2U;
  return limits;
}

[[nodiscard]] PhysicalExecutionStatistics
statistics(const std::size_t tasks, const PhysicalSortStageEstimate sort,
           const PhysicalSourceMergeRequirement requirement =
               PhysicalSourceMergeRequirement::kPreserveTaskOrder) {
  return {.source_task_count = tasks,
          .maximum_source_rows = sort.maximum_rows,
          .estimated_source_work_units = 1U << 24U,
          .source_merge_requirement = requirement,
          .sort_stages = {sort}};
}

[[nodiscard]] std::vector<std::unique_ptr<PhysicalOperator>>
sources(const QueryResourceContext& resources,
        const std::span<const std::vector<std::int64_t>> values) {
  std::vector<std::unique_ptr<PhysicalOperator>> result;
  result.reserve(values.size());
  for (const std::vector<std::int64_t>& source : values)
    result.push_back(std::make_unique<ChunkSource>(accounted_chunk(resources, source)));
  return result;
}

TEST(PhysicalOptimizerTest, SelectsInMemoryAtTheExactFiniteBoundary) {
  PhysicalOptimizerPolicy policy;
  policy.maximum_in_memory_sort_rows = 4U;
  auto optimized = OptimizedPhysicalPipelinePlan::create(sort_plan(4U),
                                                         statistics(1U, estimate(4U)), {}, policy);
  ASSERT_TRUE(optimized.has_value()) << optimized.error().to_string();
  ASSERT_EQ(optimized->sort_decisions().size(), 1U);
  EXPECT_EQ(optimized->sort_decisions().front().strategy, PhysicalSortStrategy::kInMemory);
  EXPECT_EQ(optimized->sort_decisions().front().estimated_comparison_work_units, 8U);
  EXPECT_EQ(optimized->source_merge_strategy(), PhysicalSourceMergeStrategy::kSerial);
  EXPECT_EQ(optimized->selected_parallel_workers(), 1U);
  EXPECT_GT(optimized->retained_configuration_bytes(),
            optimized->pipeline().retained_configuration_bytes());

  PhysicalExecutionStatistics missing = statistics(1U, estimate(4U));
  missing.sort_stages.clear();
  auto rejected =
      OptimizedPhysicalPipelinePlan::create(sort_plan(4U), std::move(missing), {}, policy);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kInvalidArgument);
}

TEST(PhysicalOptimizerTest, SelectsExternalOnlyWithACompatibleCapabilityAndExecutesExactOrder) {
  PhysicalOptimizerPolicy policy;
  policy.maximum_in_memory_sort_rows = 2U;
  PhysicalExecutionCapabilities capabilities;
  capabilities.spill_sorts.push_back({.stage_index = 0U, .limits = spill_limits()});
  auto optimized = OptimizedPhysicalPipelinePlan::create(
      sort_plan(2U), statistics(3U, estimate(6U)), capabilities, policy);
  ASSERT_TRUE(optimized.has_value()) << optimized.error().to_string();
  ASSERT_EQ(optimized->sort_decisions().size(), 1U);
  EXPECT_EQ(optimized->sort_decisions().front().strategy, PhysicalSortStrategy::kExternal);
  EXPECT_GT(optimized->estimated_cost().sort_io_bytes, 0U);

  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
  const std::array input{std::vector<std::int64_t>{6, 1}, std::vector<std::int64_t>{4, 3},
                         std::vector<std::int64_t>{5, 2}};
  TemporaryDirectory temporary;
  ASSERT_FALSE(temporary.path().empty());
  std::vector<ExternalSortExecutionTarget> targets;
  targets.push_back({.stage_index = 0U,
                     .spill_directory = io::PosixDirectory::open(temporary.path().string()).value(),
                     .file_prefix = "optimized"});
  auto pipeline = optimized->instantiate(resources, sources(resources, input), std::move(targets));
  ASSERT_TRUE(pipeline.has_value()) << pipeline.error().to_string();
  EXPECT_EQ(drain(**pipeline, resources), (std::vector<std::int64_t>{1, 2, 3, 4, 5, 6}));
  pipeline->reset();
  EXPECT_TRUE(io::PosixDirectory::open(temporary.path().string())->list_entries()->empty());
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);

  auto unavailable = OptimizedPhysicalPipelinePlan::create(
      sort_plan(2U), statistics(1U, estimate(6U)), {}, policy);
  ASSERT_FALSE(unavailable.has_value());
  EXPECT_EQ(unavailable.error().code(), common::StatusCode::kResourceExhausted);
}

TEST(PhysicalOptimizerTest, SelectsParallelOnlyForDeclaredUnorderedWorkWithLowerEstimatedCost) {
  PhysicalOptimizerPolicy policy;
  policy.maximum_in_memory_sort_rows = 6U;
  policy.minimum_parallel_rows = 1U;
  policy.minimum_parallel_work_units = 1U;
  policy.parallel_worker_overhead_units = 1U;
  PhysicalExecutionCapabilities capabilities;
  capabilities.available_parallel_workers = 3U;
  capabilities.parallel_limits = {.maximum_tasks = 3U,
                                  .maximum_workers = 3U,
                                  .maximum_ready_chunks = 2U,
                                  .maximum_retained_configuration_bytes = 1U << 20U};
  auto optimized = OptimizedPhysicalPipelinePlan::create(
      sort_plan(6U),
      statistics(3U, estimate(6U), PhysicalSourceMergeRequirement::kOrderIndependent), capabilities,
      policy);
  ASSERT_TRUE(optimized.has_value()) << optimized.error().to_string();
  EXPECT_EQ(optimized->source_merge_strategy(), PhysicalSourceMergeStrategy::kParallel);
  EXPECT_EQ(optimized->selected_parallel_workers(), 3U);
  EXPECT_LT(optimized->estimated_cost().selected_source_work_units,
            optimized->estimated_cost().source_work_units);

  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{16U} * 1024U * 1024U).value();
  const std::array input{std::vector<std::int64_t>{6, 1}, std::vector<std::int64_t>{4, 3},
                         std::vector<std::int64_t>{5, 2}};
  auto pipeline = optimized->instantiate(resources, sources(resources, input));
  ASSERT_TRUE(pipeline.has_value()) << pipeline.error().to_string();
  EXPECT_EQ(drain(**pipeline, resources), (std::vector<std::int64_t>{1, 2, 3, 4, 5, 6}));
  pipeline->reset();
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);

  auto ordered = OptimizedPhysicalPipelinePlan::create(sort_plan(6U), statistics(3U, estimate(6U)),
                                                       capabilities, policy);
  ASSERT_TRUE(ordered.has_value()) << ordered.error().to_string();
  EXPECT_EQ(ordered->source_merge_strategy(), PhysicalSourceMergeStrategy::kSerial);
}

TEST(PhysicalOptimizerTest, RejectsHostileCapabilitiesTargetsAndSourceShapesBeforeExecution) {
  PhysicalOptimizerPolicy policy;
  policy.maximum_in_memory_sort_rows = 2U;
  PhysicalExecutionCapabilities capabilities;
  capabilities.spill_sorts.push_back({.stage_index = 0U, .limits = spill_limits()});
  auto optimized = OptimizedPhysicalPipelinePlan::create(
      sort_plan(2U), statistics(2U, estimate(4U)), capabilities, policy);
  ASSERT_TRUE(optimized.has_value()) << optimized.error().to_string();
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  const std::array input{std::vector<std::int64_t>{4, 1}, std::vector<std::int64_t>{3, 2}};
  const std::size_t input_credit = resources.reserved_memory_bytes();
  auto rejected = optimized->instantiate(resources, sources(resources, input));
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(resources.reserved_memory_bytes(), input_credit);

  PhysicalExecutionCapabilities duplicate;
  duplicate.spill_sorts.push_back({.stage_index = 0U, .limits = spill_limits()});
  duplicate.spill_sorts.push_back({.stage_index = 0U, .limits = spill_limits()});
  auto invalid_capability = OptimizedPhysicalPipelinePlan::create(
      sort_plan(2U), statistics(1U, estimate(4U)), std::move(duplicate), policy);
  ASSERT_FALSE(invalid_capability.has_value());
  EXPECT_EQ(invalid_capability.error().code(), common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::query
