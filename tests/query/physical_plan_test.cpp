#include "chronos/common/status.hpp"
#include "chronos/query/physical_plan.hpp"
#include "chronos/schema/logical_type.hpp"

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <limits>
#include <memory>
#include <ranges>
#include <span>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

// -1 is SQL NULL, 0 is FALSE, and 1 is TRUE.
[[nodiscard]] columnar::OwnedPhysicalColumn bool_column(const std::span<const std::int8_t> values) {
  columnar::ColumnVectorBuffers buffers;
  const auto rows = static_cast<std::uint32_t>(values.size());
  buffers.validity.resize(columnar::bitmap_size(rows));
  buffers.values.resize(columnar::bitmap_size(rows));
  std::uint32_t null_count = 0U;
  for (std::uint32_t row = 0U; row < rows; ++row) {
    if (values[row] < 0) {
      ++null_count;
      continue;
    }
    buffers.validity[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
    if (values[row] > 0)
      buffers.values[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
  }
  return columnar::OwnedPhysicalColumn::create({.type = type(schema::LogicalTypeKind::kBool),
                                                .nullable = true,
                                                .row_count = rows,
                                                .null_count = null_count},
                                               std::move(buffers))
      .value();
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
             {.type = type(schema::LogicalTypeKind::kInt64),
              .nullable = false,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = 0U},
             std::move(buffers))
      .value();
}

[[nodiscard]] AccountedVectorChunk accounted_chunk(const QueryResourceContext& resources,
                                                   const std::span<const std::int64_t> values,
                                                   const std::span<const std::int8_t> predicates,
                                                   std::vector<std::uint32_t> selection) {
  QueryMemoryReservation reservation = resources.reserve(1'024U).value();
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(int64_column(values));
  columns.push_back(bool_column(predicates));
  VectorChunk chunk =
      VectorChunk::create(std::move(columns),
                          VectorSelection::from_indices(static_cast<std::uint32_t>(values.size()),
                                                        std::move(selection))
                              .value())
          .value();
  return AccountedVectorChunk::create(std::move(chunk), std::move(reservation), resources).value();
}

[[nodiscard]] AccountedVectorChunk one_column_chunk(const QueryResourceContext& resources,
                                                    const std::span<const std::int64_t> values) {
  QueryMemoryReservation reservation = resources.reserve(1'024U).value();
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(int64_column(values));
  VectorChunk chunk =
      VectorChunk::create(std::move(columns),
                          VectorSelection::all(static_cast<std::uint32_t>(values.size())).value())
          .value();
  return AccountedVectorChunk::create(std::move(chunk), std::move(reservation), resources).value();
}

class ChunkSource final : public PhysicalOperator {
public:
  explicit ChunkSource(std::vector<AccountedVectorChunk> chunks) : chunks_(std::move(chunks)) {}

  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next(const QueryResourceContext& resources) override {
    const common::Result<void> active = resources.check_cancelled();
    if (!active.has_value())
      return common::make_unexpected(active.error());
    if (next_ == chunks_.size())
      return PhysicalOperatorStep::end();
    return PhysicalOperatorStep::chunk(std::move(chunks_[next_++]));
  }

private:
  std::vector<AccountedVectorChunk> chunks_;
  std::size_t next_{};
};

[[nodiscard]] std::int64_t selected_int64(const VectorChunk& chunk,
                                          const std::size_t selected_row) {
  const common::ByteView bytes =
      chunk.cell({.column_ordinal = 0U, .selected_row = selected_row}).value().bytes().value();
  std::uint64_t bits = 0U;
  for (std::size_t byte = 0U; byte < bytes.size(); ++byte) {
    bits |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(bytes[byte])) << (byte * 8U);
  }
  return std::bit_cast<std::int64_t>(bits);
}

[[nodiscard]] std::vector<PhysicalColumnShape> input_shape() {
  return {{.type = type(schema::LogicalTypeKind::kInt64), .nullable = false},
          {.type = type(schema::LogicalTypeKind::kBool), .nullable = true}};
}

TEST(PhysicalPipelinePlanTest, PropagatesExactShapesAcrossOrderedStages) {
  auto plan = PhysicalPipelinePlan::create(
      input_shape(), {BooleanFilterStage{.predicate_column = 1U},
                      ColumnSubsetStage{.column_ordinals = {0U}}, LimitStage{.maximum_rows = 7U}});
  ASSERT_TRUE(plan.has_value());
  ASSERT_EQ(plan->input_columns().size(), 2U);
  ASSERT_EQ(plan->output_columns().size(), 1U);
  EXPECT_EQ(plan->output_columns()[0].type, type(schema::LogicalTypeKind::kInt64));
  EXPECT_FALSE(plan->output_columns()[0].nullable);
  EXPECT_EQ(plan->stages().size(), 3U);
  EXPECT_GT(plan->retained_configuration_bytes(), 0U);
  EXPECT_LE(plan->retained_configuration_bytes(), kDefaultPhysicalPipelineConfigurationByteLimit);

  auto cardinality_only =
      PhysicalPipelinePlan::create(input_shape(), {ColumnSubsetStage{.column_ordinals = {}}});
  ASSERT_TRUE(cardinality_only.has_value());
  EXPECT_TRUE(cardinality_only->output_columns().empty());
}

TEST(PhysicalPipelinePlanTest, ValidatesEveryStageAgainstItsCurrentShape) {
  EXPECT_EQ(
      PhysicalPipelinePlan::create(input_shape(), {BooleanFilterStage{.predicate_column = 2U}})
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);
  EXPECT_EQ(
      PhysicalPipelinePlan::create(input_shape(), {BooleanFilterStage{.predicate_column = 0U}})
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);
  EXPECT_EQ(
      PhysicalPipelinePlan::create(input_shape(), {ColumnSubsetStage{.column_ordinals = {0U, 0U}}})
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);
  EXPECT_EQ(
      PhysicalPipelinePlan::create(input_shape(), {ColumnSubsetStage{.column_ordinals = {1U, 0U}}})
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);
  EXPECT_EQ(
      PhysicalPipelinePlan::create(input_shape(), {ColumnSubsetStage{.column_ordinals = {2U}}})
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);
  EXPECT_EQ(
      PhysicalPipelinePlan::create(input_shape(), {ColumnSubsetStage{.column_ordinals = {0U}},
                                                   BooleanFilterStage{.predicate_column = 0U}})
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);
}

TEST(PhysicalPipelinePlanTest, EnforcesFiniteStageWidthAndRetainedConfigurationLimits) {
  EXPECT_EQ(PhysicalPipelinePlan::create(input_shape(), {},
                                         {.maximum_input_columns = 1U,
                                          .maximum_stages = 1U,
                                          .maximum_retained_configuration_bytes = 4'096U})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(PhysicalPipelinePlan::create(input_shape(), {LimitStage{1U}, LimitStage{2U}},
                                         {.maximum_input_columns = 2U,
                                          .maximum_stages = 1U,
                                          .maximum_retained_configuration_bytes = 4'096U})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);

  std::vector<PhysicalPipelineStage> retained;
  retained.reserve(64U);
  retained.emplace_back(LimitStage{1U});
  EXPECT_EQ(PhysicalPipelinePlan::create(input_shape(), std::move(retained),
                                         {.maximum_input_columns = 2U,
                                          .maximum_stages = 64U,
                                          .maximum_retained_configuration_bytes = 1U})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
}

TEST(PhysicalPipelinePlanTest, InstantiatesACompletePipelineAndReleasesUnusedInputAtLimit) {
  const auto resources = QueryResourceContext::create(8'192U).value();
  std::vector<AccountedVectorChunk> chunks;
  chunks.push_back(accounted_chunk(resources, std::vector<std::int64_t>{10, 11, 12},
                                   std::vector<std::int8_t>{1, 0, -1}, {0U, 1U, 2U}));
  chunks.push_back(accounted_chunk(resources, std::vector<std::int64_t>{20, 21, 22},
                                   std::vector<std::int8_t>{1, 1, 1}, {0U, 2U}));
  chunks.push_back(accounted_chunk(resources, std::vector<std::int64_t>{30, 31},
                                   std::vector<std::int8_t>{1, 1}, {0U, 1U}));
  ASSERT_EQ(resources.reserved_memory_bytes(), 3'072U);

  auto plan = PhysicalPipelinePlan::create(
                  input_shape(), {BooleanFilterStage{1U}, ColumnSubsetStage{{0U}}, LimitStage{2U}})
                  .value();
  auto pipeline = plan.instantiate(std::make_unique<ChunkSource>(std::move(chunks))).value();
  std::vector<std::int64_t> actual;
  for (;;) {
    auto step = pipeline->next(resources);
    ASSERT_TRUE(step.has_value());
    if (step->kind() == PhysicalOperatorStepKind::kEnd)
      break;
    ASSERT_NE(step->chunk(), nullptr);
    ASSERT_EQ(step->chunk()->chunk().column_count(), 1U);
    for (std::size_t row = 0U; row < step->chunk()->chunk().selected_row_count(); ++row)
      actual.push_back(selected_int64(step->chunk()->chunk(), row));
  }
  const std::vector<std::int64_t> expected{10, 20};
  EXPECT_EQ(actual, expected);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  EXPECT_FALSE(resources.is_cancelled());
  EXPECT_EQ(pipeline->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
}

TEST(PhysicalPipelinePlanTest, RejectsRuntimeSourceShapeMismatchAndReleasesCredit) {
  const auto plan = PhysicalPipelinePlan::create(input_shape(), {}).value();
  EXPECT_EQ(plan.instantiate({}).error().code(), common::StatusCode::kInvalidArgument);

  const auto resources = QueryResourceContext::create(4'096U).value();
  std::vector<AccountedVectorChunk> chunks;
  chunks.push_back(one_column_chunk(resources, std::vector<std::int64_t>{1, 2}));
  auto pipeline = plan.instantiate(std::make_unique<ChunkSource>(std::move(chunks))).value();
  const auto failed = pipeline->next(resources);
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(resources.is_cancelled());
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(PhysicalPipelinePlanTest, RejectsRuntimeTypeNullabilityAndQueryIdentityMismatch) {
  {
    const auto resources = QueryResourceContext::create(4'096U).value();
    std::vector<AccountedVectorChunk> chunks;
    chunks.push_back(accounted_chunk(resources, std::vector<std::int64_t>{1},
                                     std::vector<std::int8_t>{1}, {0U}));
    auto plan = PhysicalPipelinePlan::create(
                    {{.type = type(schema::LogicalTypeKind::kBool), .nullable = false},
                     {.type = type(schema::LogicalTypeKind::kBool), .nullable = true}},
                    {})
                    .value();
    auto pipeline = plan.instantiate(std::make_unique<ChunkSource>(std::move(chunks))).value();
    EXPECT_EQ(pipeline->next(resources).error().code(), common::StatusCode::kInvalidArgument);
    EXPECT_TRUE(resources.is_cancelled());
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  {
    const auto resources = QueryResourceContext::create(4'096U).value();
    std::vector<AccountedVectorChunk> chunks;
    chunks.push_back(accounted_chunk(resources, std::vector<std::int64_t>{1},
                                     std::vector<std::int8_t>{1}, {0U}));
    auto plan = PhysicalPipelinePlan::create(
                    {{.type = type(schema::LogicalTypeKind::kInt64), .nullable = true},
                     {.type = type(schema::LogicalTypeKind::kBool), .nullable = true}},
                    {})
                    .value();
    auto pipeline = plan.instantiate(std::make_unique<ChunkSource>(std::move(chunks))).value();
    EXPECT_EQ(pipeline->next(resources).error().code(), common::StatusCode::kInvalidArgument);
    EXPECT_TRUE(resources.is_cancelled());
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  {
    const auto owner = QueryResourceContext::create(4'096U).value();
    const auto impostor = QueryResourceContext::create(4'096U).value();
    std::vector<AccountedVectorChunk> chunks;
    chunks.push_back(
        accounted_chunk(owner, std::vector<std::int64_t>{1}, std::vector<std::int8_t>{1}, {0U}));
    auto plan = PhysicalPipelinePlan::create(input_shape(), {}).value();
    auto pipeline = plan.instantiate(std::make_unique<ChunkSource>(std::move(chunks))).value();
    EXPECT_EQ(pipeline->next(impostor).error().code(), common::StatusCode::kInvalidArgument);
    EXPECT_TRUE(impostor.is_cancelled());
    EXPECT_EQ(owner.reserved_memory_bytes(), 0U);
  }
}

TEST(PhysicalPipelinePlanDifferentialTest,
     MatchesScalarModelAcrossDeterministicPlansSelectionsAndChunkBoundaries) {
  std::uint32_t state = 0x6d2b'79f5U;
  for (std::size_t example = 0U; example < 128U; ++example) {
    const std::uint64_t maximum_rows = example % 23U;
    const auto resources = QueryResourceContext::create(std::size_t{256U} * 1'024U).value();
    std::vector<AccountedVectorChunk> chunks;
    std::vector<std::int64_t> expected;
    const std::size_t chunk_count = (example % 11U) + 1U;
    for (std::size_t chunk_index = 0U; chunk_index < chunk_count; ++chunk_index) {
      state = state * 1'664'525U + 1'013'904'223U;
      const std::uint32_t rows = (state % 19U) + 1U;
      std::vector<std::int64_t> values(rows);
      std::vector<std::int8_t> predicates(rows);
      std::vector<std::uint32_t> selection;
      for (std::uint32_t row = 0U; row < rows; ++row) {
        state = state * 1'664'525U + 1'013'904'223U;
        values[row] = static_cast<std::int64_t>(state) -
                      static_cast<std::int64_t>(std::numeric_limits<std::uint32_t>::max() / 2U);
        predicates[row] = static_cast<std::int8_t>(static_cast<std::int32_t>(state % 3U) - 1);
        if ((state & 3U) != 0U) {
          selection.push_back(row);
          if (predicates[row] == 1 && expected.size() < maximum_rows)
            expected.push_back(values[row]);
        }
      }
      chunks.push_back(accounted_chunk(resources, values, predicates, std::move(selection)));
    }

    const bool subset_before_limit = (example & 1U) != 0U;
    std::vector<PhysicalPipelineStage> stages;
    stages.emplace_back(BooleanFilterStage{1U});
    if (subset_before_limit)
      stages.emplace_back(ColumnSubsetStage{{0U}});
    stages.emplace_back(LimitStage{maximum_rows});
    if (!subset_before_limit)
      stages.emplace_back(ColumnSubsetStage{{0U}});
    auto plan = PhysicalPipelinePlan::create(input_shape(), std::move(stages)).value();
    auto pipeline = plan.instantiate(std::make_unique<ChunkSource>(std::move(chunks))).value();

    std::vector<std::int64_t> actual;
    for (;;) {
      auto step = pipeline->next(resources);
      ASSERT_TRUE(step.has_value());
      if (step->kind() == PhysicalOperatorStepKind::kEnd)
        break;
      ASSERT_NE(step->chunk(), nullptr);
      for (std::size_t row = 0U; row < step->chunk()->chunk().selected_row_count(); ++row)
        actual.push_back(selected_int64(step->chunk()->chunk(), row));
    }
    EXPECT_EQ(actual, expected) << "example=" << example;
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U) << "example=" << example;
    EXPECT_FALSE(resources.is_cancelled()) << "example=" << example;
  }
}

} // namespace
} // namespace chronos::query
