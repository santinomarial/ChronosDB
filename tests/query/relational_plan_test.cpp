#include "chronos/common/uuid.hpp"
#include "chronos/query/relational_plan.hpp"

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

class ChunkSource final : public PhysicalOperator {
public:
  explicit ChunkSource(AccountedVectorChunk chunk) : chunk_(std::move(chunk)) {}

  common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    if (!chunk_.has_value())
      return PhysicalOperatorStep::end();
    AccountedVectorChunk result = std::move(*chunk_);
    chunk_.reset();
    return PhysicalOperatorStep::chunk(std::move(result));
  }

private:
  std::optional<AccountedVectorChunk> chunk_;
};

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

template <typename Value>
[[nodiscard]] columnar::OwnedPhysicalColumn column(const schema::LogicalTypeKind kind,
                                                   const std::span<const Value> values) {
  static_assert(std::is_integral_v<Value>);
  columnar::ColumnVectorBuffers buffers;
  buffers.values.resize(values.size() * sizeof(Value));
  for (std::size_t row = 0U; row < values.size(); ++row) {
    using Unsigned = std::make_unsigned_t<Value>;
    const Unsigned bits = std::bit_cast<Unsigned>(values[row]);
    for (std::size_t byte = 0U; byte < sizeof(Value); ++byte) {
      buffers.values[row * sizeof(Value) + byte] =
          static_cast<std::byte>((bits >> (byte * 8U)) & Unsigned{0xffU});
    }
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = type(kind),
              .nullable = false,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = 0U},
             std::move(buffers))
      .value();
}

[[nodiscard]] columnar::OwnedPhysicalColumn uuid_column(const std::size_t rows) {
  columnar::ColumnVectorBuffers buffers;
  buffers.values.resize(rows * common::Uuid::kSize);
  return columnar::OwnedPhysicalColumn::create({.type = type(schema::LogicalTypeKind::kUuid),
                                                .nullable = false,
                                                .row_count = static_cast<std::uint32_t>(rows),
                                                .null_count = 0U},
                                               std::move(buffers))
      .value();
}

struct SourceRow {
  std::int64_t time;
  std::int64_t payload;
};

[[nodiscard]] AccountedVectorChunk source_chunk(const SourceRow row,
                                                const QueryResourceContext& resources) {
  constexpr std::array<std::int64_t, 1U> kKey{7};
  const std::array<std::int64_t, 1U> times{row.time};
  const std::array<std::int64_t, 1U> payloads{row.payload};
  constexpr std::array<std::uint64_t, 1U> kSequence{1U};
  constexpr std::array<std::uint32_t, 1U> kRow{0U};
  constexpr std::array<std::uint8_t, 1U> kOperation{1U};
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(column(schema::LogicalTypeKind::kInt64, std::span<const std::int64_t>{kKey}));
  columns.push_back(
      column(schema::LogicalTypeKind::kTimestampNs, std::span<const std::int64_t>{times}));
  columns.push_back(
      column(schema::LogicalTypeKind::kInt64, std::span<const std::int64_t>{payloads}));
  columns.push_back(uuid_column(1U));
  columns.push_back(
      column(schema::LogicalTypeKind::kUInt64, std::span<const std::uint64_t>{kSequence}));
  columns.push_back(column(schema::LogicalTypeKind::kUInt32, std::span<const std::uint32_t>{kRow}));
  columns.push_back(
      column(schema::LogicalTypeKind::kUInt8, std::span<const std::uint8_t>{kOperation}));
  VectorChunk chunk =
      VectorChunk::create(std::move(columns), VectorSelection::all(1U).value()).value();
  return AccountedVectorChunk::create(std::move(chunk), resources.reserve(4096U).value(), resources)
      .value();
}

[[nodiscard]] std::vector<PhysicalColumnShape> source_shape() {
  return {{.type = type(schema::LogicalTypeKind::kInt64), .nullable = false},
          {.type = type(schema::LogicalTypeKind::kTimestampNs), .nullable = false},
          {.type = type(schema::LogicalTypeKind::kInt64), .nullable = false},
          {.type = type(schema::LogicalTypeKind::kUuid), .nullable = false},
          {.type = type(schema::LogicalTypeKind::kUInt64), .nullable = false},
          {.type = type(schema::LogicalTypeKind::kUInt32), .nullable = false},
          {.type = type(schema::LogicalTypeKind::kUInt8), .nullable = false}};
}

[[nodiscard]] VectorAsofJoinDefinition
join_definition(const std::span<const PhysicalColumnShape> left_shape,
                const std::size_t left_key_ordinal, const std::size_t left_time_ordinal) {
  std::vector<VectorAsofColumnShape> left;
  std::vector<VectorAsofColumnShape> right;
  std::vector<std::size_t> left_outputs;
  left_outputs.reserve(left_shape.size());
  for (std::size_t ordinal = 0U; ordinal < left_shape.size(); ++ordinal) {
    const PhysicalColumnShape& shape = left_shape[ordinal];
    left.push_back({.type = shape.type, .nullable = shape.nullable});
    left_outputs.push_back(ordinal);
  }
  for (const PhysicalColumnShape& shape : source_shape())
    right.push_back({.type = shape.type, .nullable = shape.nullable});
  return {.left_input_columns = std::move(left),
          .right_input_columns = std::move(right),
          .equality_keys = {{.left_column_ordinal = left_key_ordinal, .right_column_ordinal = 0U}},
          .left_timestamp_column_ordinal = left_time_ordinal,
          .right_timestamp_column_ordinal = 1U,
          .right_physical_ordering_key_ordinals = {0U},
          .right_row_version_first_column_ordinal = 3U,
          .left_output_column_ordinals = std::move(left_outputs),
          .right_output_column_ordinals = {0U, 1U, 2U, 3U, 4U, 5U, 6U},
          .left_outer = true};
}

[[nodiscard]] PhysicalAsofPlan plan() {
  std::vector<PhysicalAsofPlanJoin> joins;
  const std::vector<PhysicalColumnShape> input_shape = source_shape();
  VectorAsofJoinDefinition definition = join_definition(input_shape, 0U, 1U);
  std::vector<PhysicalColumnShape> joined_shape;
  const std::vector<VectorAsofColumnShape> joined_shapes =
      vector_asof_join_output_shape(definition).value();
  joined_shape.reserve(joined_shapes.size());
  for (const VectorAsofColumnShape& shape : joined_shapes)
    joined_shape.push_back({.type = shape.type, .nullable = shape.nullable});
  joins.push_back({.left_preparation = PhysicalPipelinePlan::create(source_shape(), {}).value(),
                   .right_preparation = PhysicalPipelinePlan::create(source_shape(), {}).value(),
                   .definition = std::move(definition)});
  std::vector<PhysicalPipelineStage> final_stages;
  final_stages.emplace_back(ColumnSubsetStage{.column_ordinals = {2U, 9U}});
  return PhysicalAsofPlan::create(
             std::move(joins),
             PhysicalPipelinePlan::create(std::move(joined_shape), std::move(final_stages)).value())
      .value();
}

[[nodiscard]] PhysicalAsofPlan two_join_plan() {
  std::vector<PhysicalAsofPlanJoin> joins;
  const std::vector<PhysicalColumnShape> input_shape = source_shape();
  VectorAsofJoinDefinition first = join_definition(input_shape, 0U, 1U);
  std::vector<PhysicalColumnShape> first_output;
  const std::vector<VectorAsofColumnShape> first_shapes =
      vector_asof_join_output_shape(first).value();
  first_output.reserve(first_shapes.size());
  for (const VectorAsofColumnShape& shape : first_shapes)
    first_output.push_back({.type = shape.type, .nullable = shape.nullable});
  joins.push_back({.left_preparation = PhysicalPipelinePlan::create(source_shape(), {}).value(),
                   .right_preparation = PhysicalPipelinePlan::create(source_shape(), {}).value(),
                   .definition = std::move(first)});

  VectorAsofJoinDefinition second = join_definition(first_output, 7U, 8U);
  std::vector<PhysicalColumnShape> second_output;
  const std::vector<VectorAsofColumnShape> second_shapes =
      vector_asof_join_output_shape(second).value();
  second_output.reserve(second_shapes.size());
  for (const VectorAsofColumnShape& shape : second_shapes)
    second_output.push_back({.type = shape.type, .nullable = shape.nullable});
  joins.push_back({.left_preparation = PhysicalPipelinePlan::create(first_output, {}).value(),
                   .right_preparation = PhysicalPipelinePlan::create(source_shape(), {}).value(),
                   .definition = std::move(second)});
  std::vector<PhysicalPipelineStage> final_stages;
  final_stages.emplace_back(ColumnSubsetStage{.column_ordinals = {2U, 9U, 17U}});
  return PhysicalAsofPlan::create(
             std::move(joins),
             PhysicalPipelinePlan::create(std::move(second_output), std::move(final_stages))
                 .value())
      .value();
}

[[nodiscard]] std::int64_t read_i64(const VectorChunk& chunk, const std::size_t column_ordinal) {
  const common::ByteView bytes = chunk.cell({column_ordinal, 0U}).value().bytes().value();
  std::uint64_t bits = 0U;
  for (std::size_t byte = 0U; byte < bytes.size(); ++byte)
    bits |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(bytes[byte])) << (byte * 8U);
  return std::bit_cast<std::int64_t>(bits);
}

TEST(PhysicalAsofPlanTest, ValidatesInstantiatesAndHidesInternalColumns) {
  PhysicalAsofPlan configured = plan();
  EXPECT_EQ(configured.source_count(), 2U);
  EXPECT_EQ(configured.joins().size(), 1U);
  ASSERT_GT(configured.retained_configuration_bytes(), 0U);

  QueryResourceContext resources = QueryResourceContext::create(8U << 20U).value();
  std::vector<std::unique_ptr<PhysicalOperator>> sources;
  sources.push_back(
      std::make_unique<ChunkSource>(source_chunk({.time = 10, .payload = 100}, resources)));
  sources.push_back(
      std::make_unique<ChunkSource>(source_chunk({.time = 9, .payload = 200}, resources)));
  auto pipeline = configured.instantiate(std::move(sources));
  ASSERT_TRUE(pipeline.has_value()) << pipeline.error().message();
  auto step = (*pipeline)->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().message();
  ASSERT_EQ(step->kind(), PhysicalOperatorStepKind::kChunk);
  ASSERT_EQ(step->chunk()->chunk().column_count(), 2U);
  EXPECT_EQ(read_i64(step->chunk()->chunk(), 0U), 100);
  EXPECT_EQ(read_i64(step->chunk()->chunk(), 1U), 200);
  step = PhysicalOperatorStep::end();
  (*pipeline).reset();
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(PhysicalAsofPlanTest, RejectsHostileHandoffsLimitsAndSourceCounts) {
  std::vector<PhysicalAsofPlanJoin> joins;
  joins.push_back({.left_preparation = PhysicalPipelinePlan::create(source_shape(), {}).value(),
                   .right_preparation = PhysicalPipelinePlan::create(source_shape(), {}).value(),
                   .definition = join_definition(source_shape(), 0U, 1U)});
  std::vector<PhysicalColumnShape> wrong{
      {.type = type(schema::LogicalTypeKind::kInt64), .nullable = false}};
  auto invalid_plan = PhysicalAsofPlan::create(
      std::move(joins), PhysicalPipelinePlan::create(std::move(wrong), {}).value());
  ASSERT_FALSE(invalid_plan.has_value());
  EXPECT_EQ(invalid_plan.error().code(), common::StatusCode::kInvalidArgument);

  PhysicalAsofPlan configured = plan();
  std::vector<std::unique_ptr<PhysicalOperator>> absent;
  EXPECT_EQ(configured.instantiate(std::move(absent)).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(PhysicalAsofPlanTest, ExecutesCheckedLeftDeepPriorSourceReferences) {
  PhysicalAsofPlan configured = two_join_plan();
  ASSERT_EQ(configured.source_count(), 3U);
  QueryResourceContext resources = QueryResourceContext::create(8U << 20U).value();
  std::vector<std::unique_ptr<PhysicalOperator>> sources;
  sources.push_back(
      std::make_unique<ChunkSource>(source_chunk({.time = 10, .payload = 100}, resources)));
  sources.push_back(
      std::make_unique<ChunkSource>(source_chunk({.time = 9, .payload = 200}, resources)));
  sources.push_back(
      std::make_unique<ChunkSource>(source_chunk({.time = 8, .payload = 300}, resources)));
  auto pipeline = configured.instantiate(std::move(sources)).value();
  auto step = pipeline->next(resources).value();
  ASSERT_EQ(step.kind(), PhysicalOperatorStepKind::kChunk);
  ASSERT_EQ(step.chunk()->chunk().column_count(), 3U);
  EXPECT_EQ(read_i64(step.chunk()->chunk(), 0U), 100);
  EXPECT_EQ(read_i64(step.chunk()->chunk(), 1U), 200);
  EXPECT_EQ(read_i64(step.chunk()->chunk(), 2U), 300);
  step = PhysicalOperatorStep::end();
  pipeline.reset();
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

} // namespace
} // namespace chronos::query
