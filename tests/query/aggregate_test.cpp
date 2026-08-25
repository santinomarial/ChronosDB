#include "chronos/query/aggregate.hpp"
#include "chronos/query/column_output.hpp"
#include "chronos/query/distributed_vector_grouped_aggregate_exchange.hpp"
#include "chronos/query/physical_plan.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

void append_u32(std::vector<std::byte>& destination, const std::uint32_t value) {
  for (std::size_t byte = 0U; byte < sizeof(value); ++byte)
    destination.push_back(static_cast<std::byte>((value >> (byte * 8U)) & 0xffU));
}

[[nodiscard]] std::size_t signed_width(const schema::LogicalTypeKind kind) {
  switch (kind) {
  case schema::LogicalTypeKind::kInt8:
    return 1U;
  case schema::LogicalTypeKind::kInt16:
    return 2U;
  case schema::LogicalTypeKind::kInt32:
    return 4U;
  case schema::LogicalTypeKind::kInt64:
    return 8U;
  default:
    return 0U;
  }
}

[[nodiscard]] columnar::OwnedPhysicalColumn
signed_column(const schema::LogicalTypeKind kind,
              const std::span<const std::optional<std::int64_t>> values,
              const bool nullable = true) {
  columnar::ColumnVectorBuffers buffers;
  if (nullable)
    buffers.validity.resize(columnar::bitmap_size(static_cast<std::uint32_t>(values.size())));
  const std::size_t width = signed_width(kind);
  buffers.values.resize(values.size() * width);
  std::uint32_t null_count = 0U;
  for (std::size_t row = 0U; row < values.size(); ++row) {
    if (!values[row].has_value()) {
      ++null_count;
      continue;
    }
    if (nullable)
      buffers.validity[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
    const std::uint64_t bits = std::bit_cast<std::uint64_t>(
        values[row].value()); // NOLINT(bugprone-unchecked-optional-access)
    for (std::size_t byte = 0U; byte < width; ++byte) {
      buffers.values[row * width + byte] = static_cast<std::byte>((bits >> (byte * 8U)) & 0xffU);
    }
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = type(kind),
              .nullable = nullable,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = null_count},
             std::move(buffers))
      .value();
}

[[nodiscard]] columnar::OwnedPhysicalColumn
float64_column(const std::span<const std::optional<double>> values) {
  columnar::ColumnVectorBuffers buffers;
  buffers.validity.resize(columnar::bitmap_size(static_cast<std::uint32_t>(values.size())));
  buffers.values.resize(values.size() * sizeof(double));
  std::uint32_t null_count = 0U;
  for (std::size_t row = 0U; row < values.size(); ++row) {
    if (!values[row].has_value()) {
      ++null_count;
      continue;
    }
    buffers.validity[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
    const std::uint64_t bits = std::bit_cast<std::uint64_t>(
        values[row].value()); // NOLINT(bugprone-unchecked-optional-access)
    for (std::size_t byte = 0U; byte < sizeof(bits); ++byte) {
      buffers.values[row * sizeof(bits) + byte] =
          static_cast<std::byte>((bits >> (byte * 8U)) & 0xffU);
    }
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = type(schema::LogicalTypeKind::kFloat64),
              .nullable = true,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = null_count},
             std::move(buffers))
      .value();
}

[[nodiscard]] columnar::OwnedPhysicalColumn
variable_column(const schema::LogicalTypeKind kind,
                const std::span<const std::optional<std::string_view>> values) {
  columnar::ColumnVectorBuffers buffers;
  buffers.validity.resize(columnar::bitmap_size(static_cast<std::uint32_t>(values.size())));
  append_u32(buffers.offsets, 0U);
  std::uint32_t null_count = 0U;
  for (std::size_t row = 0U; row < values.size(); ++row) {
    if (!values[row].has_value()) {
      ++null_count;
    } else {
      buffers.validity[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
      for (const char byte : values[row].value()) // NOLINT(bugprone-unchecked-optional-access)
        buffers.values.push_back(static_cast<std::byte>(byte));
    }
    append_u32(buffers.offsets, static_cast<std::uint32_t>(buffers.values.size()));
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = type(kind),
              .nullable = true,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = null_count},
             std::move(buffers))
      .value();
}

[[nodiscard]] columnar::OwnedPhysicalColumn
string_column(const std::span<const std::optional<std::string_view>> values) {
  return variable_column(schema::LogicalTypeKind::kString, values);
}

[[nodiscard]] columnar::OwnedPhysicalColumn
uint64_column(const std::span<const std::optional<std::uint64_t>> values) {
  columnar::ColumnVectorBuffers buffers;
  buffers.validity.resize(columnar::bitmap_size(static_cast<std::uint32_t>(values.size())));
  buffers.values.resize(values.size() * sizeof(std::uint64_t));
  std::uint32_t null_count = 0U;
  for (std::size_t row = 0U; row < values.size(); ++row) {
    if (!values[row].has_value()) {
      ++null_count;
      continue;
    }
    buffers.validity[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
    const std::uint64_t present = values[row].value(); // NOLINT(bugprone-unchecked-optional-access)
    for (std::size_t byte = 0U; byte < sizeof(std::uint64_t); ++byte) {
      buffers.values[row * sizeof(std::uint64_t) + byte] =
          static_cast<std::byte>((present >> (byte * 8U)) & 0xffU);
    }
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = type(schema::LogicalTypeKind::kUInt64),
              .nullable = true,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = null_count},
             std::move(buffers))
      .value();
}

[[nodiscard]] Decimal128Value decimal_value(const std::int64_t coefficient) {
  Decimal128Value value;
  value.coefficient.fill(coefficient < 0 ? std::byte{0xff} : std::byte{0});
  const std::uint64_t bits = std::bit_cast<std::uint64_t>(coefficient);
  for (std::size_t byte = 0U; byte < sizeof(bits); ++byte)
    value.coefficient[byte] = static_cast<std::byte>((bits >> (byte * 8U)) & 0xffU);
  return value;
}

[[nodiscard]] columnar::OwnedPhysicalColumn
decimal_column(const schema::LogicalType decimal_type,
               const std::span<const std::optional<Decimal128Value>> values) {
  columnar::ColumnVectorBuffers buffers;
  buffers.validity.resize(columnar::bitmap_size(static_cast<std::uint32_t>(values.size())));
  buffers.values.resize(values.size() * Decimal128Value{}.coefficient.size());
  std::uint32_t null_count = 0U;
  for (std::size_t row = 0U; row < values.size(); ++row) {
    if (!values[row].has_value()) {
      ++null_count;
      continue;
    }
    buffers.validity[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
    const Decimal128Value& value =
        values[row].value(); // NOLINT(bugprone-unchecked-optional-access)
    std::copy(value.coefficient.begin(), value.coefficient.end(),
              buffers.values.begin() + static_cast<std::ptrdiff_t>(row * value.coefficient.size()));
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = decimal_type,
              .nullable = true,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = null_count},
             std::move(buffers))
      .value();
}

[[nodiscard]] AccountedVectorChunk
accounted_chunk(const QueryResourceContext& resources,
                std::vector<columnar::OwnedPhysicalColumn> columns,
                std::vector<std::uint32_t> selection = {}) {
  const std::uint32_t rows = columns.front().row_count();
  VectorSelection selected =
      selection.empty() ? VectorSelection::all(rows).value()
                        : VectorSelection::from_indices(rows, std::move(selection)).value();
  VectorChunk chunk = VectorChunk::create(std::move(columns), std::move(selected)).value();
  const std::size_t charge = chunk.retained_buffer_bytes() + 1'024U;
  return AccountedVectorChunk::create(std::move(chunk), resources.reserve(charge).value(),
                                      resources)
      .value();
}

[[nodiscard]] AccountedVectorChunk accounted_chunk(const QueryResourceContext& resources,
                                                   columnar::OwnedPhysicalColumn column,
                                                   std::vector<std::uint32_t> selection = {}) {
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(std::move(column));
  return accounted_chunk(resources, std::move(columns), std::move(selection));
}

class ManyChunkSource final : public PhysicalOperator {
public:
  explicit ManyChunkSource(std::vector<AccountedVectorChunk> chunks) : chunks_(std::move(chunks)) {}

  [[nodiscard]] common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    if (next_ == chunks_.size())
      return PhysicalOperatorStep::end();
    return PhysicalOperatorStep::chunk(std::move(chunks_[next_++]));
  }

private:
  std::vector<AccountedVectorChunk> chunks_;
  std::size_t next_{};
};

class EmptySource final : public PhysicalOperator {
public:
  [[nodiscard]] common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    return PhysicalOperatorStep::end();
  }
};

[[nodiscard]] VectorAggregateDefinition aggregate(const VectorAggregateOperation operation,
                                                  const schema::LogicalTypeKind kind,
                                                  const bool nullable = true) {
  return {.operation = operation,
          .input =
              VectorAggregateInput{.column_ordinal = 0U, .type = type(kind), .nullable = nullable}};
}

[[nodiscard]] ScalarValue cell(const VectorChunk& chunk, const std::size_t column) {
  const columnar::PhysicalColumnView* physical = chunk.column(column);
  EXPECT_NE(physical, nullptr);
  return ScalarValue::from_column_cell(
             physical->type(), chunk.cell({.column_ordinal = column, .selected_row = 0U}).value())
      .value();
}

void accumulate_column(MergeableVectorAggregateState& state,
                       const columnar::OwnedPhysicalColumn& column,
                       const QueryResourceContext& resources) {
  for (std::uint32_t row = 0U; row < column.row_count(); ++row)
    ASSERT_TRUE(state.accumulate_cell(column.cell(row).value(), resources).has_value());
}

TEST(MergeableVectorAggregateStateTest, MergesEveryNumericOperationWithoutFinalizingPartials) {
  const std::array<std::optional<std::int64_t>, 3U> left_values{1, std::nullopt, 2};
  const std::array<std::optional<std::int64_t>, 2U> right_values{3, 4};
  const auto left_column = signed_column(schema::LogicalTypeKind::kInt64, left_values);
  const auto right_column = signed_column(schema::LogicalTypeKind::kInt64, right_values);
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();

  const std::array operations{
      VectorAggregateOperation::kCount,         VectorAggregateOperation::kSum,
      VectorAggregateOperation::kAverage,       VectorAggregateOperation::kMinimum,
      VectorAggregateOperation::kMaximum,       VectorAggregateOperation::kVariancePopulation,
      VectorAggregateOperation::kVarianceSample};
  for (const VectorAggregateOperation operation : operations) {
    const VectorAggregateDefinition definition =
        aggregate(operation, schema::LogicalTypeKind::kInt64);
    auto left = MergeableVectorAggregateState::create(definition).value();
    auto right = MergeableVectorAggregateState::create(definition).value();
    accumulate_column(left, left_column, resources);
    accumulate_column(right, right_column, resources);
    ASSERT_TRUE(left.merge(right, resources).has_value());
    auto result = std::move(left).take_result().value();
    if (operation == VectorAggregateOperation::kCount) {
      EXPECT_EQ(std::get<std::int64_t>(result.storage()), 4);
    } else if (operation == VectorAggregateOperation::kSum) {
      EXPECT_EQ(std::get<std::int64_t>(result.storage()), 10);
    } else if (operation == VectorAggregateOperation::kAverage) {
      EXPECT_DOUBLE_EQ(std::get<double>(result.storage()), 2.5);
    } else if (operation == VectorAggregateOperation::kMinimum) {
      EXPECT_EQ(std::get<std::int64_t>(result.storage()), 1);
    } else if (operation == VectorAggregateOperation::kMaximum) {
      EXPECT_EQ(std::get<std::int64_t>(result.storage()), 4);
    } else if (operation == VectorAggregateOperation::kVariancePopulation) {
      EXPECT_DOUBLE_EQ(std::get<double>(result.storage()), 1.25);
    } else {
      EXPECT_DOUBLE_EQ(std::get<double>(result.storage()), 5.0 / 3.0);
    }
  }

  auto left_count = MergeableVectorAggregateState::create(
                        {.operation = VectorAggregateOperation::kCountStar, .input = std::nullopt})
                        .value();
  auto right_count = MergeableVectorAggregateState::create(
                         {.operation = VectorAggregateOperation::kCountStar, .input = std::nullopt})
                         .value();
  EXPECT_TRUE(left_count.accumulate_count_star().has_value());
  EXPECT_TRUE(right_count.accumulate_count_star().has_value());
  EXPECT_TRUE(right_count.accumulate_count_star().has_value());
  EXPECT_TRUE(left_count.merge(right_count, resources).has_value());
  EXPECT_EQ(std::get<std::int64_t>(std::move(left_count).take_result()->storage()), 3);
  // The rvalue-qualified call finalizes without move-constructing the object; exercising the
  // documented terminal-state contract therefore intentionally reuses the same storage.
  // NOLINTNEXTLINE(bugprone-use-after-move)
  EXPECT_EQ(std::move(left_count).take_result().error().code(),
            common::StatusCode::kInvalidArgument);
  // NOLINTNEXTLINE(bugprone-use-after-move)
  EXPECT_EQ(left_count.accumulate_count_star().error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(right_count.merge(left_count, resources).error().code(),
            common::StatusCode::kInvalidArgument);

  auto move_source = MergeableVectorAggregateState::create(
                         {.operation = VectorAggregateOperation::kCountStar, .input = std::nullopt})
                         .value();
  ASSERT_TRUE(move_source.accumulate_count_star().has_value());
  auto move_destination = std::move(move_source);
  // Reuse is intentional: this type strengthens the usual moved-from guarantee to a defined
  // terminal state so misuse fails deterministically.
  // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
  EXPECT_EQ(move_source.accumulate_count_star().error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(std::get<std::int64_t>(std::move(move_destination).take_result()->storage()), 1);
}

TEST(MergeableVectorAggregateStateTest, MergesExactAndVariableWidthStateUnderBounds) {
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  const std::array<std::optional<std::uint64_t>, 2U> left_unsigned{
      std::numeric_limits<std::uint64_t>::max(), std::nullopt};
  const std::array<std::optional<std::uint64_t>, 1U> right_unsigned{1U};
  auto left_sum = MergeableVectorAggregateState::create(
                      aggregate(VectorAggregateOperation::kSum, schema::LogicalTypeKind::kUInt64))
                      .value();
  auto right_sum = MergeableVectorAggregateState::create(
                       aggregate(VectorAggregateOperation::kSum, schema::LogicalTypeKind::kUInt64))
                       .value();
  const auto left_unsigned_column = uint64_column(left_unsigned);
  const auto right_unsigned_column = uint64_column(right_unsigned);
  accumulate_column(left_sum, left_unsigned_column, resources);
  accumulate_column(right_sum, right_unsigned_column, resources);
  EXPECT_TRUE(left_sum.merge(right_sum, resources).has_value());
  EXPECT_EQ(std::move(left_sum).take_result().error().code(), common::StatusCode::kOutOfRange);

  const schema::LogicalType decimal = schema::LogicalType::decimal(10U, 2U).value();
  const VectorAggregateDefinition decimal_sum{
      .operation = VectorAggregateOperation::kSum,
      .input = VectorAggregateInput{.column_ordinal = 0U, .type = decimal, .nullable = true}};
  const std::array<std::optional<Decimal128Value>, 1U> left_decimals{decimal_value(100)};
  const std::array<std::optional<Decimal128Value>, 1U> right_decimals{decimal_value(-25)};
  auto left_decimal = MergeableVectorAggregateState::create(decimal_sum).value();
  auto right_decimal = MergeableVectorAggregateState::create(decimal_sum).value();
  const auto left_decimal_column = decimal_column(decimal, left_decimals);
  const auto right_decimal_column = decimal_column(decimal, right_decimals);
  accumulate_column(left_decimal, left_decimal_column, resources);
  accumulate_column(right_decimal, right_decimal_column, resources);
  ASSERT_TRUE(left_decimal.merge(right_decimal, resources).has_value());
  EXPECT_EQ(std::get<Decimal128Value>(std::move(left_decimal).take_result()->storage()),
            decimal_value(75));

  const std::array<std::optional<std::string_view>, 1U> high{"zeta"};
  const std::array<std::optional<std::string_view>, 1U> low{"alpha"};
  auto minimum =
      MergeableVectorAggregateState::create(
          aggregate(VectorAggregateOperation::kMinimum, schema::LogicalTypeKind::kString), 5U)
          .value();
  auto candidate =
      MergeableVectorAggregateState::create(
          aggregate(VectorAggregateOperation::kMinimum, schema::LogicalTypeKind::kString), 5U)
          .value();
  const auto high_column = string_column(high);
  const auto low_column = string_column(low);
  accumulate_column(minimum, high_column, resources);
  accumulate_column(candidate, low_column, resources);
  ASSERT_TRUE(minimum.merge(candidate, resources).has_value());
  EXPECT_EQ(std::get<std::string>(std::move(minimum).take_result()->storage()), "alpha");

  auto too_small =
      MergeableVectorAggregateState::create(
          aggregate(VectorAggregateOperation::kMinimum, schema::LogicalTypeKind::kString), 4U)
          .value();
  EXPECT_EQ(too_small.merge(candidate, resources).error().code(),
            common::StatusCode::kResourceExhausted);
  auto mismatched =
      MergeableVectorAggregateState::create(
          aggregate(VectorAggregateOperation::kMaximum, schema::LogicalTypeKind::kString), 5U)
          .value();
  EXPECT_EQ(mismatched.merge(candidate, resources).error().code(),
            common::StatusCode::kInvalidArgument);

  QueryResourceContext foreign = QueryResourceContext::create(1U << 20U).value();
  auto owned =
      MergeableVectorAggregateState::create(
          aggregate(VectorAggregateOperation::kMinimum, schema::LogicalTypeKind::kString), 5U)
          .value();
  accumulate_column(owned, high_column, resources);
  EXPECT_EQ(owned.accumulate_cell(low_column.cell(0U).value(), foreign).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(std::get<std::string>(std::move(owned).take_result()->storage()), "zeta");
}

TEST(UngroupedAggregateOperatorTest, AccumulatesEveryOperationAcrossChunksAndSelections) {
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  const std::array<std::optional<std::int64_t>, 3> first{1, std::nullopt, 3};
  const std::array<std::optional<std::int64_t>, 3> second{5, 100, 7};
  std::vector<AccountedVectorChunk> chunks;
  chunks.push_back(
      accounted_chunk(resources, signed_column(schema::LogicalTypeKind::kInt64, first)));
  chunks.push_back(
      accounted_chunk(resources, signed_column(schema::LogicalTypeKind::kInt64, second), {0U, 2U}));

  std::vector<VectorAggregateDefinition> definitions{
      {.operation = VectorAggregateOperation::kCountStar, .input = std::nullopt},
      aggregate(VectorAggregateOperation::kCount, schema::LogicalTypeKind::kInt64),
      aggregate(VectorAggregateOperation::kSum, schema::LogicalTypeKind::kInt64),
      aggregate(VectorAggregateOperation::kAverage, schema::LogicalTypeKind::kInt64),
      aggregate(VectorAggregateOperation::kMinimum, schema::LogicalTypeKind::kInt64),
      aggregate(VectorAggregateOperation::kMaximum, schema::LogicalTypeKind::kInt64),
      aggregate(VectorAggregateOperation::kVariancePopulation, schema::LogicalTypeKind::kInt64),
      aggregate(VectorAggregateOperation::kVarianceSample, schema::LogicalTypeKind::kInt64)};
  auto input = std::make_unique<ManyChunkSource>(std::move(chunks));
  auto operator_ = UngroupedAggregateOperator::create(std::move(input), definitions);
  ASSERT_TRUE(operator_.has_value()) << operator_.error().to_string();

  auto step = (*operator_)->next(resources).value();
  ASSERT_EQ(step.kind(), PhysicalOperatorStepKind::kChunk);
  const VectorChunk& result = step.chunk()->chunk();
  ASSERT_EQ(result.physical_row_count(), 1U);
  ASSERT_EQ(result.selected_row_count(), 1U);
  ASSERT_EQ(result.column_count(), 8U);
  EXPECT_FALSE(result.column(0U)->nullable());
  EXPECT_FALSE(result.column(1U)->nullable());
  for (std::size_t column = 2U; column < result.column_count(); ++column) {
    EXPECT_TRUE(result.column(column)->nullable());
    EXPECT_EQ(result.column(column)->null_count(), 0U);
  }
  EXPECT_EQ(std::get<std::int64_t>(cell(result, 0U).storage()), 5);
  EXPECT_EQ(std::get<std::int64_t>(cell(result, 1U).storage()), 4);
  EXPECT_EQ(std::get<std::int64_t>(cell(result, 2U).storage()), 16);
  EXPECT_DOUBLE_EQ(std::get<double>(cell(result, 3U).storage()), 4.0);
  EXPECT_EQ(std::get<std::int64_t>(cell(result, 4U).storage()), 1);
  EXPECT_EQ(std::get<std::int64_t>(cell(result, 5U).storage()), 7);
  EXPECT_DOUBLE_EQ(std::get<double>(cell(result, 6U).storage()), 5.0);
  EXPECT_DOUBLE_EQ(std::get<double>(cell(result, 7U).storage()), 20.0 / 3.0);
  EXPECT_EQ((*operator_)->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
}

TEST(UngroupedAggregateOperatorTest, ImplementsEmptyInputAndFloatingNanSemantics) {
  std::vector<VectorAggregateDefinition> empty_definitions{
      {.operation = VectorAggregateOperation::kCountStar, .input = std::nullopt},
      aggregate(VectorAggregateOperation::kSum, schema::LogicalTypeKind::kInt64),
      aggregate(VectorAggregateOperation::kVarianceSample, schema::LogicalTypeKind::kInt64)};
  QueryResourceContext empty_resources = QueryResourceContext::create(1U << 20U).value();
  auto empty =
      UngroupedAggregateOperator::create(std::make_unique<EmptySource>(), empty_definitions)
          .value();
  auto empty_step = empty->next(empty_resources).value();
  EXPECT_FALSE(empty_step.chunk()->chunk().column(0U)->nullable());
  EXPECT_TRUE(empty_step.chunk()->chunk().column(1U)->nullable());
  EXPECT_TRUE(empty_step.chunk()->chunk().column(2U)->nullable());
  EXPECT_EQ(std::get<std::int64_t>(cell(empty_step.chunk()->chunk(), 0U).storage()), 0);
  EXPECT_TRUE(cell(empty_step.chunk()->chunk(), 1U).is_null());
  EXPECT_TRUE(cell(empty_step.chunk()->chunk(), 2U).is_null());

  const double nan = std::numeric_limits<double>::quiet_NaN();
  const std::array<std::optional<double>, 3> values{1.0, nan, 2.0};
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  std::vector<AccountedVectorChunk> chunks;
  chunks.push_back(accounted_chunk(resources, float64_column(values)));
  std::vector<VectorAggregateDefinition> definitions{
      aggregate(VectorAggregateOperation::kSum, schema::LogicalTypeKind::kFloat64),
      aggregate(VectorAggregateOperation::kAverage, schema::LogicalTypeKind::kFloat64),
      aggregate(VectorAggregateOperation::kMinimum, schema::LogicalTypeKind::kFloat64),
      aggregate(VectorAggregateOperation::kMaximum, schema::LogicalTypeKind::kFloat64),
      aggregate(VectorAggregateOperation::kVariancePopulation, schema::LogicalTypeKind::kFloat64)};
  auto operator_ = UngroupedAggregateOperator::create(
                       std::make_unique<ManyChunkSource>(std::move(chunks)), definitions)
                       .value();
  auto step = operator_->next(resources).value();
  EXPECT_TRUE(std::isnan(std::get<double>(cell(step.chunk()->chunk(), 0U).storage())));
  EXPECT_TRUE(std::isnan(std::get<double>(cell(step.chunk()->chunk(), 1U).storage())));
  EXPECT_DOUBLE_EQ(std::get<double>(cell(step.chunk()->chunk(), 2U).storage()), 1.0);
  EXPECT_TRUE(std::isnan(std::get<double>(cell(step.chunk()->chunk(), 3U).storage())));
  EXPECT_TRUE(std::isnan(std::get<double>(cell(step.chunk()->chunk(), 4U).storage())));
}

TEST(UngroupedAggregateOperatorTest,
     ComputesByteOrderedVariableExtremaAcrossChunksAndAccountsOwnedPayloads) {
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  const std::array<std::optional<std::string_view>, 4> first{"zeta", std::nullopt, "alpha", ""};
  const std::array<std::optional<std::string_view>, 3> second{"alphabet", "omega", "ignored"};
  std::vector<AccountedVectorChunk> chunks;
  const auto append_chunk = [&resources,
                             &chunks](const std::span<const std::optional<std::string_view>> values,
                                      std::vector<std::uint32_t> selection) {
    std::vector<columnar::OwnedPhysicalColumn> columns;
    columns.push_back(variable_column(schema::LogicalTypeKind::kString, values));
    columns.push_back(variable_column(schema::LogicalTypeKind::kSymbol, values));
    columns.push_back(variable_column(schema::LogicalTypeKind::kBinary, values));
    chunks.push_back(accounted_chunk(resources, std::move(columns), std::move(selection)));
  };
  append_chunk(first, {});
  append_chunk(second, {0U, 1U});
  std::vector<VectorAggregateDefinition> definitions;
  for (std::size_t column = 0U; column < 3U; ++column) {
    const schema::LogicalTypeKind kind = column == 0U   ? schema::LogicalTypeKind::kString
                                         : column == 1U ? schema::LogicalTypeKind::kSymbol
                                                        : schema::LogicalTypeKind::kBinary;
    definitions.push_back({.operation = VectorAggregateOperation::kMinimum,
                           .input = VectorAggregateInput{
                               .column_ordinal = column, .type = type(kind), .nullable = true}});
    definitions.push_back({.operation = VectorAggregateOperation::kMaximum,
                           .input = VectorAggregateInput{
                               .column_ordinal = column, .type = type(kind), .nullable = true}});
  }
  auto aggregate = UngroupedAggregateOperator::create(
                       std::make_unique<ManyChunkSource>(std::move(chunks)), definitions)
                       .value();
  auto step = aggregate->next(resources).value();
  const VectorChunk& output = step.chunk()->chunk();
  EXPECT_EQ(std::get<std::string>(cell(output, 0U).storage()), "");
  EXPECT_EQ(std::get<std::string>(cell(output, 1U).storage()), "zeta");
  EXPECT_EQ(std::get<std::string>(cell(output, 2U).storage()), "");
  EXPECT_EQ(std::get<std::string>(cell(output, 3U).storage()), "zeta");
  EXPECT_TRUE(std::get<std::vector<std::byte>>(cell(output, 4U).storage()).empty());
  const std::vector<std::byte> expected_max{std::byte{'z'}, std::byte{'e'}, std::byte{'t'},
                                            std::byte{'a'}};
  EXPECT_EQ(std::get<std::vector<std::byte>>(cell(output, 5U).storage()), expected_max);
  step = PhysicalOperatorStep::end();
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  aggregate.reset();
}

TEST(UngroupedAggregateOperatorTest, PreservesExactUnsignedAndDecimalSums) {
  QueryResourceContext unsigned_resources = QueryResourceContext::create(1U << 20U).value();
  const std::array<std::optional<std::uint64_t>, 3> unsigned_values{2U, std::nullopt, 3U};
  std::vector<AccountedVectorChunk> unsigned_chunks;
  unsigned_chunks.push_back(accounted_chunk(unsigned_resources, uint64_column(unsigned_values)));
  auto unsigned_sum =
      UngroupedAggregateOperator::create(
          std::make_unique<ManyChunkSource>(std::move(unsigned_chunks)),
          {aggregate(VectorAggregateOperation::kSum, schema::LogicalTypeKind::kUInt64)})
          .value();
  auto unsigned_step = unsigned_sum->next(unsigned_resources).value();
  EXPECT_EQ(std::get<std::uint64_t>(cell(unsigned_step.chunk()->chunk(), 0U).storage()), 5U);

  const schema::LogicalType decimal = schema::LogicalType::decimal(8U, 2U).value();
  const std::array<std::optional<Decimal128Value>, 3> decimal_values{
      decimal_value(-125), std::nullopt, decimal_value(275)};
  QueryResourceContext decimal_resources = QueryResourceContext::create(1U << 20U).value();
  std::vector<AccountedVectorChunk> decimal_chunks;
  decimal_chunks.push_back(
      accounted_chunk(decimal_resources, decimal_column(decimal, decimal_values)));
  std::vector<VectorAggregateDefinition> definitions{
      {.operation = VectorAggregateOperation::kSum,
       .input = VectorAggregateInput{.column_ordinal = 0U, .type = decimal, .nullable = true}},
      {.operation = VectorAggregateOperation::kAverage,
       .input = VectorAggregateInput{.column_ordinal = 0U, .type = decimal, .nullable = true}}};
  auto decimal_aggregate =
      UngroupedAggregateOperator::create(
          std::make_unique<ManyChunkSource>(std::move(decimal_chunks)), definitions)
          .value();
  auto decimal_step = decimal_aggregate->next(decimal_resources).value();
  EXPECT_EQ(std::get<Decimal128Value>(cell(decimal_step.chunk()->chunk(), 0U).storage()),
            decimal_value(150));
  EXPECT_DOUBLE_EQ(std::get<double>(cell(decimal_step.chunk()->chunk(), 1U).storage()), 0.75);
}

TEST(UngroupedAggregateOperatorTest, RejectsInvalidDefinitionsShapesAndFinalOverflow) {
  const schema::LogicalType string = type(schema::LogicalTypeKind::kString);
  EXPECT_EQ(
      vector_aggregate_output_shape(
          {.operation = VectorAggregateOperation::kMinimum,
           .input = VectorAggregateInput{.column_ordinal = 0U, .type = string, .nullable = true}})
          .value(),
      (VectorAggregateOutputShape{.type = string, .nullable = true}));
  EXPECT_TRUE(
      vector_aggregate_output_shape(
          {.operation = VectorAggregateOperation::kCount,
           .input = VectorAggregateInput{.column_ordinal = 0U, .type = string, .nullable = true}})
          .has_value());
  EXPECT_EQ(
      UngroupedAggregateOperator::create(
          nullptr, {aggregate(VectorAggregateOperation::kCount, schema::LogicalTypeKind::kInt64)})
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);

  QueryResourceContext mismatch_resources = QueryResourceContext::create(1U << 20U).value();
  const std::array<std::optional<std::int64_t>, 1> one{1};
  std::vector<AccountedVectorChunk> mismatch_chunks;
  mismatch_chunks.push_back(
      accounted_chunk(mismatch_resources, signed_column(schema::LogicalTypeKind::kInt64, one)));
  auto mismatch =
      UngroupedAggregateOperator::create(
          std::make_unique<ManyChunkSource>(std::move(mismatch_chunks)),
          {aggregate(VectorAggregateOperation::kSum, schema::LogicalTypeKind::kInt64, false)})
          .value();
  EXPECT_EQ(mismatch->next(mismatch_resources).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(mismatch_resources.is_cancelled());

  QueryResourceContext overflow_resources = QueryResourceContext::create(1U << 20U).value();
  const std::array<std::optional<std::int64_t>, 2> overflow_values{127, 1};
  std::vector<AccountedVectorChunk> overflow_chunks;
  overflow_chunks.push_back(accounted_chunk(
      overflow_resources, signed_column(schema::LogicalTypeKind::kInt8, overflow_values)));
  auto overflow = UngroupedAggregateOperator::create(
                      std::make_unique<ManyChunkSource>(std::move(overflow_chunks)),
                      {aggregate(VectorAggregateOperation::kSum, schema::LogicalTypeKind::kInt8)})
                      .value();
  EXPECT_EQ(overflow->next(overflow_resources).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(overflow_resources.is_cancelled());
}

TEST(UngroupedAggregateOperatorTest, EnforcesVariableExtremumLimitsAndReleasesCredit) {
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  const std::array<std::optional<std::string_view>, 1> values{"four"};
  std::vector<AccountedVectorChunk> chunks;
  chunks.push_back(accounted_chunk(resources, string_column(values)));
  auto extremum_operator =
      UngroupedAggregateOperator::create(
          std::make_unique<ManyChunkSource>(std::move(chunks)),
          {aggregate(VectorAggregateOperation::kMinimum, schema::LogicalTypeKind::kString)},
          {.maximum_variable_extremum_bytes = 3U})
          .value();
  auto failed = extremum_operator->next(resources);
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_TRUE(resources.is_cancelled());
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);

  QueryResourceContext nonwinner_resources = QueryResourceContext::create(1U << 20U).value();
  const std::array<std::optional<std::string_view>, 2> nonwinners{"a", "too-long"};
  std::vector<AccountedVectorChunk> nonwinner_chunks;
  nonwinner_chunks.push_back(accounted_chunk(nonwinner_resources, string_column(nonwinners)));
  auto bounded_min =
      UngroupedAggregateOperator::create(
          std::make_unique<ManyChunkSource>(std::move(nonwinner_chunks)),
          {aggregate(VectorAggregateOperation::kMinimum, schema::LogicalTypeKind::kString)},
          {.maximum_variable_extremum_bytes = 1U})
          .value();
  auto bounded_step = bounded_min->next(nonwinner_resources).value();
  EXPECT_EQ(std::get<std::string>(cell(bounded_step.chunk()->chunk(), 0U).storage()), "a");
  bounded_step = PhysicalOperatorStep::end();
  EXPECT_EQ(nonwinner_resources.reserved_memory_bytes(), 0U);

  EXPECT_EQ(UngroupedAggregateOperator::create(
                std::make_unique<EmptySource>(),
                {aggregate(VectorAggregateOperation::kMinimum, schema::LogicalTypeKind::kString)},
                {.maximum_variable_extremum_bytes = 0U})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

TEST(UngroupedAggregateOperatorTest, CountsVariableInputAndEnforcesQueryOwnershipAndCancellation) {
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  const std::array<std::optional<std::string_view>, 4> strings{"a", std::nullopt, "", "chronos"};
  std::vector<AccountedVectorChunk> chunks;
  chunks.push_back(accounted_chunk(resources, string_column(strings)));
  auto count = UngroupedAggregateOperator::create(
                   std::make_unique<ManyChunkSource>(std::move(chunks)),
                   {aggregate(VectorAggregateOperation::kCount, schema::LogicalTypeKind::kString)})
                   .value();
  auto count_step = count->next(resources).value();
  EXPECT_EQ(std::get<std::int64_t>(cell(count_step.chunk()->chunk(), 0U).storage()), 3);

  QueryResourceContext foreign = QueryResourceContext::create(1U << 20U).value();
  const std::array<std::optional<std::int64_t>, 1> one{1};
  std::vector<AccountedVectorChunk> foreign_chunks;
  foreign_chunks.push_back(
      accounted_chunk(foreign, signed_column(schema::LogicalTypeKind::kInt64, one)));
  auto wrong_owner =
      UngroupedAggregateOperator::create(
          std::make_unique<ManyChunkSource>(std::move(foreign_chunks)),
          {aggregate(VectorAggregateOperation::kCount, schema::LogicalTypeKind::kInt64)})
          .value();
  EXPECT_EQ(wrong_owner->next(resources).error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(resources.is_cancelled());
  EXPECT_FALSE(foreign.is_cancelled());
  EXPECT_EQ(foreign.reserved_memory_bytes(), 0U);

  QueryResourceContext cancelled = QueryResourceContext::create(1U << 20U).value();
  EXPECT_TRUE(cancelled.request_cancel());
  auto never_pulled =
      UngroupedAggregateOperator::create(
          std::make_unique<EmptySource>(),
          {{.operation = VectorAggregateOperation::kCountStar, .input = std::nullopt}})
          .value();
  EXPECT_EQ(never_pulled->next(cancelled).error().code(), common::StatusCode::kCancelled);
}

TEST(UngroupedAggregateOperatorPropertyTest, MatchesIndependentModelAcrossChunkBoundaries) {
  constexpr std::size_t kRows = 257U;
  std::array<std::optional<std::int64_t>, kRows> values{};
  std::vector<std::optional<std::int64_t>> first;
  std::vector<std::optional<std::int64_t>> second;
  std::vector<std::optional<std::int64_t>> third;
  first.reserve(31U);
  second.reserve(96U);
  third.reserve(kRows - 127U);
  std::uint64_t state = 0x243f6a8885a308d3ULL;
  std::int64_t expected_sum = 0;
  std::int64_t expected_count = 0;
  std::int64_t expected_min = std::numeric_limits<std::int64_t>::max();
  std::int64_t expected_max = std::numeric_limits<std::int64_t>::min();
  double expected_mean = 0.0;
  double expected_m2 = 0.0;
  std::size_t moment_count = 0U;
  for (std::size_t row = 0U; row < kRows; ++row) {
    state = state * 6'364'136'223'846'793'005ULL + 1'442'695'040'888'963'407ULL;
    if (row % 11U != 0U)
      values[row] = static_cast<std::int64_t>(state % 20'001U) - 10'000;
    auto& partition = row < 31U ? first : (row < 127U ? second : third);
    partition.push_back(values[row]);
    if (row % 5U == 3U || !values[row].has_value())
      continue;
    const std::int64_t value = values[row].value(); // NOLINT(bugprone-unchecked-optional-access)
    expected_sum += value;
    ++expected_count;
    expected_min = std::min(expected_min, value);
    expected_max = std::max(expected_max, value);
    ++moment_count;
    const double delta = static_cast<double>(value) - expected_mean;
    expected_mean += delta / static_cast<double>(moment_count);
    expected_m2 += delta * (static_cast<double>(value) - expected_mean);
  }
  const auto selected = [](const std::size_t begin, const std::size_t count) {
    std::vector<std::uint32_t> result;
    for (std::size_t local = 0U; local < count; ++local) {
      if ((begin + local) % 5U != 3U)
        result.push_back(static_cast<std::uint32_t>(local));
    }
    return result;
  };

  QueryResourceContext resources = QueryResourceContext::create(1U << 22U).value();
  std::vector<AccountedVectorChunk> chunks;
  chunks.push_back(accounted_chunk(resources, signed_column(schema::LogicalTypeKind::kInt64, first),
                                   selected(0U, first.size())));
  chunks.push_back(accounted_chunk(resources,
                                   signed_column(schema::LogicalTypeKind::kInt64, second),
                                   selected(31U, second.size())));
  chunks.push_back(accounted_chunk(resources, signed_column(schema::LogicalTypeKind::kInt64, third),
                                   selected(127U, third.size())));
  std::vector<VectorAggregateDefinition> definitions{
      aggregate(VectorAggregateOperation::kCount, schema::LogicalTypeKind::kInt64),
      aggregate(VectorAggregateOperation::kSum, schema::LogicalTypeKind::kInt64),
      aggregate(VectorAggregateOperation::kAverage, schema::LogicalTypeKind::kInt64),
      aggregate(VectorAggregateOperation::kMinimum, schema::LogicalTypeKind::kInt64),
      aggregate(VectorAggregateOperation::kMaximum, schema::LogicalTypeKind::kInt64),
      aggregate(VectorAggregateOperation::kVariancePopulation, schema::LogicalTypeKind::kInt64)};
  auto operator_ = UngroupedAggregateOperator::create(
                       std::make_unique<ManyChunkSource>(std::move(chunks)), definitions)
                       .value();
  auto step = operator_->next(resources).value();
  const VectorChunk& result = step.chunk()->chunk();
  EXPECT_EQ(std::get<std::int64_t>(cell(result, 0U).storage()), expected_count);
  EXPECT_EQ(std::get<std::int64_t>(cell(result, 1U).storage()), expected_sum);
  EXPECT_DOUBLE_EQ(std::get<double>(cell(result, 2U).storage()),
                   static_cast<double>(expected_sum) / static_cast<double>(expected_count));
  EXPECT_EQ(std::get<std::int64_t>(cell(result, 3U).storage()), expected_min);
  EXPECT_EQ(std::get<std::int64_t>(cell(result, 4U).storage()), expected_max);
  EXPECT_DOUBLE_EQ(std::get<double>(cell(result, 5U).storage()),
                   expected_m2 / static_cast<double>(moment_count));
}

TEST(UngroupedAggregatePlanTest, PropagatesShapesAndInstantiatesTheStage) {
  std::vector<VectorAggregateDefinition> definitions{
      {.operation = VectorAggregateOperation::kCountStar, .input = std::nullopt},
      aggregate(VectorAggregateOperation::kSum, schema::LogicalTypeKind::kInt64)};
  PhysicalPipelinePlan plan =
      PhysicalPipelinePlan::create(
          {{.type = type(schema::LogicalTypeKind::kInt64), .nullable = true}},
          {UngroupedAggregateStage{.definitions = definitions}})
          .value();
  ASSERT_EQ(plan.output_columns().size(), 2U);
  EXPECT_FALSE(plan.output_columns()[0].nullable);
  EXPECT_TRUE(plan.output_columns()[1].nullable);

  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  const std::array<std::optional<std::int64_t>, 2> values{2, 3};
  std::vector<AccountedVectorChunk> chunks;
  chunks.push_back(
      accounted_chunk(resources, signed_column(schema::LogicalTypeKind::kInt64, values)));
  auto pipeline = plan.instantiate(std::make_unique<ManyChunkSource>(std::move(chunks))).value();
  auto step = pipeline->next(resources).value();
  EXPECT_EQ(std::get<std::int64_t>(cell(step.chunk()->chunk(), 0U).storage()), 2);
  EXPECT_EQ(std::get<std::int64_t>(cell(step.chunk()->chunk(), 1U).storage()), 5);

  definitions[1] =
      aggregate(VectorAggregateOperation::kSum, schema::LogicalTypeKind::kInt64, false);
  EXPECT_EQ(PhysicalPipelinePlan::create(
                {{.type = type(schema::LogicalTypeKind::kInt64), .nullable = true}},
                {UngroupedAggregateStage{.definitions = definitions}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

TEST(GroupedAggregateOperatorTest, GroupsVariableKeysNullsAndAggregatesAcrossSelections) {
  QueryResourceContext resources = QueryResourceContext::create(8U << 20U).value();
  const std::array<std::optional<std::string_view>, 6> keys{"A",          "B",          "A",
                                                            std::nullopt, std::nullopt, "ignored"};
  const std::array<std::optional<std::int64_t>, 6> values{1, 2, std::nullopt, 4, 6, 100};
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(string_column(keys));
  columns.push_back(signed_column(schema::LogicalTypeKind::kInt64, values));
  std::vector<AccountedVectorChunk> chunks;
  chunks.push_back(accounted_chunk(resources, std::move(columns), {0U, 1U, 2U, 3U, 4U}));

  const std::vector<VectorGroupKeyDefinition> group_keys{
      {.column_ordinal = 0U, .type = type(schema::LogicalTypeKind::kString), .nullable = true}};
  const std::vector<VectorAggregateDefinition> definitions{
      {.operation = VectorAggregateOperation::kCountStar, .input = std::nullopt},
      {.operation = VectorAggregateOperation::kCount,
       .input = VectorAggregateInput{.column_ordinal = 1U,
                                     .type = type(schema::LogicalTypeKind::kInt64),
                                     .nullable = true}},
      {.operation = VectorAggregateOperation::kSum,
       .input = VectorAggregateInput{
           .column_ordinal = 1U, .type = type(schema::LogicalTypeKind::kInt64), .nullable = true}}};
  auto grouped = GroupedAggregateOperator::create(
                     std::make_unique<ManyChunkSource>(std::move(chunks)), group_keys, definitions)
                     .value();

  auto first = grouped->next(resources).value();
  ASSERT_EQ(first.kind(), PhysicalOperatorStepKind::kChunk);
  EXPECT_EQ(std::get<std::string>(cell(first.chunk()->chunk(), 0U).storage()), "A");
  EXPECT_EQ(std::get<std::int64_t>(cell(first.chunk()->chunk(), 1U).storage()), 2);
  EXPECT_EQ(std::get<std::int64_t>(cell(first.chunk()->chunk(), 2U).storage()), 1);
  EXPECT_EQ(std::get<std::int64_t>(cell(first.chunk()->chunk(), 3U).storage()), 1);
  EXPECT_TRUE(first.chunk()->chunk().column(0U)->nullable());
  first = PhysicalOperatorStep::end();

  auto second = grouped->next(resources).value();
  EXPECT_EQ(std::get<std::string>(cell(second.chunk()->chunk(), 0U).storage()), "B");
  EXPECT_EQ(std::get<std::int64_t>(cell(second.chunk()->chunk(), 1U).storage()), 1);
  EXPECT_EQ(std::get<std::int64_t>(cell(second.chunk()->chunk(), 3U).storage()), 2);
  second = PhysicalOperatorStep::end();

  auto third = grouped->next(resources).value();
  EXPECT_TRUE(cell(third.chunk()->chunk(), 0U).is_null());
  EXPECT_EQ(std::get<std::int64_t>(cell(third.chunk()->chunk(), 1U).storage()), 2);
  EXPECT_EQ(std::get<std::int64_t>(cell(third.chunk()->chunk(), 2U).storage()), 2);
  EXPECT_EQ(std::get<std::int64_t>(cell(third.chunk()->chunk(), 3U).storage()), 10);
  third = PhysicalOperatorStep::end();
  EXPECT_EQ(grouped->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(MergeableVectorGroupedAggregateTableTest,
     ExposesTheSameFirstSeenMultiKeyStatesForCanonicalWorkerEncoding) {
  QueryResourceContext resources = QueryResourceContext::create(8U << 20U).value();
  const std::array<std::optional<std::string_view>, 5> key_values{"A", "B", "A", std::nullopt,
                                                                  std::nullopt};
  const std::array<std::optional<std::int64_t>, 5> input_values{1, 2, std::nullopt, 4, 6};
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(string_column(key_values));
  columns.push_back(signed_column(schema::LogicalTypeKind::kInt64, input_values));
  auto input = accounted_chunk(resources, std::move(columns));
  const std::vector<VectorGroupKeyDefinition> group_keys{
      {.column_ordinal = 0U, .type = type(schema::LogicalTypeKind::kString), .nullable = true}};
  const std::vector<VectorAggregateDefinition> definitions{
      {.operation = VectorAggregateOperation::kCountStar, .input = std::nullopt},
      {.operation = VectorAggregateOperation::kSum,
       .input = VectorAggregateInput{
           .column_ordinal = 1U, .type = type(schema::LogicalTypeKind::kInt64), .nullable = true}}};
  auto table = MergeableVectorGroupedAggregateTable::create(group_keys, definitions);
  ASSERT_TRUE(table.has_value()) << table.error().to_string();
  ASSERT_TRUE(table->accumulate(input, resources).has_value());
  ASSERT_EQ(table->group_count(), 3U);

  const std::array<std::optional<std::string_view>, 3> expected_keys{"A", "B", std::nullopt};
  const std::array<std::int64_t, 3> expected_counts{2, 1, 2};
  const std::array<std::int64_t, 3> expected_sums{1, 2, 10};
  for (std::size_t group = 0U; group < table->group_count(); ++group) {
    auto retained_keys = table->group_keys(group);
    auto retained_states = table->group_states(group);
    ASSERT_TRUE(retained_keys.has_value());
    ASSERT_TRUE(retained_states.has_value());
    ASSERT_EQ(retained_keys->size(), 1U);
    ASSERT_EQ(retained_states->size(), 2U);
    if (expected_keys[group].has_value()) {
      EXPECT_EQ(std::get<std::string>((*retained_keys)[0].storage()),
                expected_keys[group].value_or(std::string_view{}));
    } else {
      EXPECT_TRUE((*retained_keys)[0].is_null());
    }
    common::Uuid::Bytes query_bytes{};
    common::Uuid::Bytes tablet_bytes{};
    query_bytes.back() = std::byte{1U};
    tablet_bytes.back() = std::byte{2U};
    auto encoded = encode_distributed_vector_grouped_aggregate_exchange_message(
        {.query_id = common::Uuid{query_bytes},
         .tablet_id = schema::TabletId::from_bytes(tablet_bytes).value(),
         .sequence = group + 1U,
         .group_ordinal = static_cast<std::uint32_t>(group),
         .group_count = static_cast<std::uint32_t>(table->group_count()),
         .terminal = group + 1U == table->group_count(),
         .empty = false},
        *retained_keys, *retained_states, group_keys, definitions);
    ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
    auto decoded = decode_distributed_vector_grouped_aggregate_exchange_message_exact(
        encoded->bytes(), group_keys, definitions, resources);
    ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
    auto decoded_states = std::move(*decoded).take_states();
    auto count = std::move(decoded_states[0]).take_result();
    auto sum = std::move(decoded_states[1]).take_result();
    ASSERT_TRUE(count.has_value());
    ASSERT_TRUE(sum.has_value());
    EXPECT_EQ(std::get<std::int64_t>(count->storage()), expected_counts[group]);
    EXPECT_EQ(std::get<std::int64_t>(sum->storage()), expected_sums[group]);
  }
  EXPECT_EQ(table->group_keys(3U).error().code(), common::StatusCode::kOutOfRange);
  EXPECT_EQ(table->group_states(3U).error().code(), common::StatusCode::kOutOfRange);
  table =
      common::make_unexpected(common::Status{common::StatusCode::kInternal, "drop grouped table"});
}

TEST(GroupedAggregateOperatorTest, CanonicalHashMatchesFloatGroupingEquality) {
  const double first_nan = std::bit_cast<double>(std::uint64_t{0x7ff8000000000001ULL});
  const double second_nan = std::bit_cast<double>(std::uint64_t{0xfff8000000000042ULL});
  const std::array<std::optional<double>, 6> keys{0.0,        -0.0, first_nan,
                                                  second_nan, 1.0,  std::nullopt};
  QueryResourceContext resources = QueryResourceContext::create(4U << 20U).value();
  std::vector<AccountedVectorChunk> chunks;
  chunks.push_back(accounted_chunk(resources, float64_column(keys)));
  const std::vector<VectorGroupKeyDefinition> group_keys{
      {.column_ordinal = 0U, .type = type(schema::LogicalTypeKind::kFloat64), .nullable = true}};
  const std::vector<VectorAggregateDefinition> definitions{
      {.operation = VectorAggregateOperation::kCountStar, .input = std::nullopt}};
  auto grouped = GroupedAggregateOperator::create(
                     std::make_unique<ManyChunkSource>(std::move(chunks)), group_keys, definitions)
                     .value();

  auto zero = grouped->next(resources).value();
  EXPECT_EQ(std::get<double>(cell(zero.chunk()->chunk(), 0U).storage()), 0.0);
  EXPECT_EQ(std::get<std::int64_t>(cell(zero.chunk()->chunk(), 1U).storage()), 2);
  zero = PhysicalOperatorStep::end();
  auto nan = grouped->next(resources).value();
  EXPECT_TRUE(std::isnan(std::get<double>(cell(nan.chunk()->chunk(), 0U).storage())));
  EXPECT_EQ(std::get<std::int64_t>(cell(nan.chunk()->chunk(), 1U).storage()), 2);
  nan = PhysicalOperatorStep::end();
  auto one = grouped->next(resources).value();
  EXPECT_EQ(std::get<double>(cell(one.chunk()->chunk(), 0U).storage()), 1.0);
  EXPECT_EQ(std::get<std::int64_t>(cell(one.chunk()->chunk(), 1U).storage()), 1);
  one = PhysicalOperatorStep::end();
  auto null = grouped->next(resources).value();
  EXPECT_TRUE(cell(null.chunk()->chunk(), 0U).is_null());
  EXPECT_EQ(std::get<std::int64_t>(cell(null.chunk()->chunk(), 1U).storage()), 1);
  null = PhysicalOperatorStep::end();
  EXPECT_EQ(grouped->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(GroupedAggregateOperatorTest, ResolvesHashCollisionsByExactKeyEquality) {
  // With four power-of-two buckets, these little-endian INT64 values share the same initial bucket
  // under the accepted hash but remain distinct groups after exact collision comparison.
  const std::array<std::optional<std::int64_t>, 4> keys{1, 5, 1, 5};
  QueryResourceContext resources = QueryResourceContext::create(4U << 20U).value();
  std::vector<AccountedVectorChunk> chunks;
  chunks.push_back(
      accounted_chunk(resources, signed_column(schema::LogicalTypeKind::kInt64, keys)));
  const std::vector<VectorGroupKeyDefinition> group_keys{
      {.column_ordinal = 0U, .type = type(schema::LogicalTypeKind::kInt64), .nullable = true}};
  const std::vector<VectorAggregateDefinition> definitions{
      {.operation = VectorAggregateOperation::kCountStar, .input = std::nullopt}};
  auto grouped =
      GroupedAggregateOperator::create(std::make_unique<ManyChunkSource>(std::move(chunks)),
                                       group_keys, definitions, {.maximum_groups = 2U})
          .value();

  for (const std::int64_t expected : {1, 5}) {
    auto step = grouped->next(resources).value();
    EXPECT_EQ(std::get<std::int64_t>(cell(step.chunk()->chunk(), 0U).storage()), expected);
    EXPECT_EQ(std::get<std::int64_t>(cell(step.chunk()->chunk(), 1U).storage()), 2);
  }
  EXPECT_EQ(grouped->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(GroupedAggregateOperatorTest, HashesEveryFrozenLogicalTypeAndDecimalParameters) {
  const schema::LogicalType decimal = schema::LogicalType::decimal(10U, 2U).value();
  common::Uuid::Bytes uuid_bytes{};
  uuid_bytes.front() = std::byte{0x42U};
  const std::vector<ScalarValue> values{
      ScalarValue::boolean(true).value(),
      ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt8), -7).value(),
      ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt16), -8).value(),
      ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt32), -9).value(),
      ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), -10).value(),
      ScalarValue::unsigned_value(type(schema::LogicalTypeKind::kUInt8), 7U).value(),
      ScalarValue::unsigned_value(type(schema::LogicalTypeKind::kUInt16), 8U).value(),
      ScalarValue::unsigned_value(type(schema::LogicalTypeKind::kUInt32), 9U).value(),
      ScalarValue::unsigned_value(type(schema::LogicalTypeKind::kUInt64), 10U).value(),
      ScalarValue::float32(1.25F).value(),
      ScalarValue::float64(-2.5).value(),
      ScalarValue::decimal(decimal, decimal_value(1234)).value(),
      ScalarValue::signed_value(type(schema::LogicalTypeKind::kTimestampNs), 123).value(),
      ScalarValue::signed_value(type(schema::LogicalTypeKind::kDate), 45).value(),
      ScalarValue::text(type(schema::LogicalTypeKind::kSymbol), "symbol").value(),
      ScalarValue::text(type(schema::LogicalTypeKind::kString), "string").value(),
      ScalarValue::binary({std::byte{0U}, std::byte{0xffU}}),
      ScalarValue::uuid(common::Uuid{uuid_bytes})};
  std::vector<ColumnOutputPosition> positions;
  std::vector<VectorGroupKeyDefinition> keys;
  positions.reserve(values.size());
  keys.reserve(values.size());
  // NOLINTBEGIN(bugprone-unchecked-optional-access)
  for (std::size_t ordinal = 0U; ordinal < values.size(); ++ordinal) {
    ASSERT_TRUE(values[ordinal].type().has_value());
    positions.emplace_back(
        ConstantColumnOutputPosition{.value = values[ordinal], .force_nullable = false});
    keys.push_back(
        {.column_ordinal = ordinal, .type = values[ordinal].type().value(), .nullable = false});
  }
  // NOLINTEND(bugprone-unchecked-optional-access)

  QueryResourceContext resources = QueryResourceContext::create(16U << 20U).value();
  VectorChunk cardinality = VectorChunk::create({}, VectorSelection::all(2U).value()).value();
  const std::size_t source_charge = cardinality.retained_buffer_bytes() + 1'024U;
  auto source_chunk =
      AccountedVectorChunk::create(std::move(cardinality), resources.reserve(source_charge).value(),
                                   resources)
          .value();
  std::vector<AccountedVectorChunk> chunks;
  chunks.push_back(std::move(source_chunk));
  auto materialized =
      ColumnOutputOperator::create(std::make_unique<ManyChunkSource>(std::move(chunks)),
                                   std::move(positions))
          .value();
  const std::vector<VectorAggregateDefinition> definitions{
      {.operation = VectorAggregateOperation::kCountStar, .input = std::nullopt}};
  auto grouped =
      GroupedAggregateOperator::create(std::move(materialized), keys, definitions).value();

  auto step = grouped->next(resources).value();
  ASSERT_EQ(step.chunk()->chunk().column_count(), values.size() + 1U);
  EXPECT_EQ(std::get<std::int64_t>(cell(step.chunk()->chunk(), values.size()).storage()), 2);
  for (std::size_t ordinal = 0U; ordinal < values.size(); ++ordinal) {
    EXPECT_EQ(cell(step.chunk()->chunk(), ordinal).type(), values[ordinal].type());
    EXPECT_EQ(cell(step.chunk()->chunk(), ordinal).storage(), values[ordinal].storage());
  }
  step = PhysicalOperatorStep::end();
  EXPECT_EQ(grouped->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(GroupedAggregateOperatorTest, ComputesAndBoundsVariableExtremaPerAggregateState) {
  QueryResourceContext resources = QueryResourceContext::create(8U << 20U).value();
  const std::array<std::optional<std::int64_t>, 5> keys{1, 2, 1, 2, 1};
  const std::array<std::optional<std::string_view>, 5> values{"z", "beta", "alpha", "omega", "m"};
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(signed_column(schema::LogicalTypeKind::kInt64, keys));
  columns.push_back(string_column(values));
  std::vector<AccountedVectorChunk> chunks;
  chunks.push_back(accounted_chunk(resources, std::move(columns)));
  const std::vector<VectorGroupKeyDefinition> group_keys{
      {.column_ordinal = 0U, .type = type(schema::LogicalTypeKind::kInt64), .nullable = true}};
  const std::vector<VectorAggregateDefinition> definitions{
      {.operation = VectorAggregateOperation::kMinimum,
       .input = VectorAggregateInput{.column_ordinal = 1U,
                                     .type = type(schema::LogicalTypeKind::kString),
                                     .nullable = true}},
      {.operation = VectorAggregateOperation::kMaximum,
       .input = VectorAggregateInput{.column_ordinal = 1U,
                                     .type = type(schema::LogicalTypeKind::kString),
                                     .nullable = true}}};
  auto grouped = GroupedAggregateOperator::create(
                     std::make_unique<ManyChunkSource>(std::move(chunks)), group_keys, definitions)
                     .value();
  auto first = grouped->next(resources).value();
  EXPECT_EQ(std::get<std::int64_t>(cell(first.chunk()->chunk(), 0U).storage()), 1);
  EXPECT_EQ(std::get<std::string>(cell(first.chunk()->chunk(), 1U).storage()), "alpha");
  EXPECT_EQ(std::get<std::string>(cell(first.chunk()->chunk(), 2U).storage()), "z");
  first = PhysicalOperatorStep::end();
  auto second = grouped->next(resources).value();
  EXPECT_EQ(std::get<std::int64_t>(cell(second.chunk()->chunk(), 0U).storage()), 2);
  EXPECT_EQ(std::get<std::string>(cell(second.chunk()->chunk(), 1U).storage()), "beta");
  EXPECT_EQ(std::get<std::string>(cell(second.chunk()->chunk(), 2U).storage()), "omega");
  second = PhysicalOperatorStep::end();
  EXPECT_EQ(grouped->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);

  QueryResourceContext limited_resources = QueryResourceContext::create(8U << 20U).value();
  std::vector<columnar::OwnedPhysicalColumn> limited_columns;
  limited_columns.push_back(signed_column(schema::LogicalTypeKind::kInt64, keys));
  limited_columns.push_back(string_column(values));
  std::vector<AccountedVectorChunk> limited_chunks;
  limited_chunks.push_back(accounted_chunk(limited_resources, std::move(limited_columns)));
  auto limited = GroupedAggregateOperator::create(
                     std::make_unique<ManyChunkSource>(std::move(limited_chunks)), group_keys,
                     definitions, {.maximum_variable_extremum_bytes = 3U})
                     .value();
  auto failed = limited->next(limited_resources);
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_TRUE(limited_resources.is_cancelled());
  EXPECT_EQ(limited_resources.reserved_memory_bytes(), 0U);
}

TEST(GroupedAggregateOperatorTest, EmptyInputAndResourceLimitsReleaseState) {
  const std::vector<VectorGroupKeyDefinition> keys{
      {.column_ordinal = 0U, .type = type(schema::LogicalTypeKind::kInt64), .nullable = true}};
  EXPECT_EQ(GroupedAggregateOperator::create(std::make_unique<EmptySource>(), keys, {},
                                             {.maximum_variable_extremum_bytes = 0U})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  QueryResourceContext empty_resources = QueryResourceContext::create(1U << 20U).value();
  auto empty = GroupedAggregateOperator::create(std::make_unique<EmptySource>(), keys, {}).value();
  EXPECT_EQ(empty->next(empty_resources)->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ(empty_resources.reserved_memory_bytes(), 0U);

  QueryResourceContext resources = QueryResourceContext::create(4U << 20U).value();
  const std::array<std::optional<std::int64_t>, 3> values{1, 2, 3};
  std::vector<AccountedVectorChunk> chunks;
  chunks.push_back(
      accounted_chunk(resources, signed_column(schema::LogicalTypeKind::kInt64, values)));
  auto limited =
      GroupedAggregateOperator::create(std::make_unique<ManyChunkSource>(std::move(chunks)), keys,
                                       {}, {.maximum_groups = 2U})
          .value();
  auto failed = limited->next(resources);
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_TRUE(resources.is_cancelled());
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);

  QueryResourceContext key_resources = QueryResourceContext::create(4U << 20U).value();
  const std::array<std::optional<std::string_view>, 1> long_key{"too-long"};
  std::vector<AccountedVectorChunk> key_chunks;
  key_chunks.push_back(accounted_chunk(key_resources, string_column(long_key)));
  const std::vector<VectorGroupKeyDefinition> string_keys{
      {.column_ordinal = 0U, .type = type(schema::LogicalTypeKind::kString), .nullable = true}};
  auto key_limited =
      GroupedAggregateOperator::create(std::make_unique<ManyChunkSource>(std::move(key_chunks)),
                                       string_keys, {}, {.maximum_key_bytes_per_group = 3U})
          .value();
  auto key_failed = key_limited->next(key_resources);
  ASSERT_FALSE(key_failed.has_value());
  EXPECT_EQ(key_failed.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_TRUE(key_resources.is_cancelled());
  EXPECT_EQ(key_resources.reserved_memory_bytes(), 0U);
}

TEST(GroupedAggregateOperatorTest, RejectsChunksOwnedByAnotherQuery) {
  QueryResourceContext owner = QueryResourceContext::create(4U << 20U).value();
  QueryResourceContext consumer = QueryResourceContext::create(4U << 20U).value();
  const std::array<std::optional<std::int64_t>, 1> values{1};
  std::vector<AccountedVectorChunk> chunks;
  chunks.push_back(accounted_chunk(owner, signed_column(schema::LogicalTypeKind::kInt64, values)));
  const std::vector<VectorGroupKeyDefinition> keys{
      {.column_ordinal = 0U, .type = type(schema::LogicalTypeKind::kInt64), .nullable = true}};
  auto grouped = GroupedAggregateOperator::create(
                     std::make_unique<ManyChunkSource>(std::move(chunks)), keys, {})
                     .value();
  const auto failed = grouped->next(consumer);
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(consumer.is_cancelled());
  EXPECT_EQ(owner.reserved_memory_bytes(), 0U);
  EXPECT_EQ(consumer.reserved_memory_bytes(), 0U);
}

TEST(GroupedAggregatePlanTest, PropagatesKeyAndAggregateShapesAndInstantiates) {
  const std::vector<VectorGroupKeyDefinition> keys{
      {.column_ordinal = 0U, .type = type(schema::LogicalTypeKind::kInt64), .nullable = true}};
  const std::vector<VectorAggregateDefinition> definitions{
      {.operation = VectorAggregateOperation::kCountStar, .input = std::nullopt}};
  PhysicalPipelinePlan plan =
      PhysicalPipelinePlan::create(
          {{.type = type(schema::LogicalTypeKind::kInt64), .nullable = true}},
          {GroupedAggregateStage{.keys = keys, .definitions = definitions}})
          .value();
  ASSERT_EQ(plan.output_columns().size(), 2U);
  EXPECT_TRUE(plan.output_columns()[0].nullable);
  EXPECT_FALSE(plan.output_columns()[1].nullable);

  QueryResourceContext resources = QueryResourceContext::create(4U << 20U).value();
  const std::array<std::optional<std::int64_t>, 3> values{2, 2, 3};
  std::vector<AccountedVectorChunk> chunks;
  chunks.push_back(
      accounted_chunk(resources, signed_column(schema::LogicalTypeKind::kInt64, values)));
  auto pipeline = plan.instantiate(std::make_unique<ManyChunkSource>(std::move(chunks))).value();
  auto first = pipeline->next(resources).value();
  EXPECT_EQ(std::get<std::int64_t>(cell(first.chunk()->chunk(), 0U).storage()), 2);
  EXPECT_EQ(std::get<std::int64_t>(cell(first.chunk()->chunk(), 1U).storage()), 2);

  EXPECT_EQ(PhysicalPipelinePlan::create(
                {{.type = type(schema::LogicalTypeKind::kInt64), .nullable = false}},
                {GroupedAggregateStage{.keys = keys, .definitions = definitions}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

TEST(GroupedAggregateOperatorPropertyTest, MatchesFirstSeenScalarGroupsAcrossChunks) {
  struct ExpectedGroup {
    std::optional<std::int64_t> key;
    std::int64_t rows{};
    std::int64_t present{};
    std::int64_t sum{};
  };
  std::vector<ExpectedGroup> expected;
  QueryResourceContext resources = QueryResourceContext::create(16U << 20U).value();
  std::vector<AccountedVectorChunk> chunks;
  for (const std::pair<std::size_t, std::size_t> range :
       {std::pair{0U, 73U}, std::pair{73U, 164U}, std::pair{164U, 257U}}) {
    std::vector<std::optional<std::int64_t>> keys;
    std::vector<std::optional<std::int64_t>> values;
    std::vector<std::uint32_t> selection;
    for (std::size_t row = range.first; row < range.second; ++row) {
      const std::optional<std::int64_t> key =
          row % 19U == 0U
              ? std::nullopt
              : std::optional<std::int64_t>{static_cast<std::int64_t>((row * 7U) % 13U)};
      const std::optional<std::int64_t> value =
          row % 11U == 0U ? std::nullopt
                          : std::optional<std::int64_t>{static_cast<std::int64_t>(row % 17U) - 8};
      keys.push_back(key);
      values.push_back(value);
      if (row % 5U == 0U)
        continue;
      selection.push_back(static_cast<std::uint32_t>(row - range.first));
      auto group = std::ranges::find_if(
          expected, [&](const ExpectedGroup& candidate) { return candidate.key == key; });
      if (group == expected.end()) {
        expected.push_back({.key = key});
        group = expected.end() - 1;
      }
      ++group->rows;
      if (value.has_value()) {
        ++group->present;
        group->sum += *value;
      }
    }
    std::vector<columnar::OwnedPhysicalColumn> columns;
    columns.push_back(signed_column(schema::LogicalTypeKind::kInt64, keys));
    columns.push_back(signed_column(schema::LogicalTypeKind::kInt64, values));
    chunks.push_back(accounted_chunk(resources, std::move(columns), std::move(selection)));
  }
  const std::vector<VectorGroupKeyDefinition> keys{
      {.column_ordinal = 0U, .type = type(schema::LogicalTypeKind::kInt64), .nullable = true}};
  const std::vector<VectorAggregateDefinition> definitions{
      {.operation = VectorAggregateOperation::kCountStar, .input = std::nullopt},
      {.operation = VectorAggregateOperation::kCount,
       .input = VectorAggregateInput{.column_ordinal = 1U,
                                     .type = type(schema::LogicalTypeKind::kInt64),
                                     .nullable = true}},
      {.operation = VectorAggregateOperation::kSum,
       .input = VectorAggregateInput{
           .column_ordinal = 1U, .type = type(schema::LogicalTypeKind::kInt64), .nullable = true}}};
  auto grouped = GroupedAggregateOperator::create(
                     std::make_unique<ManyChunkSource>(std::move(chunks)), keys, definitions)
                     .value();
  for (const ExpectedGroup& model : expected) {
    auto step = grouped->next(resources).value();
    ASSERT_EQ(step.kind(), PhysicalOperatorStepKind::kChunk);
    const ScalarValue actual_key = cell(step.chunk()->chunk(), 0U);
    if (model.key.has_value()) {
      EXPECT_EQ(std::get<std::int64_t>(actual_key.storage()), *model.key);
    } else {
      EXPECT_TRUE(actual_key.is_null());
    }
    EXPECT_EQ(std::get<std::int64_t>(cell(step.chunk()->chunk(), 1U).storage()), model.rows);
    EXPECT_EQ(std::get<std::int64_t>(cell(step.chunk()->chunk(), 2U).storage()), model.present);
    if (model.present == 0) {
      EXPECT_TRUE(cell(step.chunk()->chunk(), 3U).is_null());
    } else {
      EXPECT_EQ(std::get<std::int64_t>(cell(step.chunk()->chunk(), 3U).storage()), model.sum);
    }
  }
  EXPECT_EQ(grouped->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

} // namespace
} // namespace chronos::query
