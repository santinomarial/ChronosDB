#include "chronos/query/aggregate.hpp"
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
string_column(const std::span<const std::optional<std::string_view>> values) {
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
             {.type = type(schema::LogicalTypeKind::kString),
              .nullable = true,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = null_count},
             std::move(buffers))
      .value();
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

[[nodiscard]] AccountedVectorChunk accounted_chunk(const QueryResourceContext& resources,
                                                   columnar::OwnedPhysicalColumn column,
                                                   std::vector<std::uint32_t> selection = {}) {
  const std::uint32_t rows = column.row_count();
  VectorSelection selected =
      selection.empty() ? VectorSelection::all(rows).value()
                        : VectorSelection::from_indices(rows, std::move(selection)).value();
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(std::move(column));
  VectorChunk chunk = VectorChunk::create(std::move(columns), std::move(selected)).value();
  const std::size_t charge = chunk.retained_buffer_bytes() + 1'024U;
  return AccountedVectorChunk::create(std::move(chunk), resources.reserve(charge).value(),
                                      resources)
      .value();
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
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);
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

} // namespace
} // namespace chronos::query
