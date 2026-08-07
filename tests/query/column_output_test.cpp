#include "chronos/common/status.hpp"
#include "chronos/query/column_output.hpp"
#include "chronos/query/value.hpp"
#include "chronos/schema/logical_type.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <gtest/gtest.h>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

void append_u32(std::vector<std::byte>& bytes, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes.push_back(static_cast<std::byte>((value >> (index * 8U)) & 0xffU));
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

[[nodiscard]] columnar::OwnedPhysicalColumn string_column() {
  columnar::ColumnVectorBuffers buffers;
  buffers.validity = {std::byte{0x0d}};
  for (const std::uint32_t offset : {0U, 1U, 1U, 4U, 5U})
    append_u32(buffers.offsets, offset);
  for (const char value : std::string_view{"acccd"})
    buffers.values.push_back(static_cast<std::byte>(value));
  return columnar::OwnedPhysicalColumn::create({.type = type(schema::LogicalTypeKind::kString),
                                                .nullable = true,
                                                .row_count = 4U,
                                                .null_count = 1U},
                                               std::move(buffers))
      .value();
}

[[nodiscard]] columnar::OwnedPhysicalColumn
generated_string_column(const std::span<const std::optional<std::string>> values) {
  columnar::ColumnVectorBuffers buffers;
  buffers.validity.resize(columnar::bitmap_size(static_cast<std::uint32_t>(values.size())));
  append_u32(buffers.offsets, 0U);
  std::uint32_t null_count = 0U;
  for (std::size_t row = 0U; row < values.size(); ++row) {
    if (!values[row].has_value()) {
      ++null_count;
    } else {
      buffers.validity[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
      const std::string& present =
          values[row].value(); // NOLINT(bugprone-unchecked-optional-access)
      for (const char byte : present)
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

[[nodiscard]] columnar::OwnedPhysicalColumn bool_column() {
  return columnar::OwnedPhysicalColumn::create(
             {.type = type(schema::LogicalTypeKind::kBool),
              .nullable = true,
              .row_count = 4U,
              .null_count = 1U},
             {.validity = {std::byte{0x0b}}, .offsets = {}, .values = {std::byte{0x09}}})
      .value();
}

[[nodiscard]] std::vector<columnar::OwnedPhysicalColumn> sample_columns() {
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(int64_column(std::vector<std::int64_t>{10, 20, 30, 40}));
  columns.push_back(string_column());
  columns.push_back(bool_column());
  return columns;
}

[[nodiscard]] AccountedVectorChunk
accounted_chunk(const QueryResourceContext& resources,
                std::vector<columnar::OwnedPhysicalColumn> columns,
                std::vector<std::uint32_t> selection, const std::size_t charge = 4'096U) {
  VectorChunk chunk =
      VectorChunk::create(std::move(columns),
                          VectorSelection::from_indices(4U, std::move(selection)).value())
          .value();
  return AccountedVectorChunk::create(std::move(chunk), resources.reserve(charge).value(),
                                      resources)
      .value();
}

[[nodiscard]] AccountedVectorChunk
accounted_int64_chunk(const QueryResourceContext& resources,
                      const std::span<const std::int64_t> values) {
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(int64_column(values));
  VectorChunk chunk =
      VectorChunk::create(std::move(columns),
                          VectorSelection::all(static_cast<std::uint32_t>(values.size())).value())
          .value();
  return AccountedVectorChunk::create(std::move(chunk), resources.reserve(16'384U).value(),
                                      resources)
      .value();
}

class OneChunkSource final : public PhysicalOperator {
public:
  explicit OneChunkSource(AccountedVectorChunk chunk) : chunk_(std::move(chunk)) {}

  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next(const QueryResourceContext& resources) override {
    const common::Result<void> active = resources.check_cancelled();
    if (!active.has_value())
      return common::make_unexpected(active.error());
    if (!chunk_.has_value())
      return PhysicalOperatorStep::end();
    AccountedVectorChunk chunk = std::move(*chunk_);
    chunk_.reset();
    return PhysicalOperatorStep::chunk(std::move(chunk));
  }

private:
  std::optional<AccountedVectorChunk> chunk_;
};

class EmptySource final : public PhysicalOperator {
public:
  [[nodiscard]] common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    return PhysicalOperatorStep::end();
  }
};

[[nodiscard]] std::int64_t i64(const VectorChunk& chunk, const std::size_t column,
                               const std::size_t row) {
  const common::ByteView bytes =
      chunk.cell({.column_ordinal = column, .selected_row = row}).value().bytes().value();
  std::uint64_t bits = 0U;
  for (std::size_t byte = 0U; byte < bytes.size(); ++byte)
    bits |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(bytes[byte])) << (byte * 8U);
  return std::bit_cast<std::int64_t>(bits);
}

[[nodiscard]] std::string text(const VectorChunk& chunk, const std::size_t column,
                               const std::size_t row) {
  const common::ByteView bytes =
      chunk.cell({.column_ordinal = column, .selected_row = row}).value().bytes().value();
  std::string result;
  result.reserve(bytes.size());
  for (const std::byte value : bytes)
    result.push_back(static_cast<char>(std::to_integer<unsigned char>(value)));
  return result;
}

[[nodiscard]] ScalarValue representative_constant(const schema::LogicalTypeKind kind) {
  using schema::LogicalTypeKind;
  const schema::LogicalType logical_type = kind == LogicalTypeKind::kDecimal
                                               ? schema::LogicalType::decimal(38U, 9U).value()
                                               : type(kind);
  switch (kind) {
  case LogicalTypeKind::kBool:
    return ScalarValue::boolean(true).value();
  case LogicalTypeKind::kInt8:
  case LogicalTypeKind::kInt16:
  case LogicalTypeKind::kInt32:
  case LogicalTypeKind::kInt64:
  case LogicalTypeKind::kTimestampNs:
  case LogicalTypeKind::kDate:
    return ScalarValue::signed_value(logical_type, -7).value();
  case LogicalTypeKind::kUInt8:
  case LogicalTypeKind::kUInt16:
  case LogicalTypeKind::kUInt32:
  case LogicalTypeKind::kUInt64:
    return ScalarValue::unsigned_value(logical_type, 7U).value();
  case LogicalTypeKind::kFloat32:
    return ScalarValue::float32(1.25F).value();
  case LogicalTypeKind::kFloat64:
    return ScalarValue::float64(-2.5).value();
  case LogicalTypeKind::kDecimal: {
    Decimal128Value decimal;
    decimal.coefficient.front() = std::byte{42};
    return ScalarValue::decimal(logical_type, decimal).value();
  }
  case LogicalTypeKind::kSymbol:
  case LogicalTypeKind::kString:
    return ScalarValue::text(logical_type, "constant").value();
  case LogicalTypeKind::kBinary:
    return ScalarValue::binary({std::byte{0x00}, std::byte{0xff}, std::byte{0x41}});
  case LogicalTypeKind::kUuid: {
    common::Uuid::Bytes bytes{};
    bytes.front() = std::byte{0x12};
    bytes.back() = std::byte{0x34};
    return ScalarValue::uuid(common::Uuid{bytes});
  }
  }
  return ScalarValue::untyped_null();
}

[[nodiscard]] const schema::LogicalType& required_type(const ScalarValue& value) {
  const std::optional<schema::LogicalType>& logical_type = value.type();
  if (!logical_type.has_value())
    std::abort();
  return *logical_type;
}

[[nodiscard]] VectorExpression add_five_expression() {
  std::vector<VectorExpressionInstruction> instructions;
  instructions.emplace_back(VectorInputExpression{.input_column_ordinal = 0U,
                                                  .type = type(schema::LogicalTypeKind::kInt64),
                                                  .nullable = false});
  instructions.emplace_back(VectorConstantExpression{
      ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), 5).value()});
  instructions.emplace_back(VectorBinaryExpression{
      .operation = VectorBinaryOperation::kAdd, .left_instruction = 0U, .right_instruction = 1U});
  return VectorExpression::create(std::move(instructions)).value();
}

[[nodiscard]] VectorExpression null_add_expression() {
  std::vector<VectorExpressionInstruction> instructions;
  instructions.emplace_back(VectorInputExpression{.input_column_ordinal = 0U,
                                                  .type = type(schema::LogicalTypeKind::kInt64),
                                                  .nullable = false});
  instructions.emplace_back(
      VectorConstantExpression{ScalarValue::null(type(schema::LogicalTypeKind::kInt64))});
  instructions.emplace_back(VectorBinaryExpression{
      .operation = VectorBinaryOperation::kAdd, .left_instruction = 0U, .right_instruction = 1U});
  return VectorExpression::create(std::move(instructions)).value();
}

[[nodiscard]] VectorExpression short_circuit_expression() {
  std::vector<VectorExpressionInstruction> instructions;
  instructions.emplace_back(VectorConstantExpression{ScalarValue::boolean(false).value()});
  instructions.emplace_back(VectorConstantExpression{
      ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), 1).value()});
  instructions.emplace_back(VectorConstantExpression{
      ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), 0).value()});
  instructions.emplace_back(VectorBinaryExpression{.operation = VectorBinaryOperation::kDivide,
                                                   .left_instruction = 1U,
                                                   .right_instruction = 2U});
  instructions.emplace_back(VectorBinaryExpression{.operation = VectorBinaryOperation::kGreater,
                                                   .left_instruction = 3U,
                                                   .right_instruction = 2U});
  instructions.emplace_back(VectorBinaryExpression{
      .operation = VectorBinaryOperation::kAnd, .left_instruction = 0U, .right_instruction = 4U});
  return VectorExpression::create(std::move(instructions)).value();
}

[[nodiscard]] VectorExpression source_constant_expression(const VectorBinaryOperation operation,
                                                          const std::int64_t constant) {
  std::vector<VectorExpressionInstruction> instructions;
  instructions.emplace_back(VectorInputExpression{.input_column_ordinal = 0U,
                                                  .type = type(schema::LogicalTypeKind::kInt64),
                                                  .nullable = false});
  instructions.emplace_back(VectorConstantExpression{
      ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), constant).value()});
  instructions.emplace_back(VectorBinaryExpression{
      .operation = operation, .left_instruction = 0U, .right_instruction = 1U});
  return VectorExpression::create(std::move(instructions)).value();
}

[[nodiscard]] VectorExpression constant_binary_expression(ScalarValue left, ScalarValue right,
                                                          const VectorBinaryOperation operation) {
  std::vector<VectorExpressionInstruction> instructions;
  instructions.emplace_back(VectorConstantExpression{std::move(left)});
  instructions.emplace_back(VectorConstantExpression{std::move(right)});
  instructions.emplace_back(VectorBinaryExpression{
      .operation = operation, .left_instruction = 0U, .right_instruction = 1U});
  return VectorExpression::create(std::move(instructions)).value();
}

[[nodiscard]] VectorExpression constant_unary_expression(ScalarValue operand,
                                                         const VectorUnaryOperation operation) {
  std::vector<VectorExpressionInstruction> instructions;
  instructions.emplace_back(VectorConstantExpression{std::move(operand)});
  instructions.emplace_back(
      VectorUnaryExpression{.operation = operation, .operand_instruction = 0U});
  return VectorExpression::create(std::move(instructions)).value();
}

[[nodiscard]] VectorExpression constant_cast_expression(ScalarValue operand,
                                                        const schema::LogicalType& target) {
  std::vector<VectorExpressionInstruction> instructions;
  instructions.emplace_back(VectorConstantExpression{std::move(operand)});
  instructions.emplace_back(VectorCastExpression{.operand_instruction = 0U, .target_type = target});
  return VectorExpression::create(std::move(instructions)).value();
}

[[nodiscard]] common::Result<VectorExpression> variable_text_expression() {
  const schema::LogicalType string = type(schema::LogicalTypeKind::kString);
  const schema::LogicalType symbol = type(schema::LogicalTypeKind::kSymbol);
  std::vector<VectorExpressionInstruction> instructions;
  instructions.emplace_back(
      VectorInputExpression{.input_column_ordinal = 1U, .type = string, .nullable = true});
  instructions.emplace_back(VectorUnaryExpression{.operation = VectorUnaryOperation::kUpperAscii,
                                                  .operand_instruction = 0U});
  instructions.emplace_back(
      VectorConstantExpression{ScalarValue::text(string, "FaLlBaCk").value()});
  instructions.emplace_back(VectorUnaryExpression{.operation = VectorUnaryOperation::kLowerAscii,
                                                  .operand_instruction = 2U});
  instructions.emplace_back(VectorBinaryExpression{.operation = VectorBinaryOperation::kCoalesce,
                                                   .left_instruction = 1U,
                                                   .right_instruction = 3U});
  instructions.emplace_back(VectorCastExpression{.operand_instruction = 4U, .target_type = symbol});
  return VectorExpression::create(std::move(instructions));
}

[[nodiscard]] VectorExpression
text_comparison_expression(const VectorBinaryOperation operation,
                           const std::size_t input_column_ordinal = 1U,
                           const std::string_view constant = "CCC") {
  const schema::LogicalType string = type(schema::LogicalTypeKind::kString);
  std::vector<VectorExpressionInstruction> instructions;
  instructions.emplace_back(VectorInputExpression{
      .input_column_ordinal = input_column_ordinal, .type = string, .nullable = true});
  instructions.emplace_back(VectorUnaryExpression{.operation = VectorUnaryOperation::kLowerAscii,
                                                  .operand_instruction = 0U});
  instructions.emplace_back(VectorUnaryExpression{.operation = VectorUnaryOperation::kUpperAscii,
                                                  .operand_instruction = 1U});
  instructions.emplace_back(
      VectorConstantExpression{ScalarValue::text(string, std::string{constant}).value()});
  instructions.emplace_back(VectorBinaryExpression{
      .operation = operation, .left_instruction = 2U, .right_instruction = 3U});
  return VectorExpression::create(std::move(instructions)).value();
}

[[nodiscard]] VectorExpression text_null_expression(const VectorUnaryOperation operation) {
  const schema::LogicalType string = type(schema::LogicalTypeKind::kString);
  std::vector<VectorExpressionInstruction> instructions;
  instructions.emplace_back(
      VectorInputExpression{.input_column_ordinal = 1U, .type = string, .nullable = true});
  instructions.emplace_back(
      VectorUnaryExpression{.operation = operation, .operand_instruction = 0U});
  return VectorExpression::create(std::move(instructions)).value();
}

TEST(SourceColumnOutputOperatorTest, ReordersDuplicatesAndCompactsSelectedRows) {
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  auto source =
      std::make_unique<OneChunkSource>(accounted_chunk(resources, sample_columns(), {0U, 2U, 3U}));
  auto output = SourceColumnOutputOperator::create(std::move(source), {1U, 0U, 1U, 2U});
  ASSERT_TRUE(output.has_value()) << output.error().to_string();

  common::Result<PhysicalOperatorStep> step = (*output)->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().to_string();
  ASSERT_EQ(step->kind(), PhysicalOperatorStepKind::kChunk);
  const VectorChunk& chunk = step->chunk()->chunk();
  EXPECT_EQ(chunk.physical_row_count(), 3U);
  EXPECT_EQ(chunk.selected_row_count(), 3U);
  EXPECT_TRUE(chunk.selection().is_identity());
  ASSERT_EQ(chunk.column_count(), 4U);
  EXPECT_EQ(chunk.column(0U)->type().kind(), schema::LogicalTypeKind::kString);
  EXPECT_EQ(chunk.column(1U)->type().kind(), schema::LogicalTypeKind::kInt64);
  EXPECT_TRUE(chunk.column(2U)->nullable());
  EXPECT_EQ(chunk.column(3U)->type().kind(), schema::LogicalTypeKind::kBool);
  EXPECT_EQ(text(chunk, 0U, 0U), "a");
  EXPECT_EQ(text(chunk, 0U, 1U), "ccc");
  EXPECT_EQ(text(chunk, 2U, 2U), "d");
  EXPECT_EQ(i64(chunk, 1U, 0U), 10);
  EXPECT_EQ(i64(chunk, 1U, 1U), 30);
  EXPECT_EQ(i64(chunk, 1U, 2U), 40);
  EXPECT_TRUE(chunk.cell({.column_ordinal = 3U, .selected_row = 0U})->boolean().value());
  EXPECT_TRUE(chunk.cell({.column_ordinal = 3U, .selected_row = 1U})->is_null());
  EXPECT_TRUE(chunk.cell({.column_ordinal = 3U, .selected_row = 2U})->boolean().value());
  EXPECT_NE(chunk.column(0U)->values().data(), chunk.column(2U)->values().data());
  EXPECT_GE(step->chunk()->charged_memory_bytes(), chunk.retained_buffer_bytes());
  EXPECT_GT(resources.reserved_memory_bytes(), 0U);

  step = PhysicalOperatorStep::end();
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  EXPECT_FALSE(resources.is_cancelled());
  step = (*output)->next(resources);
  ASSERT_TRUE(step.has_value());
  EXPECT_EQ(step->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ((*output)->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
}

TEST(SourceColumnOutputOperatorTest, PreservesEmptyProgressAndZeroColumnCardinality) {
  {
    QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
    auto output =
        SourceColumnOutputOperator::create(
            std::make_unique<OneChunkSource>(accounted_chunk(resources, sample_columns(), {})),
            {2U, 1U, 2U})
            .value();
    auto step = output->next(resources);
    ASSERT_TRUE(step.has_value()) << step.error().to_string();
    ASSERT_EQ(step->kind(), PhysicalOperatorStepKind::kChunk);
    EXPECT_EQ(step->chunk()->chunk().physical_row_count(), 4U);
    EXPECT_EQ(step->chunk()->chunk().selected_row_count(), 0U);
    EXPECT_EQ(step->chunk()->chunk().column_count(), 3U);
    EXPECT_FALSE(step->chunk()->chunk().selection().is_identity());
    EXPECT_TRUE(step->chunk()->chunk().column(0U)->cell(2U)->is_null());
  }
  {
    QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
    auto output =
        SourceColumnOutputOperator::create(std::make_unique<OneChunkSource>(accounted_chunk(
                                               resources, sample_columns(), {1U, 3U})),
                                           {})
            .value();
    auto step = output->next(resources);
    ASSERT_TRUE(step.has_value()) << step.error().to_string();
    ASSERT_EQ(step->kind(), PhysicalOperatorStepKind::kChunk);
    EXPECT_EQ(step->chunk()->chunk().physical_row_count(), 2U);
    EXPECT_EQ(step->chunk()->chunk().selected_row_count(), 2U);
    EXPECT_EQ(step->chunk()->chunk().column_count(), 0U);
  }
}

TEST(SourceColumnOutputOperatorTest, RejectsConfigurationAndRuntimeLimitsWithoutLeakingCredit) {
  EXPECT_EQ(SourceColumnOutputOperator::create({}, {}).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(
      SourceColumnOutputOperator::create(std::make_unique<EmptySource>(), {}, {.maximum_rows = 0U})
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);
  std::vector<std::size_t> oversized_capacity;
  oversized_capacity.reserve(kMaximumSourceColumnOutputWidth + 1U);
  oversized_capacity.push_back(0U);
  EXPECT_EQ(SourceColumnOutputOperator::create(std::make_unique<EmptySource>(),
                                               std::move(oversized_capacity))
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);

  {
    QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
    auto output =
        SourceColumnOutputOperator::create(
            std::make_unique<OneChunkSource>(accounted_chunk(resources, sample_columns(), {0U})),
            {3U})
            .value();
    const auto failed = output->next(resources);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error().code(), common::StatusCode::kOutOfRange);
    EXPECT_TRUE(resources.is_cancelled());
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  {
    QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
    auto output =
        SourceColumnOutputOperator::create(std::make_unique<OneChunkSource>(accounted_chunk(
                                               resources, sample_columns(), {0U, 1U})),
                                           {0U}, {.maximum_rows = 1U})
            .value();
    const auto failed = output->next(resources);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  {
    QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
    auto output =
        SourceColumnOutputOperator::create(std::make_unique<OneChunkSource>(accounted_chunk(
                                               resources, sample_columns(), {0U, 1U})),
                                           {}, {.maximum_buffer_bytes = 1U})
            .value();
    const auto failed = output->next(resources);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  {
    QueryResourceContext resources = QueryResourceContext::create(4'096U).value();
    auto output =
        SourceColumnOutputOperator::create(std::make_unique<OneChunkSource>(accounted_chunk(
                                               resources, sample_columns(), {0U, 1U}, 4'096U)),
                                           {0U})
            .value();
    const auto failed = output->next(resources);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  {
    QueryResourceContext owner = QueryResourceContext::create(1U << 20U).value();
    QueryResourceContext impostor = QueryResourceContext::create(1U << 20U).value();
    auto output =
        SourceColumnOutputOperator::create(
            std::make_unique<OneChunkSource>(accounted_chunk(owner, sample_columns(), {0U, 1U})),
            {0U})
            .value();
    const auto failed = output->next(impostor);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error().code(), common::StatusCode::kInvalidArgument);
    EXPECT_TRUE(impostor.is_cancelled());
    EXPECT_FALSE(owner.is_cancelled());
    EXPECT_EQ(owner.reserved_memory_bytes(), 0U);
  }
}

TEST(ColumnOutputOperatorTest, InterleavesSourceAndTypedConstantsAndCompactsSparseRows) {
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  auto source =
      std::make_unique<OneChunkSource>(accounted_chunk(resources, sample_columns(), {0U, 2U, 3U}));
  std::vector<ColumnOutputPosition> positions;
  positions.emplace_back(SourceColumnOutputPosition{1U});
  positions.emplace_back(ConstantColumnOutputPosition{
      ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), 77).value()});
  positions.emplace_back(
      ConstantColumnOutputPosition{ScalarValue::null(type(schema::LogicalTypeKind::kString))});
  positions.emplace_back(ConstantColumnOutputPosition{ScalarValue::boolean(true).value()});
  positions.emplace_back(SourceColumnOutputPosition{0U});
  auto output = ColumnOutputOperator::create(std::move(source), std::move(positions)).value();

  auto step = output->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().to_string();
  ASSERT_EQ(step->kind(), PhysicalOperatorStepKind::kChunk);
  const VectorChunk& chunk = step->chunk()->chunk();
  ASSERT_EQ(chunk.column_count(), 5U);
  EXPECT_EQ(chunk.physical_row_count(), 3U);
  EXPECT_EQ(chunk.selected_row_count(), 3U);
  EXPECT_TRUE(chunk.selection().is_identity());
  EXPECT_EQ(text(chunk, 0U, 1U), "ccc");
  EXPECT_EQ(i64(chunk, 1U, 0U), 77);
  EXPECT_EQ(i64(chunk, 1U, 2U), 77);
  EXPECT_TRUE(chunk.column(2U)->nullable());
  EXPECT_EQ(chunk.column(2U)->null_count(), 3U);
  EXPECT_TRUE(chunk.cell({.column_ordinal = 2U, .selected_row = 1U})->is_null());
  EXPECT_FALSE(chunk.column(3U)->nullable());
  EXPECT_TRUE(chunk.cell({.column_ordinal = 3U, .selected_row = 2U})->boolean().value());
  EXPECT_EQ(i64(chunk, 4U, 1U), 30);

  step = PhysicalOperatorStep::end();
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  EXPECT_EQ(output->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
}

TEST(ColumnOutputOperatorTest, MaterializesCheckedComputedExpressionsWithSqlNullAndShortCircuit) {
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  std::vector<ColumnOutputPosition> positions;
  positions.emplace_back(ComputedColumnOutputPosition{add_five_expression()});
  positions.emplace_back(ComputedColumnOutputPosition{null_add_expression()});
  positions.emplace_back(ComputedColumnOutputPosition{short_circuit_expression()});
  auto output = ColumnOutputOperator::create(std::make_unique<OneChunkSource>(accounted_chunk(
                                                 resources, sample_columns(), {0U, 2U, 3U})),
                                             std::move(positions))
                    .value();

  auto step = output->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().to_string();
  const VectorChunk& chunk = step->chunk()->chunk();
  ASSERT_EQ(chunk.column_count(), 3U);
  EXPECT_TRUE(chunk.selection().is_identity());
  EXPECT_EQ(i64(chunk, 0U, 0U), 15);
  EXPECT_EQ(i64(chunk, 0U, 1U), 35);
  EXPECT_EQ(i64(chunk, 0U, 2U), 45);
  EXPECT_FALSE(chunk.column(0U)->nullable());
  EXPECT_TRUE(chunk.column(1U)->nullable());
  EXPECT_EQ(chunk.column(1U)->null_count(), 3U);
  EXPECT_TRUE(chunk.cell({.column_ordinal = 1U, .selected_row = 1U})->is_null());
  EXPECT_FALSE(chunk.column(2U)->nullable());
  EXPECT_FALSE(chunk.cell({.column_ordinal = 2U, .selected_row = 0U})->boolean().value());
  EXPECT_FALSE(chunk.cell({.column_ordinal = 2U, .selected_row = 2U})->boolean().value());
}

TEST(ColumnOutputOperatorTest, RejectsComputedShapeMismatchAndRuntimeArithmeticFailure) {
  {
    std::vector<VectorExpressionInstruction> instructions;
    instructions.emplace_back(VectorInputExpression{.input_column_ordinal = 0U,
                                                    .type = type(schema::LogicalTypeKind::kInt64),
                                                    .nullable = true});
    VectorExpression expression = VectorExpression::create(std::move(instructions)).value();
    QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
    auto output =
        ColumnOutputOperator::create(
            std::make_unique<OneChunkSource>(accounted_chunk(resources, sample_columns(), {0U})),
            {ComputedColumnOutputPosition{std::move(expression)}})
            .value();
    const auto failed = output->next(resources);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error().code(), common::StatusCode::kInvalidArgument);
    EXPECT_TRUE(resources.is_cancelled());
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  {
    std::vector<VectorExpressionInstruction> instructions;
    instructions.emplace_back(
        VectorConstantExpression{ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64),
                                                           std::numeric_limits<std::int64_t>::max())
                                     .value()});
    instructions.emplace_back(VectorConstantExpression{
        ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), 1).value()});
    instructions.emplace_back(VectorBinaryExpression{
        .operation = VectorBinaryOperation::kAdd, .left_instruction = 0U, .right_instruction = 1U});
    QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
    auto output =
        ColumnOutputOperator::create(
            std::make_unique<OneChunkSource>(accounted_chunk(resources, sample_columns(), {0U})),
            {ComputedColumnOutputPosition{
                VectorExpression::create(std::move(instructions)).value()}})
            .value();
    const auto failed = output->next(resources);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error().code(), common::StatusCode::kOutOfRange);
    EXPECT_TRUE(resources.is_cancelled());
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
}

TEST(ColumnOutputOperatorPropertyTest, CheckedSignedKernelsMatchDeterministicScalarModel) {
  std::vector<std::int64_t> values;
  values.reserve(257U);
  std::uint64_t state = 0x9e3779b97f4a7c15ULL;
  for (std::size_t index = 0U; index < 257U; ++index) {
    state = state * 6'364'136'223'846'793'005ULL + 1'442'695'040'888'963'407ULL;
    values.push_back(static_cast<std::int64_t>(state % 2'000'001U) - 1'000'000);
  }

  std::vector<ColumnOutputPosition> positions;
  positions.emplace_back(
      ComputedColumnOutputPosition{source_constant_expression(VectorBinaryOperation::kAdd, 17)});
  positions.emplace_back(ComputedColumnOutputPosition{
      source_constant_expression(VectorBinaryOperation::kSubtract, -23)});
  positions.emplace_back(ComputedColumnOutputPosition{
      source_constant_expression(VectorBinaryOperation::kMultiply, 3)});
  positions.emplace_back(
      ComputedColumnOutputPosition{source_constant_expression(VectorBinaryOperation::kDivide, 7)});
  positions.emplace_back(ComputedColumnOutputPosition{
      source_constant_expression(VectorBinaryOperation::kRemainder, 11)});
  positions.emplace_back(
      ComputedColumnOutputPosition{source_constant_expression(VectorBinaryOperation::kLess, 0)});

  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  auto output = ColumnOutputOperator::create(
                    std::make_unique<OneChunkSource>(accounted_int64_chunk(resources, values)),
                    std::move(positions),
                    {.maximum_rows = 512U,
                     .maximum_columns = 8U,
                     .maximum_buffer_bytes = 1U << 20U,
                     .maximum_retained_buffer_bytes = 1U << 20U})
                    .value();
  auto step = output->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().to_string();
  const VectorChunk& chunk = step->chunk()->chunk();
  ASSERT_EQ(chunk.selected_row_count(), values.size());
  for (std::size_t row = 0U; row < values.size(); ++row) {
    EXPECT_EQ(i64(chunk, 0U, row), values[row] + 17);
    EXPECT_EQ(i64(chunk, 1U, row), values[row] - -23);
    EXPECT_EQ(i64(chunk, 2U, row), values[row] * 3);
    EXPECT_EQ(i64(chunk, 3U, row), values[row] / 7);
    EXPECT_EQ(i64(chunk, 4U, row), values[row] % 11);
    EXPECT_EQ(chunk.cell({.column_ordinal = 5U, .selected_row = row})->boolean().value(),
              values[row] < 0);
  }
}

TEST(ColumnOutputOperatorTest, MaterializesUnsignedIeeeDecimalAndThreeValuedKernels) {
  const schema::LogicalType uint64_type = type(schema::LogicalTypeKind::kUInt64);
  const schema::LogicalType decimal_type = schema::LogicalType::decimal(10U, 0U).value();
  Decimal128Value decimal_seven;
  decimal_seven.coefficient.front() = std::byte{7U};
  Decimal128Value decimal_five;
  decimal_five.coefficient.front() = std::byte{5U};

  std::vector<ColumnOutputPosition> positions;
  positions.emplace_back(ComputedColumnOutputPosition{constant_binary_expression(
      ScalarValue::unsigned_value(uint64_type, 9U).value(),
      ScalarValue::unsigned_value(uint64_type, 7U).value(), VectorBinaryOperation::kMultiply)});
  positions.emplace_back(ComputedColumnOutputPosition{constant_binary_expression(
      ScalarValue::float32(1.5F).value(), ScalarValue::float32(2.25F).value(),
      VectorBinaryOperation::kAdd)});
  positions.emplace_back(ComputedColumnOutputPosition{constant_binary_expression(
      ScalarValue::float64(1.0).value(), ScalarValue::float64(0.0).value(),
      VectorBinaryOperation::kDivide)});
  positions.emplace_back(ComputedColumnOutputPosition{constant_binary_expression(
      ScalarValue::decimal(decimal_type, decimal_seven).value(),
      ScalarValue::decimal(decimal_type, decimal_five).value(), VectorBinaryOperation::kAdd)});
  positions.emplace_back(ComputedColumnOutputPosition{constant_unary_expression(
      ScalarValue::decimal(decimal_type, decimal_five).value(), VectorUnaryOperation::kNegative)});
  positions.emplace_back(ComputedColumnOutputPosition{constant_binary_expression(
      ScalarValue::null(type(schema::LogicalTypeKind::kBool)), ScalarValue::boolean(false).value(),
      VectorBinaryOperation::kAnd)});
  positions.emplace_back(ComputedColumnOutputPosition{
      constant_binary_expression(ScalarValue::null(type(schema::LogicalTypeKind::kBool)),
                                 ScalarValue::boolean(false).value(), VectorBinaryOperation::kOr)});
  positions.emplace_back(ComputedColumnOutputPosition{constant_binary_expression(
      ScalarValue::float64(std::numeric_limits<double>::quiet_NaN()).value(),
      ScalarValue::float64(1.0).value(), VectorBinaryOperation::kLess)});
  common::Uuid::Bytes uuid_bytes{};
  uuid_bytes.front() = std::byte{0x42U};
  positions.emplace_back(ComputedColumnOutputPosition{constant_binary_expression(
      ScalarValue::uuid(common::Uuid{uuid_bytes}), ScalarValue::uuid(common::Uuid{uuid_bytes}),
      VectorBinaryOperation::kEqual)});

  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  auto output =
      ColumnOutputOperator::create(
          std::make_unique<OneChunkSource>(accounted_chunk(resources, sample_columns(), {0U})),
          std::move(positions))
          .value();
  auto step = output->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().to_string();
  const VectorChunk& chunk = step->chunk()->chunk();
  const auto decoded = [&](const std::size_t column) {
    const columnar::PhysicalColumnView* output_column = chunk.column(column);
    return ScalarValue::from_column_cell(
               output_column->type(),
               chunk.cell({.column_ordinal = column, .selected_row = 0U}).value())
        .value();
  };
  EXPECT_EQ(std::get<std::uint64_t>(decoded(0U).storage()), 63U);
  EXPECT_FLOAT_EQ(std::get<float>(decoded(1U).storage()), 3.75F);
  EXPECT_TRUE(std::isinf(std::get<double>(decoded(2U).storage())));
  EXPECT_EQ(std::get<Decimal128Value>(decoded(3U).storage()).coefficient.front(), std::byte{12U});
  EXPECT_EQ(std::get<Decimal128Value>(decoded(4U).storage()).coefficient.front(), std::byte{0xfbU});
  EXPECT_FALSE(std::get<bool>(decoded(5U).storage()));
  EXPECT_TRUE(decoded(6U).is_null());
  EXPECT_FALSE(std::get<bool>(decoded(7U).storage()));
  EXPECT_TRUE(std::get<bool>(decoded(8U).storage()));
}

TEST(ColumnOutputOperatorTest, MaterializesFixedWidthCastsCoalesceAndTimeBucket) {
  const schema::LogicalType int8_type = type(schema::LogicalTypeKind::kInt8);
  const schema::LogicalType int64_type = type(schema::LogicalTypeKind::kInt64);
  const schema::LogicalType uint64_type = type(schema::LogicalTypeKind::kUInt64);
  const schema::LogicalType date_type = type(schema::LogicalTypeKind::kDate);
  const schema::LogicalType timestamp_type = type(schema::LogicalTypeKind::kTimestampNs);
  const schema::LogicalType decimal_type = schema::LogicalType::decimal(6U, 2U).value();

  std::vector<ColumnOutputPosition> positions;
  positions.emplace_back(ComputedColumnOutputPosition{
      constant_cast_expression(ScalarValue::signed_value(int64_type, 127).value(), int8_type)});
  positions.emplace_back(ComputedColumnOutputPosition{
      constant_cast_expression(ScalarValue::unsigned_value(uint64_type, 42U).value(), int64_type)});
  positions.emplace_back(ComputedColumnOutputPosition{
      constant_cast_expression(ScalarValue::float64(7.9).value(), int8_type)});
  positions.emplace_back(ComputedColumnOutputPosition{
      constant_cast_expression(ScalarValue::signed_value(date_type, -1).value(), timestamp_type)});
  positions.emplace_back(ComputedColumnOutputPosition{
      constant_cast_expression(ScalarValue::signed_value(timestamp_type, -1).value(), date_type)});
  positions.emplace_back(ComputedColumnOutputPosition{
      constant_cast_expression(ScalarValue::signed_value(int64_type, 7).value(), decimal_type)});

  std::vector<VectorExpressionInstruction> coalesce;
  coalesce.emplace_back(VectorConstantExpression{ScalarValue::signed_value(int64_type, 9).value()});
  coalesce.emplace_back(VectorConstantExpression{ScalarValue::signed_value(int64_type, 1).value()});
  coalesce.emplace_back(VectorConstantExpression{ScalarValue::signed_value(int64_type, 0).value()});
  coalesce.emplace_back(VectorBinaryExpression{.operation = VectorBinaryOperation::kDivide,
                                               .left_instruction = 1U,
                                               .right_instruction = 2U});
  coalesce.emplace_back(VectorBinaryExpression{.operation = VectorBinaryOperation::kCoalesce,
                                               .left_instruction = 0U,
                                               .right_instruction = 3U});
  positions.emplace_back(
      ComputedColumnOutputPosition{VectorExpression::create(std::move(coalesce)).value()});
  positions.emplace_back(ComputedColumnOutputPosition{
      constant_binary_expression(ScalarValue::signed_value(int64_type, 1'000'000'000).value(),
                                 ScalarValue::signed_value(timestamp_type, -500'000'000).value(),
                                 VectorBinaryOperation::kTimeBucket)});

  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  auto output =
      ColumnOutputOperator::create(
          std::make_unique<OneChunkSource>(accounted_chunk(resources, sample_columns(), {0U})),
          std::move(positions))
          .value();
  auto step = output->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().to_string();
  const VectorChunk& chunk = step->chunk()->chunk();
  const auto decoded = [&](const std::size_t column) {
    const columnar::PhysicalColumnView* output_column = chunk.column(column);
    return ScalarValue::from_column_cell(
               output_column->type(),
               chunk.cell({.column_ordinal = column, .selected_row = 0U}).value())
        .value();
  };
  EXPECT_EQ(std::get<std::int64_t>(decoded(0U).storage()), 127);
  EXPECT_EQ(std::get<std::int64_t>(decoded(1U).storage()), 42);
  EXPECT_EQ(std::get<std::int64_t>(decoded(2U).storage()), 7);
  EXPECT_EQ(std::get<std::int64_t>(decoded(3U).storage()), -86'400'000'000'000LL);
  EXPECT_EQ(std::get<std::int64_t>(decoded(4U).storage()), -1);
  EXPECT_EQ(std::get<Decimal128Value>(decoded(5U).storage()).coefficient.front(), std::byte{0xbcU});
  EXPECT_EQ(std::get<std::int64_t>(decoded(6U).storage()), 9);
  EXPECT_EQ(std::get<std::int64_t>(decoded(7U).storage()), -1'000'000'000);
}

TEST(ColumnOutputOperatorPropertyTest, MaterializesEveryFrozenTypedConstantAndTypedNull) {
  std::vector<ColumnOutputPosition> positions;
  std::vector<ScalarValue> expected;
  std::vector<bool> expected_nullable;
  for (std::uint16_t code = 1U; code <= 18U; ++code) {
    const schema::LogicalTypeKind kind = schema::logical_type_kind_from_code(code).value();
    ScalarValue value = representative_constant(kind);
    positions.emplace_back(ConstantColumnOutputPosition{value});
    expected.push_back(value);
    expected_nullable.push_back(false);
    positions.emplace_back(ConstantColumnOutputPosition{.value = value, .force_nullable = true});
    expected.push_back(value);
    expected_nullable.push_back(true);
    positions.emplace_back(ConstantColumnOutputPosition{ScalarValue::null(required_type(value))});
    expected.push_back(ScalarValue::null(required_type(value)));
    expected_nullable.push_back(true);
  }

  QueryResourceContext resources = QueryResourceContext::create(16U << 20U).value();
  auto source =
      std::make_unique<OneChunkSource>(accounted_chunk(resources, sample_columns(), {0U, 2U, 3U}));
  auto output = ColumnOutputOperator::create(std::move(source), std::move(positions),
                                             {.maximum_rows = 4U,
                                              .maximum_columns = expected.size(),
                                              .maximum_buffer_bytes = 1U << 20U,
                                              .maximum_retained_buffer_bytes = 2U << 20U})
                    .value();
  auto step = output->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().to_string();
  const VectorChunk& chunk = step->chunk()->chunk();
  ASSERT_EQ(chunk.column_count(), expected.size());
  for (std::size_t column = 0U; column < expected.size(); ++column) {
    ASSERT_EQ(chunk.column(column)->type(), required_type(expected[column]));
    EXPECT_EQ(chunk.column(column)->nullable(), expected_nullable[column]);
    EXPECT_EQ(chunk.column(column)->null_count(), expected[column].is_null() ? 3U : 0U);
    for (std::size_t row = 0U; row < chunk.selected_row_count(); ++row) {
      const columnar::ColumnCellView cell =
          chunk.cell({.column_ordinal = column, .selected_row = row}).value();
      const ScalarValue actual =
          ScalarValue::from_column_cell(required_type(expected[column]), cell).value();
      EXPECT_EQ(actual.type(), expected[column].type());
      EXPECT_EQ(actual.storage(), expected[column].storage());
    }
  }
}

TEST(ColumnOutputOperatorTest, PreservesEmptySelectionProgressAndRejectsInvalidInputs) {
  EXPECT_EQ(ColumnOutputOperator::create({}, {}).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(
      ColumnOutputOperator::create(std::make_unique<EmptySource>(),
                                   {ConstantColumnOutputPosition{ScalarValue::untyped_null()}})
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);

  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  auto output =
      ColumnOutputOperator::create(
          std::make_unique<OneChunkSource>(accounted_chunk(resources, sample_columns(), {})),
          {ConstantColumnOutputPosition{
               ScalarValue::text(type(schema::LogicalTypeKind::kString), "x").value()},
           SourceColumnOutputPosition{0U}})
          .value();
  auto step = output->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().to_string();
  ASSERT_EQ(step->kind(), PhysicalOperatorStepKind::kChunk);
  EXPECT_EQ(step->chunk()->chunk().physical_row_count(), 4U);
  EXPECT_EQ(step->chunk()->chunk().selected_row_count(), 0U);
  EXPECT_FALSE(step->chunk()->chunk().selection().is_identity());
  EXPECT_EQ(step->chunk()->chunk().column(0U)->values().size(), 4U);

  QueryResourceContext failed_resources = QueryResourceContext::create(1U << 20U).value();
  auto invalid_ordinal =
      ColumnOutputOperator::create(std::make_unique<OneChunkSource>(
                                       accounted_chunk(failed_resources, sample_columns(), {0U})),
                                   {SourceColumnOutputPosition{3U}})
          .value();
  const auto failed = invalid_ordinal->next(failed_resources);
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kOutOfRange);
  EXPECT_TRUE(failed_resources.is_cancelled());
  EXPECT_EQ(failed_resources.reserved_memory_bytes(), 0U);
}

TEST(ColumnOutputOperatorTest, MaterializesCanonicalBorrowedVariableExpressions) {
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  common::Result<VectorExpression> expression = variable_text_expression();
  ASSERT_TRUE(expression.has_value()) << expression.error().to_string();
  auto created =
      ColumnOutputOperator::create(std::make_unique<OneChunkSource>(accounted_chunk(
                                       resources, sample_columns(), {0U, 1U, 2U, 3U})),
                                   {ComputedColumnOutputPosition{std::move(*expression)}});
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  auto step = (*created)->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().to_string();
  const VectorChunk& chunk = step->chunk()->chunk();
  ASSERT_EQ(chunk.column_count(), 1U);
  ASSERT_EQ(chunk.physical_row_count(), 4U);
  EXPECT_EQ(chunk.column(0U)->type().kind(), schema::LogicalTypeKind::kSymbol);
  EXPECT_FALSE(chunk.column(0U)->nullable());
  EXPECT_EQ(chunk.column(0U)->null_count(), 0U);
  EXPECT_EQ(text(chunk, 0U, 0U), "A");
  EXPECT_EQ(text(chunk, 0U, 1U), "fallback");
  EXPECT_EQ(text(chunk, 0U, 2U), "CCC");
  EXPECT_EQ(text(chunk, 0U, 3U), "D");
  EXPECT_EQ(chunk.column(0U)->offsets().size(), 5U * sizeof(std::uint32_t));
  EXPECT_EQ(chunk.column(0U)->values().size(), 13U);

  QueryResourceContext limited_resources = QueryResourceContext::create(1U << 20U).value();
  auto limited_expression = variable_text_expression().value();
  auto limited =
      ColumnOutputOperator::create(std::make_unique<OneChunkSource>(accounted_chunk(
                                       limited_resources, sample_columns(), {0U, 1U, 2U, 3U})),
                                   {ComputedColumnOutputPosition{std::move(limited_expression)}},
                                   {.maximum_buffer_bytes = 48U})
          .value();
  auto rejected = limited->next(limited_resources);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_TRUE(limited_resources.is_cancelled());
  EXPECT_EQ(limited_resources.reserved_memory_bytes(), 0U);
}

TEST(ColumnOutputOperatorTest, MaterializesBorrowedTextComparisonsAndNullPredicates) {
  static constexpr std::array<VectorBinaryOperation, 6> kOperations{
      VectorBinaryOperation::kEqual,   VectorBinaryOperation::kNotEqual,
      VectorBinaryOperation::kLess,    VectorBinaryOperation::kLessEqual,
      VectorBinaryOperation::kGreater, VectorBinaryOperation::kGreaterEqual};
  static constexpr std::array<std::array<bool, 4>, 6> kExpected{{
      {false, false, true, false},
      {true, false, false, true},
      {true, false, false, false},
      {true, false, true, false},
      {false, false, false, true},
      {false, false, true, true},
  }};
  std::vector<ColumnOutputPosition> positions;
  positions.reserve(9U);
  for (const VectorBinaryOperation operation : kOperations)
    positions.emplace_back(ComputedColumnOutputPosition{text_comparison_expression(operation)});
  positions.emplace_back(
      ComputedColumnOutputPosition{text_null_expression(VectorUnaryOperation::kIsNull)});
  positions.emplace_back(
      ComputedColumnOutputPosition{text_null_expression(VectorUnaryOperation::kIsNotNull)});

  const schema::LogicalType string = type(schema::LogicalTypeKind::kString);
  std::vector<VectorExpressionInstruction> combined;
  combined.emplace_back(
      VectorInputExpression{.input_column_ordinal = 1U, .type = string, .nullable = true});
  combined.emplace_back(VectorUnaryExpression{.operation = VectorUnaryOperation::kUpperAscii,
                                              .operand_instruction = 0U});
  combined.emplace_back(VectorConstantExpression{ScalarValue::text(string, "CCC").value()});
  combined.emplace_back(VectorBinaryExpression{
      .operation = VectorBinaryOperation::kEqual, .left_instruction = 1U, .right_instruction = 2U});
  combined.emplace_back(
      VectorUnaryExpression{.operation = VectorUnaryOperation::kIsNull, .operand_instruction = 0U});
  combined.emplace_back(VectorBinaryExpression{
      .operation = VectorBinaryOperation::kOr, .left_instruction = 3U, .right_instruction = 4U});
  positions.emplace_back(
      ComputedColumnOutputPosition{VectorExpression::create(std::move(combined)).value()});

  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  auto output = ColumnOutputOperator::create(
                    std::make_unique<OneChunkSource>(accounted_chunk(
                        resources, sample_columns(), std::vector<std::uint32_t>{0U, 1U, 2U, 3U})),
                    std::move(positions))
                    .value();
  auto step = output->next(resources).value();
  const VectorChunk& actual = step.chunk()->chunk();
  ASSERT_EQ(actual.column_count(), 9U);
  for (std::size_t column = 0U; column < kOperations.size(); ++column) {
    EXPECT_TRUE(actual.column(column)->nullable());
    EXPECT_EQ(actual.column(column)->null_count(), 1U);
    for (std::size_t row = 0U; row < 4U; ++row) {
      const columnar::ColumnCellView cell =
          actual.cell({.column_ordinal = column, .selected_row = row}).value();
      if (row == 1U)
        EXPECT_TRUE(cell.is_null());
      else
        EXPECT_EQ(cell.boolean().value(), kExpected[column][row]);
    }
  }
  for (std::size_t row = 0U; row < 4U; ++row) {
    EXPECT_EQ(actual.cell({.column_ordinal = 6U, .selected_row = row})->boolean().value(),
              row == 1U);
    EXPECT_EQ(actual.cell({.column_ordinal = 7U, .selected_row = row})->boolean().value(),
              row != 1U);
    EXPECT_EQ(actual.cell({.column_ordinal = 8U, .selected_row = row})->boolean().value(),
              row == 1U || row == 2U);
  }
  EXPECT_FALSE(actual.column(6U)->nullable());
  EXPECT_FALSE(actual.column(7U)->nullable());
  EXPECT_TRUE(actual.column(8U)->nullable());
  EXPECT_EQ(actual.column(8U)->null_count(), 0U);
}

TEST(ColumnOutputOperatorPropertyTest, VariableCaseOutputMatchesIndependentByteModel) {
  constexpr std::uint32_t kRows = 257U;
  std::vector<std::optional<std::string>> values;
  std::vector<std::uint32_t> selection;
  values.reserve(kRows);
  selection.reserve(kRows);
  std::uint64_t state = 0x243f6a8885a308d3ULL;
  for (std::uint32_t row = 0U; row < kRows; ++row) {
    state = state * 6'364'136'223'846'793'005ULL + 1'442'695'040'888'963'407ULL;
    if (row % 7U == 0U) {
      values.emplace_back(std::nullopt);
    } else {
      std::string value;
      const std::size_t length = static_cast<std::size_t>(state % 31U);
      value.reserve(length + 3U);
      for (std::size_t index = 0U; index < length; ++index) {
        state = state * 6'364'136'223'846'793'005ULL + 1'442'695'040'888'963'407ULL;
        value.push_back(static_cast<char>('A' + (state % 26U)));
      }
      if (row % 11U == 0U)
        value.append("\xE2\x82\xAC");
      values.emplace_back(std::move(value));
    }
    if (row % 3U != 1U)
      selection.push_back(row);
  }

  const schema::LogicalType string = type(schema::LogicalTypeKind::kString);
  const schema::LogicalType symbol = type(schema::LogicalTypeKind::kSymbol);
  std::vector<VectorExpressionInstruction> instructions;
  instructions.emplace_back(
      VectorInputExpression{.input_column_ordinal = 0U, .type = string, .nullable = true});
  instructions.emplace_back(VectorUnaryExpression{.operation = VectorUnaryOperation::kLowerAscii,
                                                  .operand_instruction = 0U});
  instructions.emplace_back(VectorConstantExpression{ScalarValue::text(string, "empty").value()});
  instructions.emplace_back(VectorBinaryExpression{.operation = VectorBinaryOperation::kCoalesce,
                                                   .left_instruction = 1U,
                                                   .right_instruction = 2U});
  instructions.emplace_back(VectorCastExpression{.operand_instruction = 3U, .target_type = symbol});

  QueryResourceContext resources = QueryResourceContext::create(1U << 22U).value();
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(generated_string_column(values));
  VectorChunk source = VectorChunk::create(std::move(columns),
                                           VectorSelection::from_indices(kRows, selection).value())
                           .value();
  AccountedVectorChunk accounted =
      AccountedVectorChunk::create(std::move(source),
                                   resources.reserve(std::size_t{64U} * 1024U).value(), resources)
          .value();
  auto output =
      ColumnOutputOperator::create(
          std::make_unique<OneChunkSource>(std::move(accounted)),
          {ComputedColumnOutputPosition{VectorExpression::create(std::move(instructions)).value()}})
          .value();
  auto step = output->next(resources).value();
  const VectorChunk& actual = step.chunk()->chunk();
  ASSERT_EQ(actual.selected_row_count(), selection.size());
  for (std::size_t selected = 0U; selected < selection.size(); ++selected) {
    std::string expected = values[selection[selected]].value_or("empty");
    for (char& byte : expected) {
      if (byte >= 'A' && byte <= 'Z')
        byte = static_cast<char>(byte - 'A' + 'a');
    }
    EXPECT_EQ(text(actual, 0U, selected), expected);
  }
}

TEST(ColumnOutputOperatorPropertyTest, TextPredicatesMatchIndependentUnsignedByteOrder) {
  constexpr std::uint32_t kRows = 257U;
  static constexpr std::array<VectorBinaryOperation, 6> kOperations{
      VectorBinaryOperation::kEqual,   VectorBinaryOperation::kNotEqual,
      VectorBinaryOperation::kLess,    VectorBinaryOperation::kLessEqual,
      VectorBinaryOperation::kGreater, VectorBinaryOperation::kGreaterEqual};
  std::vector<std::optional<std::string>> values;
  std::vector<std::uint32_t> selection;
  values.reserve(kRows);
  selection.reserve(kRows);
  std::uint64_t state = 0x13198a2e03707344ULL;
  for (std::uint32_t row = 0U; row < kRows; ++row) {
    state = state * 2'862'933'555'777'941'757ULL + 3'037'000'493ULL;
    if (row % 13U == 0U) {
      values.emplace_back(std::nullopt);
    } else {
      std::string value;
      const std::size_t length = static_cast<std::size_t>(state % 17U);
      value.reserve(length + 3U);
      for (std::size_t index = 0U; index < length; ++index) {
        state = state * 2'862'933'555'777'941'757ULL + 3'037'000'493ULL;
        const char base = state % 2U == 0U ? 'A' : 'a';
        value.push_back(static_cast<char>(base + static_cast<char>(state % 26U)));
      }
      if (row % 17U == 0U)
        value.append("\xE2\x82\xAC");
      values.emplace_back(std::move(value));
    }
    if (row % 4U != 2U)
      selection.push_back(row);
  }

  std::vector<ColumnOutputPosition> positions;
  positions.reserve(kOperations.size());
  for (const VectorBinaryOperation operation : kOperations) {
    positions.emplace_back(
        ComputedColumnOutputPosition{text_comparison_expression(operation, 0U, "M")});
  }
  QueryResourceContext resources = QueryResourceContext::create(1U << 22U).value();
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(generated_string_column(values));
  VectorChunk source = VectorChunk::create(std::move(columns),
                                           VectorSelection::from_indices(kRows, selection).value())
                           .value();
  AccountedVectorChunk accounted =
      AccountedVectorChunk::create(std::move(source),
                                   resources.reserve(std::size_t{64U} * 1024U).value(), resources)
          .value();
  auto output = ColumnOutputOperator::create(std::make_unique<OneChunkSource>(std::move(accounted)),
                                             std::move(positions))
                    .value();
  auto step = output->next(resources).value();
  const VectorChunk& actual = step.chunk()->chunk();
  ASSERT_EQ(actual.selected_row_count(), selection.size());
  for (std::size_t selected = 0U; selected < selection.size(); ++selected) {
    const std::optional<std::string>& source_value = values[selection[selected]];
    for (std::size_t operation_index = 0U; operation_index < kOperations.size();
         ++operation_index) {
      const columnar::ColumnCellView cell =
          actual.cell({.column_ordinal = operation_index, .selected_row = selected}).value();
      if (!source_value.has_value()) {
        EXPECT_TRUE(cell.is_null());
        continue;
      }
      std::string transformed = source_value.value(); // NOLINT(bugprone-unchecked-optional-access)
      for (char& byte : transformed) {
        if (byte >= 'a' && byte <= 'z')
          byte = static_cast<char>(byte - 'a' + 'A');
      }
      constexpr std::string_view kCompared{"M"};
      int order = 0;
      const std::size_t shared = std::min(transformed.size(), kCompared.size());
      for (std::size_t index = 0U; index < shared; ++index) {
        const auto lhs = static_cast<unsigned char>(transformed[index]);
        const auto rhs = static_cast<unsigned char>(kCompared[index]);
        if (lhs != rhs) {
          order = lhs < rhs ? -1 : 1;
          break;
        }
      }
      if (order == 0 && transformed.size() != kCompared.size())
        order = transformed.size() < kCompared.size() ? -1 : 1;
      bool expected = false;
      switch (kOperations[operation_index]) {
      case VectorBinaryOperation::kEqual:
        expected = order == 0;
        break;
      case VectorBinaryOperation::kNotEqual:
        expected = order != 0;
        break;
      case VectorBinaryOperation::kLess:
        expected = order < 0;
        break;
      case VectorBinaryOperation::kLessEqual:
        expected = order <= 0;
        break;
      case VectorBinaryOperation::kGreater:
        expected = order > 0;
        break;
      case VectorBinaryOperation::kGreaterEqual:
        expected = order >= 0;
        break;
      default:
        FAIL() << "unexpected text comparison operation";
      }
      EXPECT_EQ(cell.boolean().value(), expected);
    }
  }
}

TEST(ColumnOutputOperatorTest, RejectsConfigurationRuntimeBudgetAndForeignOwnership) {
  std::vector<ColumnOutputPosition> oversized_positions;
  oversized_positions.reserve(kMaximumColumnOutputWidth + 1U);
  oversized_positions.emplace_back(SourceColumnOutputPosition{0U});
  EXPECT_EQ(
      ColumnOutputOperator::create(std::make_unique<EmptySource>(), std::move(oversized_positions))
          .error()
          .code(),
      common::StatusCode::kResourceExhausted);

  std::string oversized_constant{"x"};
  oversized_constant.reserve(4'096U);
  std::vector<ColumnOutputPosition> retained_positions;
  retained_positions.emplace_back(ConstantColumnOutputPosition{
      ScalarValue::text(type(schema::LogicalTypeKind::kString), std::move(oversized_constant))
          .value()});
  EXPECT_EQ(ColumnOutputOperator::create(std::make_unique<EmptySource>(),
                                         std::move(retained_positions),
                                         {.maximum_retained_buffer_bytes = 1'024U})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);

  {
    QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
    auto output =
        ColumnOutputOperator::create(
            std::make_unique<OneChunkSource>(
                accounted_chunk(resources, sample_columns(), {0U, 1U})),
            {ConstantColumnOutputPosition{
                ScalarValue::text(type(schema::LogicalTypeKind::kString), "long").value()}},
            {.maximum_buffer_bytes = 1U})
            .value();
    const auto failed = output->next(resources);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_TRUE(resources.is_cancelled());
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  {
    QueryResourceContext resources = QueryResourceContext::create(4'096U).value();
    auto output =
        ColumnOutputOperator::create(
            std::make_unique<OneChunkSource>(
                accounted_chunk(resources, sample_columns(), {0U, 1U}, 4'096U)),
            {ConstantColumnOutputPosition{
                ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), 1).value()}})
            .value();
    const auto failed = output->next(resources);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  {
    QueryResourceContext owner = QueryResourceContext::create(1U << 20U).value();
    QueryResourceContext impostor = QueryResourceContext::create(1U << 20U).value();
    auto output =
        ColumnOutputOperator::create(
            std::make_unique<OneChunkSource>(accounted_chunk(owner, sample_columns(), {0U, 1U})),
            {ConstantColumnOutputPosition{ScalarValue::boolean(true).value()}})
            .value();
    const auto failed = output->next(impostor);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error().code(), common::StatusCode::kInvalidArgument);
    EXPECT_TRUE(impostor.is_cancelled());
    EXPECT_FALSE(owner.is_cancelled());
    EXPECT_EQ(owner.reserved_memory_bytes(), 0U);
  }
}

[[nodiscard]] std::size_t frozen_width(const schema::LogicalTypeKind kind) {
  using schema::LogicalTypeKind;
  switch (kind) {
  case LogicalTypeKind::kInt8:
  case LogicalTypeKind::kUInt8:
    return 1U;
  case LogicalTypeKind::kInt16:
  case LogicalTypeKind::kUInt16:
    return 2U;
  case LogicalTypeKind::kInt32:
  case LogicalTypeKind::kUInt32:
  case LogicalTypeKind::kFloat32:
  case LogicalTypeKind::kDate:
    return 4U;
  case LogicalTypeKind::kInt64:
  case LogicalTypeKind::kUInt64:
  case LogicalTypeKind::kFloat64:
  case LogicalTypeKind::kTimestampNs:
    return 8U;
  case LogicalTypeKind::kDecimal:
  case LogicalTypeKind::kUuid:
    return 16U;
  case LogicalTypeKind::kBool:
  case LogicalTypeKind::kSymbol:
  case LogicalTypeKind::kString:
  case LogicalTypeKind::kBinary:
    return 0U;
  }
  return 0U;
}

[[nodiscard]] columnar::OwnedPhysicalColumn frozen_column(const std::uint16_t code) {
  const schema::LogicalTypeKind kind = schema::logical_type_kind_from_code(code).value();
  const schema::LogicalType logical_type = kind == schema::LogicalTypeKind::kDecimal
                                               ? schema::LogicalType::decimal(38U, 9U).value()
                                               : type(kind);
  columnar::ColumnVectorBuffers buffers;
  buffers.validity = {std::byte{0x05}};
  if (logical_type.is_variable_width()) {
    for (const std::uint32_t offset : {0U, 1U, 1U, 2U})
      append_u32(buffers.offsets, offset);
    buffers.values = {std::byte{'a'}, std::byte{'b'}};
  } else if (kind == schema::LogicalTypeKind::kBool) {
    buffers.values = {std::byte{0x05}};
  } else {
    buffers.values.resize(frozen_width(kind) * 3U);
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = logical_type, .nullable = true, .row_count = 3U, .null_count = 1U},
             std::move(buffers))
      .value();
}

TEST(SourceColumnOutputOperatorPropertyTest,
     EveryFrozenTypePreservesSparseCellsUnderReverseAndDuplicateOutput) {
  std::vector<columnar::OwnedPhysicalColumn> actual_columns;
  std::vector<columnar::OwnedPhysicalColumn> expected_columns;
  for (std::uint16_t code = 1U; code <= 18U; ++code) {
    actual_columns.push_back(frozen_column(code));
    expected_columns.push_back(frozen_column(code));
  }
  VectorChunk expected = VectorChunk::create(std::move(expected_columns),
                                             VectorSelection::from_indices(3U, {0U, 2U}).value())
                             .value();
  std::vector<std::size_t> outputs;
  for (std::size_t ordinal = 18U; ordinal > 0U; --ordinal)
    outputs.push_back(ordinal - 1U);
  outputs.push_back(17U);
  outputs.push_back(0U);

  QueryResourceContext resources = QueryResourceContext::create(16U << 20U).value();
  VectorChunk input = VectorChunk::create(std::move(actual_columns),
                                          VectorSelection::from_indices(3U, {0U, 2U}).value())
                          .value();
  auto source = std::make_unique<OneChunkSource>(
      AccountedVectorChunk::create(std::move(input), resources.reserve(1U << 20U).value(),
                                   resources)
          .value());
  auto output = SourceColumnOutputOperator::create(std::move(source), outputs,
                                                   {.maximum_rows = 3U,
                                                    .maximum_columns = outputs.size(),
                                                    .maximum_buffer_bytes = 1U << 20U,
                                                    .maximum_retained_buffer_bytes = 1U << 20U})
                    .value();
  auto step = output->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().to_string();
  const VectorChunk& actual = step->chunk()->chunk();
  ASSERT_EQ(actual.column_count(), outputs.size());
  for (std::size_t output_ordinal = 0U; output_ordinal < outputs.size(); ++output_ordinal) {
    const std::size_t expected_ordinal = outputs[output_ordinal];
    ASSERT_EQ(actual.column(output_ordinal)->type(), expected.column(expected_ordinal)->type());
    ASSERT_EQ(actual.column(output_ordinal)->nullable(),
              expected.column(expected_ordinal)->nullable());
    for (std::size_t row = 0U; row < 2U; ++row) {
      const columnar::ColumnCellView expected_cell =
          expected.cell({.column_ordinal = expected_ordinal, .selected_row = row}).value();
      const columnar::ColumnCellView actual_cell =
          actual.cell({.column_ordinal = output_ordinal, .selected_row = row}).value();
      ASSERT_EQ(actual_cell.kind(), expected_cell.kind());
      if (actual_cell.kind() == columnar::ColumnCellView::Kind::kBoolean) {
        EXPECT_EQ(actual_cell.boolean().value(), expected_cell.boolean().value());
      } else if (actual_cell.kind() == columnar::ColumnCellView::Kind::kBytes) {
        EXPECT_TRUE(std::ranges::equal(actual_cell.bytes().value(), expected_cell.bytes().value()));
      }
    }
  }
}

} // namespace
} // namespace chronos::query
