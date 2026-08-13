#include "chronos/common/crc32c.hpp"
#include "chronos/query/distributed_vector_aggregate_state.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <limits>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

void append_u32(std::vector<std::byte>& output, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    output.push_back(static_cast<std::byte>((value >> (index * 8U)) & 0xffU));
}

void store_u16(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint16_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
}

[[nodiscard]] std::uint16_t load_u16(const common::ByteView bytes, const std::size_t offset) {
  return static_cast<std::uint16_t>(std::to_integer<std::uint16_t>(bytes[offset]) |
                                    (std::to_integer<std::uint16_t>(bytes[offset + 1U]) << 8U));
}

[[nodiscard]] std::uint32_t load_u32(const common::ByteView bytes, const std::size_t offset) {
  std::uint32_t value{};
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    value |= std::to_integer<std::uint32_t>(bytes[offset + index]) << (index * 8U);
  return value;
}

[[nodiscard]] std::uint64_t load_u64(const common::ByteView bytes, const std::size_t offset) {
  std::uint64_t value{};
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    value |= std::to_integer<std::uint64_t>(bytes[offset + index]) << (index * 8U);
  return value;
}

void refresh_checksums(std::vector<std::byte>& bytes) {
  store_u32(bytes, 108U, common::crc32c(common::ByteView{bytes}.first(108U)));
  store_u32(bytes, bytes.size() - 4U,
            common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

[[nodiscard]] std::size_t fixed_width(const schema::LogicalTypeKind kind) {
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

[[nodiscard]] columnar::OwnedPhysicalColumn
one_value_column(const schema::LogicalType logical_type) {
  columnar::ColumnVectorBuffers buffers;
  using schema::LogicalTypeKind;
  if (logical_type.kind() == LogicalTypeKind::kBool) {
    buffers.values.push_back(std::byte{1U});
  } else if (logical_type.is_variable_width()) {
    append_u32(buffers.offsets, 0U);
    if (logical_type.kind() == LogicalTypeKind::kBinary) {
      buffers.values = {std::byte{0U}, std::byte{0xffU}};
    } else {
      buffers.values = {std::byte{0xc3U}, std::byte{0xa9U}};
    }
    append_u32(buffers.offsets, static_cast<std::uint32_t>(buffers.values.size()));
  } else {
    const std::size_t width = fixed_width(logical_type.kind());
    buffers.values.resize(width);
    if (logical_type.kind() == LogicalTypeKind::kFloat32) {
      const std::uint32_t bits = std::bit_cast<std::uint32_t>(1.5F);
      for (std::size_t index = 0U; index < sizeof(bits); ++index)
        buffers.values[index] = static_cast<std::byte>((bits >> (index * 8U)) & 0xffU);
    } else if (logical_type.kind() == LogicalTypeKind::kFloat64) {
      const std::uint64_t bits = std::bit_cast<std::uint64_t>(1.5);
      for (std::size_t index = 0U; index < sizeof(bits); ++index)
        buffers.values[index] = static_cast<std::byte>((bits >> (index * 8U)) & 0xffU);
    } else {
      buffers.values.front() = std::byte{42U};
      if (logical_type.kind() == LogicalTypeKind::kUuid)
        buffers.values.back() = std::byte{7U};
    }
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = logical_type, .nullable = false, .row_count = 1U, .null_count = 0U},
             std::move(buffers))
      .value();
}

[[nodiscard]] columnar::OwnedPhysicalColumn
signed_column(const std::span<const std::optional<std::int64_t>> values) {
  columnar::ColumnVectorBuffers buffers;
  buffers.validity.resize(columnar::bitmap_size(static_cast<std::uint32_t>(values.size())));
  buffers.values.resize(values.size() * sizeof(std::int64_t));
  std::uint32_t null_count{};
  for (std::size_t row = 0U; row < values.size(); ++row) {
    // The value access is guarded in the same conditional expression.
    // NOLINTBEGIN(bugprone-unchecked-optional-access)
    const std::int64_t* value =
        values[row].has_value() ? std::addressof(values[row].value()) : nullptr;
    // NOLINTEND(bugprone-unchecked-optional-access)
    if (value == nullptr) {
      ++null_count;
      continue;
    }
    buffers.validity[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
    const std::uint64_t bits = std::bit_cast<std::uint64_t>(*value);
    for (std::size_t index = 0U; index < sizeof(bits); ++index) {
      buffers.values[row * sizeof(bits) + index] =
          static_cast<std::byte>((bits >> (index * 8U)) & 0xffU);
    }
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = type(schema::LogicalTypeKind::kInt64),
              .nullable = true,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = null_count},
             std::move(buffers))
      .value();
}

void accumulate_column(MergeableVectorAggregateState& state,
                       const columnar::OwnedPhysicalColumn& column,
                       const QueryResourceContext& resources) {
  for (std::uint32_t row = 0U; row < column.row_count(); ++row)
    ASSERT_TRUE(state.accumulate_cell(column.cell(row).value(), resources).has_value());
}

[[nodiscard]] VectorAggregateDefinition aggregate(const VectorAggregateOperation operation) {
  return {.operation = operation,
          .input = VectorAggregateInput{.column_ordinal = 0U,
                                        .type = type(schema::LogicalTypeKind::kInt64),
                                        .nullable = true}};
}

TEST(MergeableVectorAggregateStateCodecTest, RoundTripsEverySufficientNumericState) {
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  const std::array<std::optional<std::int64_t>, 3U> values{1, std::nullopt, 4};
  const auto column = signed_column(values);
  const std::array operations{
      VectorAggregateOperation::kCount,         VectorAggregateOperation::kSum,
      VectorAggregateOperation::kAverage,       VectorAggregateOperation::kMinimum,
      VectorAggregateOperation::kMaximum,       VectorAggregateOperation::kVariancePopulation,
      VectorAggregateOperation::kVarianceSample};
  for (const VectorAggregateOperation operation : operations) {
    auto state = MergeableVectorAggregateState::create(aggregate(operation)).value();
    accumulate_column(state, column, resources);
    auto encoded = encode_mergeable_vector_aggregate_state(state);
    ASSERT_TRUE(encoded.has_value());
    auto decoded = decode_mergeable_vector_aggregate_state_exact(encoded->bytes(), resources);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->definition(), state.definition());
    auto canonical = encode_mergeable_vector_aggregate_state(*decoded);
    ASSERT_TRUE(canonical.has_value());
    EXPECT_EQ(std::vector<std::byte>(canonical->bytes().begin(), canonical->bytes().end()),
              std::vector<std::byte>(encoded->bytes().begin(), encoded->bytes().end()));
    const auto result = std::move(*decoded).take_result();
    ASSERT_TRUE(result.has_value());
    if (operation == VectorAggregateOperation::kCount)
      EXPECT_EQ(std::get<std::int64_t>(result->storage()), 2);
    else if (operation == VectorAggregateOperation::kSum)
      EXPECT_EQ(std::get<std::int64_t>(result->storage()), 5);
    else if (operation == VectorAggregateOperation::kAverage)
      EXPECT_DOUBLE_EQ(std::get<double>(result->storage()), 2.5);
    else if (operation == VectorAggregateOperation::kMinimum)
      EXPECT_EQ(std::get<std::int64_t>(result->storage()), 1);
    else if (operation == VectorAggregateOperation::kMaximum)
      EXPECT_EQ(std::get<std::int64_t>(result->storage()), 4);
    else if (operation == VectorAggregateOperation::kVariancePopulation)
      EXPECT_DOUBLE_EQ(std::get<double>(result->storage()), 2.25);
    else
      EXPECT_DOUBLE_EQ(std::get<double>(result->storage()), 4.5);
  }

  auto count_star = MergeableVectorAggregateState::create(
                        {.operation = VectorAggregateOperation::kCountStar, .input = std::nullopt})
                        .value();
  ASSERT_TRUE(count_star.accumulate_count_star().has_value());
  ASSERT_TRUE(count_star.accumulate_count_star().has_value());
  auto encoded_count = encode_mergeable_vector_aggregate_state(count_star).value();
  auto decoded_count =
      decode_mergeable_vector_aggregate_state_exact(encoded_count.bytes(), resources).value();
  EXPECT_EQ(std::get<std::int64_t>(std::move(decoded_count).take_result()->storage()), 2);

  const std::array<std::optional<std::int64_t>, 2U> negative_values{-5, 2};
  const auto negative_column = signed_column(negative_values);
  auto negative_sum =
      MergeableVectorAggregateState::create(aggregate(VectorAggregateOperation::kSum)).value();
  accumulate_column(negative_sum, negative_column, resources);
  auto encoded_negative = encode_mergeable_vector_aggregate_state(negative_sum).value();
  auto decoded_negative =
      decode_mergeable_vector_aggregate_state_exact(encoded_negative.bytes(), resources).value();
  EXPECT_EQ(std::get<std::int64_t>(std::move(decoded_negative).take_result()->storage()), -3);
}

TEST(MergeableVectorAggregateStateCodecTest, RoundTripsExtremaForEveryLogicalType) {
  QueryResourceContext resources = QueryResourceContext::create(4U << 20U).value();
  const std::array kinds{schema::LogicalTypeKind::kBool,        schema::LogicalTypeKind::kInt8,
                         schema::LogicalTypeKind::kInt16,       schema::LogicalTypeKind::kInt32,
                         schema::LogicalTypeKind::kInt64,       schema::LogicalTypeKind::kUInt8,
                         schema::LogicalTypeKind::kUInt16,      schema::LogicalTypeKind::kUInt32,
                         schema::LogicalTypeKind::kUInt64,      schema::LogicalTypeKind::kFloat32,
                         schema::LogicalTypeKind::kFloat64,     schema::LogicalTypeKind::kDecimal,
                         schema::LogicalTypeKind::kTimestampNs, schema::LogicalTypeKind::kDate,
                         schema::LogicalTypeKind::kSymbol,      schema::LogicalTypeKind::kString,
                         schema::LogicalTypeKind::kBinary,      schema::LogicalTypeKind::kUuid};
  for (const schema::LogicalTypeKind kind : kinds) {
    SCOPED_TRACE(static_cast<std::uint16_t>(kind));
    const schema::LogicalType logical_type = kind == schema::LogicalTypeKind::kDecimal
                                                 ? schema::LogicalType::decimal(10U, 2U).value()
                                                 : type(kind);
    const auto column = one_value_column(logical_type);
    const VectorAggregateDefinition definition{
        .operation = VectorAggregateOperation::kMinimum,
        .input =
            VectorAggregateInput{.column_ordinal = 7U, .type = logical_type, .nullable = false}};
    auto state = MergeableVectorAggregateState::create(definition).value();
    ASSERT_TRUE(state.accumulate_cell(column.cell(0U).value(), resources).has_value());
    auto encoded = encode_mergeable_vector_aggregate_state(state);
    ASSERT_TRUE(encoded.has_value());
    auto decoded = decode_mergeable_vector_aggregate_state_exact(encoded->bytes(), resources);
    ASSERT_TRUE(decoded.has_value());
    auto canonical = encode_mergeable_vector_aggregate_state(*decoded);
    ASSERT_TRUE(canonical.has_value());
    EXPECT_TRUE(std::ranges::equal(canonical->bytes(), encoded->bytes()));
    auto original_result = std::move(state).take_result();
    auto decoded_result = std::move(*decoded).take_result();
    ASSERT_TRUE(original_result.has_value());
    ASSERT_TRUE(decoded_result.has_value());
    EXPECT_EQ(compare_scalar_values(*original_result, *decoded_result, ScalarNullPlacement::kLast),
              common::Result<int>{0});
  }
}

TEST(MergeableVectorAggregateStateCodecTest, RoundTripsSumForEveryNumericType) {
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  const std::array kinds{schema::LogicalTypeKind::kInt8,    schema::LogicalTypeKind::kInt16,
                         schema::LogicalTypeKind::kInt32,   schema::LogicalTypeKind::kInt64,
                         schema::LogicalTypeKind::kUInt8,   schema::LogicalTypeKind::kUInt16,
                         schema::LogicalTypeKind::kUInt32,  schema::LogicalTypeKind::kUInt64,
                         schema::LogicalTypeKind::kFloat32, schema::LogicalTypeKind::kFloat64,
                         schema::LogicalTypeKind::kDecimal};
  for (const schema::LogicalTypeKind kind : kinds) {
    SCOPED_TRACE(static_cast<std::uint16_t>(kind));
    const schema::LogicalType logical_type = kind == schema::LogicalTypeKind::kDecimal
                                                 ? schema::LogicalType::decimal(10U, 2U).value()
                                                 : type(kind);
    const auto column = one_value_column(logical_type);
    const VectorAggregateDefinition definition{
        .operation = VectorAggregateOperation::kSum,
        .input =
            VectorAggregateInput{.column_ordinal = 3U, .type = logical_type, .nullable = false}};
    auto state = MergeableVectorAggregateState::create(definition).value();
    ASSERT_TRUE(state.accumulate_cell(column.cell(0U).value(), resources).has_value());
    auto encoded = encode_mergeable_vector_aggregate_state(state);
    ASSERT_TRUE(encoded.has_value());
    auto decoded = decode_mergeable_vector_aggregate_state_exact(encoded->bytes(), resources);
    ASSERT_TRUE(decoded.has_value());
    auto canonical = encode_mergeable_vector_aggregate_state(*decoded);
    ASSERT_TRUE(canonical.has_value());
    EXPECT_TRUE(std::ranges::equal(canonical->bytes(), encoded->bytes()));
    auto original_result = std::move(state).take_result();
    auto decoded_result = std::move(*decoded).take_result();
    ASSERT_TRUE(original_result.has_value());
    ASSERT_TRUE(decoded_result.has_value());
    EXPECT_EQ(compare_scalar_values(*original_result, *decoded_result, ScalarNullPlacement::kLast),
              common::Result<int>{0});
  }
}

TEST(MergeableVectorAggregateStateCodecTest, FreezesHeaderAndRejectsCanonicalDamage) {
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  auto state = MergeableVectorAggregateState::create(
                   {.operation = VectorAggregateOperation::kCountStar, .input = std::nullopt})
                   .value();
  ASSERT_TRUE(state.accumulate_count_star().has_value());
  ASSERT_TRUE(state.accumulate_count_star().has_value());
  auto encoded = encode_mergeable_vector_aggregate_state(state).value();
  const common::ByteView bytes = encoded.bytes();
  EXPECT_EQ(bytes.size(), distributed_vector_aggregate_state_format::kMinimumFrameLength);
  const std::array<std::byte, 8U> magic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                        std::byte{'V'}, std::byte{'A'}, std::byte{'G'},
                                        std::byte{'S'}, std::byte{'1'}};
  EXPECT_TRUE(std::ranges::equal(bytes.first(magic.size()), magic));
  EXPECT_EQ(load_u16(bytes, 8U), 1U);
  EXPECT_EQ(load_u16(bytes, 10U), 0U);
  EXPECT_EQ(load_u32(bytes, 12U), 112U);
  EXPECT_EQ(load_u64(bytes, 16U), bytes.size());
  EXPECT_EQ(std::to_integer<std::uint8_t>(bytes[24U]), 0U);
  EXPECT_EQ(load_u64(bytes, 40U), 2U);
  EXPECT_EQ(load_u32(bytes, 108U), common::crc32c(bytes.first(108U)));
  EXPECT_EQ(load_u32(bytes, bytes.size() - 4U), common::crc32c(bytes.first(bytes.size() - 4U)));

  EXPECT_EQ(decode_mergeable_vector_aggregate_state_exact(bytes.first(bytes.size() - 1U), resources)
                .error()
                .code(),
            common::StatusCode::kCorruption);
  std::vector<std::byte> trailing(bytes.begin(), bytes.end());
  trailing.push_back(std::byte{});
  EXPECT_EQ(decode_mergeable_vector_aggregate_state_exact(trailing, resources).error().code(),
            common::StatusCode::kCorruption);
  std::vector<std::byte> damaged(bytes.begin(), bytes.end());
  damaged.back() ^= std::byte{1U};
  EXPECT_EQ(decode_mergeable_vector_aggregate_state_exact(damaged, resources).error().code(),
            common::StatusCode::kCorruption);
  std::vector<std::byte> future(bytes.begin(), bytes.end());
  store_u16(future, 8U, 2U);
  refresh_checksums(future);
  EXPECT_EQ(decode_mergeable_vector_aggregate_state_exact(future, resources).error().code(),
            common::StatusCode::kNotSupported);
  std::vector<std::byte> noncanonical(bytes.begin(), bytes.end());
  noncanonical[25U] |= std::byte{8U};
  refresh_checksums(noncanonical);
  EXPECT_EQ(decode_mergeable_vector_aggregate_state_exact(noncanonical, resources).error().code(),
            common::StatusCode::kCorruption);
}

TEST(MergeableVectorAggregateStateCodecTest, OwnsEveryFragmentedReadAndShortWriteBoundary) {
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  const auto column = one_value_column(type(schema::LogicalTypeKind::kString));
  const VectorAggregateDefinition definition{
      .operation = VectorAggregateOperation::kMaximum,
      .input = VectorAggregateInput{
          .column_ordinal = 0U, .type = type(schema::LogicalTypeKind::kString), .nullable = false}};
  auto state = MergeableVectorAggregateState::create(definition).value();
  ASSERT_TRUE(state.accumulate_cell(column.cell(0U).value(), resources).has_value());
  auto encoded = encode_mergeable_vector_aggregate_state(state).value();
  std::vector<std::byte> doubled(encoded.bytes().begin(), encoded.bytes().end());
  doubled.insert(doubled.end(), encoded.bytes().begin(), encoded.bytes().end());

  for (std::size_t split = 0U; split < encoded.bytes().size(); ++split) {
    MergeableVectorAggregateStateReader reader{resources};
    auto first = reader.consume(encoded.bytes().first(split));
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->consumed_bytes, split);
    EXPECT_FALSE(first->state.has_value());
    auto second = reader.consume(common::ByteView{doubled}.subspan(split));
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->consumed_bytes, encoded.bytes().size() - split);
    ASSERT_TRUE(second->state.has_value());
    // The value access is guarded in the same conditional expression.
    // NOLINTBEGIN(bugprone-unchecked-optional-access)
    const MergeableVectorAggregateState* decoded_state =
        second->state.has_value() ? std::addressof(second->state.value()) : nullptr;
    // NOLINTEND(bugprone-unchecked-optional-access)
    ASSERT_NE(decoded_state, nullptr);
    EXPECT_EQ(decoded_state->definition(), definition);
    EXPECT_EQ(reader.buffered_bytes(), 0U);
  }

  std::vector<std::byte> corrupt(encoded.bytes().begin(), encoded.bytes().end());
  corrupt.front() ^= std::byte{1U};
  MergeableVectorAggregateStateReader failed{resources};
  EXPECT_EQ(failed.consume(corrupt).error().code(), common::StatusCode::kCorruption);
  EXPECT_TRUE(failed.failed());
  EXPECT_EQ(failed.consume(encoded.bytes()).error().code(), common::StatusCode::kCorruption);

  auto cursor = MergeableVectorAggregateStateWriteCursor::create(state).value();
  const std::size_t complete_size = cursor.pending_write().size();
  EXPECT_TRUE(cursor.consume_written(3U).is_ok());
  const std::size_t before = cursor.written_bytes();
  EXPECT_EQ(cursor.consume_written(cursor.pending_write().size() + 1U).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(cursor.written_bytes(), before);
  auto moved = std::move(cursor);
  // Verifies the class's documented moved-from completion contract.
  // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
  EXPECT_TRUE(cursor.complete());
  EXPECT_EQ(moved.pending_write().size(), complete_size - 3U);
  EXPECT_TRUE(moved.consume_written(moved.pending_write().size()).is_ok());
  EXPECT_TRUE(moved.complete());
}

TEST(MergeableVectorAggregateStateCodecTest, AppliesLimitsBeforeRetainingVariableState) {
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  const auto column = one_value_column(type(schema::LogicalTypeKind::kString));
  auto state = MergeableVectorAggregateState::create(
                   {.operation = VectorAggregateOperation::kMinimum,
                    .input = VectorAggregateInput{.column_ordinal = 0U,
                                                  .type = type(schema::LogicalTypeKind::kString),
                                                  .nullable = false}})
                   .value();
  ASSERT_TRUE(state.accumulate_cell(column.cell(0U).value(), resources).has_value());
  const auto encoded = encode_mergeable_vector_aggregate_state(state).value();
  const std::size_t before = resources.reserved_memory_bytes();
  DistributedVectorAggregateStateDecodeLimits payload_limit;
  payload_limit.maximum_variable_extremum_bytes = 1U;
  EXPECT_EQ(decode_mergeable_vector_aggregate_state_exact(encoded.bytes(), resources, payload_limit)
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(resources.reserved_memory_bytes(), before);

  std::vector<std::byte> malformed_text(encoded.bytes().begin(), encoded.bytes().end());
  malformed_text[distributed_vector_aggregate_state_format::kHeaderLength] = std::byte{0xffU};
  refresh_checksums(malformed_text);
  EXPECT_EQ(decode_mergeable_vector_aggregate_state_exact(malformed_text, resources).error().code(),
            common::StatusCode::kCorruption);
  EXPECT_EQ(resources.reserved_memory_bytes(), before);

  DistributedVectorAggregateStateDecodeLimits frame_limit;
  frame_limit.maximum_frame_length = distributed_vector_aggregate_state_format::kMinimumFrameLength;
  MergeableVectorAggregateStateReader reader{resources, frame_limit};
  EXPECT_EQ(
      reader
          .consume(encoded.bytes().first(distributed_vector_aggregate_state_format::kHeaderLength))
          .error()
          .code(),
      common::StatusCode::kResourceExhausted);
  EXPECT_TRUE(reader.failed());

  DistributedVectorAggregateStateDecodeLimits invalid_limits;
  invalid_limits.maximum_variable_extremum_bytes = 0U;
  MergeableVectorAggregateStateReader invalid_reader{resources, invalid_limits};
  EXPECT_EQ(invalid_reader.consume({}).error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(invalid_reader.failed());
  EXPECT_EQ(invalid_reader.consume(encoded.bytes()).error().code(),
            common::StatusCode::kInvalidArgument);

  const auto finalized = std::move(state).take_result();
  ASSERT_TRUE(finalized.has_value());
  // The rvalue-qualified call finalizes without move-constructing the object.
  // NOLINTNEXTLINE(bugprone-use-after-move)
  EXPECT_EQ(encode_mergeable_vector_aggregate_state(state).error().code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::query
