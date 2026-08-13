#include "chronos/common/crc32c.hpp"
#include "chronos/query/distributed_vector_aggregate_exchange.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  return Identifier::from_uuid(uuid(seed)).value();
}

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
  std::uint16_t value{};
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    value |= std::to_integer<std::uint16_t>(bytes[offset + index]) << (index * 8U);
  return value;
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

void refresh_outer_checksums(std::vector<std::byte>& bytes) {
  store_u32(bytes, 84U, common::crc32c(common::ByteView{bytes}.first(84U)));
  store_u32(bytes, bytes.size() - 4U,
            common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

[[nodiscard]] columnar::OwnedPhysicalColumn string_column(const std::string& value) {
  columnar::ColumnVectorBuffers buffers;
  append_u32(buffers.offsets, 0U);
  const auto bytes = std::as_bytes(std::span{value.data(), value.size()});
  buffers.values.assign(bytes.begin(), bytes.end());
  append_u32(buffers.offsets, static_cast<std::uint32_t>(buffers.values.size()));
  return columnar::OwnedPhysicalColumn::create({.type = type(schema::LogicalTypeKind::kString),
                                                .nullable = false,
                                                .row_count = 1U,
                                                .null_count = 0U},
                                               std::move(buffers))
      .value();
}

[[nodiscard]] std::vector<VectorAggregateDefinition> definitions() {
  return {{.operation = VectorAggregateOperation::kCountStar, .input = std::nullopt},
          {.operation = VectorAggregateOperation::kMaximum,
           .input = VectorAggregateInput{.column_ordinal = 1U,
                                         .type = type(schema::LogicalTypeKind::kString),
                                         .nullable = false}}};
}

[[nodiscard]] MergeableVectorAggregateState
count_state(const VectorAggregateDefinition& definition) {
  auto state = MergeableVectorAggregateState::create(definition).value();
  EXPECT_TRUE(state.accumulate_count_star().has_value());
  EXPECT_TRUE(state.accumulate_count_star().has_value());
  return state;
}

[[nodiscard]] MergeableVectorAggregateState
string_state(const VectorAggregateDefinition& definition, const QueryResourceContext& resources) {
  auto state = MergeableVectorAggregateState::create(definition).value();
  const auto column = string_column("a variable extremum larger than short string storage");
  EXPECT_TRUE(state.accumulate_cell(column.cell(0U).value(), resources).has_value());
  return state;
}

TEST(DistributedVectorAggregateExchangeTest, BindsAndRoundTripsExactFragmentDefinitions) {
  const DistributedVectorPlanIntent intent{
      .mode = DistributedVectorPlanMode::kUngroupedAggregate,
      .aggregates = {
          {.operation = VectorAggregateOperation::kCountStar, .input_index = std::nullopt},
          {.operation = VectorAggregateOperation::kSum, .input_index = 0U},
          {.operation = VectorAggregateOperation::kMaximum, .input_index = 1U}}};
  const std::array projected{PhysicalColumnShape{type(schema::LogicalTypeKind::kInt64), true},
                             PhysicalColumnShape{type(schema::LogicalTypeKind::kString), false}};
  const DistributedVectorResultSchema result_schema{
      .columns = {
          {.name = "count", .type = type(schema::LogicalTypeKind::kInt64), .nullable = false},
          {.name = "sum", .type = type(schema::LogicalTypeKind::kInt64), .nullable = true},
          {.name = "maximum", .type = type(schema::LogicalTypeKind::kString), .nullable = true}}};
  auto bound =
      bind_distributed_vector_ungrouped_aggregate_definitions(intent, projected, result_schema);
  ASSERT_TRUE(bound.has_value());
  ASSERT_EQ(bound->size(), 3U);
  EXPECT_FALSE((*bound)[0].input.has_value());
  ASSERT_TRUE((*bound)[1].input.has_value());
  // NOLINTBEGIN(bugprone-unchecked-optional-access)
  const VectorAggregateInput* sum_input = std::addressof((*bound)[1].input.value());
  // NOLINTEND(bugprone-unchecked-optional-access)
  EXPECT_EQ(sum_input->column_ordinal, 0U);
  EXPECT_EQ(sum_input->type, projected[0].type);
  EXPECT_TRUE(sum_input->nullable);
  ASSERT_TRUE((*bound)[2].input.has_value());
  // NOLINTBEGIN(bugprone-unchecked-optional-access)
  const VectorAggregateInput* maximum_input = std::addressof((*bound)[2].input.value());
  // NOLINTEND(bugprone-unchecked-optional-access)
  EXPECT_EQ(maximum_input->type, projected[1].type);

  auto state = count_state((*bound)[0]);
  DistributedVectorAggregateExchangeMessage message{{.query_id = uuid(1U),
                                                     .tablet_id = id<schema::TabletId>(2U),
                                                     .sequence = 1U,
                                                     .aggregate_ordinal = 0U,
                                                     .terminal = false},
                                                    std::move(state)};
  auto encoded = encode_distributed_vector_aggregate_exchange_message(message, *bound);
  ASSERT_TRUE(encoded.has_value());
  const common::ByteView bytes = encoded->bytes();
  const std::array<std::byte, 8U> magic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                        std::byte{'V'}, std::byte{'A'}, std::byte{'E'},
                                        std::byte{'X'}, std::byte{'1'}};
  EXPECT_TRUE(std::ranges::equal(bytes.first(magic.size()), magic));
  EXPECT_EQ(load_u16(bytes, 8U), 1U);
  EXPECT_EQ(load_u16(bytes, 10U), 0U);
  EXPECT_EQ(load_u32(bytes, 12U), 96U);
  EXPECT_EQ(load_u64(bytes, 16U), bytes.size());
  EXPECT_TRUE(std::ranges::equal(bytes.subspan(24U, 16U), message.query_id.bytes()));
  EXPECT_TRUE(std::ranges::equal(bytes.subspan(40U, 16U), message.tablet_id.bytes()));
  EXPECT_EQ(load_u64(bytes, 56U), 1U);
  EXPECT_EQ(load_u32(bytes, 64U), 0U);
  EXPECT_EQ(load_u32(bytes, 68U), bound->size());
  EXPECT_EQ(load_u32(bytes, 72U), 0U);
  EXPECT_EQ(load_u32(bytes, 76U), bytes.size() - 100U);
  EXPECT_EQ(load_u32(bytes, 84U), common::crc32c(bytes.first(84U)));
  EXPECT_TRUE(std::ranges::all_of(bytes.subspan(88U, 8U),
                                  [](const std::byte value) { return value == std::byte{}; }));
  const common::ByteView nested = bytes.subspan(96U, load_u32(bytes, 76U));
  EXPECT_EQ(load_u32(bytes, 80U), common::crc32c(nested));
  EXPECT_EQ(load_u32(bytes, bytes.size() - 4U), common::crc32c(bytes.first(bytes.size() - 4U)));

  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  auto decoded = decode_distributed_vector_aggregate_exchange_message_exact(encoded->bytes(),
                                                                            *bound, resources);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->query_id, message.query_id);
  EXPECT_EQ(decoded->tablet_id, message.tablet_id);
  EXPECT_EQ(decoded->sequence, 1U);
  EXPECT_FALSE(decoded->terminal);
  auto result = std::move(decoded->state).take_result();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(std::get<std::int64_t>(result->storage()), 2);

  DistributedVectorPlanIntent grouped = intent;
  grouped.mode = DistributedVectorPlanMode::kGroupedAggregate;
  grouped.group_key_input_indices = {0U};
  EXPECT_EQ(
      bind_distributed_vector_ungrouped_aggregate_definitions(grouped, projected, result_schema)
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);
  auto wrong_schema = result_schema;
  wrong_schema.columns[1].type = type(schema::LogicalTypeKind::kFloat64);
  EXPECT_EQ(bind_distributed_vector_ungrouped_aggregate_definitions(intent, projected, wrong_schema)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedVectorAggregateExchangeTest, RejectsSequenceSchemaAndCanonicalDamage) {
  QueryResourceContext resources = QueryResourceContext::create(4U << 20U).value();
  const auto expected = definitions();
  auto state = string_state(expected[1], resources);
  DistributedVectorAggregateExchangeMessage message{{.query_id = uuid(3U),
                                                     .tablet_id = id<schema::TabletId>(4U),
                                                     .sequence = 2U,
                                                     .aggregate_ordinal = 1U,
                                                     .terminal = true},
                                                    std::move(state)};
  const auto encoded =
      encode_distributed_vector_aggregate_exchange_message(message, expected).value();

  EXPECT_EQ(decode_distributed_vector_aggregate_exchange_message_exact(
                encoded.bytes().first(encoded.bytes().size() - 1U), expected, resources)
                .error()
                .code(),
            common::StatusCode::kCorruption);
  std::vector<std::byte> trailing(encoded.bytes().begin(), encoded.bytes().end());
  trailing.push_back(std::byte{});
  EXPECT_EQ(
      decode_distributed_vector_aggregate_exchange_message_exact(trailing, expected, resources)
          .error()
          .code(),
      common::StatusCode::kCorruption);
  std::vector<std::byte> future(encoded.bytes().begin(), encoded.bytes().end());
  store_u16(future, 8U, 2U);
  refresh_outer_checksums(future);
  EXPECT_EQ(decode_distributed_vector_aggregate_exchange_message_exact(future, expected, resources)
                .error()
                .code(),
            common::StatusCode::kNotSupported);
  std::vector<std::byte> reserved(encoded.bytes().begin(), encoded.bytes().end());
  reserved[88U] = std::byte{1U};
  refresh_outer_checksums(reserved);
  EXPECT_EQ(
      decode_distributed_vector_aggregate_exchange_message_exact(reserved, expected, resources)
          .error()
          .code(),
      common::StatusCode::kCorruption);
  std::vector<std::byte> nested_damage(encoded.bytes().begin(), encoded.bytes().end());
  nested_damage[96U] ^= std::byte{1U};
  refresh_outer_checksums(nested_damage);
  EXPECT_EQ(
      decode_distributed_vector_aggregate_exchange_message_exact(nested_damage, expected, resources)
          .error()
          .code(),
      common::StatusCode::kCorruption);

  auto wrong_expected = expected;
  wrong_expected[1] = {.operation = VectorAggregateOperation::kMinimum, .input = expected[1].input};
  EXPECT_EQ(decode_distributed_vector_aggregate_exchange_message_exact(encoded.bytes(),
                                                                       wrong_expected, resources)
                .error()
                .code(),
            common::StatusCode::kCorruption);

  auto wrong_sequence_state = count_state(expected[0]);
  DistributedVectorAggregateExchangeMessage wrong_sequence{{.query_id = uuid(3U),
                                                            .tablet_id = id<schema::TabletId>(4U),
                                                            .sequence = 2U,
                                                            .aggregate_ordinal = 0U,
                                                            .terminal = false},
                                                           std::move(wrong_sequence_state)};
  EXPECT_EQ(
      encode_distributed_vector_aggregate_exchange_message(wrong_sequence, expected).error().code(),
      common::StatusCode::kInvalidArgument);
  auto wrong_terminal_state = count_state(expected[0]);
  DistributedVectorAggregateExchangeMessage wrong_terminal{{.query_id = uuid(3U),
                                                            .tablet_id = id<schema::TabletId>(4U),
                                                            .sequence = 1U,
                                                            .aggregate_ordinal = 0U,
                                                            .terminal = true},
                                                           std::move(wrong_terminal_state)};
  EXPECT_EQ(
      encode_distributed_vector_aggregate_exchange_message(wrong_terminal, expected).error().code(),
      common::StatusCode::kInvalidArgument);

  DistributedVectorAggregateExchangeDecodeLimits limits;
  limits.maximum_frame_length = distributed_vector_aggregate_exchange_format::kMinimumFrameLength;
  EXPECT_EQ(decode_distributed_vector_aggregate_exchange_message_exact(encoded.bytes(), expected,
                                                                       resources, limits)
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
  limits = {};
  limits.maximum_aggregates = 1U;
  EXPECT_EQ(decode_distributed_vector_aggregate_exchange_message_exact(encoded.bytes(), expected,
                                                                       resources, limits)
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
  limits = {};
  limits.state.maximum_frame_length =
      distributed_vector_aggregate_state_format::kMinimumFrameLength;
  EXPECT_EQ(decode_distributed_vector_aggregate_exchange_message_exact(encoded.bytes(), expected,
                                                                       resources, limits)
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
}

TEST(DistributedVectorAggregateExchangeTest, OwnsEveryFragmentAndShortWriteBoundary) {
  QueryResourceContext resources = QueryResourceContext::create(8U << 20U).value();
  const auto expected = definitions();
  auto state = string_state(expected[1], resources);
  DistributedVectorAggregateExchangeMessage message{{.query_id = uuid(5U),
                                                     .tablet_id = id<schema::TabletId>(6U),
                                                     .sequence = 2U,
                                                     .aggregate_ordinal = 1U,
                                                     .terminal = true},
                                                    std::move(state)};
  const auto encoded =
      encode_distributed_vector_aggregate_exchange_message(message, expected).value();
  std::vector<std::byte> doubled(encoded.bytes().begin(), encoded.bytes().end());
  doubled.insert(doubled.end(), encoded.bytes().begin(), encoded.bytes().end());

  for (std::size_t split = 0U; split < encoded.bytes().size(); ++split) {
    DistributedVectorAggregateExchangeReader reader{
        std::vector<VectorAggregateDefinition>{expected.begin(), expected.end()}, resources};
    auto first = reader.consume(encoded.bytes().first(split));
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->consumed_bytes, split);
    EXPECT_FALSE(first->message.has_value());
    auto second = reader.consume(common::ByteView{doubled}.subspan(split));
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->consumed_bytes, encoded.bytes().size() - split);
    ASSERT_TRUE(second->message.has_value());
    // NOLINTBEGIN(bugprone-unchecked-optional-access)
    const DistributedVectorAggregateExchangeMessage* decoded_message =
        std::addressof(second->message.value());
    // NOLINTEND(bugprone-unchecked-optional-access)
    EXPECT_EQ(decoded_message->aggregate_ordinal, 1U);
    EXPECT_TRUE(decoded_message->terminal);
    EXPECT_EQ(reader.buffered_bytes(), 0U);
  }

  std::vector<std::byte> damaged(encoded.bytes().begin(), encoded.bytes().end());
  damaged.front() ^= std::byte{1U};
  DistributedVectorAggregateExchangeReader failed{
      std::vector<VectorAggregateDefinition>{expected.begin(), expected.end()}, resources};
  EXPECT_EQ(failed.consume(damaged).error().code(), common::StatusCode::kCorruption);
  EXPECT_TRUE(failed.failed());
  EXPECT_EQ(failed.consume(encoded.bytes()).error().code(), common::StatusCode::kCorruption);

  DistributedVectorAggregateExchangeReader invalid_reader{{}, resources};
  EXPECT_EQ(invalid_reader.consume({}).error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(invalid_reader.failed());

  auto cursor = DistributedVectorAggregateExchangeWriteCursor::create(message, expected).value();
  const std::size_t complete_size = cursor.pending_write().size();
  EXPECT_TRUE(cursor.consume_written(7U).is_ok());
  const std::size_t before = cursor.written_bytes();
  EXPECT_EQ(cursor.consume_written(cursor.pending_write().size() + 1U).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(cursor.written_bytes(), before);
  auto moved = std::move(cursor);
  // Verifies the documented moved-from completion contract.
  // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
  EXPECT_TRUE(cursor.complete());
  EXPECT_EQ(moved.pending_write().size(), complete_size - 7U);
  EXPECT_TRUE(moved.consume_written(moved.pending_write().size()).is_ok());
  EXPECT_TRUE(moved.complete());
}

} // namespace
} // namespace chronos::query
