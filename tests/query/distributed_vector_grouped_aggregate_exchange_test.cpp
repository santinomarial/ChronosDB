#include "chronos/common/crc32c.hpp"
#include "chronos/query/distributed_vector_grouped_aggregate_exchange.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
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

[[nodiscard]] std::vector<VectorGroupKeyDefinition> keys() {
  return {{.column_ordinal = 0U, .type = type(schema::LogicalTypeKind::kString), .nullable = false},
          {.column_ordinal = 1U, .type = type(schema::LogicalTypeKind::kBool), .nullable = true}};
}

[[nodiscard]] std::vector<VectorAggregateDefinition> aggregates() {
  return {{.operation = VectorAggregateOperation::kCountStar, .input = std::nullopt},
          {.operation = VectorAggregateOperation::kCount,
           .input = VectorAggregateInput{.column_ordinal = 2U,
                                         .type = type(schema::LogicalTypeKind::kInt64),
                                         .nullable = true}}};
}

[[nodiscard]] std::vector<MergeableVectorAggregateState>
states(const std::span<const VectorAggregateDefinition> definitions) {
  std::vector<MergeableVectorAggregateState> result;
  result.reserve(definitions.size());
  for (const VectorAggregateDefinition& definition : definitions)
    result.push_back(MergeableVectorAggregateState::create(definition).value());
  EXPECT_TRUE(result.front().accumulate_count_star().has_value());
  EXPECT_TRUE(result.front().accumulate_count_star().has_value());
  return result;
}

TEST(DistributedVectorGroupedAggregateExchangeTest,
     RoundTripsMultiKeyAllTypeSufficientStateWithFrozenFraming) {
  const auto expected_keys = keys();
  const auto expected_aggregates = aggregates();
  std::vector<ScalarValue> values;
  values.push_back(
      ScalarValue::text(expected_keys[0].type, "north-america/a-very-long-region").value());
  values.push_back(ScalarValue::null(expected_keys[1].type));
  DistributedVectorGroupedAggregateExchangeMessage message{{.query_id = uuid(1U),
                                                            .tablet_id = id<schema::TabletId>(2U),
                                                            .sequence = 2U,
                                                            .group_ordinal = 1U,
                                                            .group_count = 2U,
                                                            .terminal = true,
                                                            .empty = false},
                                                           std::move(values),
                                                           states(expected_aggregates)};
  auto encoded = encode_distributed_vector_grouped_aggregate_exchange_message(
      message, expected_keys, expected_aggregates);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  const common::ByteView bytes = encoded->bytes();
  const std::array<std::byte, 8U> magic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                        std::byte{'V'}, std::byte{'G'}, std::byte{'E'},
                                        std::byte{'X'}, std::byte{'1'}};
  EXPECT_TRUE(std::ranges::equal(bytes.first(magic.size()), magic));
  EXPECT_EQ(load_u16(bytes, 8U), 1U);
  EXPECT_EQ(load_u16(bytes, 10U), 0U);
  EXPECT_EQ(load_u32(bytes, 12U), 128U);
  EXPECT_EQ(load_u64(bytes, 16U), bytes.size());
  EXPECT_EQ(load_u64(bytes, 56U), 2U);
  EXPECT_EQ(load_u32(bytes, 64U), 1U);
  EXPECT_EQ(load_u32(bytes, 68U), 2U);
  EXPECT_EQ(load_u32(bytes, 72U), expected_keys.size());
  EXPECT_EQ(load_u32(bytes, 76U), expected_aggregates.size());
  EXPECT_EQ(load_u32(bytes, 80U), 1U);
  EXPECT_EQ(load_u32(bytes, 96U), common::crc32c(bytes.first(96U)));
  EXPECT_TRUE(std::ranges::all_of(bytes.subspan(100U, 28U),
                                  [](const std::byte value) { return value == std::byte{}; }));
  const std::size_t payload_length = load_u32(bytes, 84U) + load_u32(bytes, 88U);
  EXPECT_EQ(load_u32(bytes, 92U), common::crc32c(bytes.subspan(128U, payload_length)));
  EXPECT_EQ(load_u32(bytes, bytes.size() - 4U), common::crc32c(bytes.first(bytes.size() - 4U)));

  QueryResourceContext resources = QueryResourceContext::create(4U << 20U).value();
  const std::size_t before = resources.reserved_memory_bytes();
  auto decoded = decode_distributed_vector_grouped_aggregate_exchange_message_exact(
      encoded->bytes(), expected_keys, expected_aggregates, resources);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_GT(resources.reserved_memory_bytes(), before);
  EXPECT_EQ(decoded->position().query_id, message.position().query_id);
  EXPECT_EQ(decoded->position().tablet_id, message.position().tablet_id);
  ASSERT_EQ(decoded->keys().size(), 2U);
  EXPECT_EQ(std::get<std::string>(decoded->keys()[0].storage()),
            "north-america/a-very-long-region");
  EXPECT_TRUE(decoded->keys()[1].is_null());
  ASSERT_EQ(decoded->states().size(), 2U);
  auto decoded_states = std::move(*decoded).take_states();
  auto count = std::move(decoded_states[0]).take_result();
  ASSERT_TRUE(count.has_value());
  EXPECT_EQ(std::get<std::int64_t>(count->storage()), 2);
  decoded_states.clear();
  decoded = common::make_unexpected(
      common::Status{common::StatusCode::kInternal, "drop decoded grouped state"});
  EXPECT_EQ(resources.reserved_memory_bytes(), before);
}

TEST(DistributedVectorGroupedAggregateExchangeTest,
     EncodesEmptyTerminalAndRejectsIdentityShapeDamageAndLowerLimits) {
  const auto expected_keys = keys();
  const auto expected_aggregates = aggregates();
  DistributedVectorGroupedAggregateExchangeMessage empty{{.query_id = uuid(3U),
                                                          .tablet_id = id<schema::TabletId>(4U),
                                                          .sequence = 1U,
                                                          .group_ordinal = 0U,
                                                          .group_count = 0U,
                                                          .terminal = true,
                                                          .empty = true},
                                                         {},
                                                         {}};
  const auto encoded = encode_distributed_vector_grouped_aggregate_exchange_message(
                           empty, expected_keys, expected_aggregates)
                           .value();
  EXPECT_EQ(encoded.bytes().size(), 132U);
  EXPECT_EQ(load_u32(encoded.bytes(), 72U), 0U);
  EXPECT_EQ(load_u32(encoded.bytes(), 76U), 0U);
  EXPECT_EQ(load_u32(encoded.bytes(), 80U), 3U);
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  auto decoded = decode_distributed_vector_grouped_aggregate_exchange_message_exact(
      encoded.bytes(), expected_keys, expected_aggregates, resources);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_TRUE(decoded->position().empty);
  EXPECT_TRUE(decoded->keys().empty());
  EXPECT_TRUE(decoded->states().empty());
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);

  std::vector<std::byte> damaged(encoded.bytes().begin(), encoded.bytes().end());
  damaged[24U] ^= std::byte{1U};
  EXPECT_EQ(decode_distributed_vector_grouped_aggregate_exchange_message_exact(
                damaged, expected_keys, expected_aggregates, resources)
                .error()
                .code(),
            common::StatusCode::kCorruption);
  EXPECT_EQ(decode_distributed_vector_grouped_aggregate_exchange_message_exact(
                encoded.bytes().first(encoded.bytes().size() - 1U), expected_keys,
                expected_aggregates, resources)
                .error()
                .code(),
            common::StatusCode::kCorruption);
  EXPECT_EQ(decode_distributed_vector_grouped_aggregate_exchange_message_exact(
                encoded.bytes(), expected_keys, expected_aggregates, resources,
                {.maximum_frame_length = 131U})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  auto duplicated = expected_keys;
  duplicated[1].column_ordinal = duplicated[0].column_ordinal;
  EXPECT_EQ(validate_distributed_vector_grouped_aggregate_authority(duplicated, expected_aggregates)
                .code(),
            common::StatusCode::kInvalidArgument);
  DistributedVectorGroupedAggregateExchangeMessage wrong_position{
      {.query_id = uuid(5U),
       .tablet_id = id<schema::TabletId>(6U),
       .sequence = 2U,
       .group_ordinal = 0U,
       .group_count = 1U,
       .terminal = true,
       .empty = false},
      {ScalarValue::text(expected_keys[0].type, "x").value(), ScalarValue::boolean(true).value()},
      states(expected_aggregates)};
  EXPECT_EQ(encode_distributed_vector_grouped_aggregate_exchange_message(
                wrong_position, expected_keys, expected_aggregates)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedVectorGroupedAggregateExchangeTest,
     OwnsEveryFragmentedReadSplitCoalescedSuffixAndShortWrite) {
  const auto expected_keys = keys();
  const auto expected_aggregates = aggregates();
  std::vector<ScalarValue> values;
  values.push_back(ScalarValue::text(expected_keys[0].type, "fragmented-region").value());
  values.push_back(ScalarValue::boolean(false).value());
  DistributedVectorGroupedAggregateExchangeMessage message{{.query_id = uuid(7U),
                                                            .tablet_id = id<schema::TabletId>(8U),
                                                            .sequence = 1U,
                                                            .group_ordinal = 0U,
                                                            .group_count = 1U,
                                                            .terminal = true,
                                                            .empty = false},
                                                           std::move(values),
                                                           states(expected_aggregates)};
  const auto encoded = encode_distributed_vector_grouped_aggregate_exchange_message(
                           message, expected_keys, expected_aggregates)
                           .value();
  std::vector<std::byte> coalesced(encoded.bytes().begin(), encoded.bytes().end());
  coalesced.push_back(std::byte{0x5aU});

  for (std::size_t split = 0U; split < encoded.bytes().size(); ++split) {
    SCOPED_TRACE(split);
    QueryResourceContext resources = QueryResourceContext::create(4U << 20U).value();
    {
      DistributedVectorGroupedAggregateExchangeReader reader{
          std::vector<VectorGroupKeyDefinition>{expected_keys.begin(), expected_keys.end()},
          std::vector<VectorAggregateDefinition>{expected_aggregates.begin(),
                                                 expected_aggregates.end()},
          resources};
      auto first = reader.consume(common::ByteView{coalesced}.first(split));
      ASSERT_TRUE(first.has_value()) << first.error().to_string();
      EXPECT_EQ(first->consumed_bytes, split);
      EXPECT_FALSE(first->message.has_value());
      auto second = reader.consume(common::ByteView{coalesced}.subspan(split));
      ASSERT_TRUE(second.has_value()) << second.error().to_string();
      EXPECT_EQ(second->consumed_bytes, encoded.bytes().size() - split);
      ASSERT_TRUE(second->message.has_value());
      EXPECT_EQ(second->message->position().query_id, message.position().query_id);
      EXPECT_FALSE(reader.failed());
    }
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }

  auto cursor = DistributedVectorGroupedAggregateExchangeWriteCursor::create(message, expected_keys,
                                                                             expected_aggregates);
  ASSERT_TRUE(cursor.has_value());
  const std::size_t frame_length = cursor->pending_write().size();
  EXPECT_TRUE(cursor->consume_written(17U).is_ok());
  EXPECT_EQ(cursor->written_bytes(), 17U);
  EXPECT_EQ(cursor->pending_write().size(), frame_length - 17U);
  EXPECT_EQ(cursor->consume_written(frame_length).code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(cursor->written_bytes(), 17U);
  auto moved = std::move(*cursor);
  EXPECT_TRUE(cursor->complete());
  EXPECT_TRUE(moved.consume_written(frame_length - 17U).is_ok());
  EXPECT_TRUE(moved.complete());

  std::vector<std::byte> damaged(encoded.bytes().begin(), encoded.bytes().end());
  damaged.back() ^= std::byte{1U};
  DistributedVectorGroupedAggregateExchangeReader sticky{
      std::vector<VectorGroupKeyDefinition>{expected_keys.begin(), expected_keys.end()},
      std::vector<VectorAggregateDefinition>{expected_aggregates.begin(),
                                             expected_aggregates.end()},
      QueryResourceContext::create(4U << 20U).value()};
  EXPECT_EQ(sticky.consume(damaged).error().code(), common::StatusCode::kCorruption);
  EXPECT_TRUE(sticky.failed());
  EXPECT_EQ(sticky.consume(encoded.bytes()).error().code(), common::StatusCode::kCorruption);
}

} // namespace
} // namespace chronos::query
