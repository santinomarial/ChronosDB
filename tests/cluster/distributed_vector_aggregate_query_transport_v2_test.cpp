#include "chronos/cluster/distributed_vector_aggregate_query_transport_v2.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <ranges>
#include <span>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

[[nodiscard]] schema::TabletId tablet(const std::uint8_t seed) {
  return schema::TabletId::from_uuid(uuid(seed)).value();
}

[[nodiscard]] std::vector<query::VectorAggregateDefinition> definitions() {
  return {{.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
}

[[nodiscard]] query::DistributedVectorAggregateExchangeMessage message() {
  const auto expected = definitions();
  auto state = query::MergeableVectorAggregateState::create(expected.front()).value();
  EXPECT_TRUE(state.accumulate_count_star().has_value());
  EXPECT_TRUE(state.accumulate_count_star().has_value());
  return {{.query_id = uuid(1U),
           .tablet_id = tablet(2U),
           .sequence = 1U,
           .aggregate_ordinal = 0U,
           .terminal = true},
          std::move(state)};
}

[[nodiscard]] DistributedVectorAggregateQueryResponseV2 response() {
  return {.source_node_id = 2U,
          .target_node_id = 1U,
          .query_id = uuid(1U),
          .tablet_id = tablet(2U),
          .status_code = common::StatusCode::kOk,
          .payload = message(),
          .leader_hint = DistributedQueryLeaderHint{3U, 9U}};
}

void store_u16(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint16_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
}

void rewrite_checksums(std::vector<std::byte>& bytes) {
  const common::ByteView payload = common::ByteView{bytes}.subspan(
      kDistributedVectorAggregateQueryResponseV2HeaderSize,
      bytes.size() - kDistributedVectorAggregateQueryResponseV2HeaderSize -
          kDistributedVectorAggregateQueryResponseV2TrailerSize);
  store_u32(bytes, 80U, payload.empty() ? 0U : common::crc32c(payload));
  store_u32(bytes, 108U, common::crc32c(common::ByteView{bytes}.first(108U)));
  store_u32(bytes, bytes.size() - 4U,
            common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

TEST(DistributedVectorAggregateQueryTransportV2Test,
     RoundTripsDefinitionBoundStateAndCorrelatedFailure) {
  const auto expected = definitions();
  const auto encoded = encode_distributed_vector_aggregate_query_response_v2(response(), expected);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  const std::array<std::byte, 8U> magic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                        std::byte{'V'}, std::byte{'A'}, std::byte{'R'},
                                        std::byte{'P'}, std::byte{'2'}};
  EXPECT_TRUE(std::ranges::equal(common::ByteView{*encoded}.first(magic.size()), magic));
  query::QueryResourceContext resources = query::QueryResourceContext::create(1U << 20U).value();
  auto decoded =
      decode_distributed_vector_aggregate_query_response_v2_exact(*encoded, expected, resources);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->source_node_id, 2U);
  EXPECT_EQ(decoded->target_node_id, 1U);
  ASSERT_TRUE(decoded->payload.has_value());
  auto result = std::move(decoded->payload->state).take_result();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(std::get<std::int64_t>(result->storage()), 2);
  ASSERT_TRUE(decoded->leader_hint.has_value());
  EXPECT_EQ(decoded->leader_hint->node_id, 3U);

  const DistributedVectorAggregateQueryResponseV2 failure{
      .source_node_id = 2U,
      .target_node_id = 1U,
      .query_id = uuid(1U),
      .tablet_id = tablet(2U),
      .status_code = common::StatusCode::kUnavailable,
      .leader_hint = DistributedQueryLeaderHint{4U, 10U}};
  const auto encoded_failure =
      encode_distributed_vector_aggregate_query_response_v2(failure, expected);
  ASSERT_TRUE(encoded_failure.has_value());
  const auto decoded_failure = decode_distributed_vector_aggregate_query_response_v2_exact(
      *encoded_failure, expected, resources);
  ASSERT_TRUE(decoded_failure.has_value());
  EXPECT_EQ(decoded_failure->status_code, common::StatusCode::kUnavailable);
  EXPECT_FALSE(decoded_failure->payload.has_value());
}

TEST(DistributedVectorAggregateQueryTransportV2Test,
     RejectsDamageVersionConfusionCorrelationAndLowerBounds) {
  const auto expected = definitions();
  const auto encoded =
      encode_distributed_vector_aggregate_query_response_v2(response(), expected).value();
  query::QueryResourceContext resources = query::QueryResourceContext::create(1U << 20U).value();
  const query::DistributedVectorResultSchema row_schema{
      .columns = {{.name = "count",
                   .type = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(),
                   .nullable = false}}};
  EXPECT_EQ(decode_distributed_vector_query_response_v2_exact(encoded, row_schema).error().code(),
            common::StatusCode::kCorruption);

  std::vector<std::byte> future = encoded;
  store_u16(future, 8U, 3U);
  rewrite_checksums(future);
  EXPECT_EQ(decode_distributed_vector_aggregate_query_response_v2_exact(future, expected, resources)
                .error()
                .code(),
            common::StatusCode::kNotSupported);

  std::vector<std::byte> nested_damage = encoded;
  nested_damage[kDistributedVectorAggregateQueryResponseV2HeaderSize] ^= std::byte{1U};
  rewrite_checksums(nested_damage);
  EXPECT_EQ(decode_distributed_vector_aggregate_query_response_v2_exact(nested_damage, expected,
                                                                        resources)
                .error()
                .code(),
            common::StatusCode::kCorruption);

  auto wrong = expected;
  wrong.front().operation = query::VectorAggregateOperation::kMinimum;
  wrong.front().input = query::VectorAggregateInput{
      .column_ordinal = 0U,
      .type = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(),
      .nullable = false};
  EXPECT_EQ(decode_distributed_vector_aggregate_query_response_v2_exact(encoded, wrong, resources)
                .error()
                .code(),
            common::StatusCode::kCorruption);

  auto lower_definitions = definitions();
  DistributedVectorAggregateQueryResponseV2Reader lower_reader{
      std::move(lower_definitions), query::QueryResourceContext::create(1U << 20U).value(),
      encoded.size() - 1U};
  EXPECT_EQ(lower_reader.consume(encoded).error().code(), common::StatusCode::kResourceExhausted);

  auto uncorrelated = response();
  uncorrelated.query_id = uuid(9U);
  EXPECT_EQ(
      encode_distributed_vector_aggregate_query_response_v2(uncorrelated, expected).error().code(),
      common::StatusCode::kInvalidArgument);
}

TEST(DistributedVectorAggregateQueryTransportV2Test,
     ReaderOwnsEverySplitLeavesSuccessorAndFailsSticky) {
  const auto expected = definitions();
  const auto encoded =
      encode_distributed_vector_aggregate_query_response_v2(response(), expected).value();
  for (std::size_t split = 0U; split <= encoded.size(); ++split) {
    auto owned_definitions = definitions();
    DistributedVectorAggregateQueryResponseV2Reader reader{
        std::move(owned_definitions), query::QueryResourceContext::create(1U << 20U).value()};
    const auto first = reader.consume(common::ByteView{encoded}.first(split));
    ASSERT_TRUE(first.has_value()) << first.error().to_string();
    EXPECT_EQ(first->consumed_bytes, split);
    if (split != encoded.size())
      EXPECT_FALSE(first->response.has_value());
    std::array<std::byte, 3U> successor{std::byte{1U}, std::byte{2U}, std::byte{3U}};
    std::vector<std::byte> tail(common::ByteView{encoded}.subspan(split).begin(),
                                common::ByteView{encoded}.subspan(split).end());
    tail.insert(tail.end(), successor.begin(), successor.end());
    if (split == encoded.size()) {
      ASSERT_TRUE(first->response.has_value());
      continue;
    }
    const auto second = reader.consume(tail);
    ASSERT_TRUE(second.has_value()) << second.error().to_string();
    ASSERT_TRUE(second->response.has_value());
    EXPECT_EQ(second->consumed_bytes, encoded.size() - split);
  }

  auto damaged = encoded;
  damaged.back() ^= std::byte{1U};
  auto owned_definitions = definitions();
  DistributedVectorAggregateQueryResponseV2Reader reader{
      std::move(owned_definitions), query::QueryResourceContext::create(1U << 20U).value()};
  const auto failed = reader.consume(damaged);
  ASSERT_FALSE(failed.has_value());
  EXPECT_TRUE(reader.failed());
  EXPECT_EQ(reader.consume({}).error().code(), failed.error().code());
}

TEST(DistributedVectorAggregateQueryTransportV2Test, CursorOwnsValidatedShortWriteProgress) {
  const auto expected = definitions();
  auto cursor = DistributedVectorAggregateQueryResponseV2WriteCursor::create(response(), expected);
  ASSERT_TRUE(cursor.has_value()) << cursor.error().to_string();
  const std::size_t size = cursor->pending_write().size();
  ASSERT_GT(size, 2U);
  EXPECT_TRUE(cursor->consume_written(1U).is_ok());
  EXPECT_EQ(cursor->written_bytes(), 1U);
  EXPECT_EQ(cursor->consume_written(size).code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(cursor->written_bytes(), 1U);
  DistributedVectorAggregateQueryResponseV2WriteCursor moved{std::move(*cursor)};
  EXPECT_TRUE(cursor->complete());
  EXPECT_TRUE(moved.consume_written(size - 1U).is_ok());
  EXPECT_TRUE(moved.complete());
}

} // namespace
} // namespace chronos::cluster
