#include "chronos/cluster/distributed_vector_aggregate_query_transport_v2.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_query_transport_v2.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <ranges>
#include <string>
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

[[nodiscard]] schema::LogicalType string_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
}

[[nodiscard]] std::vector<query::VectorGroupKeyDefinition> keys() {
  return {{.column_ordinal = 0U, .type = string_type(), .nullable = false}};
}

[[nodiscard]] std::vector<query::VectorAggregateDefinition> aggregates() {
  return {{.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
}

[[nodiscard]] DistributedVectorGroupedAggregateQueryResponseV2 response() {
  const auto expected_aggregates = aggregates();
  auto state = query::MergeableVectorAggregateState::create(expected_aggregates.front()).value();
  EXPECT_TRUE(state.accumulate_count_star().has_value());
  EXPECT_TRUE(state.accumulate_count_star().has_value());
  std::vector<query::ScalarValue> values;
  values.push_back(query::ScalarValue::text(string_type(), "west").value());
  std::vector<query::MergeableVectorAggregateState> states;
  states.push_back(std::move(state));
  return {.source_node_id = 2U,
          .target_node_id = 1U,
          .query_id = uuid(1U),
          .tablet_id = tablet(2U),
          .status_code = common::StatusCode::kOk,
          .payload =
              query::DistributedVectorGroupedAggregateExchangeMessage{{.query_id = uuid(1U),
                                                                       .tablet_id = tablet(2U),
                                                                       .sequence = 1U,
                                                                       .group_ordinal = 0U,
                                                                       .group_count = 1U,
                                                                       .terminal = true,
                                                                       .empty = false},
                                                                      std::move(values),
                                                                      std::move(states)},
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
      kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize,
      bytes.size() - kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize -
          kDistributedVectorGroupedAggregateQueryResponseV2TrailerSize);
  store_u32(bytes, 80U, payload.empty() ? 0U : common::crc32c(payload));
  store_u32(bytes, 108U, common::crc32c(common::ByteView{bytes}.first(108U)));
  store_u32(bytes, bytes.size() - 4U,
            common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

TEST(DistributedVectorGroupedAggregateQueryTransportV2Test,
     RoundTripsAuthorityBoundStateAndCorrelatedFailure) {
  const auto expected_keys = keys();
  const auto expected_aggregates = aggregates();
  const auto encoded = encode_distributed_vector_grouped_aggregate_query_response_v2(
      response(), expected_keys, expected_aggregates);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  const std::array<std::byte, 8U> magic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                        std::byte{'V'}, std::byte{'G'}, std::byte{'R'},
                                        std::byte{'P'}, std::byte{'2'}};
  EXPECT_TRUE(std::ranges::equal(common::ByteView{*encoded}.first(magic.size()), magic));

  auto resources = query::QueryResourceContext::create(1U << 20U).value();
  auto decoded = decode_distributed_vector_grouped_aggregate_query_response_v2_exact(
      *encoded, expected_keys, expected_aggregates, resources);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->source_node_id, 2U);
  EXPECT_EQ(decoded->target_node_id, 1U);
  ASSERT_TRUE(decoded->payload.has_value());
  EXPECT_EQ(std::get<std::string>(decoded->payload->keys().front().storage()), "west");
  auto states = std::move(*decoded->payload).take_states();
  ASSERT_EQ(states.size(), 1U);
  auto value = std::move(states.front()).take_result();
  ASSERT_TRUE(value.has_value());
  EXPECT_EQ(std::get<std::int64_t>(value->storage()), 2);
  ASSERT_TRUE(decoded->leader_hint.has_value());
  EXPECT_EQ(decoded->leader_hint->node_id, 3U);

  const DistributedVectorGroupedAggregateQueryResponseV2 failure{
      .source_node_id = 2U,
      .target_node_id = 1U,
      .query_id = uuid(1U),
      .tablet_id = tablet(2U),
      .status_code = common::StatusCode::kUnavailable,
      .leader_hint = DistributedQueryLeaderHint{4U, 10U}};
  const auto encoded_failure = encode_distributed_vector_grouped_aggregate_query_response_v2(
      failure, expected_keys, expected_aggregates);
  ASSERT_TRUE(encoded_failure.has_value());
  const auto decoded_failure = decode_distributed_vector_grouped_aggregate_query_response_v2_exact(
      *encoded_failure, expected_keys, expected_aggregates, resources);
  ASSERT_TRUE(decoded_failure.has_value());
  EXPECT_EQ(decoded_failure->status_code, common::StatusCode::kUnavailable);
  EXPECT_FALSE(decoded_failure->payload.has_value());
}

TEST(DistributedVectorGroupedAggregateQueryTransportV2Test,
     RejectsDamageVersionTypeConfusionCorrelationAndLowerBounds) {
  const auto expected_keys = keys();
  const auto expected_aggregates = aggregates();
  const auto encoded = encode_distributed_vector_grouped_aggregate_query_response_v2(
                           response(), expected_keys, expected_aggregates)
                           .value();
  auto resources = query::QueryResourceContext::create(1U << 20U).value();
  EXPECT_EQ(decode_distributed_vector_aggregate_query_response_v2_exact(
                encoded, expected_aggregates, resources)
                .error()
                .code(),
            common::StatusCode::kCorruption);

  std::vector<std::byte> future = encoded;
  store_u16(future, 8U, 3U);
  rewrite_checksums(future);
  EXPECT_EQ(decode_distributed_vector_grouped_aggregate_query_response_v2_exact(
                future, expected_keys, expected_aggregates, resources)
                .error()
                .code(),
            common::StatusCode::kNotSupported);

  std::vector<std::byte> nested_damage = encoded;
  nested_damage[kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize] ^= std::byte{1U};
  rewrite_checksums(nested_damage);
  EXPECT_EQ(decode_distributed_vector_grouped_aggregate_query_response_v2_exact(
                nested_damage, expected_keys, expected_aggregates, resources)
                .error()
                .code(),
            common::StatusCode::kCorruption);

  auto wrong_keys = keys();
  wrong_keys.front().nullable = true;
  EXPECT_EQ(decode_distributed_vector_grouped_aggregate_query_response_v2_exact(
                encoded, wrong_keys, expected_aggregates, resources)
                .error()
                .code(),
            common::StatusCode::kCorruption);

  auto owned_keys = keys();
  auto owned_aggregates = aggregates();
  DistributedVectorGroupedAggregateQueryResponseV2Reader lower_reader{
      std::move(owned_keys), std::move(owned_aggregates),
      query::QueryResourceContext::create(1U << 20U).value(), encoded.size() - 1U};
  EXPECT_EQ(lower_reader.consume(encoded).error().code(), common::StatusCode::kResourceExhausted);

  auto uncorrelated = response();
  uncorrelated.query_id = uuid(9U);
  EXPECT_EQ(encode_distributed_vector_grouped_aggregate_query_response_v2(
                uncorrelated, expected_keys, expected_aggregates)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedVectorGroupedAggregateQueryTransportV2Test,
     ReaderOwnsEverySplitLeavesSuccessorAndFailsSticky) {
  const auto expected_keys = keys();
  const auto expected_aggregates = aggregates();
  const auto encoded = encode_distributed_vector_grouped_aggregate_query_response_v2(
                           response(), expected_keys, expected_aggregates)
                           .value();
  for (std::size_t split = 0U; split <= encoded.size(); ++split) {
    auto owned_keys = keys();
    auto owned_aggregates = aggregates();
    DistributedVectorGroupedAggregateQueryResponseV2Reader reader{
        std::move(owned_keys), std::move(owned_aggregates),
        query::QueryResourceContext::create(1U << 20U).value()};
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
  auto owned_keys = keys();
  auto owned_aggregates = aggregates();
  DistributedVectorGroupedAggregateQueryResponseV2Reader reader{
      std::move(owned_keys), std::move(owned_aggregates),
      query::QueryResourceContext::create(1U << 20U).value()};
  const auto failed = reader.consume(damaged);
  ASSERT_FALSE(failed.has_value());
  EXPECT_TRUE(reader.failed());
  EXPECT_EQ(reader.consume({}).error(), failed.error());
}

TEST(DistributedVectorGroupedAggregateQueryTransportV2Test, CursorOwnsValidatedShortWriteProgress) {
  const auto expected_keys = keys();
  const auto expected_aggregates = aggregates();
  auto cursor = DistributedVectorGroupedAggregateQueryResponseV2WriteCursor::create(
      response(), expected_keys, expected_aggregates);
  ASSERT_TRUE(cursor.has_value()) << cursor.error().to_string();
  const std::size_t size = cursor->pending_write().size();
  ASSERT_GT(size, 2U);
  EXPECT_TRUE(cursor->consume_written(1U).is_ok());
  EXPECT_EQ(cursor->written_bytes(), 1U);
  EXPECT_EQ(cursor->consume_written(size).code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(cursor->written_bytes(), 1U);
  DistributedVectorGroupedAggregateQueryResponseV2WriteCursor moved{std::move(*cursor)};
  EXPECT_TRUE(cursor->complete());
  EXPECT_TRUE(moved.consume_written(size - 1U).is_ok());
  EXPECT_TRUE(moved.complete());
}

} // namespace
} // namespace chronos::cluster
