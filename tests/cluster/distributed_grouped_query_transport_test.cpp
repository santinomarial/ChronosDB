#include "chronos/cluster/distributed_grouped_query_transport.hpp"
#include "chronos/cluster/distributed_query_transport.hpp"
#include "chronos/common/crc32c.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <variant>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

template <typename Id> [[nodiscard]] Id id(const std::uint8_t seed) {
  return Id::from_uuid(uuid(seed)).value();
}

[[nodiscard]] query::DistributedGroupedFloat64FragmentDispatch dispatch() {
  return {
      .raft_group_id = uuid(9U),
      .fragment = {
          .aggregate = {.query_id = uuid(1U),
                        .database_id = id<manifest::DatabaseId>(2U),
                        .table_id = id<schema::TableId>(3U),
                        .tablet_id = id<schema::TabletId>(4U),
                        .destination_schema_id = id<schema::SchemaId>(5U),
                        .snapshot_generation = 6U,
                        .serving_node = 2U,
                        .applied_position = 10U,
                        .observed_leader_commit_position = 10U,
                        .placement_epoch = 8U,
                        .read_policy = {.consistency =
                                            query::DistributedReadConsistency::kLeaderLinearizable,
                                        .maximum_staleness_positions = std::nullopt},
                        .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
                        .destination_column_ordinals = {1U},
                        .aggregate_input_index = 0U,
                        .event_time_predicate = std::nullopt},
          .group_key_input_index = 0U}};
}

[[nodiscard]] query::GroupedFloat64ExchangeMessage partial() {
  query::MergeableAggregateState aggregate;
  EXPECT_TRUE(aggregate.add(2.5).is_ok());
  return {.query_id = uuid(1U),
          .tablet_id = id<schema::TabletId>(4U),
          .sequence = 2U,
          .group_key = -0.0,
          .partial = aggregate,
          .terminal = true};
}

void store_u16(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint16_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
}

void rewrite_request_checksums(std::vector<std::byte>& bytes) {
  const common::ByteView payload = common::ByteView{bytes}.subspan(
      kDistributedGroupedQueryRequestHeaderSize,
      bytes.size() - kDistributedGroupedQueryRequestHeaderSize - 4U);
  store_u32(bytes, 48U, common::crc32c(payload));
  store_u32(bytes, 76U, common::crc32c(common::ByteView{bytes}.first(76U)));
  store_u32(bytes, bytes.size() - 4U,
            common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

void rewrite_response_checksums(std::vector<std::byte>& bytes) {
  const common::ByteView payload = common::ByteView{bytes}.subspan(
      kDistributedGroupedQueryResponseHeaderSize,
      bytes.size() - kDistributedGroupedQueryResponseHeaderSize - 4U);
  store_u32(bytes, 80U, payload.empty() ? 0U : common::crc32c(payload));
  store_u32(bytes, 108U, common::crc32c(common::ByteView{bytes}.first(108U)));
  store_u32(bytes, bytes.size() - 4U,
            common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

TEST(DistributedGroupedQueryTransportCodecTest, RoundTripsDistinctRequestAndEveryResponseKind) {
  const DistributedGroupedQueryRequest request{1U, 2U, dispatch()};
  const auto encoded_request = encode_distributed_grouped_query_request_v1(request);
  ASSERT_TRUE(encoded_request.has_value()) << encoded_request.error().to_string();
  EXPECT_EQ(encoded_request->size(), 436U);
  const auto decoded_request = decode_distributed_grouped_query_request_v1(*encoded_request);
  ASSERT_TRUE(decoded_request.has_value()) << decoded_request.error().to_string();
  EXPECT_EQ(decoded_request->source_node_id, 1U);
  EXPECT_EQ(decoded_request->target_node_id, 2U);
  EXPECT_EQ(decoded_request->dispatch, request.dispatch);
  EXPECT_EQ(decode_distributed_query_request_v1(*encoded_request).error().code(),
            common::StatusCode::kCorruption);
  const auto ungrouped_request = encode_distributed_query_request_v1(
      {1U, 2U,
       query::DistributedAggregateFragmentDispatch{.raft_group_id = uuid(9U),
                                                   .fragment = dispatch().fragment.aggregate}});
  ASSERT_TRUE(ungrouped_request.has_value()) << ungrouped_request.error().to_string();
  EXPECT_EQ(decode_distributed_grouped_query_request_v1(*ungrouped_request).error().code(),
            common::StatusCode::kCorruption);

  const DistributedGroupedQueryResponse grouped{
      .source_node_id = 2U,
      .target_node_id = 1U,
      .query_id = uuid(1U),
      .tablet_id = id<schema::TabletId>(4U),
      .status_code = common::StatusCode::kOk,
      .payload = DistributedGroupedQueryResponsePayload{partial()},
      .leader_hint = DistributedQueryLeaderHint{2U, 8U}};
  const auto encoded_grouped = encode_distributed_grouped_query_response_v1(grouped);
  ASSERT_TRUE(encoded_grouped.has_value()) << encoded_grouped.error().to_string();
  EXPECT_EQ(encoded_grouped->size(), kMaximumDistributedGroupedQueryResponseSize);
  const auto decoded_grouped = decode_distributed_grouped_query_response_v1(*encoded_grouped);
  ASSERT_TRUE(decoded_grouped.has_value()) << decoded_grouped.error().to_string();
  EXPECT_EQ(decode_distributed_query_response_v1(*encoded_grouped).error().code(),
            common::StatusCode::kCorruption);
  const auto* decoded_partial =
      std::get_if<query::GroupedFloat64ExchangeMessage>(&*decoded_grouped->payload);
  ASSERT_NE(decoded_partial, nullptr);
  EXPECT_EQ(decoded_partial->sequence, 2U);
  EXPECT_EQ(decoded_partial->group_key, 0.0);
  EXPECT_EQ(decoded_partial->partial.sum, 2.5);
  EXPECT_EQ(decoded_grouped->leader_hint, grouped.leader_hint);

  const DistributedGroupedQueryResponse terminal{
      .source_node_id = 2U,
      .target_node_id = 1U,
      .query_id = uuid(1U),
      .tablet_id = id<schema::TabletId>(4U),
      .status_code = common::StatusCode::kOk,
      .payload = DistributedGroupedQueryResponsePayload{query::GroupedExchangeTerminalMessage{
          .query_id = uuid(1U), .tablet_id = id<schema::TabletId>(4U), .sequence = 1U}}};
  const auto encoded_terminal = encode_distributed_grouped_query_response_v1(terminal);
  ASSERT_TRUE(encoded_terminal.has_value()) << encoded_terminal.error().to_string();
  EXPECT_EQ(encoded_terminal->size(), kDistributedGroupedQueryResponseHeaderSize + 64U + 4U);
  const auto decoded_terminal = decode_distributed_grouped_query_response_v1(*encoded_terminal);
  ASSERT_TRUE(decoded_terminal.has_value()) << decoded_terminal.error().to_string();
  EXPECT_TRUE(
      std::holds_alternative<query::GroupedExchangeTerminalMessage>(*decoded_terminal->payload));

  const DistributedGroupedQueryResponse failure{.source_node_id = 2U,
                                                .target_node_id = 1U,
                                                .query_id = uuid(1U),
                                                .tablet_id = id<schema::TabletId>(4U),
                                                .status_code = common::StatusCode::kUnavailable};
  const auto encoded_failure = encode_distributed_grouped_query_response_v1(failure);
  ASSERT_TRUE(encoded_failure.has_value()) << encoded_failure.error().to_string();
  EXPECT_EQ(encoded_failure->size(), kDistributedGroupedQueryResponseHeaderSize + 4U);
  const auto decoded_failure = decode_distributed_grouped_query_response_v1(*encoded_failure);
  ASSERT_TRUE(decoded_failure.has_value());
  EXPECT_EQ(decoded_failure->status_code, common::StatusCode::kUnavailable);
  EXPECT_FALSE(decoded_failure->payload.has_value());
  const auto ungrouped_failure =
      encode_distributed_query_response_v1({.source_node_id = 2U,
                                            .target_node_id = 1U,
                                            .query_id = uuid(1U),
                                            .tablet_id = id<schema::TabletId>(4U),
                                            .status_code = common::StatusCode::kUnavailable});
  ASSERT_TRUE(ungrouped_failure.has_value()) << ungrouped_failure.error().to_string();
  EXPECT_EQ(decode_distributed_grouped_query_response_v1(*ungrouped_failure).error().code(),
            common::StatusCode::kCorruption);

  constexpr std::array status_codes{
      common::StatusCode::kCancelled,       common::StatusCode::kInvalidArgument,
      common::StatusCode::kOutOfRange,      common::StatusCode::kNotFound,
      common::StatusCode::kAlreadyExists,   common::StatusCode::kCorruption,
      common::StatusCode::kIoError,         common::StatusCode::kResourceExhausted,
      common::StatusCode::kUnavailable,     common::StatusCode::kNotSupported,
      common::StatusCode::kUnauthenticated, common::StatusCode::kInternal};
  for (std::size_t index = 0U; index < status_codes.size(); ++index) {
    auto encoded =
        encode_distributed_grouped_query_response_v1({.source_node_id = 2U,
                                                      .target_node_id = 1U,
                                                      .query_id = uuid(1U),
                                                      .tablet_id = id<schema::TabletId>(4U),
                                                      .status_code = status_codes[index]});
    ASSERT_TRUE(encoded.has_value()) << index;
    EXPECT_EQ(std::to_integer<std::uint8_t>((*encoded)[72U]), index + 1U);
    EXPECT_EQ(decode_distributed_grouped_query_response_v1(*encoded)->status_code,
              status_codes[index]);
  }
}

TEST(DistributedGroupedQueryTransportCodecTest, RejectsDamageTypeConfusionAndMiscalculation) {
  auto damaged_request = encode_distributed_grouped_query_request_v1({1U, 2U, dispatch()}).value();
  damaged_request[kDistributedGroupedQueryRequestHeaderSize + 24U] ^= std::byte{1U};
  rewrite_request_checksums(damaged_request);
  EXPECT_EQ(decode_distributed_grouped_query_request_v1(damaged_request).error().code(),
            common::StatusCode::kCorruption);

  auto future_request = encode_distributed_grouped_query_request_v1({1U, 2U, dispatch()}).value();
  store_u16(future_request, 8U, 2U);
  rewrite_request_checksums(future_request);
  EXPECT_EQ(decode_distributed_grouped_query_request_v1(future_request).error().code(),
            common::StatusCode::kNotSupported);

  DistributedGroupedQueryResponse wrong{.source_node_id = 2U,
                                        .target_node_id = 1U,
                                        .query_id = uuid(7U),
                                        .tablet_id = id<schema::TabletId>(4U),
                                        .status_code = common::StatusCode::kOk,
                                        .payload =
                                            DistributedGroupedQueryResponsePayload{partial()}};
  EXPECT_EQ(encode_distributed_grouped_query_response_v1(wrong).error().code(),
            common::StatusCode::kInvalidArgument);

  auto wrong_kind = encode_distributed_grouped_query_response_v1(
                        {.source_node_id = 2U,
                         .target_node_id = 1U,
                         .query_id = uuid(1U),
                         .tablet_id = id<schema::TabletId>(4U),
                         .status_code = common::StatusCode::kOk,
                         .payload = DistributedGroupedQueryResponsePayload{partial()}})
                        .value();
  wrong_kind[73U] = std::byte{2U};
  rewrite_response_checksums(wrong_kind);
  EXPECT_EQ(decode_distributed_grouped_query_response_v1(wrong_kind).error().code(),
            common::StatusCode::kCorruption);

  auto damaged_payload = encode_distributed_grouped_query_response_v1(
                             {.source_node_id = 2U,
                              .target_node_id = 1U,
                              .query_id = uuid(1U),
                              .tablet_id = id<schema::TabletId>(4U),
                              .status_code = common::StatusCode::kOk,
                              .payload = DistributedGroupedQueryResponsePayload{partial()}})
                             .value();
  damaged_payload[kDistributedGroupedQueryResponseHeaderSize + 56U] ^= std::byte{1U};
  rewrite_response_checksums(damaged_payload);
  EXPECT_EQ(decode_distributed_grouped_query_response_v1(damaged_payload).error().code(),
            common::StatusCode::kCorruption);

  auto future_response = encode_distributed_grouped_query_response_v1(
                             {.source_node_id = 2U,
                              .target_node_id = 1U,
                              .query_id = uuid(1U),
                              .tablet_id = id<schema::TabletId>(4U),
                              .status_code = common::StatusCode::kUnavailable})
                             .value();
  store_u16(future_response, 8U, 2U);
  rewrite_response_checksums(future_response);
  EXPECT_EQ(decode_distributed_grouped_query_response_v1(future_response).error().code(),
            common::StatusCode::kNotSupported);
}

} // namespace
} // namespace chronos::cluster
