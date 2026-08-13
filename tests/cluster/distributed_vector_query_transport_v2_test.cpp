#include "chronos/cluster/distributed_vector_query_transport.hpp"
#include "chronos/cluster/distributed_vector_query_transport_v2.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <ranges>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

template <typename Id> [[nodiscard]] Id id(const std::uint8_t seed) {
  return Id::from_uuid(uuid(seed)).value();
}

[[nodiscard]] query::DistributedVectorResultSchema result_schema() {
  return {
      .columns = {
          {"key", schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(), false},
          {"total", schema::LogicalType::create(schema::LogicalTypeKind::kFloat64).value(), true}}};
}

[[nodiscard]] query::DistributedVectorFragmentDispatchV2 dispatch_v2() {
  return {.dispatch = {.query_id = uuid(1U),
                       .database_id = id<manifest::DatabaseId>(2U),
                       .table_id = id<schema::TableId>(3U),
                       .tablet_id = id<schema::TabletId>(4U),
                       .destination_schema_id = id<schema::SchemaId>(5U),
                       .raft_group_id = uuid(9U),
                       .snapshot_generation = 6U,
                       .serving_node = 2U,
                       .applied_position = 10U,
                       .observed_leader_commit_position = 10U,
                       .placement_epoch = 8U,
                       .read_policy = {.consistency =
                                           query::DistributedReadConsistency::kLeaderLinearizable},
                       .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
                       .destination_column_ordinals = {0U, 1U},
                       .plan = {.mode = query::DistributedVectorPlanMode::kGroupedAggregate,
                                .group_key_input_indices = {0U},
                                .aggregates = {{.operation = query::VectorAggregateOperation::kSum,
                                                .input_index = 1U}},
                                .order_keys = {{.output_index = 1U,
                                                .direction =
                                                    query::PhysicalSortDirection::kDescending}},
                                .limit = 3U}},
          .result_schema = result_schema()};
}

[[nodiscard]] std::vector<std::byte> zero_row_batch() {
  const query::DistributedVectorResultSchema schema_value = result_schema();
  const std::array<network::QueryResultColumn, 2U> columns{
      network::QueryResultColumn{schema_value.columns[0].name, schema_value.columns[0].type,
                                 schema_value.columns[0].nullable},
      network::QueryResultColumn{schema_value.columns[1].name, schema_value.columns[1].type,
                                 schema_value.columns[1].nullable}};
  return network::encode_query_result_batch(0U, columns, {}).value();
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
  const common::ByteView payload =
      common::ByteView{bytes}.subspan(kDistributedVectorQueryRequestV2HeaderSize,
                                      bytes.size() - kDistributedVectorQueryRequestV2HeaderSize -
                                          kDistributedVectorQueryRequestV2TrailerSize);
  store_u32(bytes, 48U, common::crc32c(payload));
  store_u32(bytes, 76U, common::crc32c(common::ByteView{bytes}.first(76U)));
  store_u32(bytes, bytes.size() - 4U,
            common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

void rewrite_response_checksums(std::vector<std::byte>& bytes) {
  const common::ByteView payload =
      common::ByteView{bytes}.subspan(kDistributedVectorQueryResponseV2HeaderSize,
                                      bytes.size() - kDistributedVectorQueryResponseV2HeaderSize -
                                          kDistributedVectorQueryResponseV2TrailerSize);
  store_u32(bytes, 80U, payload.empty() ? 0U : common::crc32c(payload));
  store_u32(bytes, 108U, common::crc32c(common::ByteView{bytes}.first(108U)));
  store_u32(bytes, bytes.size() - 4U,
            common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

TEST(DistributedVectorQueryTransportV2Test, RoundTripsSchemaBoundRequestResponseAndFailure) {
  const DistributedVectorQueryRequestV2 request{1U, 2U, dispatch_v2()};
  const auto encoded_request = encode_distributed_vector_query_request_v2(request);
  ASSERT_TRUE(encoded_request.has_value()) << encoded_request.error().to_string();
  const auto decoded_request = decode_distributed_vector_query_request_v2_exact(*encoded_request);
  ASSERT_TRUE(decoded_request.has_value()) << decoded_request.error().to_string();
  EXPECT_EQ(*decoded_request, request);
  EXPECT_EQ(decode_distributed_vector_query_request_v1(*encoded_request).error().code(),
            common::StatusCode::kCorruption);
  const auto encoded_v1_request =
      encode_distributed_vector_query_request_v1({1U, 2U, dispatch_v2().dispatch});
  ASSERT_TRUE(encoded_v1_request.has_value());
  EXPECT_EQ(decode_distributed_vector_query_request_v2_exact(*encoded_v1_request).error().code(),
            common::StatusCode::kCorruption);

  const query::DistributedVectorResultSchema schema_value = result_schema();
  const DistributedVectorQueryResponseV2 response{
      .source_node_id = 2U,
      .target_node_id = 1U,
      .query_id = uuid(1U),
      .tablet_id = id<schema::TabletId>(4U),
      .status_code = common::StatusCode::kOk,
      .payload = DistributedVectorResultExchangeMessage{.query_id = uuid(1U),
                                                        .tablet_id = id<schema::TabletId>(4U),
                                                        .sequence = 1U,
                                                        .terminal = true,
                                                        .encoded_result_batch = zero_row_batch()}};
  const auto encoded_response = encode_distributed_vector_query_response_v2(response, schema_value);
  ASSERT_TRUE(encoded_response.has_value()) << encoded_response.error().to_string();
  const auto decoded_response =
      decode_distributed_vector_query_response_v2_exact(*encoded_response, schema_value);
  ASSERT_TRUE(decoded_response.has_value()) << decoded_response.error().to_string();
  ASSERT_TRUE(decoded_response->payload.has_value());
  EXPECT_EQ(decoded_response->payload->encoded_result_batch,
            response.payload->encoded_result_batch);
  EXPECT_TRUE(decoded_response->payload->terminal);
  EXPECT_EQ(decode_distributed_vector_query_response_v1(*encoded_response).error().code(),
            common::StatusCode::kCorruption);
  const auto encoded_v1_response = encode_distributed_vector_query_response_v1(
      {.source_node_id = 2U,
       .target_node_id = 1U,
       .query_id = uuid(1U),
       .tablet_id = id<schema::TabletId>(4U),
       .status_code = common::StatusCode::kUnavailable});
  ASSERT_TRUE(encoded_v1_response.has_value());
  EXPECT_EQ(decode_distributed_vector_query_response_v2_exact(*encoded_v1_response, schema_value)
                .error()
                .code(),
            common::StatusCode::kCorruption);

  const DistributedVectorQueryResponseV2 failure{.source_node_id = 2U,
                                                 .target_node_id = 1U,
                                                 .query_id = uuid(1U),
                                                 .tablet_id = id<schema::TabletId>(4U),
                                                 .status_code = common::StatusCode::kUnavailable,
                                                 .leader_hint = DistributedQueryLeaderHint{3U, 9U}};
  const auto encoded_failure = encode_distributed_vector_query_response_v2(failure, schema_value);
  ASSERT_TRUE(encoded_failure.has_value());
  const auto decoded_failure =
      decode_distributed_vector_query_response_v2_exact(*encoded_failure, schema_value);
  ASSERT_TRUE(decoded_failure.has_value());
  EXPECT_FALSE(decoded_failure->payload.has_value());
  EXPECT_EQ(decoded_failure->leader_hint, failure.leader_hint);
}

TEST(DistributedVectorQueryTransportV2Test, RejectsSchemaMismatchDamageAndVersionConfusion) {
  const auto encoded_request = encode_distributed_vector_query_request_v2({1U, 2U, dispatch_v2()});
  ASSERT_TRUE(encoded_request.has_value());
  EXPECT_EQ(decode_distributed_vector_query_request_v2_exact(
                common::ByteView{*encoded_request}.first(encoded_request->size() - 1U))
                .error()
                .code(),
            common::StatusCode::kCorruption);
  std::vector<std::byte> future_request = *encoded_request;
  store_u16(future_request, 8U, 3U);
  rewrite_request_checksums(future_request);
  EXPECT_EQ(decode_distributed_vector_query_request_v2_exact(future_request).error().code(),
            common::StatusCode::kNotSupported);
  std::vector<std::byte> nested_request = *encoded_request;
  nested_request[kDistributedVectorQueryRequestV2HeaderSize] ^= std::byte{1U};
  rewrite_request_checksums(nested_request);
  EXPECT_EQ(decode_distributed_vector_query_request_v2_exact(nested_request).error().code(),
            common::StatusCode::kCorruption);

  const query::DistributedVectorResultSchema schema_value = result_schema();
  const DistributedVectorQueryResponseV2 response{
      .source_node_id = 2U,
      .target_node_id = 1U,
      .query_id = uuid(1U),
      .tablet_id = id<schema::TabletId>(4U),
      .status_code = common::StatusCode::kOk,
      .payload = DistributedVectorResultExchangeMessage{.query_id = uuid(1U),
                                                        .tablet_id = id<schema::TabletId>(4U),
                                                        .sequence = 1U,
                                                        .terminal = true,
                                                        .encoded_result_batch = zero_row_batch()}};
  const auto encoded_response = encode_distributed_vector_query_response_v2(response, schema_value);
  ASSERT_TRUE(encoded_response.has_value());
  query::DistributedVectorResultSchema mismatch = schema_value;
  mismatch.columns.front().name = "other";
  EXPECT_EQ(
      decode_distributed_vector_query_response_v2_exact(*encoded_response, mismatch).error().code(),
      common::StatusCode::kCorruption);
  EXPECT_EQ(encode_distributed_vector_query_response_v2(response, mismatch).error().code(),
            common::StatusCode::kInvalidArgument);

  std::vector<std::byte> correlation = *encoded_response;
  correlation[40U] ^= std::byte{1U};
  rewrite_response_checksums(correlation);
  EXPECT_EQ(
      decode_distributed_vector_query_response_v2_exact(correlation, schema_value).error().code(),
      common::StatusCode::kCorruption);
  std::vector<std::byte> future_response = *encoded_response;
  store_u16(future_response, 8U, 3U);
  rewrite_response_checksums(future_response);
  EXPECT_EQ(decode_distributed_vector_query_response_v2_exact(future_response, schema_value)
                .error()
                .code(),
            common::StatusCode::kNotSupported);
}

TEST(DistributedVectorQueryTransportV2Test, OwnsFragmentedFramesAndTypedShortWrites) {
  const DistributedVectorQueryRequestV2 request{1U, 2U, dispatch_v2()};
  const auto encoded_request = encode_distributed_vector_query_request_v2(request);
  ASSERT_TRUE(encoded_request.has_value());
  for (std::size_t split = 0U; split <= encoded_request->size(); ++split) {
    DistributedVectorQueryRequestV2Reader reader;
    const auto prefix = reader.consume(common::ByteView{*encoded_request}.first(split));
    ASSERT_TRUE(prefix.has_value()) << "split=" << split;
    const auto suffix = reader.consume(common::ByteView{*encoded_request}.subspan(split));
    ASSERT_TRUE(suffix.has_value()) << "split=" << split;
    ASSERT_TRUE(prefix->request.has_value() || suffix->request.has_value()) << "split=" << split;
    EXPECT_EQ(prefix->consumed_bytes + suffix->consumed_bytes, encoded_request->size())
        << "split=" << split;
  }

  const query::DistributedVectorResultSchema schema_value = result_schema();
  const DistributedVectorQueryResponseV2 response{
      .source_node_id = 2U,
      .target_node_id = 1U,
      .query_id = uuid(1U),
      .tablet_id = id<schema::TabletId>(4U),
      .status_code = common::StatusCode::kOk,
      .payload = DistributedVectorResultExchangeMessage{.query_id = uuid(1U),
                                                        .tablet_id = id<schema::TabletId>(4U),
                                                        .sequence = 1U,
                                                        .terminal = true}};
  const auto encoded_response = encode_distributed_vector_query_response_v2(response, schema_value);
  ASSERT_TRUE(encoded_response.has_value());
  for (std::size_t split = 0U; split <= encoded_response->size(); ++split) {
    DistributedVectorQueryResponseV2Reader reader{result_schema()};
    const auto prefix = reader.consume(common::ByteView{*encoded_response}.first(split));
    ASSERT_TRUE(prefix.has_value()) << "split=" << split;
    const auto suffix = reader.consume(common::ByteView{*encoded_response}.subspan(split));
    ASSERT_TRUE(suffix.has_value()) << "split=" << split;
    ASSERT_TRUE(prefix->response.has_value() || suffix->response.has_value()) << "split=" << split;
    EXPECT_EQ(prefix->consumed_bytes + suffix->consumed_bytes, encoded_response->size())
        << "split=" << split;
  }

  std::vector<std::byte> coalesced(encoded_request->begin(), encoded_request->end());
  coalesced.insert(coalesced.end(), encoded_request->begin(), encoded_request->end());
  DistributedVectorQueryRequestV2Reader coalesced_reader;
  const auto first = coalesced_reader.consume(coalesced);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(first->request.has_value());
  EXPECT_EQ(first->consumed_bytes, encoded_request->size());
  const auto second =
      coalesced_reader.consume(common::ByteView{coalesced}.subspan(first->consumed_bytes));
  ASSERT_TRUE(second.has_value());
  ASSERT_TRUE(second->request.has_value());

  std::vector<std::byte> coalesced_responses(encoded_response->begin(), encoded_response->end());
  coalesced_responses.insert(coalesced_responses.end(), encoded_response->begin(),
                             encoded_response->end());
  DistributedVectorQueryResponseV2Reader coalesced_response_reader{result_schema()};
  const auto first_response = coalesced_response_reader.consume(coalesced_responses);
  ASSERT_TRUE(first_response.has_value());
  ASSERT_TRUE(first_response->response.has_value());
  EXPECT_EQ(first_response->consumed_bytes, encoded_response->size());
  const auto second_response = coalesced_response_reader.consume(
      common::ByteView{coalesced_responses}.subspan(first_response->consumed_bytes));
  ASSERT_TRUE(second_response.has_value());
  ASSERT_TRUE(second_response->response.has_value());

  std::vector<std::byte> corrupt = *encoded_request;
  corrupt.front() ^= std::byte{1U};
  DistributedVectorQueryRequestV2Reader failed_reader;
  const auto rejected = failed_reader.consume(corrupt);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_TRUE(failed_reader.failed());
  EXPECT_EQ(failed_reader.consume(*encoded_request).error(), rejected.error());
  std::vector<std::byte> corrupt_response = *encoded_response;
  corrupt_response.front() ^= std::byte{1U};
  DistributedVectorQueryResponseV2Reader failed_response_reader{result_schema()};
  const auto rejected_response = failed_response_reader.consume(corrupt_response);
  ASSERT_FALSE(rejected_response.has_value());
  EXPECT_TRUE(failed_response_reader.failed());
  EXPECT_EQ(failed_response_reader.consume(*encoded_response).error(), rejected_response.error());
  DistributedVectorQueryRequestV2Reader limited_request(encoded_request->size() - 1U);
  EXPECT_EQ(limited_request.consume(*encoded_request).error().code(),
            common::StatusCode::kResourceExhausted);
  DistributedVectorQueryResponseV2Reader limited_response{result_schema(),
                                                          encoded_response->size() - 1U};
  EXPECT_EQ(limited_response.consume(*encoded_response).error().code(),
            common::StatusCode::kResourceExhausted);

  auto request_cursor = DistributedVectorQueryFrameV2WriteCursor::create_request(request);
  ASSERT_TRUE(request_cursor.has_value());
  EXPECT_TRUE(std::ranges::equal(request_cursor->pending_write(), *encoded_request));
  ASSERT_TRUE(request_cursor->consume_written(29U).is_ok());
  EXPECT_EQ(request_cursor->consume_written(request_cursor->pending_write().size() + 1U).code(),
            common::StatusCode::kInvalidArgument);
  DistributedVectorQueryFrameV2WriteCursor moved = std::move(*request_cursor);
  EXPECT_TRUE(request_cursor->complete());
  ASSERT_TRUE(moved.consume_written(moved.pending_write().size()).is_ok());
  EXPECT_TRUE(moved.complete());
  auto response_cursor =
      DistributedVectorQueryFrameV2WriteCursor::create_response(response, schema_value);
  ASSERT_TRUE(response_cursor.has_value());
  EXPECT_TRUE(std::ranges::equal(response_cursor->pending_write(), *encoded_response));
}

} // namespace
} // namespace chronos::cluster
