#include "chronos/cluster/distributed_grouped_query_transport.hpp"
#include "chronos/cluster/distributed_vector_query_transport.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <ranges>
#include <utility>
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

[[nodiscard]] query::DistributedVectorFragmentDispatch dispatch() {
  return {.query_id = uuid(1U),
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
          .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
          .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
          .destination_column_ordinals = {0U, 1U},
          .plan = {.mode = query::DistributedVectorPlanMode::kGroupedAggregate,
                   .group_key_input_indices = {0U},
                   .aggregates = {{.operation = query::VectorAggregateOperation::kSum,
                                   .input_index = 1U}},
                   .order_keys = {{.output_index = 1U,
                                   .direction = query::PhysicalSortDirection::kDescending}},
                   .limit = 3U}};
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
  const common::ByteView payload =
      common::ByteView{bytes}.subspan(kDistributedVectorQueryRequestHeaderSize,
                                      bytes.size() - kDistributedVectorQueryRequestHeaderSize -
                                          kDistributedVectorQueryRequestTrailerSize);
  store_u32(bytes, 48U, common::crc32c(payload));
  store_u32(bytes, 76U, common::crc32c(common::ByteView{bytes}.first(76U)));
  store_u32(bytes, bytes.size() - 4U,
            common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

void rewrite_response_checksums(std::vector<std::byte>& bytes) {
  const common::ByteView payload =
      common::ByteView{bytes}.subspan(kDistributedVectorQueryResponseHeaderSize,
                                      bytes.size() - kDistributedVectorQueryResponseHeaderSize -
                                          kDistributedVectorQueryResponseTrailerSize);
  store_u32(bytes, 80U, payload.empty() ? 0U : common::crc32c(payload));
  store_u32(bytes, 108U, common::crc32c(common::ByteView{bytes}.first(108U)));
  store_u32(bytes, bytes.size() - 4U,
            common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

TEST(DistributedVectorQueryTransportTest, RoundTripsDistinctProofBoundRequest) {
  const DistributedVectorQueryRequest request{1U, 2U, dispatch()};
  const auto encoded = encode_distributed_vector_query_request_v1(request);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  EXPECT_LE(encoded->size(), kMaximumDistributedVectorQueryRequestSize);
  const auto decoded = decode_distributed_vector_query_request_v1(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, request);
  EXPECT_EQ(decode_distributed_grouped_query_request_v1(*encoded).error().code(),
            common::StatusCode::kCorruption);

  DistributedVectorQueryRequest invalid_route = request;
  invalid_route.target_node_id = invalid_route.source_node_id;
  EXPECT_EQ(encode_distributed_vector_query_request_v1(invalid_route).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedVectorQueryTransportTest, RejectsOuterAndNestedDamageAndFutureVersion) {
  const auto encoded = encode_distributed_vector_query_request_v1({1U, 2U, dispatch()});
  ASSERT_TRUE(encoded.has_value());
  EXPECT_EQ(decode_distributed_vector_query_request_v1(
                common::ByteView{*encoded}.first(encoded->size() - 1U))
                .error()
                .code(),
            common::StatusCode::kCorruption);

  std::vector<std::byte> future = *encoded;
  store_u16(future, 8U, 2U);
  rewrite_checksums(future);
  EXPECT_EQ(decode_distributed_vector_query_request_v1(future).error().code(),
            common::StatusCode::kNotSupported);

  std::vector<std::byte> nested = *encoded;
  nested[kDistributedVectorQueryRequestHeaderSize + 24U] ^= std::byte{1U};
  rewrite_checksums(nested);
  EXPECT_EQ(decode_distributed_vector_query_request_v1(nested).error().code(),
            common::StatusCode::kCorruption);

  std::vector<std::byte> reserved = *encoded;
  reserved[52U] = std::byte{1U};
  rewrite_checksums(reserved);
  EXPECT_EQ(decode_distributed_vector_query_request_v1(reserved).error().code(),
            common::StatusCode::kCorruption);
}

TEST(DistributedVectorQueryTransportTest, RoundTripsCorrelatedTerminalAndFailureResponses) {
  const query::DistributedVectorExchangeMessage terminal{.query_id = uuid(1U),
                                                         .tablet_id = id<schema::TabletId>(4U),
                                                         .sequence = 1U,
                                                         .terminal = true};
  const DistributedVectorQueryResponse success{.source_node_id = 2U,
                                               .target_node_id = 1U,
                                               .query_id = uuid(1U),
                                               .tablet_id = id<schema::TabletId>(4U),
                                               .status_code = common::StatusCode::kOk,
                                               .payload = terminal};
  const auto encoded_success = encode_distributed_vector_query_response_v1(success);
  ASSERT_TRUE(encoded_success.has_value()) << encoded_success.error().to_string();
  const auto decoded_success = decode_distributed_vector_query_response_v1(*encoded_success);
  ASSERT_TRUE(decoded_success.has_value()) << decoded_success.error().to_string();
  EXPECT_EQ(decode_distributed_grouped_query_response_v1(*encoded_success).error().code(),
            common::StatusCode::kCorruption);
  ASSERT_TRUE(decoded_success->payload.has_value());
  EXPECT_EQ(decoded_success->source_node_id, 2U);
  EXPECT_EQ(decoded_success->target_node_id, 1U);
  EXPECT_EQ(decoded_success->payload->query_id, terminal.query_id);
  EXPECT_EQ(decoded_success->payload->tablet_id, terminal.tablet_id);
  EXPECT_EQ(decoded_success->payload->sequence, 1U);
  EXPECT_TRUE(decoded_success->payload->terminal);
  EXPECT_TRUE(decoded_success->payload->encoded_batch.empty());

  const DistributedVectorQueryResponse failure{.source_node_id = 2U,
                                               .target_node_id = 1U,
                                               .query_id = uuid(1U),
                                               .tablet_id = id<schema::TabletId>(4U),
                                               .status_code = common::StatusCode::kUnavailable,
                                               .leader_hint = DistributedQueryLeaderHint{3U, 9U}};
  const auto encoded_failure = encode_distributed_vector_query_response_v1(failure);
  ASSERT_TRUE(encoded_failure.has_value()) << encoded_failure.error().to_string();
  EXPECT_EQ(encoded_failure->size(), kDistributedVectorQueryResponseHeaderSize + 4U);
  const auto decoded_failure = decode_distributed_vector_query_response_v1(*encoded_failure);
  ASSERT_TRUE(decoded_failure.has_value());
  EXPECT_EQ(decoded_failure->status_code, common::StatusCode::kUnavailable);
  EXPECT_FALSE(decoded_failure->payload.has_value());
  EXPECT_EQ(decoded_failure->leader_hint, failure.leader_hint);

  constexpr std::array failure_codes{
      common::StatusCode::kCancelled,       common::StatusCode::kInvalidArgument,
      common::StatusCode::kOutOfRange,      common::StatusCode::kNotFound,
      common::StatusCode::kAlreadyExists,   common::StatusCode::kCorruption,
      common::StatusCode::kIoError,         common::StatusCode::kResourceExhausted,
      common::StatusCode::kUnavailable,     common::StatusCode::kNotSupported,
      common::StatusCode::kUnauthenticated, common::StatusCode::kInternal};
  for (const common::StatusCode code : failure_codes) {
    auto candidate = failure;
    candidate.status_code = code;
    candidate.leader_hint.reset();
    const auto encoded_candidate = encode_distributed_vector_query_response_v1(candidate);
    ASSERT_TRUE(encoded_candidate.has_value());
    const auto decoded_candidate = decode_distributed_vector_query_response_v1(*encoded_candidate);
    ASSERT_TRUE(decoded_candidate.has_value());
    EXPECT_EQ(decoded_candidate->status_code, code);
  }

  auto mismatched = success;
  mismatched.query_id = uuid(8U);
  EXPECT_EQ(encode_distributed_vector_query_response_v1(mismatched).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedVectorQueryTransportTest, RejectsResponseKindCorrelationAndNestedDamage) {
  const DistributedVectorQueryResponse success{
      .source_node_id = 2U,
      .target_node_id = 1U,
      .query_id = uuid(1U),
      .tablet_id = id<schema::TabletId>(4U),
      .status_code = common::StatusCode::kOk,
      .payload = query::DistributedVectorExchangeMessage{.query_id = uuid(1U),
                                                         .tablet_id = id<schema::TabletId>(4U),
                                                         .sequence = 1U,
                                                         .terminal = true}};
  const auto encoded = encode_distributed_vector_query_response_v1(success);
  ASSERT_TRUE(encoded.has_value());

  std::vector<std::byte> kind = *encoded;
  kind[73U] = std::byte{2U};
  rewrite_response_checksums(kind);
  EXPECT_EQ(decode_distributed_vector_query_response_v1(kind).error().code(),
            common::StatusCode::kCorruption);

  std::vector<std::byte> correlation = *encoded;
  correlation[40U] ^= std::byte{1U};
  rewrite_response_checksums(correlation);
  EXPECT_EQ(decode_distributed_vector_query_response_v1(correlation).error().code(),
            common::StatusCode::kCorruption);

  std::vector<std::byte> nested = *encoded;
  nested[kDistributedVectorQueryResponseHeaderSize] ^= std::byte{1U};
  rewrite_response_checksums(nested);
  EXPECT_EQ(decode_distributed_vector_query_response_v1(nested).error().code(),
            common::StatusCode::kCorruption);
}

TEST(DistributedVectorQueryTransportTest, OwnsFragmentedFramesAndCheckedShortWrites) {
  const auto encoded_request = encode_distributed_vector_query_request_v1({1U, 2U, dispatch()});
  ASSERT_TRUE(encoded_request.has_value());
  for (std::size_t split = 0U; split <= encoded_request->size(); ++split) {
    DistributedVectorQueryRequestReader reader;
    const auto prefix = reader.consume(common::ByteView{*encoded_request}.first(split));
    ASSERT_TRUE(prefix.has_value()) << "split=" << split;
    EXPECT_EQ(prefix->consumed_bytes, split) << "split=" << split;
    const auto suffix = reader.consume(common::ByteView{*encoded_request}.subspan(split));
    ASSERT_TRUE(suffix.has_value()) << "split=" << split;
    ASSERT_TRUE(prefix->request.has_value() || suffix->request.has_value()) << "split=" << split;
    EXPECT_EQ(suffix->consumed_bytes, encoded_request->size() - split) << "split=" << split;
  }

  const DistributedVectorQueryResponse success{
      .source_node_id = 2U,
      .target_node_id = 1U,
      .query_id = uuid(1U),
      .tablet_id = id<schema::TabletId>(4U),
      .status_code = common::StatusCode::kOk,
      .payload = query::DistributedVectorExchangeMessage{.query_id = uuid(1U),
                                                         .tablet_id = id<schema::TabletId>(4U),
                                                         .sequence = 1U,
                                                         .terminal = true}};
  const auto encoded_response = encode_distributed_vector_query_response_v1(success);
  ASSERT_TRUE(encoded_response.has_value());
  for (std::size_t split = 0U; split <= encoded_response->size(); ++split) {
    DistributedVectorQueryResponseReader reader;
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
  DistributedVectorQueryRequestReader coalesced_reader;
  const auto first = coalesced_reader.consume(coalesced);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(first->request.has_value());
  EXPECT_EQ(first->consumed_bytes, encoded_request->size());
  const auto second =
      coalesced_reader.consume(common::ByteView{coalesced}.subspan(first->consumed_bytes));
  ASSERT_TRUE(second.has_value());
  ASSERT_TRUE(second->request.has_value());

  std::vector<std::byte> corrupt = *encoded_request;
  corrupt.front() ^= std::byte{1U};
  DistributedVectorQueryRequestReader failed_reader;
  const auto rejected = failed_reader.consume(corrupt);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_TRUE(failed_reader.failed());
  EXPECT_EQ(failed_reader.consume(*encoded_request).error(), rejected.error());

  DistributedVectorQueryRequestReader limited_request(encoded_request->size() - 1U);
  EXPECT_EQ(limited_request.consume(*encoded_request).error().code(),
            common::StatusCode::kResourceExhausted);
  DistributedVectorQueryResponseReader limited_response(encoded_response->size() - 1U);
  EXPECT_EQ(limited_response.consume(*encoded_response).error().code(),
            common::StatusCode::kResourceExhausted);
  DistributedVectorQueryRequestReader invalid_limit(0U);
  EXPECT_EQ(invalid_limit.consume(*encoded_request).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(invalid_limit.buffered_bytes(), 0U);
  EXPECT_FALSE(invalid_limit.failed());

  auto cursor = DistributedVectorQueryFrameWriteCursor::create(*encoded_request);
  ASSERT_TRUE(cursor.has_value());
  EXPECT_TRUE(std::ranges::equal(cursor->pending_write(), *encoded_request));
  ASSERT_TRUE(cursor->consume_written(23U).is_ok());
  EXPECT_EQ(cursor->written_bytes(), 23U);
  EXPECT_EQ(cursor->consume_written(cursor->pending_write().size() + 1U).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(cursor->written_bytes(), 23U);
  DistributedVectorQueryFrameWriteCursor moved = std::move(*cursor);
  EXPECT_TRUE(cursor->complete());
  EXPECT_TRUE(cursor->pending_write().empty());
  ASSERT_TRUE(moved.consume_written(moved.pending_write().size()).is_ok());
  EXPECT_TRUE(moved.complete());

  auto response_cursor = DistributedVectorQueryFrameWriteCursor::create(*encoded_response);
  ASSERT_TRUE(response_cursor.has_value());
  EXPECT_EQ(response_cursor->pending_write().size(), encoded_response->size());
  std::vector<std::byte> invalid_frame = *encoded_request;
  invalid_frame.back() ^= std::byte{1U};
  EXPECT_EQ(DistributedVectorQueryFrameWriteCursor::create(std::move(invalid_frame)).error().code(),
            common::StatusCode::kCorruption);
}

} // namespace
} // namespace chronos::cluster
