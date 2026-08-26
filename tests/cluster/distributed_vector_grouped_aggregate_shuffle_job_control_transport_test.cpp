#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_control_transport.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleJobPrepare prepare() {
  const auto string = schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const auto count = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  return {.coordinator_node_id = 9U,
          .target_node_id = 7U,
          .coordinator_result_endpoint = {{127U, 0U, 0U, 1U}, 8137U},
          .execution_timeout = std::chrono::milliseconds{30'000},
          .authority = DistributedVectorGroupedAggregateShuffleAuthority::create(
                           uuid(1U), {{schema::TabletId::from_uuid(uuid(2U)).value(), 3U}},
                           {{0U, 7U}}, {{0U, string, false}},
                           {{query::VectorAggregateOperation::kCountStar, std::nullopt}})
                           .value(),
          .result_schema = {.columns = {{"region", string, false}, {"count", count, false}}}};
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleJobControlResponse response() {
  return {.action = DistributedVectorGroupedAggregateShuffleJobControlAction::kPrepare,
          .status_code = common::StatusCode::kOk,
          .query_id = uuid(1U),
          .coordinator_node_id = 9U,
          .target_node_id = 7U,
          .reducer_shuffle_endpoint = network::Ipv4Endpoint{{127U, 0U, 0U, 1U}, 9123U}};
}

TEST(DistributedVectorGroupedAggregateShuffleJobControlTransportTest,
     ReadsHeaderFirstFragmentedPrepareAndLeavesCoalescedSuffix) {
  auto expected = prepare();
  auto encoded =
      encode_distributed_vector_grouped_aggregate_shuffle_job_prepare_v1(expected).value();
  std::vector<std::byte> carrier(encoded.bytes().begin(), encoded.bytes().end());
  carrier.insert(carrier.end(), 3U, std::byte{0x5aU});
  auto reader = DistributedVectorGroupedAggregateShuffleJobControlRequestReader::create().value();

  auto prefix = reader.consume(common::ByteView{carrier}.first(17U));
  ASSERT_TRUE(prefix.has_value());
  EXPECT_EQ(prefix->consumed_bytes, 17U);
  EXPECT_FALSE(prefix->request.has_value());
  EXPECT_FALSE(reader.expected_frame_bytes().has_value());

  constexpr std::size_t kHeaderLength =
      distributed_vector_grouped_aggregate_shuffle_job_control_format::kHeaderLength;
  auto header = reader.consume(common::ByteView{carrier}.subspan(17U, kHeaderLength - 17U));
  ASSERT_TRUE(header.has_value()) << header.error().to_string();
  EXPECT_FALSE(header->request.has_value());
  ASSERT_TRUE(reader.expected_frame_bytes().has_value());
  EXPECT_EQ(*reader.expected_frame_bytes(), encoded.bytes().size());

  auto completed = reader.consume(common::ByteView{carrier}.subspan(kHeaderLength));
  ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  ASSERT_TRUE(completed->request.has_value());
  EXPECT_EQ(completed->consumed_bytes, encoded.bytes().size() - kHeaderLength);
  const auto* actual =
      std::get_if<DistributedVectorGroupedAggregateShuffleJobPrepare>(&*completed->request);
  ASSERT_NE(actual, nullptr);
  EXPECT_EQ(actual->authority.query_id(), expected.authority.query_id());
  EXPECT_EQ(actual->coordinator_result_endpoint, expected.coordinator_result_endpoint);
  EXPECT_EQ(reader.buffered_bytes(), 0U);
}

TEST(DistributedVectorGroupedAggregateShuffleJobControlTransportTest,
     RejectsDamagedHeaderAndCallerFrameLimitBeforePayloadAllocation) {
  auto encoded =
      encode_distributed_vector_grouped_aggregate_shuffle_job_prepare_v1(prepare()).value();
  constexpr std::size_t kHeaderLength =
      distributed_vector_grouped_aggregate_shuffle_job_control_format::kHeaderLength;
  DistributedVectorGroupedAggregateShuffleJobControlDecodeLimits limits;
  limits.maximum_frame_length = encoded.bytes().size() - 1U;
  auto limited = distributed_vector_grouped_aggregate_shuffle_job_control_request_frame_length_v1(
      encoded.bytes().first(kHeaderLength), limits);
  ASSERT_FALSE(limited.has_value());
  EXPECT_EQ(limited.error().code(), common::StatusCode::kResourceExhausted);

  std::vector<std::byte> damaged(encoded.bytes().begin(),
                                 encoded.bytes().begin() +
                                     static_cast<std::ptrdiff_t>(kHeaderLength));
  damaged[24U] ^= std::byte{1U};
  auto reader = DistributedVectorGroupedAggregateShuffleJobControlRequestReader::create().value();
  auto rejected = reader.consume(damaged);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kCorruption);
  EXPECT_TRUE(reader.failed());
  EXPECT_EQ(reader.consume({}).error().code(), common::StatusCode::kCorruption);
}

TEST(DistributedVectorGroupedAggregateShuffleJobControlTransportTest,
     ReadsFixedResponseAndTransfersPartialWriteObligationsOnMove) {
  auto encoded_response =
      encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v1(response())
          .value();
  std::vector<std::byte> carrier(encoded_response.bytes().begin(), encoded_response.bytes().end());
  carrier.push_back(std::byte{0x5aU});
  DistributedVectorGroupedAggregateShuffleJobControlResponseReader response_reader;
  auto prefix = response_reader.consume(common::ByteView{carrier}.first(11U));
  ASSERT_TRUE(prefix.has_value());
  EXPECT_FALSE(prefix->response.has_value());
  auto completed = response_reader.consume(common::ByteView{carrier}.subspan(11U));
  ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  ASSERT_TRUE(completed->response.has_value());
  EXPECT_EQ(*completed->response, response());
  EXPECT_EQ(completed->consumed_bytes, encoded_response.bytes().size() - 11U);

  auto encoded_request =
      encode_distributed_vector_grouped_aggregate_shuffle_job_seal_v1({uuid(1U), 9U, 7U}).value();
  const std::size_t request_size = encoded_request.bytes().size();
  auto request_writer =
      DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor::create(
          std::move(encoded_request));
  EXPECT_TRUE(request_writer.consume_written(13U).is_ok());
  auto moved = std::move(request_writer);
  EXPECT_TRUE(request_writer.complete()); // NOLINT(bugprone-use-after-move): documented contract.
  EXPECT_EQ(moved.pending_write().size(), request_size - 13U);
  EXPECT_TRUE(moved.consume_written(request_size - 13U).is_ok());
  EXPECT_TRUE(moved.complete());

  auto response_writer =
      DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor::create(
          std::move(encoded_response));
  EXPECT_EQ(response_writer.consume_written(response_writer.pending_write().size() + 1U).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_FALSE(response_writer.complete());
}

} // namespace
} // namespace chronos::cluster
