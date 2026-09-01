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
  const auto expected_frame_bytes = reader.expected_frame_bytes();
  if (!expected_frame_bytes.has_value()) {
    FAIL() << "complete control header produced no expected frame length";
  }
  EXPECT_EQ(expected_frame_bytes.value(), encoded.bytes().size());

  auto completed = reader.consume(common::ByteView{carrier}.subspan(kHeaderLength));
  ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  const auto& completed_request = completed->request;
  if (!completed_request.has_value()) {
    FAIL() << "complete prepare frame produced no request";
  }
  EXPECT_EQ(completed->consumed_bytes, encoded.bytes().size() - kHeaderLength);
  const auto* actual =
      std::get_if<DistributedVectorGroupedAggregateShuffleJobPrepare>(&completed_request.value());
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
  const auto& completed_response = completed->response;
  if (!completed_response.has_value()) {
    FAIL() << "complete response frame produced no response";
  }
  EXPECT_EQ(completed_response.value(), response());
  EXPECT_EQ(completed->consumed_bytes, encoded_response.bytes().size() - 11U);

  auto encoded_request =
      encode_distributed_vector_grouped_aggregate_shuffle_job_seal_v1({uuid(1U), 9U, 7U}).value();
  const std::size_t request_size = encoded_request.bytes().size();
  auto request_writer =
      DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor::create(
          std::move(encoded_request));
  EXPECT_TRUE(request_writer.consume_written(13U).is_ok());
  auto moved = std::move(request_writer);
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

TEST(DistributedVectorGroupedAggregateShuffleJobControlTransportTest,
     DispatchesFragmentedVersionTwoRouteFramesAndResponses) {
  const DistributedVectorGroupedAggregateShuffleJobInstallRoutes routes{
      .query_id = uuid(1U),
      .coordinator_node_id = 9U,
      .target_node_id = 7U,
      .routes = {{.node_id = 7U, .endpoint = {{127U, 0U, 0U, 1U}, 9123U}}}};
  auto encoded =
      encode_distributed_vector_grouped_aggregate_shuffle_job_install_routes_v2(routes).value();
  auto reader = DistributedVectorGroupedAggregateShuffleJobControlRequestReader::create().value();
  auto prefix = reader.consume(encoded.bytes().first(31U));
  ASSERT_TRUE(prefix.has_value());
  EXPECT_FALSE(prefix->request.has_value());
  auto completed = reader.consume(encoded.bytes().subspan(31U));
  ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  const auto& completed_request = completed->request;
  if (!completed_request.has_value()) {
    FAIL() << "complete route frame produced no request";
  }
  const auto* actual = std::get_if<DistributedVectorGroupedAggregateShuffleJobInstallRoutes>(
      &completed_request.value());
  ASSERT_NE(actual, nullptr);
  EXPECT_EQ(*actual, routes);

  const DistributedVectorGroupedAggregateShuffleJobControlResponse response{
      .action = DistributedVectorGroupedAggregateShuffleJobControlAction::kInstallRoutes,
      .status_code = common::StatusCode::kOk,
      .query_id = uuid(1U),
      .coordinator_node_id = 9U,
      .target_node_id = 7U};
  auto response_bytes =
      encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v2(response).value();
  DistributedVectorGroupedAggregateShuffleJobControlResponseReader response_reader;
  auto response_prefix = response_reader.consume(response_bytes.bytes().first(19U));
  ASSERT_TRUE(response_prefix.has_value());
  EXPECT_FALSE(response_prefix->response.has_value());
  auto response_complete = response_reader.consume(response_bytes.bytes().subspan(19U));
  ASSERT_TRUE(response_complete.has_value()) << response_complete.error().to_string();
  const auto& completed_response = response_complete->response;
  if (!completed_response.has_value()) {
    FAIL() << "complete route response frame produced no response";
  }
  EXPECT_EQ(completed_response.value(), response);
}

TEST(DistributedVectorGroupedAggregateShuffleJobControlTransportTest,
     DispatchesFragmentedVersionThreeCancellationFramesAndResponses) {
  const DistributedVectorGroupedAggregateShuffleJobCancel cancel{
      .query_id = uuid(1U), .coordinator_node_id = 9U, .target_node_id = 7U};
  auto encoded = encode_distributed_vector_grouped_aggregate_shuffle_job_cancel_v3(cancel).value();
  auto reader = DistributedVectorGroupedAggregateShuffleJobControlRequestReader::create().value();
  auto prefix = reader.consume(encoded.bytes().first(17U));
  ASSERT_TRUE(prefix.has_value());
  EXPECT_FALSE(prefix->request.has_value());
  auto completed = reader.consume(encoded.bytes().subspan(17U));
  ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  const auto& completed_request = completed->request;
  if (!completed_request.has_value()) {
    FAIL() << "complete cancellation frame produced no request";
  }
  const auto* actual =
      std::get_if<DistributedVectorGroupedAggregateShuffleJobCancel>(&completed_request.value());
  ASSERT_NE(actual, nullptr);
  EXPECT_EQ(*actual, cancel);

  const DistributedVectorGroupedAggregateShuffleJobControlResponse response{
      .action = DistributedVectorGroupedAggregateShuffleJobControlAction::kCancel,
      .status_code = common::StatusCode::kOk,
      .query_id = uuid(1U),
      .coordinator_node_id = 9U,
      .target_node_id = 7U};
  auto response_bytes =
      encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v3(response).value();
  DistributedVectorGroupedAggregateShuffleJobControlResponseReader response_reader;
  auto response_prefix = response_reader.consume(response_bytes.bytes().first(23U));
  ASSERT_TRUE(response_prefix.has_value());
  EXPECT_FALSE(response_prefix->response.has_value());
  auto response_complete = response_reader.consume(response_bytes.bytes().subspan(23U));
  ASSERT_TRUE(response_complete.has_value()) << response_complete.error().to_string();
  const auto& completed_response = response_complete->response;
  if (!completed_response.has_value()) {
    FAIL() << "complete cancellation response frame produced no response";
  }
  EXPECT_EQ(completed_response.value(), response);
}

TEST(DistributedVectorGroupedAggregateShuffleJobControlTransportTest,
     DispatchesFragmentedVersionFourLeaseRenewalFramesAndResponses) {
  const DistributedVectorGroupedAggregateShuffleJobRenewLease renewal{
      .query_id = uuid(1U),
      .coordinator_node_id = 9U,
      .target_node_id = 7U,
      .lease_duration = std::chrono::milliseconds{5000}};
  auto encoded =
      encode_distributed_vector_grouped_aggregate_shuffle_job_renew_lease_v4(renewal).value();
  auto reader = DistributedVectorGroupedAggregateShuffleJobControlRequestReader::create().value();
  auto prefix = reader.consume(encoded.bytes().first(29U));
  ASSERT_TRUE(prefix.has_value());
  EXPECT_FALSE(prefix->request.has_value());
  auto completed = reader.consume(encoded.bytes().subspan(29U));
  ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  const auto& completed_request = completed->request;
  if (!completed_request.has_value()) {
    FAIL() << "complete lease-renewal frame produced no request";
  }
  const auto* actual = std::get_if<DistributedVectorGroupedAggregateShuffleJobRenewLease>(
      &completed_request.value());
  ASSERT_NE(actual, nullptr);
  EXPECT_EQ(*actual, renewal);

  const DistributedVectorGroupedAggregateShuffleJobControlResponse response{
      .action = DistributedVectorGroupedAggregateShuffleJobControlAction::kRenewLease,
      .status_code = common::StatusCode::kOk,
      .query_id = uuid(1U),
      .coordinator_node_id = 9U,
      .target_node_id = 7U};
  auto response_bytes =
      encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v4(response).value();
  DistributedVectorGroupedAggregateShuffleJobControlResponseReader response_reader;
  auto response_prefix = response_reader.consume(response_bytes.bytes().first(31U));
  ASSERT_TRUE(response_prefix.has_value());
  EXPECT_FALSE(response_prefix->response.has_value());
  auto response_complete = response_reader.consume(response_bytes.bytes().subspan(31U));
  ASSERT_TRUE(response_complete.has_value()) << response_complete.error().to_string();
  const auto& completed_response = response_complete->response;
  if (!completed_response.has_value()) {
    FAIL() << "complete lease-renewal response frame produced no response";
  }
  EXPECT_EQ(completed_response.value(), response);
}

} // namespace
} // namespace chronos::cluster
