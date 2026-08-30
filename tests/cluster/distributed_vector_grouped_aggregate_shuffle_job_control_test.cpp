#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_control.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <ranges>
#include <variant>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleAuthority authority() {
  return DistributedVectorGroupedAggregateShuffleAuthority::create(
             uuid(1U),
             {{schema::TabletId::from_uuid(uuid(2U)).value(), 3U},
              {schema::TabletId::from_uuid(uuid(3U)).value(), 4U}},
             {{0U, 7U}, {1U, 8U}}, {{0U, type(schema::LogicalTypeKind::kString), true}},
             {{query::VectorAggregateOperation::kCountStar, std::nullopt}})
      .value();
}

[[nodiscard]] query::DistributedVectorResultSchema result_schema() {
  return {.columns = {{"region", type(schema::LogicalTypeKind::kString), true},
                      {"count", type(schema::LogicalTypeKind::kInt64), false}}};
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleJobPrepare prepare() {
  return {.coordinator_node_id = 9U,
          .target_node_id = 7U,
          .coordinator_result_endpoint = {{127U, 0U, 0U, 1U}, 8137U},
          .execution_timeout = std::chrono::milliseconds{30'000},
          .authority = authority(),
          .result_schema = result_schema()};
}

void store_u16(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint16_t value) {
  bytes[offset] = static_cast<std::byte>(value & 0xffU);
  bytes[offset + 1U] = static_cast<std::byte>((value >> 8U) & 0xffU);
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < 4U; ++index)
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
}

void refresh_checksums(std::vector<std::byte>& bytes) {
  store_u32(bytes, 124U, common::crc32c(common::ByteView{bytes}.first(124U)));
  store_u32(bytes, bytes.size() - 4U,
            common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

TEST(DistributedVectorGroupedAggregateShuffleJobControlTest,
     RoundTripsCompletePrepareAndCanonicalSeal) {
  auto expected = prepare();
  auto encoded = encode_distributed_vector_grouped_aggregate_shuffle_job_prepare_v1(expected);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  auto decoded = decode_distributed_vector_grouped_aggregate_shuffle_job_control_request_v1_exact(
      encoded->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  const auto* actual = std::get_if<DistributedVectorGroupedAggregateShuffleJobPrepare>(&*decoded);
  ASSERT_NE(actual, nullptr);
  EXPECT_EQ(actual->coordinator_node_id, expected.coordinator_node_id);
  EXPECT_EQ(actual->target_node_id, expected.target_node_id);
  EXPECT_EQ(actual->coordinator_result_endpoint, expected.coordinator_result_endpoint);
  EXPECT_EQ(actual->execution_timeout, expected.execution_timeout);
  EXPECT_EQ(actual->authority.query_id(), expected.authority.query_id());
  EXPECT_TRUE(std::ranges::equal(actual->authority.sources(), expected.authority.sources()));
  EXPECT_TRUE(
      std::ranges::equal(actual->authority.destinations(), expected.authority.destinations()));
  EXPECT_EQ(actual->result_schema, expected.result_schema);

  const DistributedVectorGroupedAggregateShuffleJobSeal seal{uuid(1U), 9U, 7U};
  auto encoded_seal = encode_distributed_vector_grouped_aggregate_shuffle_job_seal_v1(seal);
  ASSERT_TRUE(encoded_seal.has_value()) << encoded_seal.error().to_string();
  auto decoded_seal =
      decode_distributed_vector_grouped_aggregate_shuffle_job_control_request_v1_exact(
          encoded_seal->bytes());
  ASSERT_TRUE(decoded_seal.has_value()) << decoded_seal.error().to_string();
  const auto* actual_seal =
      std::get_if<DistributedVectorGroupedAggregateShuffleJobSeal>(&*decoded_seal);
  ASSERT_NE(actual_seal, nullptr);
  EXPECT_EQ(*actual_seal, seal);
}

TEST(DistributedVectorGroupedAggregateShuffleJobControlTest,
     RejectsDamageUnknownVersionNoncanonicalSealSchemaDriftAndCallerTimeout) {
  auto expected = prepare();
  auto encoded =
      encode_distributed_vector_grouped_aggregate_shuffle_job_prepare_v1(expected).value();
  std::vector<std::byte> bytes(encoded.bytes().begin(), encoded.bytes().end());

  auto damaged = bytes;
  damaged.back() ^= std::byte{1U};
  EXPECT_EQ(
      decode_distributed_vector_grouped_aggregate_shuffle_job_control_request_v1_exact(damaged)
          .error()
          .code(),
      common::StatusCode::kCorruption);

  auto future = bytes;
  store_u16(future, 8U, 2U);
  refresh_checksums(future);
  EXPECT_EQ(decode_distributed_vector_grouped_aggregate_shuffle_job_control_request_v1_exact(future)
                .error()
                .code(),
            common::StatusCode::kNotSupported);

  auto zero_timeout = bytes;
  std::fill(zero_timeout.begin() + 72, zero_timeout.begin() + 80, std::byte{});
  refresh_checksums(zero_timeout);
  EXPECT_EQ(
      decode_distributed_vector_grouped_aggregate_shuffle_job_control_request_v1_exact(zero_timeout)
          .error()
          .code(),
      common::StatusCode::kCorruption);

  DistributedVectorGroupedAggregateShuffleJobControlDecodeLimits limits;
  limits.maximum_execution_timeout = std::chrono::milliseconds{1000};
  EXPECT_EQ(decode_distributed_vector_grouped_aggregate_shuffle_job_control_request_v1_exact(bytes,
                                                                                             limits)
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);

  auto wrong_schema = prepare();
  wrong_schema.result_schema.columns.back().nullable = true;
  EXPECT_EQ(encode_distributed_vector_grouped_aggregate_shuffle_job_prepare_v1(wrong_schema)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  auto seal =
      encode_distributed_vector_grouped_aggregate_shuffle_job_seal_v1({uuid(1U), 9U, 7U}).value();
  std::vector<std::byte> noncanonical(seal.bytes().begin(), seal.bytes().end());
  noncanonical[64U] = std::byte{127U};
  refresh_checksums(noncanonical);
  EXPECT_EQ(
      decode_distributed_vector_grouped_aggregate_shuffle_job_control_request_v1_exact(noncanonical)
          .error()
          .code(),
      common::StatusCode::kCorruption);

  DistributedVectorGroupedAggregateShuffleJobControlDecodeLimits invalid_limits;
  invalid_limits.result_schema.maximum_columns = 0U;
  EXPECT_EQ(decode_distributed_vector_grouped_aggregate_shuffle_job_control_request_v1_exact(
                seal.bytes(), invalid_limits)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedVectorGroupedAggregateShuffleJobControlTest,
     RoundTripsCorrelatedPrepareAndSealResponsesAndRejectsNoncanonicalEndpoint) {
  const DistributedVectorGroupedAggregateShuffleJobControlResponse prepared{
      .action = DistributedVectorGroupedAggregateShuffleJobControlAction::kPrepare,
      .status_code = common::StatusCode::kOk,
      .query_id = uuid(1U),
      .coordinator_node_id = 9U,
      .target_node_id = 7U,
      .reducer_shuffle_endpoint = network::Ipv4Endpoint{{127U, 0U, 0U, 1U}, 9123U}};
  auto encoded =
      encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v1(prepared);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  auto decoded = decode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v1_exact(
      encoded->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, prepared);

  auto local_only_prepared = prepared;
  local_only_prepared.reducer_shuffle_endpoint.reset();
  EXPECT_TRUE(encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v1(
                  local_only_prepared)
                  .has_value());

  const DistributedVectorGroupedAggregateShuffleJobControlResponse sealed{
      .action = DistributedVectorGroupedAggregateShuffleJobControlAction::kSeal,
      .status_code = common::StatusCode::kOk,
      .query_id = uuid(1U),
      .coordinator_node_id = 9U,
      .target_node_id = 7U};
  auto encoded_seal =
      encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v1(sealed);
  ASSERT_TRUE(encoded_seal.has_value()) << encoded_seal.error().to_string();
  EXPECT_EQ(decode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v1_exact(
                encoded_seal->bytes())
                .value(),
            sealed);

  auto failed = prepared;
  failed.status_code = common::StatusCode::kResourceExhausted;
  failed.reducer_shuffle_endpoint.reset();
  EXPECT_TRUE(encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v1(failed)
                  .has_value());

  auto invalid = sealed;
  invalid.reducer_shuffle_endpoint = network::Ipv4Endpoint{{127U, 0U, 0U, 1U}, 9123U};
  EXPECT_EQ(encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v1(invalid)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  std::vector<std::byte> damaged(encoded->bytes().begin(), encoded->bytes().end());
  damaged.back() ^= std::byte{1U};
  EXPECT_EQ(
      decode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v1_exact(damaged)
          .error()
          .code(),
      common::StatusCode::kCorruption);
}

TEST(DistributedVectorGroupedAggregateShuffleJobControlTest,
     RoundTripsVersionTwoRouteInstallationAndCorrelatedResponse) {
  const DistributedVectorGroupedAggregateShuffleJobInstallRoutes expected{
      .query_id = uuid(1U),
      .coordinator_node_id = 9U,
      .target_node_id = 7U,
      .routes = {{.node_id = 7U, .endpoint = {{127U, 0U, 0U, 1U}, 9123U}},
                 {.node_id = 8U, .endpoint = {{127U, 0U, 0U, 2U}, 9124U}}}};
  auto encoded =
      encode_distributed_vector_grouped_aggregate_shuffle_job_install_routes_v2(expected);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  auto decoded = decode_distributed_vector_grouped_aggregate_shuffle_job_control_request_v2_exact(
      encoded->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  const auto* actual =
      std::get_if<DistributedVectorGroupedAggregateShuffleJobInstallRoutes>(&*decoded);
  ASSERT_NE(actual, nullptr);
  EXPECT_EQ(*actual, expected);

  DistributedVectorGroupedAggregateShuffleJobControlDecodeLimits limited;
  limited.maximum_routes = 1U;
  EXPECT_EQ(decode_distributed_vector_grouped_aggregate_shuffle_job_control_request_v2_exact(
                encoded->bytes(), limited)
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);

  auto noncanonical = expected;
  std::ranges::reverse(noncanonical.routes);
  EXPECT_EQ(encode_distributed_vector_grouped_aggregate_shuffle_job_install_routes_v2(noncanonical)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  const DistributedVectorGroupedAggregateShuffleJobControlResponse response{
      .action = DistributedVectorGroupedAggregateShuffleJobControlAction::kInstallRoutes,
      .status_code = common::StatusCode::kOk,
      .query_id = uuid(1U),
      .coordinator_node_id = 9U,
      .target_node_id = 7U};
  auto encoded_response =
      encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v2(response);
  ASSERT_TRUE(encoded_response.has_value()) << encoded_response.error().to_string();
  auto decoded_response =
      decode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v2_exact(
          encoded_response->bytes());
  ASSERT_TRUE(decoded_response.has_value()) << decoded_response.error().to_string();
  EXPECT_EQ(*decoded_response, response);

  std::vector<std::byte> damaged(encoded->bytes().begin(), encoded->bytes().end());
  damaged[distributed_vector_grouped_aggregate_shuffle_job_control_v2_format::kHeaderLength] ^=
      std::byte{1U};
  EXPECT_EQ(
      decode_distributed_vector_grouped_aggregate_shuffle_job_control_request_v2_exact(damaged)
          .error()
          .code(),
      common::StatusCode::kCorruption);
}

TEST(DistributedVectorGroupedAggregateShuffleJobControlTest,
     RoundTripsVersionThreeCancellationAndCorrelatedResponse) {
  const DistributedVectorGroupedAggregateShuffleJobCancel expected{
      .query_id = uuid(1U), .coordinator_node_id = 9U, .target_node_id = 7U};
  auto encoded = encode_distributed_vector_grouped_aggregate_shuffle_job_cancel_v3(expected);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  EXPECT_EQ(encoded->bytes().size(),
            distributed_vector_grouped_aggregate_shuffle_job_control_v3_format::kFrameLength);
  auto decoded = decode_distributed_vector_grouped_aggregate_shuffle_job_control_request_v3_exact(
      encoded->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  const auto* actual = std::get_if<DistributedVectorGroupedAggregateShuffleJobCancel>(&*decoded);
  ASSERT_NE(actual, nullptr);
  EXPECT_EQ(*actual, expected);

  const DistributedVectorGroupedAggregateShuffleJobControlResponse response{
      .action = DistributedVectorGroupedAggregateShuffleJobControlAction::kCancel,
      .status_code = common::StatusCode::kOk,
      .query_id = uuid(1U),
      .coordinator_node_id = 9U,
      .target_node_id = 7U};
  auto encoded_response =
      encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v3(response);
  ASSERT_TRUE(encoded_response.has_value()) << encoded_response.error().to_string();
  auto decoded_response =
      decode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v3_exact(
          encoded_response->bytes());
  ASSERT_TRUE(decoded_response.has_value()) << decoded_response.error().to_string();
  EXPECT_EQ(*decoded_response, response);

  std::vector<std::byte> damaged(encoded->bytes().begin(), encoded->bytes().end());
  damaged[80U] ^= std::byte{1U};
  EXPECT_EQ(
      decode_distributed_vector_grouped_aggregate_shuffle_job_control_request_v3_exact(damaged)
          .error()
          .code(),
      common::StatusCode::kCorruption);
}

} // namespace
} // namespace chronos::cluster
