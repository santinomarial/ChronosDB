#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_tcp_execution.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_tcp_server.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] std::filesystem::path fixture(const char* name) {
  return std::filesystem::path{CHRONOS_NETWORK_FIXTURE_DIR} / "tls" / name;
}

[[nodiscard]] network::TlsServerConfig server_tls() {
  return {.certificate_chain_file = fixture("server.pem").string(),
          .private_key_file = fixture("server-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string()};
}

[[nodiscard]] network::TlsClientConfig client_tls() {
  return {.certificate_chain_file = fixture("client.pem").string(),
          .private_key_file = fixture("client-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string(),
          .expected_server_identity = "127.0.0.1"};
}

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

[[nodiscard]] schema::LogicalType string_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
}

[[nodiscard]] schema::LogicalType i64_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleAuthority authority() {
  const auto tablet = schema::TabletId::from_uuid(uuid(2U)).value();
  return DistributedVectorGroupedAggregateShuffleAuthority::create(
             uuid(1U), {{tablet, 2U}}, {{0U, 3U}}, {{0U, string_type(), false}},
             {{query::VectorAggregateOperation::kCountStar, std::nullopt}})
      .value();
}

[[nodiscard]] query::DistributedVectorResultSchema result_schema() {
  return {.columns = {{"region", string_type(), false}, {"count", i64_type(), false}}};
}

[[nodiscard]] std::vector<std::byte> batch() {
  const std::string value{"scheduler-result"};
  const std::array<std::byte, sizeof(std::int64_t)> count{std::byte{1U}};
  const std::array columns{network::QueryResultColumn{"region", string_type(), false},
                           network::QueryResultColumn{"count", i64_type(), false}};
  const std::array cells{network::QueryResultCell{.value = std::as_bytes(std::span{value})},
                         network::QueryResultCell{.value = count}};
  return network::encode_query_result_batch(1U, columns, cells).value();
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleResultTlsLimits carrier_limits() {
  return {.handshake_timeout = std::chrono::milliseconds{1000},
          .exchange_timeout = std::chrono::milliseconds{1000},
          .stream = {.maximum_frames = 1U, .maximum_encoded_bytes = 1U << 20U}};
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleResultRetry
retry(const DistributedVectorGroupedAggregateShuffleAuthority& expected,
      const query::DistributedVectorResultSchema& schema) {
  std::vector<std::vector<std::byte>> batches;
  batches.push_back(batch());
  return DistributedVectorGroupedAggregateShuffleResultRetry::create(
             expected, schema,
             {.partition_id = 0U, .source_node_id = 3U, .coordinator_node_id = 9U},
             std::move(batches),
             {.retry = {.maximum_attempts = 2U,
                        .initial_backoff = std::chrono::milliseconds{1},
                        .maximum_backoff = std::chrono::milliseconds{1}},
              .stream = carrier_limits().stream})
      .value();
}

class Authenticator final : public network::ConnectionAuthenticator {
public:
  explicit Authenticator(const std::uint64_t principal) : principal_(principal) {}

  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest& request) override {
    saw_fingerprint = request.peer_certificate_sha256.has_value();
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = principal_};
  }

  bool saw_fingerprint{};

private:
  std::uint64_t principal_{};
};

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal,
                                      const raft::NodeId node) const override {
    return common::Result<bool>{(principal == 91U && node == 3U) ||
                                (principal == 92U && node == 9U)};
  }
};

TEST(DistributedVectorGroupedAggregateShuffleResultTcpExecutionTest,
     RotatesCoordinatorAddressesAndCompletesOnlyAfterReceipt) {
  auto expected = authority();
  auto schema = result_schema();
  Authenticator reducer_authenticator{91U};
  Authenticator coordinator_authenticator{92U};
  Authorizer authorizer;
  auto server = DistributedVectorGroupedAggregateShuffleResultTcpServer::start(
      {.listener = {},
       .tls = server_tls(),
       .authenticator = &reducer_authenticator,
       .node_authorizer = &authorizer,
       .authority = &expected,
       .result_schema = &schema,
       .coordinator_node_id = 9U,
       .carrier_limits = carrier_limits(),
       .maximum_retained_streams = 2U,
       .maximum_accepts_per_poll = 1U});
  auto client_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(server.has_value()) << server.error().to_string();
  ASSERT_TRUE(client_context.has_value());

  network::Ipv4Endpoint refused;
  {
    auto temporary = network::TcpListener::bind();
    ASSERT_TRUE(temporary.has_value());
    refused = temporary->bound_endpoint();
  }
  std::vector<DistributedVectorGroupedAggregateShuffleResultRetry> retries;
  retries.push_back(retry(expected, schema));
  auto execution = DistributedVectorGroupedAggregateShuffleResultTcpExecution::create(
      expected, schema, std::move(retries),
      {.authenticator = &coordinator_authenticator,
       .node_authorizer = &authorizer,
       .routes = {{.node_id = 9U,
                   .endpoints = {refused, server->bound_endpoint()},
                   .tls_context = &*client_context}},
       .carrier_limits = carrier_limits(),
       .connect_timeout = std::chrono::milliseconds{1000},
       .execution_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5}});
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();

  for (std::size_t iteration = 0U; iteration < 4096U; ++iteration) {
    const common::Status driven = execution->poll_once(std::chrono::milliseconds{1});
    ASSERT_TRUE(driven.is_ok()) << driven.to_string();
    ASSERT_TRUE(server->poll_once(std::chrono::milliseconds{1}).is_ok());
    if (execution->state() ==
            DistributedVectorGroupedAggregateShuffleResultTcpExecutionState::kComplete &&
        server->metrics().retained_streams == 1U) {
      break;
    }
  }

  ASSERT_EQ(execution->state(),
            DistributedVectorGroupedAggregateShuffleResultTcpExecutionState::kComplete);
  const auto metrics = execution->metrics();
  EXPECT_EQ(metrics.attempts_started, 2U);
  EXPECT_EQ(metrics.retries_started, 1U);
  EXPECT_EQ(metrics.transport_failed_attempts, 1U);
  EXPECT_EQ(metrics.transport_completed_attempts, 1U);
  EXPECT_EQ(metrics.active_attempts, 0U);
  EXPECT_EQ(metrics.succeeded_partitions, 1U);
  EXPECT_EQ(metrics.total_partitions, 1U);
  EXPECT_TRUE(reducer_authenticator.saw_fingerprint);
  EXPECT_TRUE(coordinator_authenticator.saw_fingerprint);
  auto stream = server->take_next_complete_stream();
  ASSERT_TRUE(stream.has_value()) << stream.error().to_string();
  EXPECT_EQ(stream->partition_id, 0U);
  EXPECT_EQ(stream->source_node_id, 3U);
  EXPECT_EQ(stream->target_node_id, 9U);
  EXPECT_TRUE(server->shutdown().is_ok());
}

TEST(DistributedVectorGroupedAggregateShuffleResultTcpExecutionTest,
     RejectsDifferentProofObjectsAndCancelsBeforeAttempt) {
  auto expected = authority();
  auto schema = result_schema();
  auto client_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(client_context.has_value());
  Authenticator authenticator{92U};
  Authorizer authorizer;
  const DistributedVectorGroupedAggregateShuffleResultTcpExecutionConfig config{
      .authenticator = &authenticator,
      .node_authorizer = &authorizer,
      .routes = {{.node_id = 9U,
                  .endpoints = {{{127U, 0U, 0U, 1U}, 9U}},
                  .tls_context = &*client_context}},
      .carrier_limits = carrier_limits(),
      .connect_timeout = std::chrono::milliseconds{1000}};
  EXPECT_EQ(DistributedVectorGroupedAggregateShuffleResultTcpExecution::create(expected, schema, {},
                                                                               config)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  auto copied_schema = result_schema();
  std::vector<DistributedVectorGroupedAggregateShuffleResultRetry> mismatched;
  mismatched.push_back(retry(expected, schema));
  EXPECT_EQ(DistributedVectorGroupedAggregateShuffleResultTcpExecution::create(
                expected, copied_schema, std::move(mismatched), config)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  auto copied_authority = authority();
  mismatched.clear();
  mismatched.push_back(retry(expected, schema));
  EXPECT_EQ(DistributedVectorGroupedAggregateShuffleResultTcpExecution::create(
                copied_authority, schema, std::move(mismatched), config)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  std::vector<DistributedVectorGroupedAggregateShuffleResultRetry> retries;
  retries.push_back(retry(expected, schema));
  auto execution = DistributedVectorGroupedAggregateShuffleResultTcpExecution::create(
      expected, schema, std::move(retries), config);
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();
  EXPECT_EQ(execution->cancel().code(), common::StatusCode::kCancelled);
  EXPECT_EQ(execution->state(),
            DistributedVectorGroupedAggregateShuffleResultTcpExecutionState::kCancelled);
  EXPECT_EQ(execution->metrics().attempts_started, 0U);
}

} // namespace
} // namespace chronos::cluster
