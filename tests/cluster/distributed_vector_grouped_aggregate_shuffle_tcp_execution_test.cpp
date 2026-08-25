#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_reducer.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_tcp_execution.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_tcp_server.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
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

[[nodiscard]] schema::TabletId tablet() {
  return schema::TabletId::from_uuid(uuid(2U)).value();
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

[[nodiscard]] DistributedVectorGroupedAggregateShuffleAuthority authority() {
  return DistributedVectorGroupedAggregateShuffleAuthority::create(
             uuid(1U), {{.tablet_id = tablet(), .node_id = 2U}},
             {{.partition_id = 0U, .node_id = 3U}}, keys(), aggregates())
      .value();
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleTlsLimits carrier_limits() {
  return {.handshake_timeout = std::chrono::milliseconds{1000},
          .exchange_timeout = std::chrono::milliseconds{1000},
          .stream = {.maximum_frames = 1U, .maximum_encoded_bytes = 1U << 20U}};
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleRetry
retry(const DistributedVectorGroupedAggregateShuffleAuthority& expected,
      const query::QueryResourceContext& resources) {
  auto state = query::MergeableVectorAggregateState::create(aggregates().front()).value();
  EXPECT_TRUE(state.accumulate_count_star().has_value());
  std::vector<query::ScalarValue> values;
  values.push_back(query::ScalarValue::text(string_type(), "remote-key").value());
  std::vector<query::MergeableVectorAggregateState> states;
  states.push_back(std::move(state));
  std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> messages;
  messages.push_back(query::encode_distributed_vector_grouped_aggregate_exchange_message(
                         {.query_id = uuid(1U),
                          .tablet_id = tablet(),
                          .sequence = 1U,
                          .group_ordinal = 0U,
                          .group_count = 1U,
                          .terminal = true,
                          .empty = false},
                         values, states, keys(), aggregates())
                         .value());
  return DistributedVectorGroupedAggregateShuffleRetry::create(
             expected,
             {.tablet_id = tablet(),
              .partition_id = 0U,
              .source_node_id = 2U,
              .target_node_id = 3U,
              .hash_version = expected.hash_version()},
             std::move(messages), resources,
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
    return common::Result<bool>{(principal == 91U && node == 2U) ||
                                (principal == 92U && node == 3U)};
  }
};

TEST(DistributedVectorGroupedAggregateShuffleTcpExecutionTest,
     RotatesAddressAndCompletesEveryRemoteEdgeAfterAuthenticatedReceipt) {
  auto expected = authority();
  auto resources = query::QueryResourceContext::create(8U << 20U).value();
  Authenticator client_authenticator{91U};
  Authenticator server_authenticator{92U};
  Authorizer authorizer;
  auto server = DistributedVectorGroupedAggregateShuffleTcpServer::start(
      {.listener = {},
       .tls = server_tls(),
       .authenticator = &client_authenticator,
       .node_authorizer = &authorizer,
       .authority = &expected,
       .local_node_id = 3U,
       .resources = resources,
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
  std::vector<DistributedVectorGroupedAggregateShuffleRetry> retries;
  retries.push_back(retry(expected, resources));
  auto execution = DistributedVectorGroupedAggregateShuffleTcpExecution::create(
      expected, std::move(retries),
      {.authenticator = &server_authenticator,
       .node_authorizer = &authorizer,
       .routes = {{.node_id = 3U,
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
            DistributedVectorGroupedAggregateShuffleTcpExecutionState::kComplete &&
        server->metrics().retained_streams == 1U) {
      break;
    }
  }

  ASSERT_EQ(execution->state(),
            DistributedVectorGroupedAggregateShuffleTcpExecutionState::kComplete);
  const auto metrics = execution->metrics();
  EXPECT_EQ(metrics.attempts_started, 2U);
  EXPECT_EQ(metrics.retries_started, 1U);
  EXPECT_EQ(metrics.transport_failed_attempts, 1U);
  EXPECT_EQ(metrics.transport_completed_attempts, 1U);
  EXPECT_EQ(metrics.active_attempts, 0U);
  EXPECT_EQ(metrics.succeeded_edges, 1U);
  EXPECT_EQ(metrics.total_edges, 1U);
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
  EXPECT_TRUE(server_authenticator.saw_fingerprint);

  auto stream = server->take_next_complete_stream();
  ASSERT_TRUE(stream.has_value()) << stream.error().to_string();
  auto reducer = DistributedVectorGroupedAggregateShuffleReducer::create(expected, 0U, 3U).value();
  EXPECT_TRUE(reducer.accept_stream(*stream).is_ok());
  EXPECT_TRUE(reducer.finish().is_ok());
  EXPECT_TRUE(reducer.next().has_value());
  EXPECT_TRUE(server->shutdown().is_ok());
}

TEST(DistributedVectorGroupedAggregateShuffleTcpExecutionTest,
     RejectsIncompleteAuthorityAndCancelsBeforeOpeningAnAttempt) {
  auto expected = authority();
  auto resources = query::QueryResourceContext::create(4U << 20U).value();
  auto client_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(client_context.has_value());
  Authenticator authenticator{92U};
  Authorizer authorizer;
  const DistributedVectorGroupedAggregateShuffleTcpExecutionConfig config{
      .authenticator = &authenticator,
      .node_authorizer = &authorizer,
      .routes = {{.node_id = 3U,
                  .endpoints = {{{127U, 0U, 0U, 1U}, 9U}},
                  .tls_context = &*client_context}},
      .carrier_limits = carrier_limits(),
      .connect_timeout = std::chrono::milliseconds{1000}};
  EXPECT_EQ(DistributedVectorGroupedAggregateShuffleTcpExecution::create(expected, {}, config)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  std::vector<DistributedVectorGroupedAggregateShuffleRetry> retries;
  retries.push_back(retry(expected, resources));
  auto execution = DistributedVectorGroupedAggregateShuffleTcpExecution::create(
      expected, std::move(retries), config);
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();
  EXPECT_EQ(execution->cancel().code(), common::StatusCode::kCancelled);
  EXPECT_EQ(execution->state(),
            DistributedVectorGroupedAggregateShuffleTcpExecutionState::kCancelled);
  EXPECT_EQ(execution->metrics().attempts_started, 0U);
  EXPECT_EQ(execution->metrics().active_attempts, 0U);
}

} // namespace
} // namespace chronos::cluster
