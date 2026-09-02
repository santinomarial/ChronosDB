#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_tcp_client.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_tcp_server.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <poll.h>
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

[[nodiscard]] DistributedVectorGroupedAggregateShuffleAuthority authority() {
  const auto tablet = schema::TabletId::from_uuid(uuid(2U)).value();
  return DistributedVectorGroupedAggregateShuffleAuthority::create(
             uuid(1U), {{tablet, 2U}}, {{0U, 3U}}, {{0U, string_type(), false}},
             {{query::VectorAggregateOperation::kCountStar, std::nullopt}})
      .value();
}

[[nodiscard]] query::DistributedVectorResultSchema result_schema() {
  return {.columns = {{"region", string_type(), false}}};
}

[[nodiscard]] std::vector<std::byte> batch(const std::string& value) {
  const std::array columns{network::QueryResultColumn{"region", string_type(), false}};
  const std::array cells{network::QueryResultCell{.value = std::as_bytes(std::span{value})}};
  return network::encode_query_result_batch(1U, columns, cells).value();
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleResultAttempt
attempt(const DistributedVectorGroupedAggregateShuffleAuthority& expected,
        const query::DistributedVectorResultSchema& schema) {
  const std::vector<std::vector<std::byte>> batches{batch("east")};
  auto stream = DistributedVectorGroupedAggregateShuffleResultStreamSender::create(
                    expected, schema, 0U, 3U, 9U, batches)
                    .value();
  return {.attempt_number = 1U, .target_node_id = 9U, .stream = std::move(stream)};
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleResultTlsLimits limits() {
  return {.handshake_timeout = std::chrono::milliseconds{1000},
          .exchange_timeout = std::chrono::milliseconds{1000},
          .stream = {.maximum_frames = 1U, .maximum_encoded_bytes = 1U << 20U}};
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

[[nodiscard]] DistributedVectorGroupedAggregateShuffleResultTcpServerConfig
server_config(DistributedVectorGroupedAggregateShuffleAuthority& expected,
              query::DistributedVectorResultSchema& schema, Authenticator& authenticator,
              Authorizer& authorizer) {
  return {.listener = {},
          .tls = server_tls(),
          .authenticator = &authenticator,
          .node_authorizer = &authorizer,
          .authority = &expected,
          .result_schema = &schema,
          .coordinator_node_id = 9U,
          .carrier_limits = limits(),
          .maximum_retained_streams = 8U,
          .maximum_accepts_per_poll = 2U};
}

TEST(DistributedVectorGroupedAggregateShuffleResultTcpServerTest,
     ReservesCompletionBeforeAdmissionAndPublishesAcknowledgedResult) {
  auto expected = authority();
  auto schema = result_schema();
  Authenticator reducer_authenticator{91U};
  Authenticator coordinator_authenticator{92U};
  Authorizer authorizer;
  auto server = DistributedVectorGroupedAggregateShuffleResultTcpServer::start(
      server_config(expected, schema, reducer_authenticator, authorizer));
  auto client_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(server.has_value()) << server.error().to_string();
  ASSERT_TRUE(client_context.has_value());
  auto client = DistributedVectorGroupedAggregateShuffleResultTcpClient::begin(
      attempt(expected, schema), expected, schema,
      {.remote_endpoint = server->bound_endpoint(),
       .tls_context = &*client_context,
       .carrier = {.authenticator = &coordinator_authenticator,
                   .node_authorizer = &authorizer,
                   .peer_ipv4_address = {127U, 0U, 0U, 1U},
                   .limits = limits()},
       .connect_timeout = std::chrono::milliseconds{1000}},
      DistributedVectorGroupedAggregateShuffleResultTcpClient::TimePoint::clock::now());
  ASSERT_TRUE(client.has_value()) << client.error().to_string();

  for (std::size_t iteration = 0U; iteration < 4096U; ++iteration) {
    const auto interest = client->interest();
    pollfd descriptor{.fd = client->descriptor(),
                      .events = static_cast<short>((interest.want_read ? POLLIN : 0) |
                                                   (interest.want_write ? POLLOUT : 0)),
                      .revents = 0};
    ASSERT_GE(::poll(&descriptor, 1U, 1), 0);
    ASSERT_TRUE(
        client
            ->on_ready(
                (descriptor.revents & POLLIN) != 0, (descriptor.revents & POLLOUT) != 0,
                DistributedVectorGroupedAggregateShuffleResultTcpClient::TimePoint::clock::now())
            .is_ok())
        << client->failure().to_string();
    ASSERT_TRUE(server->poll_once(std::chrono::milliseconds{1}).is_ok());
    if (client->state() ==
            DistributedVectorGroupedAggregateShuffleResultTcpClientState::kComplete &&
        server->metrics().retained_streams == 1U) {
      break;
    }
  }

  ASSERT_EQ(client->state(),
            DistributedVectorGroupedAggregateShuffleResultTcpClientState::kComplete);
  const auto metrics = server->metrics();
  EXPECT_EQ(metrics.accepted_connections, 1U);
  EXPECT_EQ(metrics.completed_connections, 1U);
  EXPECT_EQ(metrics.failed_connections, 0U);
  EXPECT_EQ(metrics.active_connections, 0U);
  EXPECT_EQ(metrics.retained_streams, 1U);
  EXPECT_TRUE(reducer_authenticator.saw_fingerprint);
  EXPECT_TRUE(coordinator_authenticator.saw_fingerprint);
  auto stream = server->take_next_complete_stream();
  ASSERT_TRUE(stream.has_value()) << stream.error().to_string();
  EXPECT_EQ(stream->partition_id, 0U);
  EXPECT_EQ(stream->source_node_id, 3U);
  EXPECT_EQ(stream->target_node_id, 9U);
  ASSERT_EQ(stream->encoded_result_batches.size(), 1U);
  EXPECT_EQ(server->metrics().retained_streams, 0U);
  EXPECT_FALSE(server->take_next_complete_stream().has_value());
  EXPECT_TRUE(server->shutdown().is_ok());
  EXPECT_TRUE(server->shutdown().is_ok());
  EXPECT_FALSE(server->is_running());
}

TEST(DistributedVectorGroupedAggregateShuffleResultTcpServerTest,
     BoundsAdmissionAndRejectsInvalidConfiguration) {
  auto expected = authority();
  auto schema = result_schema();
  Authenticator authenticator{91U};
  Authorizer authorizer;
  auto invalid_config = server_config(expected, schema, authenticator, authorizer);
  invalid_config.maximum_retained_streams = 0U;
  EXPECT_EQ(
      DistributedVectorGroupedAggregateShuffleResultTcpServer::start(std::move(invalid_config))
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);

  auto config = server_config(expected, schema, authenticator, authorizer);
  config.maximum_retained_streams = 1U;
  auto server = DistributedVectorGroupedAggregateShuffleResultTcpServer::start(std::move(config));
  ASSERT_TRUE(server.has_value()) << server.error().to_string();
  EXPECT_EQ(server->poll_once(std::chrono::milliseconds{-1}).code(),
            common::StatusCode::kInvalidArgument);
  auto first = network::TcpSocket::begin_connect(server->bound_endpoint());
  auto second = network::TcpSocket::begin_connect(server->bound_endpoint());
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  for (std::size_t iteration = 0U; iteration < 64U && server->metrics().rejected_connections == 0U;
       ++iteration) {
    ASSERT_TRUE(server->poll_once(std::chrono::milliseconds{1}).is_ok());
    for (network::TcpSocket* socket : {&*first, &*second}) {
      if (socket->valid() && socket->connect_state() == network::TcpConnectState::kInProgress) {
        pollfd descriptor{.fd = socket->descriptor(), .events = POLLOUT, .revents = 0};
        if (::poll(&descriptor, 1U, 0) > 0) {
          const auto connected = socket->finish_connect();
          ASSERT_TRUE(connected.has_value());
        }
      }
    }
  }
  const auto metrics = server->metrics();
  EXPECT_EQ(metrics.accepted_connections, 1U);
  EXPECT_EQ(metrics.rejected_connections, 1U);
  EXPECT_EQ(metrics.active_connections, 1U);
  EXPECT_EQ(metrics.retained_streams, 0U);
  EXPECT_TRUE(server->shutdown().is_ok());
  EXPECT_EQ(server->metrics().active_connections, 0U);
}

} // namespace
} // namespace chronos::cluster
