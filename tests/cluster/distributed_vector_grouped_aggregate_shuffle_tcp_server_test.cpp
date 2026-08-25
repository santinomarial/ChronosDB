#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_tcp_client.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_tcp_server.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <poll.h>
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

[[nodiscard]] DistributedVectorGroupedAggregateShuffleAttempt
attempt(const DistributedVectorGroupedAggregateShuffleAuthority& expected,
        const query::QueryResourceContext& resources) {
  auto state = query::MergeableVectorAggregateState::create(aggregates().front()).value();
  EXPECT_TRUE(state.accumulate_count_star().has_value());
  std::vector<query::ScalarValue> values;
  values.push_back(query::ScalarValue::text(string_type(), "east").value());
  std::vector<query::MergeableVectorAggregateState> states;
  states.push_back(std::move(state));
  query::DistributedVectorGroupedAggregateExchangeMessage message{{.query_id = uuid(1U),
                                                                   .tablet_id = tablet(),
                                                                   .sequence = 1U,
                                                                   .group_ordinal = 0U,
                                                                   .group_count = 1U,
                                                                   .terminal = true,
                                                                   .empty = false},
                                                                  std::move(values),
                                                                  std::move(states)};
  std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> encoded;
  encoded.push_back(query::encode_distributed_vector_grouped_aggregate_exchange_message(
                        message, keys(), aggregates())
                        .value());
  const DistributedVectorGroupedAggregateShuffleEdge edge{.tablet_id = tablet(),
                                                          .partition_id = 0U,
                                                          .source_node_id = 2U,
                                                          .target_node_id = 3U,
                                                          .hash_version = expected.hash_version()};
  auto stream = DistributedVectorGroupedAggregateShuffleStreamSender::create(expected, edge,
                                                                             encoded, resources)
                    .value();
  return {.attempt_number = 1U, .target_node_id = 3U, .stream = std::move(stream)};
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleTlsLimits limits() {
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
    return common::Result<bool>{(principal == 91U && node == 2U) ||
                                (principal == 92U && node == 3U)};
  }
};

[[nodiscard]] DistributedVectorGroupedAggregateShuffleTcpServerConfig
server_config(DistributedVectorGroupedAggregateShuffleAuthority& expected,
              query::QueryResourceContext resources, Authenticator& authenticator,
              Authorizer& authorizer) {
  return {.listener = {},
          .tls = server_tls(),
          .authenticator = &authenticator,
          .node_authorizer = &authorizer,
          .authority = &expected,
          .local_node_id = 3U,
          .resources = std::move(resources),
          .carrier_limits = limits(),
          .maximum_retained_streams = 8U,
          .maximum_accepts_per_poll = 2U};
}

TEST(DistributedVectorGroupedAggregateShuffleTcpServerTest,
     ReservesCompletionBeforeAdmissionAndPublishesAcknowledgedStream) {
  auto expected = authority();
  auto resources = query::QueryResourceContext::create(4U << 20U).value();
  Authenticator client_authenticator{91U};
  Authenticator server_authenticator{92U};
  Authorizer authorizer;
  auto server = DistributedVectorGroupedAggregateShuffleTcpServer::start(
      server_config(expected, resources, client_authenticator, authorizer));
  auto client_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(server.has_value()) << server.error().to_string();
  ASSERT_TRUE(client_context.has_value());
  auto client = DistributedVectorGroupedAggregateShuffleTcpClient::begin(
      attempt(expected, resources), expected,
      {.remote_endpoint = server->bound_endpoint(),
       .tls_context = &*client_context,
       .carrier = {.authenticator = &server_authenticator,
                   .node_authorizer = &authorizer,
                   .peer_ipv4_address = {127U, 0U, 0U, 1U},
                   .limits = limits()},
       .connect_timeout = std::chrono::milliseconds{1000}},
      DistributedVectorGroupedAggregateShuffleTcpClient::TimePoint::clock::now());
  ASSERT_TRUE(client.has_value()) << client.error().to_string();

  for (std::size_t iteration = 0U; iteration < 4096U; ++iteration) {
    const auto interest = client->interest();
    pollfd descriptor{.fd = client->descriptor(),
                      .events = static_cast<short>((interest.want_read ? POLLIN : 0) |
                                                   (interest.want_write ? POLLOUT : 0))};
    ASSERT_GE(::poll(&descriptor, 1U, 1), 0);
    ASSERT_TRUE(
        client
            ->on_ready((descriptor.revents & POLLIN) != 0, (descriptor.revents & POLLOUT) != 0,
                       DistributedVectorGroupedAggregateShuffleTcpClient::TimePoint::clock::now())
            .is_ok())
        << client->failure().to_string();
    ASSERT_TRUE(server->poll_once(std::chrono::milliseconds{1}).is_ok());
    if (client->state() == DistributedVectorGroupedAggregateShuffleTcpClientState::kComplete &&
        server->metrics().retained_streams == 1U) {
      break;
    }
  }

  ASSERT_EQ(client->state(), DistributedVectorGroupedAggregateShuffleTcpClientState::kComplete);
  auto metrics = server->metrics();
  EXPECT_EQ(metrics.accepted_connections, 1U);
  EXPECT_EQ(metrics.completed_connections, 1U);
  EXPECT_EQ(metrics.failed_connections, 0U);
  EXPECT_EQ(metrics.active_connections, 0U);
  EXPECT_EQ(metrics.retained_streams, 1U);
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
  EXPECT_TRUE(server_authenticator.saw_fingerprint);
  auto stream = server->take_next_complete_stream();
  ASSERT_TRUE(stream.has_value()) << stream.error().to_string();
  ASSERT_EQ(stream->messages.size(), 1U);
  EXPECT_EQ(stream->edge.source_node_id, 2U);
  EXPECT_EQ(server->metrics().retained_streams, 0U);
  EXPECT_FALSE(server->take_next_complete_stream().has_value());
  EXPECT_TRUE(server->shutdown().is_ok());
  EXPECT_TRUE(server->shutdown().is_ok());
  EXPECT_FALSE(server->is_running());
}

TEST(DistributedVectorGroupedAggregateShuffleTcpServerTest,
     BoundsAdmissionAndRejectsInvalidConfiguration) {
  auto expected = authority();
  auto resources = query::QueryResourceContext::create(4U << 20U).value();
  Authenticator authenticator{91U};
  Authorizer authorizer;
  auto invalid_config = server_config(expected, resources, authenticator, authorizer);
  invalid_config.maximum_retained_streams = 0U;
  EXPECT_EQ(DistributedVectorGroupedAggregateShuffleTcpServer::start(std::move(invalid_config))
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  auto config = server_config(expected, resources, authenticator, authorizer);
  config.maximum_retained_streams = 1U;
  auto server = DistributedVectorGroupedAggregateShuffleTcpServer::start(std::move(config));
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
        pollfd descriptor{.fd = socket->descriptor(), .events = POLLOUT};
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
