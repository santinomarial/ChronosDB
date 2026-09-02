#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_tcp_client.hpp"

#include <array>
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

[[nodiscard]] DistributedVectorGroupedAggregateShuffleEdge edge() {
  return {.tablet_id = tablet(),
          .partition_id = 0U,
          .source_node_id = 2U,
          .target_node_id = 3U,
          .hash_version = kDistributedVectorGroupedAggregateShuffleHashVersionV1};
}

[[nodiscard]] std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage>
messages() {
  std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> result;
  for (std::size_t ordinal = 0U; ordinal < 2U; ++ordinal) {
    auto state = query::MergeableVectorAggregateState::create(aggregates().front()).value();
    EXPECT_TRUE(state.accumulate_count_star().has_value());
    std::vector<query::ScalarValue> values;
    values.push_back(
        query::ScalarValue::text(string_type(), ordinal == 0U ? "east" : "west").value());
    std::vector<query::MergeableVectorAggregateState> states;
    states.push_back(std::move(state));
    query::DistributedVectorGroupedAggregateExchangeMessage message{
        {.query_id = uuid(1U),
         .tablet_id = tablet(),
         .sequence = ordinal + 1U,
         .group_ordinal = static_cast<std::uint32_t>(ordinal),
         .group_count = 2U,
         .terminal = ordinal == 1U,
         .empty = false},
        std::move(values),
        std::move(states)};
    result.push_back(query::encode_distributed_vector_grouped_aggregate_exchange_message(
                         message, keys(), aggregates())
                         .value());
  }
  return result;
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleAttempt
attempt(const DistributedVectorGroupedAggregateShuffleAuthority& expected,
        const query::QueryResourceContext& resources) {
  auto stream = DistributedVectorGroupedAggregateShuffleStreamSender::create(expected, edge(),
                                                                             messages(), resources)
                    .value();
  return {.attempt_number = 1U, .target_node_id = 3U, .stream = std::move(stream)};
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleTlsLimits limits() {
  return {.handshake_timeout = std::chrono::milliseconds{1000},
          .exchange_timeout = std::chrono::milliseconds{1000},
          .stream = {.maximum_frames = 2U, .maximum_encoded_bytes = 1U << 20U}};
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

TEST(DistributedVectorGroupedAggregateShuffleTcpClientTest,
     OwnsConnectAndCompletesOnlyAfterAuthenticatedReceipt) {
  auto expected = authority();
  auto resources = query::QueryResourceContext::create(4U << 20U).value();
  auto listener = network::TcpListener::bind();
  auto server_context = network::TlsServerContext::create(server_tls());
  auto client_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(listener.has_value());
  ASSERT_TRUE(server_context.has_value());
  ASSERT_TRUE(client_context.has_value());
  Authenticator client_authenticator{91U};
  Authenticator server_authenticator{92U};
  Authorizer authorizer;
  const auto start = DistributedVectorGroupedAggregateShuffleTcpClient::TimePoint::clock::now();
  auto client = DistributedVectorGroupedAggregateShuffleTcpClient::begin(
      attempt(expected, resources), expected,
      {.remote_endpoint = listener->bound_endpoint(),
       .tls_context = &*client_context,
       .carrier = {.authenticator = &server_authenticator,
                   .node_authorizer = &authorizer,
                   .peer_ipv4_address = {127U, 0U, 0U, 1U},
                   .limits = limits()},
       .connect_timeout = std::chrono::milliseconds{1000}},
      start);
  ASSERT_TRUE(client.has_value()) << client.error().to_string();
  EXPECT_EQ(client->attempt_number(), 1U);
  EXPECT_EQ(client->target_node_id(), 3U);

  std::optional<network::TcpSocket> accepted_socket;
  std::optional<DistributedVectorGroupedAggregateShuffleTlsServer> server;
  for (std::size_t iteration = 0U; iteration < 4096U; ++iteration) {
    if (!accepted_socket.has_value()) {
      auto accepted = listener->accept_one();
      ASSERT_TRUE(accepted.has_value());
      auto accepted_connection = std::move(accepted).value();
      if (accepted_connection.has_value()) {
        accepted_socket.emplace(std::move(accepted_connection).value());
        auto tls = network::TlsSocket::accept(*server_context, accepted_socket->descriptor());
        ASSERT_TRUE(tls.has_value());
        const auto peer_endpoint = accepted_socket->peer_endpoint();
        ASSERT_TRUE(peer_endpoint.has_value());
        auto carrier = DistributedVectorGroupedAggregateShuffleTlsServer::create(
            std::move(*tls), resources,
            {.authenticator = &client_authenticator,
             .node_authorizer = &authorizer,
             .authority = &expected,
             .local_node_id = 3U,
             .peer_ipv4_address = peer_endpoint->address,
             .limits = limits()},
            DistributedVectorGroupedAggregateShuffleTlsServer::TimePoint::clock::now());
        ASSERT_TRUE(carrier.has_value()) << carrier.error().to_string();
        server.emplace(std::move(*carrier));
      }
    }

    std::array<pollfd, 2U> descriptors{};
    std::size_t count{};
    const auto client_interest = client->interest();
    descriptors[count++] = {.fd = client->descriptor(),
                            .events =
                                static_cast<short>((client_interest.want_read ? POLLIN : 0) |
                                                   (client_interest.want_write ? POLLOUT : 0)),
                            .revents = 0};
    if (server.has_value()) {
      if (!accepted_socket.has_value()) {
        FAIL() << "TLS shuffle server exists without its accepted TCP socket";
      }
      const auto server_interest = server->interest();
      descriptors[count++] = {.fd = accepted_socket->descriptor(),
                              .events =
                                  static_cast<short>((server_interest.want_read ? POLLIN : 0) |
                                                     (server_interest.want_write ? POLLOUT : 0)),
                              .revents = 0};
    }
    ASSERT_GE(::poll(descriptors.data(), static_cast<nfds_t>(count), 1), 0);
    const auto now = DistributedVectorGroupedAggregateShuffleTcpClient::TimePoint::clock::now();
    ASSERT_TRUE(client
                    ->on_ready((descriptors[0].revents & POLLIN) != 0,
                               (descriptors[0].revents & POLLOUT) != 0, now)
                    .is_ok())
        << client->failure().to_string();
    if (server.has_value()) {
      ASSERT_TRUE(server
                      ->on_ready((descriptors[1].revents & POLLIN) != 0,
                                 (descriptors[1].revents & POLLOUT) != 0, now)
                      .is_ok())
          << server->failure().to_string();
    }
    if (client->state() == DistributedVectorGroupedAggregateShuffleTcpClientState::kComplete &&
        server.has_value() &&
        server->state() == DistributedVectorGroupedAggregateShuffleTlsState::kComplete) {
      break;
    }
  }

  ASSERT_EQ(client->state(), DistributedVectorGroupedAggregateShuffleTcpClientState::kComplete);
  if (!server.has_value()) {
    FAIL() << "completed TCP shuffle client produced no TLS shuffle server";
  }
  auto& completed_server = server.value();
  ASSERT_EQ(completed_server.state(), DistributedVectorGroupedAggregateShuffleTlsState::kComplete);
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
  EXPECT_TRUE(server_authenticator.saw_fingerprint);
  auto complete = completed_server.take_complete_stream();
  ASSERT_TRUE(complete.has_value()) << complete.error().to_string();
  EXPECT_EQ(complete->messages.size(), 2U);
  EXPECT_EQ(complete->edge.target_node_id, 3U);
}

TEST(DistributedVectorGroupedAggregateShuffleTcpClientTest,
     ValidatesRouteBeforeConnectAndExpiresAtExactDeadline) {
  auto expected = authority();
  auto resources = query::QueryResourceContext::create(4U << 20U).value();
  auto listener = network::TcpListener::bind();
  auto client_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(listener.has_value());
  ASSERT_TRUE(client_context.has_value());
  Authenticator authenticator{92U};
  Authorizer authorizer;
  const auto start = DistributedVectorGroupedAggregateShuffleTcpClient::TimePoint{};
  auto config = DistributedVectorGroupedAggregateShuffleTcpClientConfig{
      .remote_endpoint = listener->bound_endpoint(),
      .tls_context = &*client_context,
      .carrier = {.authenticator = &authenticator,
                  .node_authorizer = &authorizer,
                  .peer_ipv4_address = {127U, 0U, 0U, 1U},
                  .limits = limits()},
      .connect_timeout = std::chrono::milliseconds{5}};
  auto client = DistributedVectorGroupedAggregateShuffleTcpClient::begin(
      attempt(expected, resources), expected, config, start);
  ASSERT_TRUE(client.has_value()) << client.error().to_string();
  EXPECT_TRUE(client->on_ready(false, false, start + std::chrono::milliseconds{4}).is_ok());
  const common::Status expired =
      client->on_ready(false, false, start + std::chrono::milliseconds{5});
  EXPECT_EQ(expired.code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(client->state(), DistributedVectorGroupedAggregateShuffleTcpClientState::kFailed);
  EXPECT_EQ(client->descriptor(), -1);
  EXPECT_EQ(client->on_ready(true, true, start + std::chrono::milliseconds{6}), expired);

  config.carrier.peer_ipv4_address = {127U, 0U, 0U, 2U};
  auto invalid = DistributedVectorGroupedAggregateShuffleTcpClient::begin(
      attempt(expected, resources), expected, config, start);
  ASSERT_FALSE(invalid.has_value());
  EXPECT_EQ(invalid.error().code(), common::StatusCode::kInvalidArgument);

  auto wrong_target = attempt(expected, resources);
  wrong_target.target_node_id = 4U;
  config.carrier.peer_ipv4_address = config.remote_endpoint.address;
  auto mismatched = DistributedVectorGroupedAggregateShuffleTcpClient::begin(
      std::move(wrong_target), expected, config, start);
  ASSERT_FALSE(mismatched.has_value());
  EXPECT_EQ(mismatched.error().code(), common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::cluster
