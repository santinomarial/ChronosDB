#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_tls.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

struct SocketPair {
  std::array<int, 2> sockets{-1, -1};
  SocketPair() = default;
  SocketPair(const SocketPair&) = delete;
  SocketPair& operator=(const SocketPair&) = delete;
  SocketPair(SocketPair&& other) noexcept : sockets(other.sockets) {
    other.sockets = {-1, -1};
  }
  ~SocketPair() {
    for (const int socket : sockets) {
      if (socket >= 0)
        ::close(socket);
    }
  }
};

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
[[nodiscard]] SocketPair socket_pair() {
  SocketPair pair;
  EXPECT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, pair.sockets.data()), 0);
  for (const int socket : pair.sockets) {
    const int flags = ::fcntl(socket, F_GETFL, 0);
    EXPECT_GE(flags, 0);
    EXPECT_EQ(::fcntl(socket, F_SETFL, flags | O_NONBLOCK), 0);
  }
  return pair;
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
                                (allow_server && principal == 92U && node == 3U)};
  }
  bool allow_server{true};
};

struct Carriers {
  network::TlsServerContext server_context;
  network::TlsClientContext client_context;
  SocketPair sockets;
  DistributedVectorGroupedAggregateShuffleTlsServer server;
  DistributedVectorGroupedAggregateShuffleTlsClient client;
};

struct CarrierAuthenticators {
  Authenticator* client{};
  Authenticator* server{};
};

[[nodiscard]] common::Result<Carriers>
create_carriers(DistributedVectorGroupedAggregateShuffleAuthority& expected, Authorizer& authorizer,
                const CarrierAuthenticators authenticators,
                const query::QueryResourceContext& resources,
                const DistributedVectorGroupedAggregateShuffleTlsLimits limits) {
  auto encoded = messages();
  auto sender = DistributedVectorGroupedAggregateShuffleStreamSender::create(
      expected, edge(), encoded, resources, limits.stream);
  if (!sender.has_value())
    return common::make_unexpected(sender.error());
  auto server_context = network::TlsServerContext::create(server_tls());
  if (!server_context.has_value())
    return common::make_unexpected(server_context.error());
  auto client_context = network::TlsClientContext::create(client_tls());
  if (!client_context.has_value())
    return common::make_unexpected(client_context.error());
  SocketPair sockets = socket_pair();
  auto server_socket = network::TlsSocket::accept(*server_context, sockets.sockets[0]);
  if (!server_socket.has_value())
    return common::make_unexpected(server_socket.error());
  auto client_socket = network::TlsSocket::connect(*client_context, sockets.sockets[1]);
  if (!client_socket.has_value())
    return common::make_unexpected(client_socket.error());
  auto server = DistributedVectorGroupedAggregateShuffleTlsServer::create(
      std::move(*server_socket), resources,
      {.authenticator = authenticators.client,
       .node_authorizer = &authorizer,
       .authority = &expected,
       .local_node_id = 3U,
       .peer_ipv4_address = {127U, 0U, 0U, 1U},
       .limits = limits},
      {});
  if (!server.has_value())
    return common::make_unexpected(server.error());
  auto client = DistributedVectorGroupedAggregateShuffleTlsClient::create(
      std::move(*client_socket), std::move(*sender), expected,
      {.authenticator = authenticators.server,
       .node_authorizer = &authorizer,
       .peer_ipv4_address = {127U, 0U, 0U, 1U},
       .limits = limits},
      {});
  if (!client.has_value())
    return common::make_unexpected(client.error());
  return Carriers{std::move(*server_context), std::move(*client_context), std::move(sockets),
                  std::move(*server), std::move(*client)};
}

TEST(DistributedVectorGroupedAggregateShuffleTlsTest,
     AuthenticatesCarriesCompleteStreamAndRequiresExactAcknowledgment) {
  auto expected = authority();
  Authorizer authorizer;
  Authenticator client_authenticator{91U};
  Authenticator server_authenticator{92U};
  const DistributedVectorGroupedAggregateShuffleTlsLimits limits{
      .handshake_timeout = std::chrono::milliseconds{100},
      .exchange_timeout = std::chrono::milliseconds{100},
      .stream = {.maximum_frames = 2U, .maximum_encoded_bytes = 1U << 20U}};
  auto resources = query::QueryResourceContext::create(4U << 20U).value();
  auto carriers = create_carriers(
      expected, authorizer, {.client = &client_authenticator, .server = &server_authenticator},
      resources, limits);
  ASSERT_TRUE(carriers.has_value()) << carriers.error().to_string();
  EXPECT_FALSE(carriers->server.take_complete_stream().has_value());
  const auto now =
      DistributedVectorGroupedAggregateShuffleTlsClient::TimePoint{} + std::chrono::milliseconds{1};
  for (std::size_t iteration = 0U; iteration < 4096U; ++iteration) {
    ASSERT_TRUE(carriers->client.on_ready(true, true, now).is_ok())
        << carriers->client.failure().to_string();
    ASSERT_TRUE(carriers->server.on_ready(true, true, now).is_ok())
        << carriers->server.failure().to_string();
    if (carriers->client.state() == DistributedVectorGroupedAggregateShuffleTlsState::kComplete &&
        carriers->server.state() == DistributedVectorGroupedAggregateShuffleTlsState::kComplete)
      break;
  }
  ASSERT_EQ(carriers->client.state(), DistributedVectorGroupedAggregateShuffleTlsState::kComplete);
  ASSERT_EQ(carriers->server.state(), DistributedVectorGroupedAggregateShuffleTlsState::kComplete);
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
  EXPECT_TRUE(server_authenticator.saw_fingerprint);
  auto stream = carriers->server.take_complete_stream();
  ASSERT_TRUE(stream.has_value()) << stream.error().to_string();
  EXPECT_EQ(stream->messages.size(), 2U);
  EXPECT_EQ(stream->edge.partition_id, 0U);
  EXPECT_FALSE(carriers->server.take_complete_stream().has_value());
}

TEST(DistributedVectorGroupedAggregateShuffleTlsTest,
     RejectsServerPrincipalBeforeStreamWriteAndExpiresExactly) {
  auto expected = authority();
  auto resources = query::QueryResourceContext::create(4U << 20U).value();
  Authenticator client_authenticator{91U};
  Authenticator server_authenticator{92U};
  Authorizer authorizer;
  authorizer.allow_server = false;
  const DistributedVectorGroupedAggregateShuffleTlsLimits limits{
      .handshake_timeout = std::chrono::milliseconds{100},
      .exchange_timeout = std::chrono::milliseconds{100}};
  auto denied = create_carriers(expected, authorizer,
                                {.client = &client_authenticator, .server = &server_authenticator},
                                resources, limits);
  ASSERT_TRUE(denied.has_value()) << denied.error().to_string();
  const auto progress =
      DistributedVectorGroupedAggregateShuffleTlsClient::TimePoint{} + std::chrono::milliseconds{1};
  for (std::size_t iteration = 0U; iteration < 4096U; ++iteration) {
    const common::Status client_status = denied->client.on_ready(true, true, progress);
    if (denied->client.state() == DistributedVectorGroupedAggregateShuffleTlsState::kFailed) {
      EXPECT_EQ(client_status.code(), common::StatusCode::kUnauthenticated);
      break;
    }
    ASSERT_TRUE(client_status.is_ok());
    ASSERT_TRUE(denied->server.on_ready(true, true, progress).is_ok());
  }
  ASSERT_EQ(denied->client.state(), DistributedVectorGroupedAggregateShuffleTlsState::kFailed);
  EXPECT_FALSE(denied->server.take_complete_stream().has_value());

  auto encoded = messages();
  auto sender = DistributedVectorGroupedAggregateShuffleStreamSender::create(expected, edge(),
                                                                             encoded, resources)
                    .value();
  Authenticator authenticator{92U};
  Authorizer deadline_authorizer;
  DistributedVectorGroupedAggregateShuffleTlsClientConfig config{
      .authenticator = &authenticator,
      .node_authorizer = &deadline_authorizer,
      .limits = {.handshake_timeout = std::chrono::milliseconds{5},
                 .exchange_timeout = std::chrono::milliseconds{5}}};
  auto client = DistributedVectorGroupedAggregateShuffleTlsClient::create(
      network::TlsSocket{}, std::move(sender), expected, config, {});
  ASSERT_TRUE(client.has_value());
  const auto before =
      DistributedVectorGroupedAggregateShuffleTlsClient::TimePoint{} + std::chrono::milliseconds{4};
  EXPECT_TRUE(client->on_ready(false, false, before).is_ok());
  const auto at =
      DistributedVectorGroupedAggregateShuffleTlsClient::TimePoint{} + std::chrono::milliseconds{5};
  EXPECT_EQ(client->on_ready(false, false, at).code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(client->state(), DistributedVectorGroupedAggregateShuffleTlsState::kFailed);
}

} // namespace
} // namespace chronos::cluster
