#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_tls.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <span>
#include <string>
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

[[nodiscard]] query::DistributedVectorResultSchema schema_value() {
  return {.columns = {{"region", string_type(), false}}};
}

[[nodiscard]] std::vector<std::byte> batch(const std::string& value) {
  const std::array columns{network::QueryResultColumn{"region", string_type(), false}};
  const std::array cells{network::QueryResultCell{.value = std::as_bytes(std::span{value})}};
  return network::encode_query_result_batch(1U, columns, cells).value();
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
                                (allow_coordinator && principal == 92U && node == 9U)};
  }
  bool allow_coordinator{true};
};

struct Carriers {
  network::TlsServerContext server_context;
  network::TlsClientContext client_context;
  SocketPair sockets;
  DistributedVectorGroupedAggregateShuffleResultTlsServer server;
  DistributedVectorGroupedAggregateShuffleResultTlsClient client;
};

struct CarrierAuthenticators {
  Authenticator* client{};
  Authenticator* server{};
};

[[nodiscard]] common::Result<Carriers>
create_carriers(DistributedVectorGroupedAggregateShuffleAuthority& expected,
                query::DistributedVectorResultSchema& schema, Authorizer& authorizer,
                const CarrierAuthenticators authenticators,
                const DistributedVectorGroupedAggregateShuffleResultTlsLimits limits) {
  const std::vector<std::vector<std::byte>> batches{batch("east"), batch("west")};
  auto sender = DistributedVectorGroupedAggregateShuffleResultStreamSender::create(
      expected, schema, 0U, 3U, 9U, batches, limits.stream);
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
  auto server = DistributedVectorGroupedAggregateShuffleResultTlsServer::create(
      std::move(*server_socket),
      {.authenticator = authenticators.client,
       .node_authorizer = &authorizer,
       .authority = &expected,
       .result_schema = &schema,
       .coordinator_node_id = 9U,
       .peer_ipv4_address = {127U, 0U, 0U, 1U},
       .limits = limits},
      {});
  if (!server.has_value())
    return common::make_unexpected(server.error());
  auto client = DistributedVectorGroupedAggregateShuffleResultTlsClient::create(
      std::move(*client_socket), std::move(*sender), expected, schema, 9U,
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

TEST(DistributedVectorGroupedAggregateShuffleResultTlsTest,
     AuthenticatesCarriesCompleteResultAndRequiresExactReceipt) {
  auto expected = authority();
  auto schema = schema_value();
  Authorizer authorizer;
  Authenticator client_authenticator{91U};
  Authenticator server_authenticator{92U};
  const DistributedVectorGroupedAggregateShuffleResultTlsLimits limits{
      .handshake_timeout = std::chrono::milliseconds{100},
      .exchange_timeout = std::chrono::milliseconds{100},
      .stream = {.maximum_frames = 2U, .maximum_encoded_bytes = 1U << 20U}};
  auto carriers =
      create_carriers(expected, schema, authorizer,
                      {.client = &client_authenticator, .server = &server_authenticator}, limits);
  ASSERT_TRUE(carriers.has_value()) << carriers.error().to_string();
  EXPECT_FALSE(carriers->server.take_complete_stream().has_value());
  const auto now = DistributedVectorGroupedAggregateShuffleResultTlsClient::TimePoint{} +
                   std::chrono::milliseconds{1};
  for (std::size_t iteration = 0U; iteration < 4096U; ++iteration) {
    ASSERT_TRUE(carriers->client.on_ready(true, true, now).is_ok())
        << carriers->client.failure().to_string();
    ASSERT_TRUE(carriers->server.on_ready(true, true, now).is_ok())
        << carriers->server.failure().to_string();
    if (carriers->client.state() ==
            DistributedVectorGroupedAggregateShuffleResultTlsState::kComplete &&
        carriers->server.state() ==
            DistributedVectorGroupedAggregateShuffleResultTlsState::kComplete) {
      break;
    }
  }
  ASSERT_EQ(carriers->client.state(),
            DistributedVectorGroupedAggregateShuffleResultTlsState::kComplete);
  ASSERT_EQ(carriers->server.state(),
            DistributedVectorGroupedAggregateShuffleResultTlsState::kComplete);
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
  EXPECT_TRUE(server_authenticator.saw_fingerprint);
  auto stream = carriers->server.take_complete_stream();
  ASSERT_TRUE(stream.has_value()) << stream.error().to_string();
  EXPECT_EQ(stream->partition_id, 0U);
  EXPECT_EQ(stream->frame_count, 2U);
  EXPECT_EQ(stream->encoded_result_batches.size(), 2U);
  EXPECT_FALSE(carriers->server.take_complete_stream().has_value());
}

TEST(DistributedVectorGroupedAggregateShuffleResultTlsTest,
     RejectsCoordinatorPrincipalBeforeWriteAndExpiresExactly) {
  auto expected = authority();
  auto schema = schema_value();
  Authorizer authorizer;
  authorizer.allow_coordinator = false;
  Authenticator client_authenticator{91U};
  Authenticator server_authenticator{92U};
  const DistributedVectorGroupedAggregateShuffleResultTlsLimits limits{
      .handshake_timeout = std::chrono::milliseconds{100},
      .exchange_timeout = std::chrono::milliseconds{100}};
  auto denied =
      create_carriers(expected, schema, authorizer,
                      {.client = &client_authenticator, .server = &server_authenticator}, limits);
  ASSERT_TRUE(denied.has_value()) << denied.error().to_string();
  const auto progress = DistributedVectorGroupedAggregateShuffleResultTlsClient::TimePoint{} +
                        std::chrono::milliseconds{1};
  for (std::size_t iteration = 0U; iteration < 4096U; ++iteration) {
    const common::Status client_status = denied->client.on_ready(true, true, progress);
    if (denied->client.state() == DistributedVectorGroupedAggregateShuffleResultTlsState::kFailed) {
      EXPECT_EQ(client_status.code(), common::StatusCode::kUnauthenticated);
      break;
    }
    ASSERT_TRUE(client_status.is_ok());
    ASSERT_TRUE(denied->server.on_ready(true, true, progress).is_ok());
  }
  EXPECT_EQ(denied->client.state(),
            DistributedVectorGroupedAggregateShuffleResultTlsState::kFailed);
  EXPECT_FALSE(denied->server.take_complete_stream().has_value());

  const std::vector<std::vector<std::byte>> batches{batch("one")};
  auto sender = DistributedVectorGroupedAggregateShuffleResultStreamSender::create(
                    expected, schema, 0U, 3U, 9U, batches)
                    .value();
  Authorizer deadline_authorizer;
  DistributedVectorGroupedAggregateShuffleResultTlsClientConfig config{
      .authenticator = &server_authenticator,
      .node_authorizer = &deadline_authorizer,
      .limits = {.handshake_timeout = std::chrono::milliseconds{5},
                 .exchange_timeout = std::chrono::milliseconds{5}}};
  auto client = DistributedVectorGroupedAggregateShuffleResultTlsClient::create(
      network::TlsSocket{}, std::move(sender), expected, schema, 9U, config, {});
  ASSERT_TRUE(client.has_value());
  const auto before = DistributedVectorGroupedAggregateShuffleResultTlsClient::TimePoint{} +
                      std::chrono::milliseconds{4};
  EXPECT_TRUE(client->on_ready(false, false, before).is_ok());
  const auto at = DistributedVectorGroupedAggregateShuffleResultTlsClient::TimePoint{} +
                  std::chrono::milliseconds{5};
  EXPECT_EQ(client->on_ready(false, false, at).code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(client->state(), DistributedVectorGroupedAggregateShuffleResultTlsState::kFailed);
}

} // namespace
} // namespace chronos::cluster
