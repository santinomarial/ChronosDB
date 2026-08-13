#include "chronos/cluster/raft_observation_tls_client.hpp"
#include "chronos/cluster/raft_observation_tls_server.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <filesystem>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>

namespace chronos::cluster {
namespace {

struct SocketPair {
  std::array<int, 2U> sockets{-1, -1};
  ~SocketPair() {
    for (const int socket : sockets)
      if (socket >= 0)
        ::close(socket);
  }
};

[[nodiscard]] std::filesystem::path fixture(const char* name) {
  return std::filesystem::path{CHRONOS_NETWORK_FIXTURE_DIR} / "tls" / name;
}

[[nodiscard]] network::TlsServerConfig server_tls_config() {
  return {.certificate_chain_file = fixture("server.pem").string(),
          .private_key_file = fixture("server-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string()};
}

[[nodiscard]] network::TlsClientConfig client_tls_config() {
  return {.certificate_chain_file = fixture("client.pem").string(),
          .private_key_file = fixture("client-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string(),
          .expected_server_identity = "127.0.0.1"};
}

[[nodiscard]] SocketPair sockets() {
  SocketPair pair;
  EXPECT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, pair.sockets.data()), 0);
  for (const int descriptor : pair.sockets) {
    const int flags = ::fcntl(descriptor, F_GETFL, 0);
    EXPECT_GE(flags, 0);
    EXPECT_EQ(::fcntl(descriptor, F_SETFL, flags | O_NONBLOCK), 0);
  }
  return pair;
}

[[nodiscard]] raft::GroupId group() {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{7U};
  return raft::GroupId{bytes};
}

[[nodiscard]] raft::RaftGroupObservation observation() {
  return {.group_id = group(),
          .node_id = 2U,
          .role = raft::Role::kFollower,
          .current_term = 5U,
          .leader_id = 1U,
          .last_log_index = 12U,
          .commit_index = 11U,
          .applied_index = 10U,
          .voters = {1U, 2U, 3U},
          .committed_voters = {1U, 2U, 3U}};
}

class Authenticator final : public network::ConnectionAuthenticator {
public:
  explicit Authenticator(const std::uint64_t principal) : principal_(principal) {}
  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest& request) override {
    saw_fingerprint = request.peer_certificate_sha256.has_value();
    return network::PeerAuthenticationResult{.authorized = allow,
                                             .principal_id = allow ? principal_ : 0U};
  }
  bool allow{true};
  bool saw_fingerprint{};

private:
  std::uint64_t principal_{};
};

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal,
                                      const raft::NodeId node) const override {
    return (principal == 91U && node == 1U) || (principal == 92U && node == 2U);
  }
};

class Service final : public RaftObservationService {
public:
  common::Result<raft::RaftGroupObservation> observe(const raft::GroupId& requested) override {
    ++calls;
    EXPECT_EQ(requested, group());
    return observation();
  }
  std::size_t calls{};
};

[[nodiscard]] RaftObservationTlsClientConfig client_config(Authenticator& authenticator,
                                                           Authorizer& authorizer) {
  return {.authenticator = &authenticator,
          .node_authorizer = &authorizer,
          .peer_ipv4_address = {127U, 0U, 0U, 1U},
          .request = {1U, 2U, group(), 19U},
          .limits = {.handshake_timeout = std::chrono::milliseconds{100},
                     .exchange_timeout = std::chrono::milliseconds{100}}};
}

[[nodiscard]] RaftObservationTlsServerConfig server_config(Authenticator& authenticator,
                                                           RaftObservationReceiver& receiver) {
  return {.authenticator = &authenticator,
          .receiver = &receiver,
          .peer_ipv4_address = {127U, 0U, 0U, 1U},
          .limits = {.handshake_timeout = std::chrono::milliseconds{100},
                     .exchange_timeout = std::chrono::milliseconds{100}}};
}

TEST(RaftObservationTlsServerTest, ServesOneAuthenticatedObservationExactlyOnce) {
  Authorizer authorizer;
  Service service;
  auto receiver = RaftObservationReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .service = &service});
  auto server_context = network::TlsServerContext::create(server_tls_config());
  auto client_context = network::TlsClientContext::create(client_tls_config());
  ASSERT_TRUE(receiver.has_value());
  ASSERT_TRUE(server_context.has_value());
  ASSERT_TRUE(client_context.has_value());
  SocketPair pair = sockets();
  auto server_socket = network::TlsSocket::accept(*server_context, pair.sockets[0]);
  auto client_socket = network::TlsSocket::connect(*client_context, pair.sockets[1]);
  ASSERT_TRUE(server_socket.has_value());
  ASSERT_TRUE(client_socket.has_value());
  Authenticator client_authenticator{91U};
  Authenticator server_authenticator{92U};
  const auto start = RaftObservationTlsServer::TimePoint{};
  auto server = RaftObservationTlsServer::create(
      std::move(*server_socket), server_config(client_authenticator, *receiver), start);
  auto client = RaftObservationTlsClient::create(
      std::move(*client_socket), client_config(server_authenticator, authorizer), start);
  ASSERT_TRUE(server.has_value());
  ASSERT_TRUE(client.has_value());
  for (std::size_t iteration = 0U; iteration < 1024U; ++iteration) {
    ASSERT_TRUE(client->on_ready(true, true, start + std::chrono::milliseconds{1}).is_ok())
        << client->failure().to_string();
    ASSERT_TRUE(server->on_ready(true, true, start + std::chrono::milliseconds{1}).is_ok())
        << server->failure().to_string();
    if (client->state() == RaftObservationTlsClientState::kComplete &&
        server->state() == RaftObservationTlsServerState::kComplete)
      break;
  }
  ASSERT_EQ(client->state(), RaftObservationTlsClientState::kComplete);
  ASSERT_EQ(server->state(), RaftObservationTlsServerState::kComplete);
  EXPECT_EQ(service.calls, 1U);
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
  EXPECT_TRUE(server_authenticator.saw_fingerprint);
  auto acquired = client->result();
  ASSERT_TRUE(acquired.has_value()) << acquired.error().to_string();
  EXPECT_EQ(*acquired, observation());
}

TEST(RaftObservationTlsServerTest, RejectsPrincipalBeforeObservationService) {
  Authorizer authorizer;
  Service service;
  auto receiver = RaftObservationReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .service = &service});
  auto server_context = network::TlsServerContext::create(server_tls_config());
  auto client_context = network::TlsClientContext::create(client_tls_config());
  ASSERT_TRUE(receiver.has_value());
  ASSERT_TRUE(server_context.has_value());
  ASSERT_TRUE(client_context.has_value());
  SocketPair pair = sockets();
  auto server_socket = network::TlsSocket::accept(*server_context, pair.sockets[0]);
  auto client_socket = network::TlsSocket::connect(*client_context, pair.sockets[1]);
  ASSERT_TRUE(server_socket.has_value());
  ASSERT_TRUE(client_socket.has_value());
  Authenticator client_authenticator{91U};
  client_authenticator.allow = false;
  Authenticator server_authenticator{92U};
  const auto start = RaftObservationTlsServer::TimePoint{};
  auto server = RaftObservationTlsServer::create(
      std::move(*server_socket), server_config(client_authenticator, *receiver), start);
  auto client = RaftObservationTlsClient::create(
      std::move(*client_socket), client_config(server_authenticator, authorizer), start);
  ASSERT_TRUE(server.has_value());
  ASSERT_TRUE(client.has_value());
  common::Status progress = common::Status::ok();
  for (std::size_t iteration = 0U; iteration < 1024U && progress.is_ok(); ++iteration) {
    (void)client->on_ready(true, true, start + std::chrono::milliseconds{1});
    progress = server->on_ready(true, true, start + std::chrono::milliseconds{1});
  }
  EXPECT_EQ(progress.code(), common::StatusCode::kUnauthenticated);
  EXPECT_EQ(service.calls, 0U);
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
}

TEST(RaftObservationTlsServerTest, ConfigurationAndDeadlineFailClosed) {
  Authorizer authorizer;
  Service service;
  auto receiver = RaftObservationReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .service = &service});
  ASSERT_TRUE(receiver.has_value());
  Authenticator authenticator{91U};
  auto config = server_config(authenticator, *receiver);
  config.limits.handshake_timeout = std::chrono::milliseconds{0};
  EXPECT_EQ(RaftObservationTlsServer::create(network::TlsSocket{}, config, {}).error().code(),
            common::StatusCode::kInvalidArgument);
  config.limits.handshake_timeout = std::chrono::milliseconds{5};
  auto server = RaftObservationTlsServer::create(network::TlsSocket{}, config, {});
  ASSERT_TRUE(server.has_value());
  const auto start = RaftObservationTlsServer::TimePoint{};
  EXPECT_TRUE(server->on_ready(false, false, start + std::chrono::milliseconds{4}).is_ok());
  const auto failure = server->on_ready(false, false, start + std::chrono::milliseconds{5});
  EXPECT_EQ(failure.code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(server->state(), RaftObservationTlsServerState::kFailed);
  EXPECT_EQ(server->on_ready(true, true, start + std::chrono::milliseconds{6}), failure);
}

} // namespace
} // namespace chronos::cluster
