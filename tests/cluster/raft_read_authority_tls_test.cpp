#include "chronos/cluster/raft_read_authority_tls_client.hpp"
#include "chronos/cluster/raft_read_authority_tls_server.hpp"

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

[[nodiscard]] RaftReadAuthority authority() {
  return {
      .barrier = {.group_id = group(), .barrier = {.term = 7U, .context = 9U, .read_index = 11U}},
      .observation = {.group_id = group(),
                      .node_id = 2U,
                      .role = raft::Role::kLeader,
                      .current_term = 7U,
                      .leader_id = 2U,
                      .last_log_index = 12U,
                      .commit_index = 11U,
                      .applied_index = 10U,
                      .voters = {1U, 2U, 3U},
                      .committed_voters = {1U, 2U, 3U}}};
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

class Service final : public RaftReadAuthorityService {
public:
  common::Result<RaftReadAuthority> acquire(const raft::GroupId& requested) override {
    ++calls;
    EXPECT_EQ(requested, group());
    return authority();
  }

  std::size_t calls{};
};

[[nodiscard]] RaftReadAuthorityTlsClientConfig client_config(Authenticator& authenticator,
                                                             Authorizer& authorizer) {
  return {.authenticator = &authenticator,
          .node_authorizer = &authorizer,
          .peer_ipv4_address = {127U, 0U, 0U, 1U},
          .request = {1U, 2U, group(), 19U},
          .limits = {.handshake_timeout = std::chrono::milliseconds{100},
                     .exchange_timeout = std::chrono::milliseconds{100}}};
}

[[nodiscard]] RaftReadAuthorityTlsServerConfig server_config(Authenticator& authenticator,
                                                             RaftReadAuthorityReceiver& receiver) {
  return {.authenticator = &authenticator,
          .receiver = &receiver,
          .peer_ipv4_address = {127U, 0U, 0U, 1U},
          .limits = {.handshake_timeout = std::chrono::milliseconds{100},
                     .exchange_timeout = std::chrono::milliseconds{100}}};
}

TEST(RaftReadAuthorityTlsTest, ServesOneAuthenticatedAuthorityExactlyOnce) {
  Authorizer authorizer;
  Service service;
  auto receiver = RaftReadAuthorityReceiver::create(
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
  const auto start = RaftReadAuthorityTlsClient::TimePoint{};
  auto server = RaftReadAuthorityTlsServer::create(
      std::move(*server_socket), server_config(client_authenticator, *receiver), start);
  auto client = RaftReadAuthorityTlsClient::create(
      std::move(*client_socket), client_config(server_authenticator, authorizer), start);
  ASSERT_TRUE(server.has_value()) << server.error().to_string();
  ASSERT_TRUE(client.has_value()) << client.error().to_string();

  for (std::size_t iteration = 0U; iteration < 1024U; ++iteration) {
    ASSERT_TRUE(client->on_ready(true, true, start + std::chrono::milliseconds{1}).is_ok())
        << client->failure().to_string();
    ASSERT_TRUE(server->on_ready(true, true, start + std::chrono::milliseconds{1}).is_ok())
        << server->failure().to_string();
    if (client->state() == RaftReadAuthorityTlsClientState::kComplete &&
        server->state() == RaftReadAuthorityTlsServerState::kComplete) {
      break;
    }
  }

  ASSERT_EQ(client->state(), RaftReadAuthorityTlsClientState::kComplete);
  ASSERT_EQ(server->state(), RaftReadAuthorityTlsServerState::kComplete);
  EXPECT_EQ(service.calls, 1U);
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
  EXPECT_TRUE(server_authenticator.saw_fingerprint);
  auto acquired = client->result();
  ASSERT_TRUE(acquired.has_value()) << acquired.error().to_string();
  EXPECT_EQ(*acquired, authority());
}

TEST(RaftReadAuthorityTlsTest, RejectsClientPrincipalBeforeAuthorityService) {
  Authorizer authorizer;
  Service service;
  auto receiver = RaftReadAuthorityReceiver::create(
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
  const auto start = RaftReadAuthorityTlsClient::TimePoint{};
  auto server = RaftReadAuthorityTlsServer::create(
      std::move(*server_socket), server_config(client_authenticator, *receiver), start);
  auto client = RaftReadAuthorityTlsClient::create(
      std::move(*client_socket), client_config(server_authenticator, authorizer), start);
  ASSERT_TRUE(server.has_value());
  ASSERT_TRUE(client.has_value());

  common::Status progress = common::Status::ok();
  for (std::size_t iteration = 0U; iteration < 1024U && progress.is_ok(); ++iteration) {
    (void)client->on_ready(true, true, start + std::chrono::milliseconds{1});
    progress = server->on_ready(true, true, start + std::chrono::milliseconds{1});
  }
  EXPECT_EQ(progress.code(), common::StatusCode::kUnauthenticated);
  EXPECT_EQ(server->state(), RaftReadAuthorityTlsServerState::kFailed);
  EXPECT_EQ(service.calls, 0U);
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
}

TEST(RaftReadAuthorityTlsTest, ConfigurationAndDeadlinesFailClosed) {
  Authorizer authorizer;
  Service service;
  auto receiver = RaftReadAuthorityReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .service = &service});
  ASSERT_TRUE(receiver.has_value());
  Authenticator client_authenticator{91U};
  Authenticator server_authenticator{92U};
  auto client_value = client_config(server_authenticator, authorizer);
  client_value.limits.handshake_timeout = std::chrono::milliseconds{5};
  const auto start = RaftReadAuthorityTlsClient::TimePoint{};
  auto client = RaftReadAuthorityTlsClient::create(network::TlsSocket{}, client_value, start);
  ASSERT_TRUE(client.has_value());
  EXPECT_TRUE(client->on_ready(false, false, start + std::chrono::milliseconds{4}).is_ok());
  const common::Status client_failure =
      client->on_ready(false, false, start + std::chrono::milliseconds{5});
  EXPECT_EQ(client_failure.code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(client->state(), RaftReadAuthorityTlsClientState::kFailed);
  EXPECT_EQ(client->on_ready(true, true, start + std::chrono::milliseconds{6}), client_failure);
  EXPECT_EQ(client->result().error().code(), common::StatusCode::kInvalidArgument);

  auto server_value = server_config(client_authenticator, *receiver);
  server_value.limits.handshake_timeout = std::chrono::milliseconds{0};
  EXPECT_EQ(
      RaftReadAuthorityTlsServer::create(network::TlsSocket{}, server_value, start).error().code(),
      common::StatusCode::kInvalidArgument);
  server_value.limits.handshake_timeout = std::chrono::milliseconds{5};
  auto server = RaftReadAuthorityTlsServer::create(network::TlsSocket{}, server_value, start);
  ASSERT_TRUE(server.has_value());
  const common::Status server_failure =
      server->on_ready(false, false, start + std::chrono::milliseconds{5});
  EXPECT_EQ(server_failure.code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(server->state(), RaftReadAuthorityTlsServerState::kFailed);
  EXPECT_EQ(server->on_ready(true, true, start + std::chrono::milliseconds{6}), server_failure);
}

} // namespace
} // namespace chronos::cluster
