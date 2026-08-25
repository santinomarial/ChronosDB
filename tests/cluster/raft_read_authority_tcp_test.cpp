#include "chronos/cluster/raft_read_authority_tcp_client.hpp"
#include "chronos/cluster/raft_read_authority_tcp_server.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <poll.h>

namespace chronos::cluster {
namespace {

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

[[nodiscard]] RaftReadAuthorityTcpServerConfig server_config(Authenticator& authenticator,
                                                             RaftReadAuthorityReceiver& receiver) {
  return {.listener = {},
          .tls = server_tls_config(),
          .authenticator = &authenticator,
          .receiver = &receiver,
          .session_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                             .exchange_timeout = std::chrono::milliseconds{1000}},
          .maximum_connections = 8U,
          .maximum_accepts_per_poll = 8U};
}

[[nodiscard]] RaftReadAuthorityTcpClientConfig
client_config(const network::Ipv4Endpoint endpoint, const network::TlsClientContext& context,
              Authenticator& authenticator, Authorizer& authorizer) {
  return {.remote_endpoint = endpoint,
          .tls_context = &context,
          .carrier = {.authenticator = &authenticator,
                      .node_authorizer = &authorizer,
                      .peer_ipv4_address = endpoint.address,
                      .request = {1U, 2U, group(), 19U},
                      .limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                                 .exchange_timeout = std::chrono::milliseconds{1000}}},
          .connect_timeout = std::chrono::milliseconds{1000}};
}

TEST(RaftReadAuthorityTcpTest, ServesOneRealTcpMutualTlsAuthority) {
  Authorizer authorizer;
  Service service;
  auto receiver = RaftReadAuthorityReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .service = &service});
  ASSERT_TRUE(receiver.has_value());
  Authenticator client_authenticator{91U};
  auto server = RaftReadAuthorityTcpServer::start(server_config(client_authenticator, *receiver));
  ASSERT_TRUE(server.has_value()) << server.error().to_string();
  auto tls_context = network::TlsClientContext::create(client_tls_config());
  ASSERT_TRUE(tls_context.has_value());
  Authenticator server_authenticator{92U};
  const auto start = RaftReadAuthorityTcpClient::TimePoint::clock::now();
  auto client = RaftReadAuthorityTcpClient::begin(
      client_config(server->bound_endpoint(), *tls_context, server_authenticator, authorizer),
      start);
  ASSERT_TRUE(client.has_value()) << client.error().to_string();

  for (std::size_t iteration = 0U; iteration < 2048U; ++iteration) {
    const auto interest = client->interest();
    pollfd descriptor{.fd = client->descriptor(),
                      .events = static_cast<short>((interest.want_read ? POLLIN : 0) |
                                                   (interest.want_write ? POLLOUT : 0))};
    ASSERT_GE(::poll(&descriptor, 1U, 1), 0);
    ASSERT_TRUE(client
                    ->on_ready((descriptor.revents & POLLIN) != 0,
                               (descriptor.revents & POLLOUT) != 0,
                               RaftReadAuthorityTcpClient::TimePoint::clock::now())
                    .is_ok())
        << client->failure().to_string();
    ASSERT_TRUE(server->poll_once(std::chrono::milliseconds{1}).is_ok());
    if (client->state() == RaftReadAuthorityTcpClientState::kComplete)
      break;
  }

  ASSERT_EQ(client->state(), RaftReadAuthorityTcpClientState::kComplete);
  auto acquired = client->result();
  ASSERT_TRUE(acquired.has_value()) << acquired.error().to_string();
  EXPECT_EQ(*acquired, authority());
  EXPECT_EQ(service.calls, 1U);
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
  EXPECT_TRUE(server_authenticator.saw_fingerprint);
  const auto metrics = server->metrics();
  EXPECT_EQ(metrics.accepted_connections, 1U);
  EXPECT_EQ(metrics.completed_connections, 1U);
  EXPECT_EQ(metrics.active_connections, 0U);
  EXPECT_TRUE(server->shutdown().is_ok());
  EXPECT_FALSE(server->is_running());
}

TEST(RaftReadAuthorityTcpTest, RejectsRouteMismatchExpiresAndBoundsAdmission) {
  Authorizer authorizer;
  Service service;
  auto receiver = RaftReadAuthorityReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .service = &service});
  ASSERT_TRUE(receiver.has_value());
  Authenticator client_authenticator{91U};
  auto config = server_config(client_authenticator, *receiver);
  config.maximum_connections = 1U;
  auto server = RaftReadAuthorityTcpServer::start(std::move(config));
  ASSERT_TRUE(server.has_value()) << server.error().to_string();
  auto tls_context = network::TlsClientContext::create(client_tls_config());
  ASSERT_TRUE(tls_context.has_value());
  Authenticator server_authenticator{92U};

  auto invalid =
      client_config(server->bound_endpoint(), *tls_context, server_authenticator, authorizer);
  invalid.carrier.peer_ipv4_address = {127U, 0U, 0U, 2U};
  EXPECT_EQ(RaftReadAuthorityTcpClient::begin(invalid, {}).error().code(),
            common::StatusCode::kInvalidArgument);

  auto expiring =
      client_config(server->bound_endpoint(), *tls_context, server_authenticator, authorizer);
  expiring.connect_timeout = std::chrono::milliseconds{5};
  const auto start = RaftReadAuthorityTcpClient::TimePoint{};
  auto client = RaftReadAuthorityTcpClient::begin(expiring, start);
  ASSERT_TRUE(client.has_value()) << client.error().to_string();
  EXPECT_TRUE(client->on_ready(false, false, start + std::chrono::milliseconds{4}).is_ok());
  const auto failure = client->on_ready(false, false, start + std::chrono::milliseconds{5});
  EXPECT_EQ(failure.code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(client->descriptor(), -1);
  EXPECT_EQ(client->on_ready(true, true, start + std::chrono::milliseconds{6}), failure);

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
          auto connected = socket->finish_connect();
          ASSERT_TRUE(connected.has_value()) << connected.error().to_string();
        }
      }
    }
  }
  EXPECT_EQ(server->metrics().accepted_connections, 1U);
  EXPECT_EQ(server->metrics().rejected_connections, 1U);
  EXPECT_EQ(server->metrics().active_connections, 1U);
  EXPECT_TRUE(server->shutdown().is_ok());
  EXPECT_EQ(server->metrics().active_connections, 0U);
}

} // namespace
} // namespace chronos::cluster
