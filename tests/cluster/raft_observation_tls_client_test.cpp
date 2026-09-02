#include "chronos/cluster/raft_observation_tls_client.hpp"

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

[[nodiscard]] network::TlsServerConfig server_config() {
  return {.certificate_chain_file = fixture("server.pem").string(),
          .private_key_file = fixture("server-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string()};
}

[[nodiscard]] network::TlsClientConfig client_config() {
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
  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest& request) override {
    saw_fingerprint = request.peer_certificate_sha256.has_value();
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = 91U};
  }
  bool saw_fingerprint{};
};

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal,
                                      const raft::NodeId node) const override {
    return allow && principal == 91U && node == 2U;
  }
  bool allow{true};
};

[[nodiscard]] RaftObservationTlsClientConfig config(Authenticator& authenticator,
                                                    Authorizer& authorizer) {
  return {.authenticator = &authenticator,
          .node_authorizer = &authorizer,
          .peer_ipv4_address = {127U, 0U, 0U, 1U},
          .request = {1U, 2U, group(), 19U},
          .limits = {.handshake_timeout = std::chrono::milliseconds{100},
                     .exchange_timeout = std::chrono::milliseconds{100}}};
}

TEST(RaftObservationTlsClientTest, AuthenticatesAndAcquiresExactCorrelatedObservation) {
  auto server_context = network::TlsServerContext::create(server_config());
  auto client_context = network::TlsClientContext::create(client_config());
  ASSERT_TRUE(server_context.has_value());
  ASSERT_TRUE(client_context.has_value());
  SocketPair pair = sockets();
  auto server = network::TlsSocket::accept(*server_context, pair.sockets[0]);
  auto client_socket = network::TlsSocket::connect(*client_context, pair.sockets[1]);
  ASSERT_TRUE(server.has_value());
  ASSERT_TRUE(client_socket.has_value());
  Authenticator authenticator;
  Authorizer authorizer;
  const auto start = RaftObservationTlsClient::TimePoint{};
  auto client = RaftObservationTlsClient::create(std::move(*client_socket),
                                                 config(authenticator, authorizer), start);
  ASSERT_TRUE(client.has_value()) << client.error().to_string();

  RaftObservationRequestReader request_reader;
  std::array<std::byte, 256U> scratch{};
  std::optional<RaftObservationFrameWriteCursor> response;
  for (std::size_t iteration = 0U; iteration < 1024U; ++iteration) {
    ASSERT_TRUE(client->on_ready(true, true, start + std::chrono::milliseconds{1}).is_ok())
        << client->failure().to_string();
    if (!server->handshake_complete()) {
      auto progress = server->handshake();
      ASSERT_TRUE(progress.has_value()) << progress.error().to_string();
    } else if (!response.has_value()) {
      auto progress = server->read(scratch);
      ASSERT_TRUE(progress.has_value()) << progress.error().to_string();
      if (progress->state == network::TlsIoState::kComplete) {
        auto step =
            request_reader.consume(common::ByteView{scratch}.first(progress->bytes_transferred));
        ASSERT_TRUE(step.has_value()) << step.error().to_string();
        if (step->request.has_value()) {
          auto encoded =
              encode_raft_observation_response_v1({.source_node_id = 2U,
                                                   .target_node_id = 1U,
                                                   .group_id = group(),
                                                   .correlation_id = 19U,
                                                   .status_code = common::StatusCode::kOk,
                                                   .observation = observation()});
          ASSERT_TRUE(encoded.has_value());
          response.emplace(RaftObservationFrameWriteCursor::create(std::move(*encoded)).value());
        }
      }
    } else if (!response->complete()) {
      auto progress = server->write(response->pending_write());
      ASSERT_TRUE(progress.has_value()) << progress.error().to_string();
      if (progress->state == network::TlsIoState::kComplete) {
        ASSERT_TRUE(response->consume_written(progress->bytes_transferred).is_ok());
      }
    }
    if (client->state() == RaftObservationTlsClientState::kComplete) {
      break;
    }
  }
  ASSERT_EQ(client->state(), RaftObservationTlsClientState::kComplete);
  EXPECT_TRUE(authenticator.saw_fingerprint);
  EXPECT_FALSE(client->interest().want_read);
  EXPECT_FALSE(client->interest().want_write);
  auto acquired = client->result();
  ASSERT_TRUE(acquired.has_value()) << acquired.error().to_string();
  EXPECT_EQ(*acquired, observation());
}

TEST(RaftObservationTlsClientTest, DeadlineIsExactAndSticky) {
  Authenticator authenticator;
  Authorizer authorizer;
  auto value = config(authenticator, authorizer);
  value.limits.handshake_timeout = std::chrono::milliseconds{5};
  const auto start = RaftObservationTlsClient::TimePoint{};
  auto client = RaftObservationTlsClient::create(network::TlsSocket{}, value, start);
  ASSERT_TRUE(client.has_value());
  EXPECT_EQ(client->result().error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(client->on_ready(false, false, start + std::chrono::milliseconds{4}).is_ok());
  const auto failure = client->on_ready(false, false, start + std::chrono::milliseconds{5});
  EXPECT_EQ(failure.code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(client->state(), RaftObservationTlsClientState::kFailed);
  EXPECT_FALSE(client->interest().want_read);
  EXPECT_FALSE(client->interest().want_write);
  EXPECT_EQ(client->result().error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(client->on_ready(true, true, start + std::chrono::milliseconds{6}), failure);
}

TEST(RaftObservationTlsClientTest, RejectsServerPrincipalBeforeRequestWrite) {
  auto server_context = network::TlsServerContext::create(server_config());
  auto client_context = network::TlsClientContext::create(client_config());
  ASSERT_TRUE(server_context.has_value());
  ASSERT_TRUE(client_context.has_value());
  SocketPair pair = sockets();
  auto server = network::TlsSocket::accept(*server_context, pair.sockets[0]);
  auto socket = network::TlsSocket::connect(*client_context, pair.sockets[1]);
  ASSERT_TRUE(server.has_value());
  ASSERT_TRUE(socket.has_value());
  Authenticator authenticator;
  Authorizer authorizer;
  authorizer.allow = false;
  const auto start = RaftObservationTlsClient::TimePoint{};
  auto client = RaftObservationTlsClient::create(std::move(*socket),
                                                 config(authenticator, authorizer), start);
  ASSERT_TRUE(client.has_value());
  common::Status progress = common::Status::ok();
  for (std::size_t iteration = 0U; iteration < 1024U && progress.is_ok(); ++iteration) {
    progress = client->on_ready(true, true, start + std::chrono::milliseconds{1});
    if (!server->handshake_complete()) {
      auto handshake = server->handshake();
      ASSERT_TRUE(handshake.has_value()) << handshake.error().to_string();
    }
  }
  EXPECT_EQ(progress.code(), common::StatusCode::kUnauthenticated);
  EXPECT_EQ(client->state(), RaftObservationTlsClientState::kFailed);
  EXPECT_TRUE(authenticator.saw_fingerprint);
  EXPECT_EQ(server->pending_plaintext_bytes(), 0U);
  EXPECT_EQ(client->result().error().code(), common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::cluster
