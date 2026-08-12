#include "chronos/cluster/raft_transport_tls_client.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <filesystem>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

struct SocketPair {
  std::array<int, 2> sockets{-1, -1};
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

[[nodiscard]] network::TlsServerConfig tls_server_config() {
  return {.certificate_chain_file = fixture("server.pem").string(),
          .private_key_file = fixture("server-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string()};
}

[[nodiscard]] network::TlsClientConfig tls_client_config() {
  return {.certificate_chain_file = fixture("client.pem").string(),
          .private_key_file = fixture("client-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string(),
          .expected_server_identity = "127.0.0.1"};
}

[[nodiscard]] SocketPair nonblocking_socket_pair() {
  SocketPair pair;
  EXPECT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, pair.sockets.data()), 0);
  for (const int socket : pair.sockets) {
    const int flags = ::fcntl(socket, F_GETFL, 0);
    EXPECT_GE(flags, 0);
    EXPECT_EQ(::fcntl(socket, F_SETFL, flags | O_NONBLOCK), 0);
  }
  return pair;
}

[[nodiscard]] raft::GroupId group() {
  common::Uuid::Bytes bytes{};
  bytes.fill(std::byte{9U});
  return raft::GroupId{bytes};
}

[[nodiscard]] std::vector<std::byte> frame(const raft::Term term) {
  return raft::encode_raft_transport_envelope_v1(
             {.group_id = group(),
              .source = 1U,
              .destination = 2U,
              .message = raft::RequestVoteRequest{term, 1U, 0U, 0U}})
      .value();
}

class Authenticator final : public network::ConnectionAuthenticator {
public:
  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest& request) override {
    saw_fingerprint = request.peer_certificate_sha256.has_value();
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = 800U};
  }
  bool saw_fingerprint{};
};

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal_id,
                                      const raft::NodeId node_id) const override {
    return allow && principal_id == 800U && node_id == 2U;
  }
  bool allow{true};
};

[[nodiscard]] RaftTransportTlsClientConfig client_config(Authenticator& authenticator,
                                                         Authorizer& authorizer) {
  return {.local_node_id = 1U,
          .peer_node_id = 2U,
          .authenticator = &authenticator,
          .node_authorizer = &authorizer,
          .peer_ipv4_address = {127U, 0U, 0U, 1U},
          .limits = {.maximum_queued_frames = 2U,
                     .maximum_queued_bytes = 1024U,
                     .handshake_timeout = std::chrono::milliseconds{100},
                     .frame_write_timeout = std::chrono::milliseconds{100}}};
}

TEST(RaftTransportTlsClientTest, AuthenticatesQueuesAndWritesPersistentFrames) {
  auto server_context = network::TlsServerContext::create(tls_server_config());
  auto client_context = network::TlsClientContext::create(tls_client_config());
  ASSERT_TRUE(server_context.has_value());
  ASSERT_TRUE(client_context.has_value());
  SocketPair sockets = nonblocking_socket_pair();
  auto server = network::TlsSocket::accept(*server_context, sockets.sockets[0]);
  auto client_socket = network::TlsSocket::connect(*client_context, sockets.sockets[1]);
  ASSERT_TRUE(server.has_value());
  ASSERT_TRUE(client_socket.has_value());
  Authenticator authenticator;
  Authorizer authorizer;
  const auto now = RaftTransportTlsClient::TimePoint{};
  auto client = RaftTransportTlsClient::create(std::move(*client_socket),
                                               client_config(authenticator, authorizer), now);
  ASSERT_TRUE(client.has_value());
  ASSERT_TRUE(client->next_deadline().has_value());
  EXPECT_EQ(*client->next_deadline(), now + std::chrono::milliseconds{100});
  auto first = frame(1U);
  auto second = frame(2U);
  auto overflow = frame(3U);
  const auto first_copy = first;
  ASSERT_TRUE(client->try_enqueue(first, now).is_ok());
  ASSERT_TRUE(first.empty());
  ASSERT_TRUE(client->try_enqueue(second, now).is_ok());
  EXPECT_EQ(client->try_enqueue(overflow, now).code(), common::StatusCode::kResourceExhausted);
  EXPECT_FALSE(overflow.empty());

  auto reader = raft::RaftTransportFrameReader::create();
  ASSERT_TRUE(reader.has_value());
  std::vector<raft::RaftTransportEnvelope> received;
  std::array<std::byte, 1U> byte{};
  for (std::size_t iteration = 0U; iteration < 8192U && received.size() != 2U; ++iteration) {
    if (!server->handshake_complete()) {
      auto progress = server->handshake();
      ASSERT_TRUE(progress.has_value()) << progress.error().to_string();
    }
    ASSERT_TRUE(client->on_ready(true, true, now + std::chrono::milliseconds{1}).is_ok())
        << client->failure().to_string();
    if (server->handshake_complete()) {
      auto progress = server->read(byte);
      ASSERT_TRUE(progress.has_value()) << progress.error().to_string();
      if (progress->state == network::TlsIoState::kComplete && progress->bytes_transferred == 1U) {
        auto step = reader->consume(byte);
        ASSERT_TRUE(step.has_value()) << step.error().to_string();
        if (step->envelope.has_value())
          received.push_back(std::move(*step->envelope));
      }
    }
  }
  ASSERT_EQ(received.size(), 2U);
  EXPECT_EQ(std::get<raft::RequestVoteRequest>(received[0].message).term, 1U);
  EXPECT_EQ(std::get<raft::RequestVoteRequest>(received[1].message).term, 2U);
  EXPECT_EQ(client->queued_frames(), 0U);
  EXPECT_EQ(client->queued_bytes(), 0U);
  EXPECT_FALSE(client->next_deadline().has_value());
  EXPECT_TRUE(authenticator.saw_fingerprint);
  EXPECT_EQ(first_copy.size(), frame(1U).size());
}

TEST(RaftTransportTlsClientTest, RetainsWholeFramesAcrossFailureForReconnectRetry) {
  Authenticator authenticator;
  Authorizer authorizer;
  auto config = client_config(authenticator, authorizer);
  config.limits.handshake_timeout = std::chrono::milliseconds{5};
  const auto start = RaftTransportTlsClient::TimePoint{};
  auto client = RaftTransportTlsClient::create(network::TlsSocket{}, config, start);
  ASSERT_TRUE(client.has_value());
  ASSERT_TRUE(client->next_deadline().has_value());
  EXPECT_EQ(*client->next_deadline(), start + std::chrono::milliseconds{5});
  auto queued = frame(1U);
  const auto expected = queued;
  ASSERT_TRUE(client->try_enqueue(queued, start).is_ok());
  EXPECT_EQ(client->on_ready(false, false, start + std::chrono::milliseconds{5}).code(),
            common::StatusCode::kUnavailable);
  auto retry = client->drain_retry_frames();
  ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
  ASSERT_EQ(retry->size(), 1U);
  EXPECT_EQ(retry->front(), expected);
  EXPECT_EQ(client->queued_frames(), 0U);
  EXPECT_FALSE(client->next_deadline().has_value());

  auto wrong_route =
      raft::encode_raft_transport_envelope_v1({.group_id = group(),
                                               .source = 3U,
                                               .destination = 2U,
                                               .message = raft::RequestVoteResponse{1U, false}})
          .value();
  auto fresh = RaftTransportTlsClient::create(network::TlsSocket{}, config, start);
  ASSERT_TRUE(fresh.has_value());
  EXPECT_EQ(fresh->try_enqueue(wrong_route, start).code(), common::StatusCode::kInvalidArgument);
  EXPECT_FALSE(wrong_route.empty());
}

TEST(RaftTransportTlsClientTest, RejectsServerPrincipalBeforeWritingQueuedFrame) {
  auto server_context = network::TlsServerContext::create(tls_server_config());
  auto client_context = network::TlsClientContext::create(tls_client_config());
  ASSERT_TRUE(server_context.has_value());
  ASSERT_TRUE(client_context.has_value());
  SocketPair sockets = nonblocking_socket_pair();
  auto server = network::TlsSocket::accept(*server_context, sockets.sockets[0]);
  auto client_socket = network::TlsSocket::connect(*client_context, sockets.sockets[1]);
  ASSERT_TRUE(server.has_value());
  ASSERT_TRUE(client_socket.has_value());
  Authenticator authenticator;
  Authorizer authorizer;
  authorizer.allow = false;
  const auto now = RaftTransportTlsClient::TimePoint{};
  auto client = RaftTransportTlsClient::create(std::move(*client_socket),
                                               client_config(authenticator, authorizer), now);
  ASSERT_TRUE(client.has_value());
  auto queued = frame(1U);
  const auto expected = queued;
  ASSERT_TRUE(client->try_enqueue(queued, now).is_ok());

  common::Status progress = common::Status::ok();
  for (std::size_t iteration = 0U; iteration < 1024U && progress.is_ok(); ++iteration) {
    if (!server->handshake_complete())
      (void)server->handshake();
    progress = client->on_ready(true, true, now + std::chrono::milliseconds{1});
  }
  EXPECT_EQ(progress.code(), common::StatusCode::kUnauthenticated);
  EXPECT_EQ(client->state(), RaftTransportTlsClientState::kFailed);
  auto retry = client->drain_retry_frames();
  ASSERT_TRUE(retry.has_value());
  ASSERT_EQ(retry->size(), 1U);
  EXPECT_EQ(retry->front(), expected);
}

} // namespace
} // namespace chronos::cluster
