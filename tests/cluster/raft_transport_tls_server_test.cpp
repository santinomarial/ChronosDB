#include "chronos/cluster/raft_transport_tls_server.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <sys/socket.h>
#include <system_error>
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

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-raft-tls-server-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
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

class Authenticator final : public network::ConnectionAuthenticator {
public:
  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest& request) override {
    saw_fingerprint = request.peer_certificate_sha256.has_value();
    return network::PeerAuthenticationResult{.authorized = allow,
                                             .principal_id = allow ? 700U : 0U};
  }

  bool allow{true};
  bool saw_fingerprint{};
};

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal_id,
                                      const raft::NodeId node_id) const override {
    return principal_id == 700U && node_id == 1U;
  }
};

[[nodiscard]] std::vector<std::byte> vote_request(const raft::Term term) {
  return raft::encode_raft_transport_envelope_v1(
             {.group_id = group(),
              .source = 1U,
              .destination = 2U,
              .message = raft::RequestVoteRequest{term, 1U, 0U, 0U}})
      .value();
}

[[nodiscard]] RaftTransportTlsServerConfig server_config(Authenticator& authenticator,
                                                         RaftTransportReceiver& receiver) {
  return {.authenticator = &authenticator,
          .receiver = &receiver,
          .peer_ipv4_address = {127U, 0U, 0U, 1U},
          .limits = {.handshake_timeout = std::chrono::milliseconds{100},
                     .frame_read_timeout = std::chrono::milliseconds{100}}};
}

void finish_handshake(network::TlsSocket& client, RaftTransportTlsServer& server,
                      const RaftTransportTlsServer::TimePoint now) {
  for (std::size_t iteration = 0U; iteration < 1024U; ++iteration) {
    if (!client.handshake_complete()) {
      auto progress = client.handshake();
      ASSERT_TRUE(progress.has_value()) << progress.error().to_string();
    }
    ASSERT_TRUE(server.on_ready(true, true, now).is_ok()) << server.failure().to_string();
    if (client.handshake_complete() && server.state() == RaftTransportTlsServerState::kReadingFrame)
      return;
  }
  FAIL() << "mutual TLS handshake did not complete";
}

void send_frame(network::TlsSocket& client, RaftTransportTlsServer& server,
                const common::ByteView frame, const RaftTransportTlsServer::TimePoint now) {
  std::size_t written = 0U;
  for (std::size_t iteration = 0U; iteration < 4096U; ++iteration) {
    if (written < frame.size()) {
      const std::size_t chunk = std::min<std::size_t>(7U, frame.size() - written);
      auto progress = client.write(frame.subspan(written, chunk));
      ASSERT_TRUE(progress.has_value()) << progress.error().to_string();
      if (progress->state == network::TlsIoState::kComplete)
        written += progress->bytes_transferred;
    }
    ASSERT_TRUE(server.on_ready(true, true, now).is_ok()) << server.failure().to_string();
    if (written == frame.size() && server.state() == RaftTransportTlsServerState::kResultReady)
      return;
  }
  FAIL() << "Raft TLS frame did not reach a durable result";
}

TEST(RaftTransportTlsServerTest, AuthenticatesFragmentsAndServesPersistentFrames) {
  TemporaryDirectory directory;
  Authorizer authorizer;
  auto runtime = raft::AsyncDurableMultiRaftRuntime::create_new(
      2U, {.directory_path = directory.path().string()}, {{group(), {1U, 2U}}});
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  auto receiver = RaftTransportReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .runtime = &*runtime});
  ASSERT_TRUE(receiver.has_value());
  auto server_context = network::TlsServerContext::create(tls_server_config());
  auto client_context = network::TlsClientContext::create(tls_client_config());
  ASSERT_TRUE(server_context.has_value());
  ASSERT_TRUE(client_context.has_value());
  SocketPair sockets = nonblocking_socket_pair();
  auto server_socket = network::TlsSocket::accept(*server_context, sockets.sockets[0]);
  auto client_socket = network::TlsSocket::connect(*client_context, sockets.sockets[1]);
  ASSERT_TRUE(server_socket.has_value());
  ASSERT_TRUE(client_socket.has_value());
  Authenticator authenticator;
  const auto now = RaftTransportTlsServer::TimePoint{};
  auto server = RaftTransportTlsServer::create(std::move(*server_socket),
                                               server_config(authenticator, *receiver), now);
  ASSERT_TRUE(server.has_value()) << server.error().to_string();
  finish_handshake(*client_socket, *server, now + std::chrono::milliseconds{1});
  EXPECT_TRUE(authenticator.saw_fingerprint);

  const auto first = vote_request(1U);
  send_frame(*client_socket, *server, first, now + std::chrono::milliseconds{2});
  auto completed = server->take_completed(now + std::chrono::milliseconds{3});
  ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  EXPECT_EQ(completed->group_id, group());
  EXPECT_EQ(completed->source_node_id, 1U);
  ASSERT_TRUE(completed->result.status.is_ok()) << completed->result.status.to_string();
  ASSERT_TRUE(completed->result.transition.has_value());
  EXPECT_TRUE(completed->result.transition->persistence.has_value());
  EXPECT_EQ(server->state(), RaftTransportTlsServerState::kReadingFrame);

  const auto second = vote_request(2U);
  send_frame(*client_socket, *server, second, now + std::chrono::milliseconds{4});
  auto second_completed = server->take_completed(now + std::chrono::milliseconds{5});
  ASSERT_TRUE(second_completed.has_value()) << second_completed.error().to_string();
  EXPECT_EQ(second_completed->result.transition->persistence->state.current_term, 2U);
  ASSERT_TRUE(runtime->shutdown().is_ok());
}

TEST(RaftTransportTlsServerTest, RejectsPrincipalAndExactHandshakeDeadline) {
  TemporaryDirectory directory;
  Authorizer authorizer;
  auto runtime = raft::AsyncDurableMultiRaftRuntime::create_new(
      2U, {.directory_path = directory.path().string()}, {{group(), {1U, 2U}}});
  ASSERT_TRUE(runtime.has_value());
  auto receiver = RaftTransportReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .runtime = &*runtime});
  ASSERT_TRUE(receiver.has_value());
  Authenticator authenticator;
  auto config = server_config(authenticator, *receiver);
  config.limits.handshake_timeout = std::chrono::milliseconds{5};
  auto timeout_server = RaftTransportTlsServer::create(network::TlsSocket{}, config, {});
  ASSERT_TRUE(timeout_server.has_value());
  const auto start = RaftTransportTlsServer::TimePoint{};
  EXPECT_TRUE(timeout_server->on_ready(false, false, start + std::chrono::milliseconds{4}).is_ok());
  EXPECT_EQ(timeout_server->on_ready(false, false, start + std::chrono::milliseconds{5}).code(),
            common::StatusCode::kUnavailable);

  auto server_context = network::TlsServerContext::create(tls_server_config());
  auto client_context = network::TlsClientContext::create(tls_client_config());
  ASSERT_TRUE(server_context.has_value());
  ASSERT_TRUE(client_context.has_value());
  SocketPair sockets = nonblocking_socket_pair();
  auto server_socket = network::TlsSocket::accept(*server_context, sockets.sockets[0]);
  auto client_socket = network::TlsSocket::connect(*client_context, sockets.sockets[1]);
  ASSERT_TRUE(server_socket.has_value());
  ASSERT_TRUE(client_socket.has_value());
  authenticator.allow = false;
  auto denied = RaftTransportTlsServer::create(std::move(*server_socket), config, {});
  ASSERT_TRUE(denied.has_value());
  common::Status progress = common::Status::ok();
  for (std::size_t iteration = 0U; iteration < 1024U && progress.is_ok(); ++iteration) {
    if (!client_socket->handshake_complete())
      (void)client_socket->handshake();
    progress = denied->on_ready(true, true, start + std::chrono::milliseconds{1});
  }
  EXPECT_EQ(progress.code(), common::StatusCode::kUnauthenticated);
  EXPECT_EQ(denied->state(), RaftTransportTlsServerState::kFailed);
  EXPECT_EQ(runtime->metrics().admitted_batches, 0U);
  ASSERT_TRUE(runtime->shutdown().is_ok());
}

} // namespace
} // namespace chronos::cluster
