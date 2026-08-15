#include "chronos/cluster/raft_transport_tcp_connector.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] std::filesystem::path fixture(const char* name) {
  return std::filesystem::path{CHRONOS_NETWORK_FIXTURE_DIR} / "tls" / name;
}
[[nodiscard]] network::TlsServerConfig server_config() {
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
  bytes.fill(std::byte{9U});
  return raft::GroupId{bytes};
}
[[nodiscard]] std::vector<std::byte> frame() {
  return raft::encode_raft_transport_envelope_v1({.group_id = group(),
                                                  .source = 1U,
                                                  .destination = 2U,
                                                  .message = raft::RequestVoteResponse{1U, true}})
      .value();
}

class Authenticator final : public network::ConnectionAuthenticator {
public:
  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest&) override {
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = 800U};
  }
};
class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal,
                                      const raft::NodeId node) const override {
    return principal == 800U && node == 2U;
  }
};

[[nodiscard]] RaftTransportTcpConnectorConfig connector_config(const network::Ipv4Endpoint endpoint,
                                                               const network::TlsClientContext& tls,
                                                               Authenticator& authenticator,
                                                               Authorizer& authorizer) {
  return {.remote_endpoint = endpoint,
          .tls_context = &tls,
          .carrier = {.local_node_id = 1U,
                      .peer_node_id = 2U,
                      .authenticator = &authenticator,
                      .node_authorizer = &authorizer,
                      .peer_ipv4_address = endpoint.address,
                      .limits = {.maximum_queued_frames = 2U,
                                 .maximum_queued_bytes = 4096U,
                                 .handshake_timeout = std::chrono::milliseconds{100},
                                 .frame_write_timeout = std::chrono::milliseconds{100}}},
          .connect_timeout = std::chrono::milliseconds{10}};
}

TEST(RaftTransportTcpConnectorTest, TransfersDescriptorAndRetryFrameIntoPersistentPeerPool) {
  auto server_context = network::TlsServerContext::create(server_config());
  auto client_context = network::TlsClientContext::create(client_tls_config());
  auto listener = network::TcpListener::bind({});
  ASSERT_TRUE(server_context.has_value());
  ASSERT_TRUE(client_context.has_value());
  ASSERT_TRUE(listener.has_value()) << listener.error().to_string();
  Authenticator authenticator;
  Authorizer authorizer;
  const auto now = RaftTransportTcpConnector::TimePoint{};
  std::vector<std::vector<std::byte>> retry{frame()};
  std::optional<network::TcpSocket> accepted;
  std::optional<RaftTransportConnectedPeer> connected;
  {
    auto connector = RaftTransportTcpConnector::begin(
        std::move(retry),
        connector_config(listener->bound_endpoint(), *client_context, authenticator, authorizer),
        now);
    ASSERT_TRUE(connector.has_value()) << connector.error().to_string();
    EXPECT_EQ(connector->next_deadline(), std::optional{now + std::chrono::milliseconds{10}});
    for (std::size_t iteration = 0U;
         iteration < 1024U && connector->state() == RaftTransportTcpConnectorState::kConnecting;
         ++iteration) {
      if (!accepted.has_value()) {
        auto next = listener->accept_one();
        ASSERT_TRUE(next.has_value()) << next.error().to_string();
        if (next->has_value())
          accepted.emplace(std::move(**next));
      }
      ASSERT_TRUE(connector->on_ready(true, now + std::chrono::milliseconds{1}).is_ok())
          << connector->failure().to_string();
    }
    ASSERT_EQ(connector->state(), RaftTransportTcpConnectorState::kCarrierReady);
    auto taken = connector->take_connected_peer();
    ASSERT_TRUE(taken.has_value()) << taken.error().to_string();
    connected.emplace(std::move(*taken));
    EXPECT_EQ(connector->take_connected_peer().error().code(), common::StatusCode::kUnavailable);
  }
  for (std::size_t iteration = 0U; iteration < 1024U && !accepted.has_value(); ++iteration) {
    auto next = listener->accept_one();
    ASSERT_TRUE(next.has_value()) << next.error().to_string();
    if (next->has_value())
      accepted.emplace(std::move(**next));
  }
  ASSERT_TRUE(accepted.has_value());

  auto pool = RaftTransportPeerPool::create(1U, {.maximum_peers = 1U});
  ASSERT_TRUE(pool.has_value());
  ASSERT_TRUE(pool->add_connected_peer(std::move(*connected)).is_ok());
  ASSERT_NE(pool->find_peer(2U), nullptr);
  EXPECT_EQ(pool->find_peer(2U)->queued_frames(), 1U);
  auto server = network::TlsSocket::accept(*server_context, accepted->descriptor());
  ASSERT_TRUE(server.has_value()) << server.error().to_string();
  auto reader = raft::RaftTransportFrameReader::create();
  ASSERT_TRUE(reader.has_value());
  std::optional<raft::RaftTransportEnvelope> received;
  std::array<std::byte, 1U> byte{};
  for (std::size_t iteration = 0U; iteration < 8192U && !received.has_value(); ++iteration) {
    if (!server->handshake_complete()) {
      auto progress = server->handshake();
      ASSERT_TRUE(progress.has_value()) << progress.error().to_string();
    }
    ASSERT_TRUE(pool->on_ready(2U, true, true, now + std::chrono::milliseconds{2}).is_ok());
    if (server->handshake_complete()) {
      auto progress = server->read(byte);
      ASSERT_TRUE(progress.has_value()) << progress.error().to_string();
      if (progress->state == network::TlsIoState::kComplete && progress->bytes_transferred == 1U) {
        auto step = reader->consume(byte);
        ASSERT_TRUE(step.has_value()) << step.error().to_string();
        if (step->envelope.has_value())
          received = std::move(step->envelope);
      }
    }
  }
  ASSERT_TRUE(received.has_value());
  EXPECT_EQ(received->source, 1U);
  EXPECT_EQ(received->destination, 2U);
  EXPECT_TRUE(std::get<raft::RequestVoteResponse>(received->message).granted);
}

TEST(RaftTransportTcpConnectorTest, TimeoutReturnsEveryCompleteRetryFrame) {
  auto client_context = network::TlsClientContext::create(client_tls_config());
  auto listener = network::TcpListener::bind({});
  ASSERT_TRUE(client_context.has_value());
  ASSERT_TRUE(listener.has_value());
  Authenticator authenticator;
  Authorizer authorizer;
  const auto now = RaftTransportTcpConnector::TimePoint{};
  const auto expected = frame();
  std::vector<std::vector<std::byte>> retry{expected};
  auto connector = RaftTransportTcpConnector::begin(
      std::move(retry),
      connector_config(listener->bound_endpoint(), *client_context, authenticator, authorizer),
      now);
  ASSERT_TRUE(connector.has_value());
  EXPECT_EQ(connector->next_deadline(), std::optional{now + std::chrono::milliseconds{10}});
  EXPECT_EQ(connector->on_ready(false, now + std::chrono::milliseconds{10}).code(),
            common::StatusCode::kUnavailable);
  EXPECT_FALSE(connector->next_deadline().has_value());
  EXPECT_EQ(connector->descriptor(), -1);
  auto returned = connector->take_retry_frames();
  ASSERT_TRUE(returned.has_value());
  ASSERT_EQ(returned->size(), 1U);
  EXPECT_EQ(returned->front(), expected);
}

TEST(RaftTransportTcpConnectorTest, RejectsForeignRetryRouteWithoutConsumingCallerFrames) {
  auto client_context = network::TlsClientContext::create(client_tls_config());
  ASSERT_TRUE(client_context.has_value());
  Authenticator authenticator;
  Authorizer authorizer;
  std::vector<std::vector<std::byte>> retry{
      raft::encode_raft_transport_envelope_v1({.group_id = group(),
                                               .source = 3U,
                                               .destination = 2U,
                                               .message = raft::RequestVoteResponse{1U, true}})
          .value()};
  const auto expected = retry;
  const network::Ipv4Endpoint endpoint{{127U, 0U, 0U, 1U}, 1U};
  auto rejected = RaftTransportTcpConnector::begin(
      std::move(retry), connector_config(endpoint, *client_context, authenticator, authorizer), {});
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kInvalidArgument);
  // NOLINTNEXTLINE(bugprone-use-after-move)
  EXPECT_EQ(retry, expected);
}

} // namespace
} // namespace chronos::cluster
