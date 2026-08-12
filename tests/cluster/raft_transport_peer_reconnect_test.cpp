#include "chronos/cluster/raft_transport_peer_reconnect.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] std::filesystem::path fixture(const char* name) {
  return std::filesystem::path{CHRONOS_NETWORK_FIXTURE_DIR} / "tls" / name;
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
[[nodiscard]] RaftTransportPeerReconnectConfig config(const network::Ipv4Endpoint endpoint,
                                                      const network::TlsClientContext& tls,
                                                      Authenticator& authenticator,
                                                      Authorizer& authorizer) {
  return {
      .connector = {.remote_endpoint = endpoint,
                    .tls_context = &tls,
                    .carrier = {.local_node_id = 1U,
                                .peer_node_id = 2U,
                                .authenticator = &authenticator,
                                .node_authorizer = &authorizer,
                                .peer_ipv4_address = endpoint.address,
                                .limits = {.maximum_queued_frames = 2U,
                                           .maximum_queued_bytes = 4096U,
                                           .handshake_timeout = std::chrono::milliseconds{5},
                                           .frame_write_timeout = std::chrono::milliseconds{5}}},
                    .connect_timeout = std::chrono::milliseconds{10}},
      .limits = {.initial_backoff = std::chrono::milliseconds{5},
                 .maximum_backoff = std::chrono::milliseconds{10}}};
}
[[nodiscard]] network::TlsClientConfig tls_config() {
  return {.certificate_chain_file = fixture("client.pem").string(),
          .private_key_file = fixture("client-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string(),
          .expected_server_identity = "127.0.0.1"};
}
[[nodiscard]] std::vector<std::byte> frame() {
  common::Uuid::Bytes bytes{};
  bytes.fill(std::byte{9U});
  return raft::encode_raft_transport_envelope_v1({.group_id = raft::GroupId{bytes},
                                                  .source = 1U,
                                                  .destination = 2U,
                                                  .message = raft::RequestVoteResponse{1U, true}})
      .value();
}

TEST(RaftTransportPeerReconnectTest, AppliesCappedBackoffAtExactAttemptDeadlines) {
  auto tls = network::TlsClientContext::create(tls_config());
  auto listener = network::TcpListener::bind({});
  ASSERT_TRUE(tls.has_value());
  ASSERT_TRUE(listener.has_value());
  Authenticator authenticator;
  Authorizer authorizer;
  auto reconnect = RaftTransportPeerReconnect::create(
      config(listener->bound_endpoint(), *tls, authenticator, authorizer));
  ASSERT_TRUE(reconnect.has_value());
  const auto start = RaftTransportPeerReconnect::TimePoint{};
  ASSERT_TRUE(reconnect->drive(start).is_ok());
  EXPECT_EQ(reconnect->attempts_started(), 1U);
  EXPECT_EQ(reconnect->on_ready(false, start + std::chrono::milliseconds{10}).code(),
            common::StatusCode::kUnavailable);
  ASSERT_TRUE(reconnect->next_attempt_not_before().has_value());
  EXPECT_EQ(*reconnect->next_attempt_not_before(), start + std::chrono::milliseconds{15});
  ASSERT_TRUE(reconnect->drive(start + std::chrono::milliseconds{14}).is_ok());
  EXPECT_EQ(reconnect->attempts_started(), 1U);
  ASSERT_TRUE(reconnect->drive(start + std::chrono::milliseconds{15}).is_ok());
  EXPECT_EQ(reconnect->attempts_started(), 2U);
  EXPECT_EQ(reconnect->on_ready(false, start + std::chrono::milliseconds{25}).code(),
            common::StatusCode::kUnavailable);
  EXPECT_EQ(*reconnect->next_attempt_not_before(), start + std::chrono::milliseconds{35});
}

TEST(RaftTransportPeerReconnectTest, ReclaimsFailedPoolPeerAndRetainsItsRetryFrames) {
  auto tls = network::TlsClientContext::create(tls_config());
  auto listener = network::TcpListener::bind({});
  ASSERT_TRUE(tls.has_value());
  ASSERT_TRUE(listener.has_value());
  Authenticator authenticator;
  Authorizer authorizer;
  auto reconnect = RaftTransportPeerReconnect::create(
      config(listener->bound_endpoint(), *tls, authenticator, authorizer));
  ASSERT_TRUE(reconnect.has_value());
  const auto start = RaftTransportPeerReconnect::TimePoint{};
  ASSERT_TRUE(reconnect->drive(start).is_ok());
  std::optional<network::TcpSocket> accepted;
  for (std::size_t iteration = 0U;
       iteration < 1024U && reconnect->state() == RaftTransportPeerReconnectState::kConnecting;
       ++iteration) {
    auto next = listener->accept_one();
    ASSERT_TRUE(next.has_value());
    if (next->has_value())
      accepted.emplace(std::move(**next));
    ASSERT_TRUE(reconnect->on_ready(true, start + std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(reconnect->state(), RaftTransportPeerReconnectState::kCarrierReady);
  auto connected = reconnect->take_connected_peer();
  ASSERT_TRUE(connected.has_value());
  auto pool = RaftTransportPeerPool::create(1U, {.maximum_peers = 1U});
  ASSERT_TRUE(pool.has_value());
  ASSERT_TRUE(pool->add_connected_peer(std::move(*connected)).is_ok());
  auto queued = frame();
  ASSERT_TRUE(pool->find_peer(2U)->try_enqueue(queued, start).is_ok());
  EXPECT_EQ(pool->on_ready(2U, false, false, start + std::chrono::milliseconds{6}).code(),
            common::StatusCode::kUnavailable);
  auto failed = pool->take_failed_peer(2U);
  ASSERT_TRUE(failed.has_value());
  ASSERT_TRUE(
      reconnect->accept_failed_peer(std::move(*failed), start + std::chrono::milliseconds{6})
          .is_ok());
  EXPECT_EQ(reconnect->state(), RaftTransportPeerReconnectState::kBackoff);
  EXPECT_EQ(reconnect->retry_frame_count(), 1U);
  EXPECT_EQ(*reconnect->next_attempt_not_before(), start + std::chrono::milliseconds{11});
}

} // namespace
} // namespace chronos::cluster
