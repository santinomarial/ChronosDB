#include "chronos/cluster/raft_transport_peer_manager.hpp"

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
    return principal == 800U && (node == 2U || node == 3U);
  }
};
[[nodiscard]] network::TlsClientConfig tls_config() {
  return {.certificate_chain_file = fixture("client.pem").string(),
          .private_key_file = fixture("client-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string(),
          .expected_server_identity = "127.0.0.1"};
}
[[nodiscard]] RaftTransportPeerReconnectConfig
route(const raft::NodeId peer, const network::Ipv4Endpoint endpoint,
      const network::TlsClientContext& tls, Authenticator& authenticator, Authorizer& authorizer) {
  return {
      .connector = {.remote_endpoint = endpoint,
                    .tls_context = &tls,
                    .carrier = {.local_node_id = 1U,
                                .peer_node_id = peer,
                                .authenticator = &authenticator,
                                .node_authorizer = &authorizer,
                                .peer_ipv4_address = endpoint.address,
                                .limits = {.maximum_queued_frames = 4U,
                                           .maximum_queued_bytes = 4096U,
                                           .handshake_timeout = std::chrono::milliseconds{5},
                                           .frame_write_timeout = std::chrono::milliseconds{5}}},
                    .connect_timeout = std::chrono::milliseconds{5}},
      .limits = {.initial_backoff = std::chrono::milliseconds{5},
                 .maximum_backoff = std::chrono::milliseconds{10}}};
}
[[nodiscard]] raft::GroupId group() {
  common::Uuid::Bytes bytes{};
  bytes.fill(std::byte{9U});
  return raft::GroupId{bytes};
}
[[nodiscard]] raft::DurableRaftResult outbound() {
  return {common::Status::ok(),
          raft::MultiRaftTransition{
              .outbound = {{group(), 1U, {2U, raft::RequestVoteResponse{1U, true}}},
                           {group(), 1U, {3U, raft::RequestVoteResponse{1U, false}}}}},
          std::nullopt};
}

TEST(RaftTransportPeerManagerTest, ConnectsFixedRoutesRoutesAtomicallyAndRecyclesFailure) {
  auto tls = network::TlsClientContext::create(tls_config());
  auto listener2 = network::TcpListener::bind({});
  auto listener3 = network::TcpListener::bind({});
  ASSERT_TRUE(tls.has_value());
  ASSERT_TRUE(listener2.has_value());
  ASSERT_TRUE(listener3.has_value());
  Authenticator authenticator;
  Authorizer authorizer;
  auto manager = RaftTransportPeerManager::create(
      {.local_node_id = 1U,
       .peers = {route(2U, listener2->bound_endpoint(), *tls, authenticator, authorizer),
                 route(3U, listener3->bound_endpoint(), *tls, authenticator, authorizer)},
       .pool = {.maximum_peers = 2U}});
  ASSERT_TRUE(manager.has_value()) << manager.error().to_string();
  const auto start = RaftTransportPeerManager::TimePoint{};
  ASSERT_TRUE(manager->drive(start).is_ok());
  EXPECT_EQ(manager->next_deadline(), std::optional{start + std::chrono::milliseconds{5}});
  auto interests = manager->interests();
  ASSERT_TRUE(interests.has_value());
  ASSERT_EQ(interests->size(), 2U);
  for (const RaftTransportPeerInterest& interest : *interests) {
    EXPECT_GE(interest.descriptor, 0);
    EXPECT_TRUE(interest.want_write);
    ASSERT_TRUE(
        manager->on_ready(interest.peer_node_id, false, true, start + std::chrono::milliseconds{1})
            .is_ok());
  }
  EXPECT_EQ(manager->connected_peer_count(), 2U);
  EXPECT_EQ(manager->next_deadline(), std::optional{start + std::chrono::milliseconds{6}});
  auto result = outbound();
  ASSERT_TRUE(manager->route_result(group(), result, start).is_ok());
  interests = manager->interests();
  ASSERT_TRUE(interests.has_value());
  ASSERT_EQ(interests->size(), 2U);
  for (const RaftTransportPeerInterest& interest : *interests) {
    EXPECT_GE(interest.descriptor, 0);
    EXPECT_FALSE(interest.want_read);
    EXPECT_TRUE(interest.want_write);
  }

  ASSERT_TRUE(manager->on_transport_closed(2U, start + std::chrono::milliseconds{6}).is_ok());
  EXPECT_EQ(manager->connected_peer_count(), 1U);
  EXPECT_TRUE(manager->route_result(group(), result, start).is_ok());
  ASSERT_TRUE(manager->drive(start + std::chrono::milliseconds{10}).is_ok());
  interests = manager->interests();
  ASSERT_TRUE(interests.has_value());
  EXPECT_EQ(interests->size(), 1U);
  ASSERT_TRUE(manager->drive(start + std::chrono::milliseconds{11}).is_ok());
  interests = manager->interests();
  ASSERT_TRUE(interests.has_value());
  ASSERT_EQ(interests->size(), 2U);
}

TEST(RaftTransportPeerManagerTest, RejectsUndercapacityDuplicateAndForeignRoutes) {
  auto tls = network::TlsClientContext::create(tls_config());
  auto listener = network::TcpListener::bind({});
  ASSERT_TRUE(tls.has_value());
  ASSERT_TRUE(listener.has_value());
  Authenticator authenticator;
  Authorizer authorizer;
  const auto peer = route(2U, listener->bound_endpoint(), *tls, authenticator, authorizer);
  EXPECT_EQ(RaftTransportPeerManager::create(
                {.local_node_id = 1U, .peers = {peer}, .pool = {.maximum_peers = 0U}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(RaftTransportPeerManager::create(
                {.local_node_id = 1U, .peers = {peer, peer}, .pool = {.maximum_peers = 2U}})
                .error()
                .code(),
            common::StatusCode::kAlreadyExists);
  auto foreign = peer;
  foreign.connector.carrier.local_node_id = 3U;
  EXPECT_EQ(RaftTransportPeerManager::create(
                {.local_node_id = 1U, .peers = {foreign}, .pool = {.maximum_peers = 1U}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::cluster
