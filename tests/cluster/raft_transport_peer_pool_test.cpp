#include "chronos/cluster/raft_transport_peer_pool.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] raft::GroupId group(const std::byte seed = std::byte{9U}) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return raft::GroupId{bytes};
}

class Authenticator final : public network::ConnectionAuthenticator {
public:
  [[nodiscard]] common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest&) override {
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = 800U};
  }
};

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  [[nodiscard]] common::Result<bool>
  authorize_node(const std::uint64_t principal_id,
                 const raft::NodeId claimed_node_id) const override {
    return principal_id == 800U && (claimed_node_id == 2U || claimed_node_id == 3U);
  }
};

[[nodiscard]] RaftTransportTlsClient make_client(const raft::NodeId peer,
                                                 Authenticator& authenticator,
                                                 Authorizer& authorizer,
                                                 const std::size_t maximum_frames = 2U) {
  auto created = RaftTransportTlsClient::create(
      network::TlsSocket{},
      {.local_node_id = 1U,
       .peer_node_id = peer,
       .authenticator = &authenticator,
       .node_authorizer = &authorizer,
       .peer_ipv4_address = {127U, 0U, 0U, 1U},
       .limits = {.maximum_queued_frames = maximum_frames,
                  .maximum_queued_bytes = 4096U,
                  .handshake_timeout = std::chrono::milliseconds{5},
                  .frame_write_timeout = std::chrono::milliseconds{5}}},
      RaftTransportTlsClient::TimePoint{});
  EXPECT_TRUE(created.has_value()) << created.error().to_string();
  return std::move(created).value();
}

[[nodiscard]] raft::DurableRaftResult result_for(std::vector<raft::GroupOutboundMessage> outbound) {
  return {common::Status::ok(), raft::MultiRaftTransition{.outbound = std::move(outbound)},
          std::nullopt};
}

[[nodiscard]] raft::GroupOutboundMessage vote_response(const raft::NodeId destination,
                                                       const bool granted = true) {
  return {
      .group_id = group(),
      .source = 1U,
      .outbound = {.destination = destination, .message = raft::RequestVoteResponse{1U, granted}}};
}

TEST(RaftTransportPeerPoolTest, PreflightsEveryDestinationBeforeEnqueueingAnyFrame) {
  Authenticator authenticator;
  Authorizer authorizer;
  auto pool = RaftTransportPeerPool::create(1U, {.maximum_peers = 2U});
  ASSERT_TRUE(pool.has_value()) << pool.error().to_string();
  ASSERT_TRUE(pool->add_peer(make_client(2U, authenticator, authorizer)).is_ok());

  auto missing = result_for({vote_response(2U), vote_response(3U)});
  EXPECT_EQ(pool->route_result(group(), missing, RaftTransportTlsClient::TimePoint{}).code(),
            common::StatusCode::kNotFound);
  ASSERT_NE(pool->find_peer(2U), nullptr);
  EXPECT_EQ(pool->find_peer(2U)->queued_frames(), 0U);

  ASSERT_TRUE(pool->add_peer(make_client(3U, authenticator, authorizer, 1U)).is_ok());
  auto first = result_for({vote_response(2U), vote_response(3U, false)});
  ASSERT_TRUE(pool->route_result(group(), first, RaftTransportTlsClient::TimePoint{}).is_ok());
  EXPECT_EQ(pool->find_peer(2U)->queued_frames(), 1U);
  EXPECT_EQ(pool->find_peer(3U)->queued_frames(), 1U);

  auto full = result_for({vote_response(2U, false), vote_response(3U)});
  EXPECT_EQ(pool->route_result(group(), full, RaftTransportTlsClient::TimePoint{}).code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(pool->find_peer(2U)->queued_frames(), 1U);
  EXPECT_EQ(pool->find_peer(3U)->queued_frames(), 1U);
}

TEST(RaftTransportPeerPoolTest, RejectsAggregateDemandWithoutPartialAdmission) {
  Authenticator authenticator;
  Authorizer authorizer;
  auto pool = RaftTransportPeerPool::create(1U, {.maximum_peers = 1U});
  ASSERT_TRUE(pool.has_value());
  ASSERT_TRUE(pool->add_peer(make_client(2U, authenticator, authorizer, 1U)).is_ok());
  auto batch = result_for({vote_response(2U), vote_response(2U, false)});

  EXPECT_EQ(pool->route_result(group(), batch, RaftTransportTlsClient::TimePoint{}).code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(pool->find_peer(2U)->queued_frames(), 0U);
}

TEST(RaftTransportPeerPoolTest, ReturnsCompleteRetryFramesOnlyAfterCarrierFailure) {
  Authenticator authenticator;
  Authorizer authorizer;
  auto pool = RaftTransportPeerPool::create(1U, {.maximum_peers = 1U});
  ASSERT_TRUE(pool.has_value());
  ASSERT_TRUE(pool->add_peer(make_client(2U, authenticator, authorizer)).is_ok());
  auto routed = result_for({vote_response(2U)});
  const auto start = RaftTransportTlsClient::TimePoint{};
  ASSERT_TRUE(pool->route_result(group(), routed, start).is_ok());
  EXPECT_EQ(pool->take_failed_peer(2U).error().code(), common::StatusCode::kUnavailable);

  EXPECT_EQ(pool->on_ready(2U, false, false, start + std::chrono::milliseconds{5}).code(),
            common::StatusCode::kUnavailable);
  auto after_failure = result_for({vote_response(2U, false)});
  EXPECT_EQ(pool->route_result(group(), after_failure, start).code(),
            common::StatusCode::kUnavailable);
  EXPECT_EQ(pool->find_peer(2U)->queued_frames(), 1U);
  auto failed = pool->take_failed_peer(2U);
  ASSERT_TRUE(failed.has_value()) << failed.error().to_string();
  EXPECT_EQ(failed->peer_node_id, 2U);
  ASSERT_EQ(failed->retry_frames.size(), 1U);
  auto decoded = raft::decode_raft_transport_envelope_v1(failed->retry_frames.front());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->group_id, group());
  EXPECT_EQ(decoded->source, 1U);
  EXPECT_EQ(decoded->destination, 2U);
  ASSERT_TRUE(std::holds_alternative<raft::RequestVoteResponse>(decoded->message));
  EXPECT_TRUE(std::get<raft::RequestVoteResponse>(decoded->message).granted);
  EXPECT_EQ(pool->peer_count(), 0U);
  EXPECT_EQ(pool->find_peer(2U), nullptr);
}

TEST(RaftTransportPeerPoolTest, EnforcesPoolAndCarrierOwnership) {
  Authenticator authenticator;
  Authorizer authorizer;
  auto pool = RaftTransportPeerPool::create(1U, {.maximum_peers = 1U});
  ASSERT_TRUE(pool.has_value());
  ASSERT_TRUE(pool->add_peer(make_client(2U, authenticator, authorizer)).is_ok());
  EXPECT_EQ(pool->add_peer(make_client(2U, authenticator, authorizer)).code(),
            common::StatusCode::kAlreadyExists);
  EXPECT_EQ(pool->add_peer(make_client(3U, authenticator, authorizer)).code(),
            common::StatusCode::kResourceExhausted);

  auto wrong_pool = RaftTransportPeerPool::create(2U, {.maximum_peers = 1U});
  ASSERT_TRUE(wrong_pool.has_value());
  EXPECT_EQ(wrong_pool->add_peer(make_client(3U, authenticator, authorizer)).code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::cluster
