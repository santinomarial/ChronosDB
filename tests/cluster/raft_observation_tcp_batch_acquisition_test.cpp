#include "chronos/cluster/raft_observation_tcp_batch_acquisition.hpp"
#include "chronos/cluster/raft_observation_tcp_server.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <limits>
#include <map>

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

[[nodiscard]] raft::GroupId group(const std::uint8_t value) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(value);
  return raft::GroupId{bytes};
}

[[nodiscard]] schema::TabletId tablet(const std::uint8_t value) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(value);
  return schema::TabletId::from_bytes(bytes).value();
}

[[nodiscard]] schema::TableId table(const std::uint8_t value) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(value);
  return schema::TableId::from_bytes(bytes).value();
}

[[nodiscard]] raft::RaftGroupObservation observation(const raft::GroupId& group_id,
                                                     const raft::NodeId node) {
  return {.group_id = group_id,
          .node_id = node,
          .role = node == 1U ? raft::Role::kLeader : raft::Role::kFollower,
          .current_term = 8U,
          .leader_id = 1U,
          .last_log_index = 15U,
          .commit_index = node == 1U ? 14U : 13U,
          .applied_index = node == 1U ? 14U : 12U,
          .voters = {1U, 2U, 3U},
          .committed_voters = {1U, 2U, 3U}};
}

class Authenticator final : public network::ConnectionAuthenticator {
public:
  explicit Authenticator(const std::uint64_t principal) : principal_(principal) {}
  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest&) override {
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = principal_};
  }

private:
  std::uint64_t principal_{};
};

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal,
                                      const raft::NodeId node) const override {
    return (principal == 91U && node == 1U) || (principal == 92U && node == 2U) ||
           (principal == 93U && node == 3U);
  }
};

class Service final : public RaftObservationService {
public:
  explicit Service(const raft::NodeId node) : node_(node) {}
  common::Result<raft::RaftGroupObservation> observe(const raft::GroupId& requested) override {
    ++calls[requested];
    return observation(requested, node_);
  }
  std::map<raft::GroupId, std::size_t> calls;

private:
  raft::NodeId node_{};
};

[[nodiscard]] RaftObservationTcpAcquisitionConfig
target_config(const raft::GroupId& group_id, const raft::NodeId target,
              const std::uint64_t correlation, const network::Ipv4Endpoint endpoint,
              const network::TlsClientContext& tls_context, Authenticator& authenticator,
              Authorizer& authorizer) {
  return {.route = {.node_id = target, .endpoints = {endpoint}, .tls_context = &tls_context},
          .authenticator = &authenticator,
          .node_authorizer = &authorizer,
          .request = {3U, target, group_id, correlation},
          .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                             .exchange_timeout = std::chrono::milliseconds{1000}},
          .connect_timeout = std::chrono::milliseconds{1000},
          .retry = {.maximum_attempts = 1U,
                    .initial_backoff = std::chrono::milliseconds{1},
                    .maximum_backoff = std::chrono::milliseconds{1}}};
}

[[nodiscard]] RaftObservationTcpPairAcquisitionConfig
pair_config(const raft::GroupId& group_id, const std::uint64_t correlation,
            const network::Ipv4Endpoint leader_endpoint,
            const network::Ipv4Endpoint follower_endpoint,
            const network::TlsClientContext& tls_context, Authenticator& leader_authenticator,
            Authenticator& follower_authenticator, Authorizer& authorizer) {
  return {.leader = target_config(group_id, 1U, correlation, leader_endpoint, tls_context,
                                  leader_authenticator, authorizer),
          .follower = target_config(group_id, 2U, correlation + 1U, follower_endpoint, tls_context,
                                    follower_authenticator, authorizer)};
}

TEST(RaftObservationTcpBatchAcquisitionTest, PublishesOnlyCompleteCanonicalGroupBatch) {
  Authorizer authorizer;
  Authenticator client_authenticator{93U};
  Authenticator leader_authenticator{91U};
  Authenticator follower_authenticator{92U};
  Service leader_service{1U};
  Service follower_service{2U};
  auto leader_receiver = RaftObservationReceiver::create(
      {.local_node_id = 1U, .authorizer = &authorizer, .service = &leader_service});
  auto follower_receiver = RaftObservationReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .service = &follower_service});
  ASSERT_TRUE(leader_receiver.has_value());
  ASSERT_TRUE(follower_receiver.has_value());
  auto leader_server = RaftObservationTcpServer::start(
      {.listener = {},
       .tls = server_tls_config(),
       .authenticator = &client_authenticator,
       .receiver = &*leader_receiver,
       .session_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000}},
       .maximum_connections = 8U,
       .maximum_accepts_per_poll = 8U});
  auto follower_server = RaftObservationTcpServer::start(
      {.listener = {},
       .tls = server_tls_config(),
       .authenticator = &client_authenticator,
       .receiver = &*follower_receiver,
       .session_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000}},
       .maximum_connections = 8U,
       .maximum_accepts_per_poll = 8U});
  auto tls_context = network::TlsClientContext::create(client_tls_config());
  ASSERT_TRUE(leader_server.has_value());
  ASSERT_TRUE(follower_server.has_value());
  ASSERT_TRUE(tls_context.has_value());
  auto make_config = [&] {
    std::vector<RaftObservationTcpPairAcquisitionConfig> pairs;
    pairs.push_back(pair_config(group(10U), 51U, leader_server->bound_endpoint(),
                                follower_server->bound_endpoint(), *tls_context,
                                leader_authenticator, follower_authenticator, authorizer));
    pairs.push_back(pair_config(group(11U), 61U, leader_server->bound_endpoint(),
                                follower_server->bound_endpoint(), *tls_context,
                                leader_authenticator, follower_authenticator, authorizer));
    return RaftObservationTcpBatchAcquisitionConfig{.pairs = std::move(pairs)};
  };
  auto acquisition = RaftObservationTcpBatchAcquisition::create(make_config());
  ASSERT_TRUE(acquisition.has_value()) << acquisition.error().to_string();
  EXPECT_EQ(acquisition->result().error().code(), common::StatusCode::kInvalidArgument);
  for (std::size_t iteration = 0U;
       iteration < 4096U &&
       acquisition->state() == RaftObservationTcpBatchAcquisitionState::kRunning;
       ++iteration) {
    ASSERT_TRUE(acquisition->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(leader_server->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(follower_server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(acquisition->state(), RaftObservationTcpBatchAcquisitionState::kComplete)
      << acquisition->failure().to_string();
  auto authorities = acquisition->result();
  ASSERT_TRUE(authorities.has_value()) << authorities.error().to_string();
  ASSERT_EQ(authorities->size(), 2U);
  EXPECT_EQ((*authorities)[0].leader_observation.group_id, group(10U));
  EXPECT_EQ((*authorities)[1].leader_observation.group_id, group(11U));
  EXPECT_EQ(leader_service.calls[group(10U)], 1U);
  EXPECT_EQ(leader_service.calls[group(11U)], 1U);
  EXPECT_EQ(follower_service.calls[group(10U)], 1U);
  EXPECT_EQ(follower_service.calls[group(11U)], 1U);
  EXPECT_EQ(acquisition->metrics().completed_pairs, 2U);
  EXPECT_EQ(acquisition->metrics().active_pairs, 0U);

  auto cancelled = RaftObservationTcpBatchAcquisition::create(make_config());
  ASSERT_TRUE(cancelled.has_value());
  ASSERT_TRUE(cancelled->poll_once(std::chrono::milliseconds{0}).is_ok());
  EXPECT_EQ(cancelled->metrics().active_pairs, 2U);
  EXPECT_EQ(cancelled->cancel().code(), common::StatusCode::kCancelled);
  EXPECT_EQ(cancelled->metrics().active_pairs, 0U);

  auto duplicated = make_config();
  duplicated.pairs[1].leader.request.group_id = group(10U);
  duplicated.pairs[1].follower.request.group_id = group(10U);
  EXPECT_EQ(RaftObservationTcpBatchAcquisition::create(std::move(duplicated)).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(RaftObservationTcpBatchAcquisitionTest, ConstructsCanonicalPairsFromCommittedPlacement) {
  Authorizer authorizer;
  Authenticator authenticator{91U};
  auto tls_context = network::TlsClientContext::create(client_tls_config());
  ASSERT_TRUE(tls_context.has_value());
  const raft::MetadataCatalogSnapshot catalog{
      .applied_index = 9U,
      .cluster_nodes = {{1U, "127.0.0.1:11001"}, {2U, "127.0.0.1:11002"}, {3U, "127.0.0.1:11003"}},
      .tablet_placements = {{.table_id = table(1U),
                             .tablet_id = tablet(1U),
                             .placement_epoch = 1U,
                             .replicas = {1U, 2U, 3U},
                             .leader_hint = 1U},
                            {.table_id = table(1U),
                             .tablet_id = tablet(2U),
                             .placement_epoch = 1U,
                             .replicas = {1U, 2U},
                             .leader_hint = 1U}},
      .tablet_group_bindings = {{tablet(1U), group(11U)}, {tablet(2U), group(10U)}}};
  common::Uuid::Bytes query_bytes{};
  query_bytes.front() = std::byte{1U};
  const query::DistributedAggregatePlan plan{
      .query_id = common::Uuid{query_bytes},
      .read_policy = {.consistency = query::DistributedReadConsistency::kFollowerBoundedStale,
                      .maximum_staleness_positions = 3U},
      .fragments = {{.tablet_id = tablet(1U), .leader_node = 1U},
                    {.tablet_id = tablet(2U), .leader_node = 1U}}};
  const std::array contexts{RaftObservationNodeTlsContext{1U, &*tls_context},
                            RaftObservationNodeTlsContext{2U, &*tls_context},
                            RaftObservationNodeTlsContext{3U, &*tls_context}};
  const RaftObservationTcpBatchConstructionConfig config{.source_node_id = 3U,
                                                         .first_correlation_id = 100U,
                                                         .tls_contexts = contexts,
                                                         .authenticator = &authenticator,
                                                         .node_authorizer = &authorizer};
  auto constructed = construct_raft_observation_tcp_batch(plan, catalog, config);
  ASSERT_TRUE(constructed.has_value()) << constructed.error().to_string();
  ASSERT_EQ(constructed->pairs.size(), 2U);
  EXPECT_EQ(constructed->pairs[0].leader.request.group_id, group(10U));
  EXPECT_EQ(constructed->pairs[0].leader.request.target_node_id, 1U);
  EXPECT_EQ(constructed->pairs[0].follower.request.target_node_id, 2U);
  EXPECT_EQ(constructed->pairs[0].leader.request.correlation_id, 100U);
  EXPECT_EQ(constructed->pairs[0].follower.request.correlation_id, 101U);
  EXPECT_EQ(constructed->pairs[1].leader.request.group_id, group(11U));
  EXPECT_EQ(constructed->pairs[1].follower.request.target_node_id, 3U);
  EXPECT_EQ(constructed->pairs[1].leader.request.correlation_id, 102U);
  EXPECT_EQ(constructed->pairs[1].follower.request.correlation_id, 103U);

  const query::DistributedVectorQueryPlan vector_plan{
      .query_id = plan.query_id,
      .read_policy = plan.read_policy,
      .fragments = plan.fragments,
      .intent = {.mode = query::DistributedVectorPlanMode::kUngroupedAggregate,
                 .aggregates = {{.operation = query::VectorAggregateOperation::kCountStar}}}};
  auto vector_constructed = construct_raft_observation_tcp_batch(vector_plan, catalog, config);
  ASSERT_TRUE(vector_constructed.has_value()) << vector_constructed.error().to_string();
  ASSERT_EQ(vector_constructed->pairs.size(), constructed->pairs.size());
  for (std::size_t index = 0U; index < constructed->pairs.size(); ++index) {
    EXPECT_EQ(vector_constructed->pairs[index].leader.request,
              constructed->pairs[index].leader.request);
    EXPECT_EQ(vector_constructed->pairs[index].follower.request,
              constructed->pairs[index].follower.request);
    EXPECT_EQ(vector_constructed->pairs[index].leader.route.node_id,
              constructed->pairs[index].leader.route.node_id);
    EXPECT_EQ(vector_constructed->pairs[index].follower.route.node_id,
              constructed->pairs[index].follower.route.node_id);
  }

  auto overflow = config;
  overflow.first_correlation_id = std::numeric_limits<std::uint64_t>::max();
  EXPECT_EQ(construct_raft_observation_tcp_batch(plan, catalog, overflow).error().code(),
            common::StatusCode::kOutOfRange);
}

} // namespace
} // namespace chronos::cluster
