#include "chronos/cluster/raft_observation_tcp_acquisition.hpp"
#include "chronos/cluster/raft_observation_tcp_server.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <ranges>

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
  bytes.front() = std::byte{8U};
  return raft::GroupId{bytes};
}

[[nodiscard]] raft::RaftGroupObservation observation() {
  return {.group_id = group(),
          .node_id = 2U,
          .role = raft::Role::kFollower,
          .current_term = 6U,
          .leader_id = 1U,
          .last_log_index = 13U,
          .commit_index = 12U,
          .applied_index = 11U,
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
    return (principal == 91U && node == 1U) || (principal == 92U && node == 2U);
  }
};

class Service final : public RaftObservationService {
public:
  common::Result<raft::RaftGroupObservation> observe(const raft::GroupId& requested) override {
    ++calls;
    EXPECT_EQ(requested, group());
    return observation();
  }
  std::size_t calls{};
};

[[nodiscard]] RaftObservationTcpAcquisitionConfig
acquisition_config(std::vector<network::Ipv4Endpoint> endpoints,
                   const network::TlsClientContext& tls_context, Authenticator& authenticator,
                   Authorizer& authorizer) {
  return {.route = {.node_id = 2U, .endpoints = std::move(endpoints), .tls_context = &tls_context},
          .authenticator = &authenticator,
          .node_authorizer = &authorizer,
          .request = {1U, 2U, group(), 20U},
          .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                             .exchange_timeout = std::chrono::milliseconds{1000}},
          .connect_timeout = std::chrono::milliseconds{1000},
          .retry = {.maximum_attempts = 2U,
                    .initial_backoff = std::chrono::milliseconds{1},
                    .maximum_backoff = std::chrono::milliseconds{1}}};
}

TEST(RaftObservationTcpAcquisitionTest, RotatesAddressesWithinOneFiniteRequestBudget) {
  auto refused_listener = network::TcpListener::bind();
  ASSERT_TRUE(refused_listener.has_value());
  const network::Ipv4Endpoint refused = refused_listener->bound_endpoint();
  ASSERT_TRUE(refused_listener->close().is_ok());

  Authorizer authorizer;
  Service service;
  auto receiver = RaftObservationReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .service = &service});
  ASSERT_TRUE(receiver.has_value());
  Authenticator client_authenticator{91U};
  auto server = RaftObservationTcpServer::start(
      {.listener = {},
       .tls = server_tls_config(),
       .authenticator = &client_authenticator,
       .receiver = &*receiver,
       .session_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000}},
       .maximum_connections = 4U,
       .maximum_accepts_per_poll = 4U});
  ASSERT_TRUE(server.has_value()) << server.error().to_string();
  auto tls_context = network::TlsClientContext::create(client_tls_config());
  ASSERT_TRUE(tls_context.has_value());
  Authenticator server_authenticator{92U};
  auto acquisition = RaftObservationTcpAcquisition::create(acquisition_config(
      {refused, server->bound_endpoint()}, *tls_context, server_authenticator, authorizer));
  ASSERT_TRUE(acquisition.has_value()) << acquisition.error().to_string();
  EXPECT_EQ(acquisition->metrics().attempts_started, 0U);

  for (std::size_t iteration = 0U;
       iteration < 4096U && acquisition->state() == RaftObservationTcpAcquisitionState::kRunning;
       ++iteration) {
    const common::Status progress = acquisition->poll_once(std::chrono::milliseconds{1});
    ASSERT_TRUE(progress.is_ok()) << progress.to_string();
    ASSERT_TRUE(server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(acquisition->state(), RaftObservationTcpAcquisitionState::kComplete)
      << acquisition->failure().to_string();
  auto acquired = acquisition->result();
  ASSERT_TRUE(acquired.has_value()) << acquired.error().to_string();
  EXPECT_EQ(*acquired, observation());
  EXPECT_EQ(acquisition->descriptor(), -1);
  EXPECT_FALSE(acquisition->interest().want_read);
  EXPECT_FALSE(acquisition->interest().want_write);
  EXPECT_FALSE(acquisition->wake_deadline().has_value());
  EXPECT_EQ(service.calls, 1U);
  const auto metrics = acquisition->metrics();
  EXPECT_EQ(metrics.attempts_started, 2U);
  EXPECT_EQ(metrics.retries_started, 1U);
  EXPECT_EQ(metrics.failed_attempts, 1U);
  EXPECT_EQ(metrics.completed_attempts, 1U);
  EXPECT_EQ(metrics.active_attempts, 0U);
  EXPECT_EQ(server->metrics().accepted_connections, 1U);
}

TEST(RaftObservationTcpAcquisitionTest, RejectsAmbiguousRoutesAndCancelsActiveAttempt) {
  Authorizer authorizer;
  Authenticator authenticator{92U};
  auto tls_context = network::TlsClientContext::create(client_tls_config());
  ASSERT_TRUE(tls_context.has_value());
  const network::Ipv4Endpoint endpoint{{127U, 0U, 0U, 1U}, 12345U};
  auto duplicate =
      acquisition_config({endpoint, endpoint}, *tls_context, authenticator, authorizer);
  EXPECT_EQ(RaftObservationTcpAcquisition::create(std::move(duplicate)).error().code(),
            common::StatusCode::kInvalidArgument);
  auto wrong_node = acquisition_config({endpoint}, *tls_context, authenticator, authorizer);
  wrong_node.route.node_id = 3U;
  EXPECT_EQ(RaftObservationTcpAcquisition::create(std::move(wrong_node)).error().code(),
            common::StatusCode::kInvalidArgument);

  auto listener = network::TcpListener::bind();
  ASSERT_TRUE(listener.has_value());
  auto acquisition = RaftObservationTcpAcquisition::create(
      acquisition_config({listener->bound_endpoint()}, *tls_context, authenticator, authorizer));
  ASSERT_TRUE(acquisition.has_value());
  ASSERT_TRUE(acquisition->poll_once(std::chrono::milliseconds{0}).is_ok());
  EXPECT_EQ(acquisition->metrics().attempts_started, 1U);
  EXPECT_EQ(acquisition->metrics().active_attempts, 1U);
  const common::Status cancelled = acquisition->cancel();
  EXPECT_EQ(cancelled.code(), common::StatusCode::kCancelled);
  EXPECT_EQ(acquisition->state(), RaftObservationTcpAcquisitionState::kCancelled);
  EXPECT_EQ(acquisition->metrics().active_attempts, 0U);
  EXPECT_EQ(acquisition->descriptor(), -1);
  EXPECT_FALSE(acquisition->interest().want_read);
  EXPECT_FALSE(acquisition->interest().want_write);
  EXPECT_FALSE(acquisition->wake_deadline().has_value());
  EXPECT_EQ(acquisition->result().error(), cancelled);
  EXPECT_EQ(acquisition->poll_once(std::chrono::milliseconds{0}), cancelled);
}

TEST(RaftObservationTcpAcquisitionTest, ResolvesCommittedNumericAndDnsRoutesCanonically) {
  auto tls_context = network::TlsClientContext::create(client_tls_config());
  ASSERT_TRUE(tls_context.has_value());
  const raft::MetadataCatalogSnapshot catalog{
      .applied_index = 4U, .cluster_nodes = {{1U, "127.0.0.1:10001"}, {2U, "localhost:10002"}}};
  const std::array<raft::NodeId, 2U> targets{1U, 2U};
  const std::array contexts{RaftObservationNodeTlsContext{1U, &*tls_context},
                            RaftObservationNodeTlsContext{2U, &*tls_context}};
  auto routes = resolve_raft_observation_tcp_routes(catalog, targets, contexts);
  ASSERT_TRUE(routes.has_value()) << routes.error().to_string();
  ASSERT_EQ(routes->size(), 2U);
  EXPECT_EQ((*routes)[0].node_id, 1U);
  ASSERT_EQ((*routes)[0].endpoints.size(), 1U);
  EXPECT_EQ((*routes)[0].endpoints.front(), (network::Ipv4Endpoint{{127U, 0U, 0U, 1U}, 10001U}));
  EXPECT_EQ((*routes)[1].node_id, 2U);
  ASSERT_FALSE((*routes)[1].endpoints.empty());
  EXPECT_TRUE(
      std::ranges::all_of((*routes)[1].endpoints, [](const network::Ipv4Endpoint& endpoint) {
        return endpoint.port == 10002U &&
               endpoint.address == std::array<std::uint8_t, 4U>{127U, 0U, 0U, 1U};
      }));
  EXPECT_EQ((*routes)[0].tls_context, &*tls_context);
  EXPECT_EQ((*routes)[1].tls_context, &*tls_context);

  const std::array<raft::NodeId, 2U> duplicate_targets{1U, 1U};
  EXPECT_EQ(
      resolve_raft_observation_tcp_routes(catalog, duplicate_targets, contexts).error().code(),
      common::StatusCode::kInvalidArgument);
  const std::array missing_context{RaftObservationNodeTlsContext{1U, &*tls_context}};
  EXPECT_EQ(resolve_raft_observation_tcp_routes(catalog, targets, missing_context).error().code(),
            common::StatusCode::kUnavailable);
  EXPECT_EQ(resolve_raft_observation_tcp_routes(catalog, targets, contexts, {.maximum_routes = 1U})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
}

} // namespace
} // namespace chronos::cluster
