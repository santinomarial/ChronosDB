#include "chronos/cluster/raft_observation_tcp_pair_acquisition.hpp"
#include "chronos/cluster/raft_observation_tcp_server.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <stdexcept>

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
  bytes.front() = std::byte{9U};
  return raft::GroupId{bytes};
}

[[nodiscard]] raft::RaftGroupObservation leader_observation() {
  return {.group_id = group(),
          .node_id = 1U,
          .role = raft::Role::kLeader,
          .current_term = 7U,
          .leader_id = 1U,
          .last_log_index = 14U,
          .commit_index = 13U,
          .applied_index = 13U,
          .voters = {1U, 2U, 3U},
          .committed_voters = {1U, 2U, 3U}};
}

[[nodiscard]] raft::RaftGroupObservation follower_observation(const raft::Term term = 7U) {
  return {.group_id = group(),
          .node_id = 2U,
          .role = raft::Role::kFollower,
          .current_term = term,
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
    return (principal == 91U && node == 1U) || (principal == 92U && node == 2U) ||
           (principal == 93U && node == 3U);
  }
};

class Service final : public RaftObservationService {
public:
  explicit Service(raft::RaftGroupObservation value) : value_(std::move(value)) {}
  common::Result<raft::RaftGroupObservation> observe(const raft::GroupId& requested) override {
    ++calls;
    EXPECT_EQ(requested, group());
    return value_;
  }
  std::size_t calls{};

private:
  raft::RaftGroupObservation value_;
};

class FailingService final : public RaftObservationService {
public:
  common::Result<raft::RaftGroupObservation> observe(const raft::GroupId&) override {
    ++calls;
    return common::make_unexpected(
        common::Status{common::StatusCode::kInvalidArgument, "observation rejected"});
  }
  std::size_t calls{};
};

[[nodiscard]] RaftObservationTcpServerConfig server_config(Authenticator& authenticator,
                                                           RaftObservationReceiver& receiver) {
  return {.listener = {},
          .tls = server_tls_config(),
          .authenticator = &authenticator,
          .receiver = &receiver,
          .session_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                             .exchange_timeout = std::chrono::milliseconds{1000}},
          .maximum_connections = 4U,
          .maximum_accepts_per_poll = 4U};
}

[[nodiscard]] RaftObservationTcpAcquisitionConfig
target_config(const raft::NodeId target, const std::uint64_t correlation,
              const network::Ipv4Endpoint endpoint, const network::TlsClientContext& tls_context,
              Authenticator& authenticator, Authorizer& authorizer) {
  return {.route = {.node_id = target, .endpoints = {endpoint}, .tls_context = &tls_context},
          .authenticator = &authenticator,
          .node_authorizer = &authorizer,
          .request = {3U, target, group(), correlation},
          .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                             .exchange_timeout = std::chrono::milliseconds{1000}},
          .connect_timeout = std::chrono::milliseconds{1000},
          .retry = {.maximum_attempts = 1U,
                    .initial_backoff = std::chrono::milliseconds{1},
                    .maximum_backoff = std::chrono::milliseconds{1}}};
}

struct PairFixture {
  Authorizer authorizer;
  Authenticator client_authenticator{93U};
  Authenticator leader_authenticator{91U};
  Authenticator follower_authenticator{92U};
  std::unique_ptr<Service> leader_service;
  std::unique_ptr<Service> follower_service;
  std::unique_ptr<RaftObservationReceiver> leader_receiver;
  std::unique_ptr<RaftObservationReceiver> follower_receiver;
  std::unique_ptr<RaftObservationTcpServer> leader_server;
  std::unique_ptr<RaftObservationTcpServer> follower_server;
  std::unique_ptr<network::TlsClientContext> tls_context;

  explicit PairFixture(const raft::Term follower_term) {
    leader_service = std::make_unique<Service>(leader_observation());
    follower_service = std::make_unique<Service>(follower_observation(follower_term));
    auto leader_receiver_value = RaftObservationReceiver::create(
        {.local_node_id = 1U, .authorizer = &authorizer, .service = leader_service.get()});
    auto follower_receiver_value = RaftObservationReceiver::create(
        {.local_node_id = 2U, .authorizer = &authorizer, .service = follower_service.get()});
    if (!leader_receiver_value.has_value() || !follower_receiver_value.has_value())
      throw std::runtime_error("creating pair observation receivers failed");
    leader_receiver = std::make_unique<RaftObservationReceiver>(*leader_receiver_value);
    follower_receiver = std::make_unique<RaftObservationReceiver>(*follower_receiver_value);
    auto leader_server_value =
        RaftObservationTcpServer::start(server_config(client_authenticator, *leader_receiver));
    auto follower_server_value =
        RaftObservationTcpServer::start(server_config(client_authenticator, *follower_receiver));
    auto tls_context_value = network::TlsClientContext::create(client_tls_config());
    if (!leader_server_value.has_value() || !follower_server_value.has_value() ||
        !tls_context_value.has_value()) {
      throw std::runtime_error("creating pair observation transports failed");
    }
    leader_server = std::make_unique<RaftObservationTcpServer>(std::move(*leader_server_value));
    follower_server = std::make_unique<RaftObservationTcpServer>(std::move(*follower_server_value));
    tls_context = std::make_unique<network::TlsClientContext>(std::move(*tls_context_value));
  }

  [[nodiscard]] RaftObservationTcpPairAcquisitionConfig config() {
    return {.leader = target_config(1U, 31U, leader_server->bound_endpoint(), *tls_context,
                                    leader_authenticator, authorizer),
            .follower = target_config(2U, 32U, follower_server->bound_endpoint(), *tls_context,
                                      follower_authenticator, authorizer)};
  }

  void drive_servers() const {
    ASSERT_TRUE(leader_server->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(follower_server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
};

TEST(RaftObservationTcpPairAcquisitionTest, FansOutAndPublishesOnlyCompleteCorrelatedPair) {
  PairFixture fixture{7U};
  auto acquisition = RaftObservationTcpPairAcquisition::create(fixture.config());
  ASSERT_TRUE(acquisition.has_value()) << acquisition.error().to_string();
  ASSERT_TRUE(acquisition->poll_once(std::chrono::milliseconds{0}).is_ok());
  EXPECT_EQ(acquisition->metrics().leader.attempts_started, 1U);
  EXPECT_EQ(acquisition->metrics().follower.attempts_started, 1U);
  EXPECT_EQ(acquisition->result().error().code(), common::StatusCode::kInvalidArgument);

  for (std::size_t iteration = 0U;
       iteration < 4096U &&
       acquisition->state() == RaftObservationTcpPairAcquisitionState::kRunning;
       ++iteration) {
    fixture.drive_servers();
    ASSERT_TRUE(acquisition->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(acquisition->state(), RaftObservationTcpPairAcquisitionState::kComplete)
      << acquisition->failure().to_string();
  EXPECT_EQ(acquisition->poll_targets().size, 0U);
  EXPECT_FALSE(acquisition->wake_deadline().has_value());
  auto authority = acquisition->result();
  ASSERT_TRUE(authority.has_value()) << authority.error().to_string();
  EXPECT_EQ(authority->leader_observation, leader_observation());
  EXPECT_EQ(authority->follower_observation, follower_observation());
  EXPECT_EQ(fixture.leader_service->calls, 1U);
  EXPECT_EQ(fixture.follower_service->calls, 1U);
  EXPECT_EQ(acquisition->metrics().leader.completed_attempts, 1U);
  EXPECT_EQ(acquisition->metrics().follower.completed_attempts, 1U);
}

TEST(RaftObservationTcpPairAcquisitionTest, RejectsTermMismatchAfterBothExactObservations) {
  PairFixture fixture{8U};
  auto acquisition = RaftObservationTcpPairAcquisition::create(fixture.config());
  ASSERT_TRUE(acquisition.has_value()) << acquisition.error().to_string();
  for (std::size_t iteration = 0U;
       iteration < 4096U &&
       acquisition->state() == RaftObservationTcpPairAcquisitionState::kRunning;
       ++iteration) {
    const common::Status progress = acquisition->poll_once(std::chrono::milliseconds{1});
    ASSERT_TRUE(progress.is_ok() || progress.code() == common::StatusCode::kUnavailable);
    fixture.drive_servers();
  }
  EXPECT_EQ(acquisition->state(), RaftObservationTcpPairAcquisitionState::kFailed);
  EXPECT_EQ(acquisition->failure().code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(acquisition->result().error(), acquisition->failure());
  EXPECT_EQ(acquisition->poll_targets().size, 0U);
  EXPECT_FALSE(acquisition->wake_deadline().has_value());
  EXPECT_EQ(fixture.leader_service->calls, 1U);
  EXPECT_EQ(fixture.follower_service->calls, 1U);
}

TEST(RaftObservationTcpPairAcquisitionTest, CancelsFollowerWhenLeaderFails) {
  Authorizer authorizer;
  Authenticator client_authenticator{93U};
  Authenticator leader_authenticator{91U};
  Authenticator follower_authenticator{92U};
  FailingService service;
  auto receiver = RaftObservationReceiver::create(
      {.local_node_id = 1U, .authorizer = &authorizer, .service = &service});
  ASSERT_TRUE(receiver.has_value());
  auto leader_server =
      RaftObservationTcpServer::start(server_config(client_authenticator, *receiver));
  auto unresponsive_follower = network::TcpListener::bind();
  auto tls_context = network::TlsClientContext::create(client_tls_config());
  ASSERT_TRUE(leader_server.has_value());
  ASSERT_TRUE(unresponsive_follower.has_value());
  ASSERT_TRUE(tls_context.has_value());
  auto acquisition = RaftObservationTcpPairAcquisition::create(
      {.leader = target_config(1U, 41U, leader_server->bound_endpoint(), *tls_context,
                               leader_authenticator, authorizer),
       .follower = target_config(2U, 42U, unresponsive_follower->bound_endpoint(), *tls_context,
                                 follower_authenticator, authorizer)});
  ASSERT_TRUE(acquisition.has_value());
  for (std::size_t iteration = 0U;
       iteration < 4096U &&
       acquisition->state() == RaftObservationTcpPairAcquisitionState::kRunning;
       ++iteration) {
    const common::Status progress = acquisition->poll_once(std::chrono::milliseconds{1});
    ASSERT_TRUE(progress.is_ok() || progress.code() == common::StatusCode::kInvalidArgument);
    ASSERT_TRUE(leader_server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(acquisition->state(), RaftObservationTcpPairAcquisitionState::kFailed);
  EXPECT_EQ(acquisition->failure().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(service.calls, 1U);
  EXPECT_EQ(acquisition->metrics().follower.attempts_started, 1U);
  EXPECT_EQ(acquisition->metrics().follower.active_attempts, 0U);
  EXPECT_EQ(acquisition->metrics().follower.completed_attempts, 0U);
}

} // namespace
} // namespace chronos::cluster
