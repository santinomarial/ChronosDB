#include "chronos/cluster/raft_read_authority_tcp_batch_acquisition.hpp"
#include "chronos/cluster/raft_read_authority_tcp_server.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <utility>
#include <vector>

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

[[nodiscard]] raft::GroupId group(const std::uint8_t tag) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(tag);
  return raft::GroupId{bytes};
}

[[nodiscard]] RaftReadAuthority authority(const raft::GroupId& group_id,
                                          const raft::NodeId leader) {
  return {
      .barrier = {.group_id = group_id, .barrier = {.term = 6U, .context = 10U, .read_index = 12U}},
      .observation = {.group_id = group_id,
                      .node_id = leader,
                      .role = raft::Role::kLeader,
                      .current_term = 6U,
                      .leader_id = leader,
                      .last_log_index = 13U,
                      .commit_index = 12U,
                      .applied_index = 11U,
                      .voters = {1U, 2U, 3U},
                      .committed_voters = {1U, 2U, 3U}}};
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

class Service final : public RaftReadAuthorityService {
public:
  Service(raft::GroupId expected_group, const raft::NodeId leader)
      : expected_group_(expected_group), authority_(authority(expected_group, leader)) {}

  common::Result<RaftReadAuthority> acquire(const raft::GroupId& requested) override {
    ++calls;
    EXPECT_EQ(requested, expected_group_);
    return authority_;
  }

  std::size_t calls{};

private:
  raft::GroupId expected_group_;
  RaftReadAuthority authority_;
};

[[nodiscard]] RaftReadAuthorityTcpAcquisitionConfig
acquisition_config(const network::Ipv4Endpoint endpoint,
                   const network::TlsClientContext& tls_context, Authenticator& authenticator,
                   Authorizer& authorizer, const raft::NodeId target, const raft::GroupId& group_id,
                   const std::uint64_t correlation) {
  return {.route = {.node_id = target, .endpoints = {endpoint}, .tls_context = &tls_context},
          .authenticator = &authenticator,
          .node_authorizer = &authorizer,
          .request = {1U, target, group_id, correlation},
          .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                             .exchange_timeout = std::chrono::milliseconds{1000}},
          .connect_timeout = std::chrono::milliseconds{1000},
          .retry = {.maximum_attempts = 1U,
                    .initial_backoff = std::chrono::milliseconds{1},
                    .maximum_backoff = std::chrono::milliseconds{1}}};
}

TEST(RaftReadAuthorityTcpBatchAcquisitionTest, PublishesOnlyCompleteCanonicalAuthorityVector) {
  Authorizer authorizer;
  Authenticator client_authenticator{91U};
  Service service_two{group(2U), 2U};
  Service service_three{group(3U), 3U};
  auto receiver_two = RaftReadAuthorityReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .service = &service_two});
  auto receiver_three = RaftReadAuthorityReceiver::create(
      {.local_node_id = 3U, .authorizer = &authorizer, .service = &service_three});
  ASSERT_TRUE(receiver_two.has_value());
  ASSERT_TRUE(receiver_three.has_value());
  auto server_two = RaftReadAuthorityTcpServer::start(
      {.listener = {},
       .tls = server_tls_config(),
       .authenticator = &client_authenticator,
       .receiver = &*receiver_two,
       .session_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000}},
       .maximum_connections = 4U,
       .maximum_accepts_per_poll = 4U});
  auto server_three = RaftReadAuthorityTcpServer::start(
      {.listener = {},
       .tls = server_tls_config(),
       .authenticator = &client_authenticator,
       .receiver = &*receiver_three,
       .session_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000}},
       .maximum_connections = 4U,
       .maximum_accepts_per_poll = 4U});
  ASSERT_TRUE(server_two.has_value());
  ASSERT_TRUE(server_three.has_value());
  auto tls_context = network::TlsClientContext::create(client_tls_config());
  ASSERT_TRUE(tls_context.has_value());
  Authenticator server_two_authenticator{92U};
  Authenticator server_three_authenticator{93U};

  std::vector<RaftReadAuthorityTcpAcquisitionConfig> groups;
  groups.push_back(acquisition_config(server_two->bound_endpoint(), *tls_context,
                                      server_two_authenticator, authorizer, 2U, group(2U), 20U));
  groups.push_back(acquisition_config(server_three->bound_endpoint(), *tls_context,
                                      server_three_authenticator, authorizer, 3U, group(3U), 21U));
  auto batch = RaftReadAuthorityTcpBatchAcquisition::create({.groups = std::move(groups)});
  ASSERT_TRUE(batch.has_value()) << batch.error().to_string();
  EXPECT_EQ(batch->metrics().total_groups, 2U);
  EXPECT_EQ(batch->result().error().code(), common::StatusCode::kInvalidArgument);

  for (std::size_t iteration = 0U;
       iteration < 4096U && batch->state() == RaftReadAuthorityTcpBatchAcquisitionState::kRunning;
       ++iteration) {
    ASSERT_TRUE(batch->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(server_two->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(server_three->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(batch->state(), RaftReadAuthorityTcpBatchAcquisitionState::kComplete)
      << batch->failure().to_string();
  auto result = batch->result();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_EQ(result->size(), 2U);
  EXPECT_EQ(result->at(0).barrier, authority(group(2U), 2U).barrier);
  EXPECT_EQ(result->at(0).observation, authority(group(2U), 2U).observation);
  EXPECT_EQ(result->at(1).barrier, authority(group(3U), 3U).barrier);
  EXPECT_EQ(result->at(1).observation, authority(group(3U), 3U).observation);
  EXPECT_EQ(service_two.calls, 1U);
  EXPECT_EQ(service_three.calls, 1U);
  const auto metrics = batch->metrics();
  EXPECT_EQ(metrics.completed_groups, 2U);
  EXPECT_EQ(metrics.active_groups, 0U);
}

TEST(RaftReadAuthorityTcpBatchAcquisitionTest, RejectsAmbiguityAndCancelsWholeFailedAttempt) {
  Authorizer authorizer;
  auto tls_context = network::TlsClientContext::create(client_tls_config());
  ASSERT_TRUE(tls_context.has_value());
  Authenticator server_two_authenticator{92U};
  Authenticator server_three_authenticator{93U};
  auto open_listener = network::TcpListener::bind();
  auto refused_listener = network::TcpListener::bind();
  ASSERT_TRUE(open_listener.has_value());
  ASSERT_TRUE(refused_listener.has_value());
  const auto refused_endpoint = refused_listener->bound_endpoint();
  ASSERT_TRUE(refused_listener->close().is_ok());

  const auto first = acquisition_config(open_listener->bound_endpoint(), *tls_context,
                                        server_two_authenticator, authorizer, 2U, group(2U), 20U);
  const auto second = acquisition_config(refused_endpoint, *tls_context, server_three_authenticator,
                                         authorizer, 3U, group(3U), 21U);
  auto duplicate_group = RaftReadAuthorityTcpBatchAcquisition::create({.groups = {first, first}});
  EXPECT_EQ(duplicate_group.error().code(), common::StatusCode::kInvalidArgument);
  auto duplicate_correlation = second;
  duplicate_correlation.request.correlation_id = 20U;
  auto ambiguous =
      RaftReadAuthorityTcpBatchAcquisition::create({.groups = {first, duplicate_correlation}});
  EXPECT_EQ(ambiguous.error().code(), common::StatusCode::kInvalidArgument);

  auto batch = RaftReadAuthorityTcpBatchAcquisition::create({.groups = {first, second}});
  ASSERT_TRUE(batch.has_value()) << batch.error().to_string();
  for (std::size_t iteration = 0U;
       iteration < 1024U && batch->state() == RaftReadAuthorityTcpBatchAcquisitionState::kRunning;
       ++iteration) {
    static_cast<void>(batch->poll_once(std::chrono::milliseconds{1}));
  }
  ASSERT_EQ(batch->state(), RaftReadAuthorityTcpBatchAcquisitionState::kFailed);
  // A refused nonblocking connect is reported as POLLERR on some kernels and SO_ERROR on others.
  EXPECT_TRUE(batch->failure().code() == common::StatusCode::kUnavailable ||
              batch->failure().code() == common::StatusCode::kIoError);
  EXPECT_EQ(batch->result().error(), batch->failure());
  EXPECT_EQ(batch->metrics().completed_groups, 0U);
  EXPECT_EQ(batch->metrics().active_groups, 0U);
  EXPECT_EQ(batch->poll_once(std::chrono::milliseconds{0}), batch->failure());

  auto second_open_listener = network::TcpListener::bind();
  ASSERT_TRUE(second_open_listener.has_value());
  const auto open_second =
      acquisition_config(second_open_listener->bound_endpoint(), *tls_context,
                         server_three_authenticator, authorizer, 3U, group(3U), 21U);
  auto cancelled = RaftReadAuthorityTcpBatchAcquisition::create({.groups = {first, open_second}});
  ASSERT_TRUE(cancelled.has_value());
  ASSERT_TRUE(cancelled->poll_once(std::chrono::milliseconds{0}).is_ok());
  EXPECT_EQ(cancelled->metrics().active_groups, 2U);
  const common::Status cancellation = cancelled->cancel();
  EXPECT_EQ(cancellation.code(), common::StatusCode::kCancelled);
  EXPECT_EQ(cancelled->state(), RaftReadAuthorityTcpBatchAcquisitionState::kCancelled);
  EXPECT_EQ(cancelled->metrics().active_groups, 0U);
  EXPECT_EQ(cancelled->result().error(), cancellation);
  EXPECT_EQ(cancelled->poll_once(std::chrono::milliseconds{0}), cancellation);
}

} // namespace
} // namespace chronos::cluster
