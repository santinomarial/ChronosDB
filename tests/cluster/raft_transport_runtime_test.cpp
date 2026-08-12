#include "chronos/cluster/raft_transport_runtime.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <system_error>
#include <unistd.h>

namespace chronos::cluster {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-raft-transport-runtime-XXXXXX").string();
    if (char* created = ::mkdtemp(pattern.data()); created != nullptr)
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
[[nodiscard]] raft::GroupId group() {
  common::Uuid::Bytes bytes{};
  bytes.fill(std::byte{9U});
  return raft::GroupId{bytes};
}

class Authenticator final : public network::ConnectionAuthenticator {
public:
  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest&) override {
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = principal};
  }
  std::uint64_t principal{800U};
};

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal,
                                      const raft::NodeId node) const override {
    return (principal == 700U && node == 1U) || (principal == 800U && node == 2U);
  }
};

class DeadlineSource final : public raft::RaftElectionDeadlineSource {
public:
  common::Result<raft::RaftTimerRuntime::TimePoint>
  next_election_deadline(const raft::GroupId&, raft::Term,
                         const raft::RaftTimerRuntime::TimePoint now) override {
    return now + delay;
  }
  std::chrono::milliseconds delay{5};
};

[[nodiscard]] network::TlsClientConfig client_tls_config() {
  return {.certificate_chain_file = fixture("client.pem").string(),
          .private_key_file = fixture("client-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string(),
          .expected_server_identity = "127.0.0.1"};
}

TEST(RaftTransportRuntimeTest, PollsDeadlineAndDurableWakeupIntoOwnedTimerResult) {
  TemporaryDirectory directory;
  auto durable = raft::AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{group(), {1U}}});
  ASSERT_TRUE(durable.has_value()) << durable.error().to_string();
  Authorizer authorizer;
  Authenticator inbound_authenticator;
  inbound_authenticator.principal = 700U;
  auto receiver = RaftTransportReceiver::create(
      {.local_node_id = 1U, .authorizer = &authorizer, .runtime = &*durable});
  ASSERT_TRUE(receiver.has_value());
  auto inbound = RaftTransportTcpServer::start(
      {.tls = {.certificate_chain_file = fixture("server.pem").string(),
               .private_key_file = fixture("server-key.pem").string(),
               .trust_store_file = fixture("ca.pem").string()},
       .authenticator = &inbound_authenticator,
       .receiver = &*receiver,
       .maximum_connections = 2U});
  ASSERT_TRUE(inbound.has_value()) << inbound.error().to_string();

  auto client_context =
      network::TlsClientContext::create({.certificate_chain_file = fixture("client.pem").string(),
                                         .private_key_file = fixture("client-key.pem").string(),
                                         .trust_store_file = fixture("ca.pem").string(),
                                         .expected_server_identity = "127.0.0.1"});
  auto dummy_peer = network::TcpListener::bind({});
  ASSERT_TRUE(client_context.has_value());
  ASSERT_TRUE(dummy_peer.has_value());
  Authenticator outbound_authenticator;
  auto outbound = RaftTransportPeerManager::create(
      {.local_node_id = 1U,
       .peers = {{.connector =
                      {.remote_endpoint = dummy_peer->bound_endpoint(),
                       .tls_context = &*client_context,
                       .carrier = {.local_node_id = 1U,
                                   .peer_node_id = 2U,
                                   .authenticator = &outbound_authenticator,
                                   .node_authorizer = &authorizer,
                                   .peer_ipv4_address = dummy_peer->bound_endpoint().address,
                                   .limits = {.maximum_queued_frames = 2U,
                                              .maximum_queued_bytes = 4096U,
                                              .handshake_timeout = std::chrono::milliseconds{100},
                                              .frame_write_timeout =
                                                  std::chrono::milliseconds{100}}},
                       .connect_timeout = std::chrono::milliseconds{100}},
                  .limits = {.initial_backoff = std::chrono::milliseconds{10},
                             .maximum_backoff = std::chrono::milliseconds{20}}}},
       .pool = {.maximum_peers = 1U}});
  ASSERT_TRUE(outbound.has_value()) << outbound.error().to_string();
  DeadlineSource deadlines;
  auto timers = raft::RaftTimerDriver::create(
      {.runtime = &*durable,
       .election_deadlines = &deadlines,
       .limits = {.maximum_inflight_actions = 2U,
                  .maximum_completed_actions = 2U,
                  .timers = {.maximum_groups = 1U,
                             .maximum_actions_per_poll = 1U,
                             .heartbeat_interval = std::chrono::milliseconds{20}}}});
  ASSERT_TRUE(timers.has_value());

  {
    auto transport = RaftTransportRuntime::create(&*durable, std::move(*timers),
                                                  std::move(*inbound), std::move(*outbound),
                                                  {.maximum_pending_results = 2U,
                                                   .maximum_pending_application_requests = 1U,
                                                   .maximum_results_per_poll = 2U,
                                                   .maximum_poll_descriptors = 8U});
    ASSERT_TRUE(transport.has_value()) << transport.error().to_string();
    const auto start = std::chrono::steady_clock::now();
    ASSERT_TRUE(transport
                    ->add_group({.group_id = group(),
                                 .node_id = 1U,
                                 .role = raft::Role::kFollower,
                                 .current_term = 0U},
                                start)
                    .is_ok());
    common::Result<RaftTransportRuntimeResult> result = common::make_unexpected(
        common::Status{common::StatusCode::kUnavailable, "result not ready"});
    for (std::size_t iteration = 0U; iteration < 1024U; ++iteration) {
      ASSERT_TRUE(transport->poll_once(std::chrono::milliseconds{100}).is_ok())
          << transport->failure().to_string();
      result = transport->take_completed();
      if (result.has_value())
        break;
      ASSERT_EQ(result.error().code(), common::StatusCode::kUnavailable);
    }
    ASSERT_TRUE(result.has_value()) << result.error().to_string();
    EXPECT_EQ(result->submission_sequence, 1U);
    EXPECT_EQ(result->origin, RaftTransportRuntimeResultOrigin::kTimer);
    EXPECT_EQ(result->group_id, group());
    ASSERT_TRUE(result->timer_action.has_value());
    EXPECT_EQ(result->timer_action->kind, raft::RaftTimerActionKind::kStartElection);
    ASSERT_TRUE(result->result.status.is_ok());
    ASSERT_TRUE(result->result.transition.has_value());
    ASSERT_TRUE(result->observation.has_value());
    EXPECT_EQ(result->observation->role, raft::Role::kLeader);
    EXPECT_GE(transport->metrics().durable_wakeups, 1U);
    EXPECT_EQ(transport->metrics().completed_results, 1U);

    auto invalid_application =
        transport->try_submit_application({group(), raft::HeartbeatOperation{}});
    ASSERT_FALSE(invalid_application.has_value());
    EXPECT_EQ(invalid_application.error().code(), common::StatusCode::kInvalidArgument);
    auto application = transport->try_submit_application(
        {group(), raft::ProposeOperation{.type = 1U, .payload = {std::byte{0x42U}}}});
    ASSERT_TRUE(application.has_value()) << application.error().to_string();
    auto overflow = transport->try_submit_application({group(), raft::BeginReadBarrierOperation{}});
    ASSERT_FALSE(overflow.has_value());
    EXPECT_EQ(overflow.error().code(), common::StatusCode::kResourceExhausted);
    std::optional<RaftTransportRuntimeResult> application_result;
    for (std::size_t iteration = 0U; iteration < 1024U && !application_result.has_value();
         ++iteration) {
      ASSERT_TRUE(transport->poll_once(std::chrono::milliseconds{100}).is_ok())
          << transport->failure().to_string();
      auto next = transport->take_completed();
      if (!next.has_value()) {
        ASSERT_EQ(next.error().code(), common::StatusCode::kUnavailable);
        continue;
      }
      if (next->submission_sequence == *application)
        application_result.emplace(std::move(*next));
    }
    ASSERT_TRUE(application_result.has_value());
    EXPECT_EQ(application_result->origin, RaftTransportRuntimeResultOrigin::kApplication);
    EXPECT_EQ(application_result->group_id, group());
    ASSERT_TRUE(application_result->result.status.is_ok())
        << application_result->result.status.to_string();
    ASSERT_TRUE(application_result->result.transition.has_value());
    ASSERT_TRUE(application_result->observation.has_value());
    EXPECT_EQ(application_result->observation->group_id, group());

    auto barrier = transport->try_submit_application({group(), raft::BeginReadBarrierOperation{}});
    ASSERT_TRUE(barrier.has_value()) << barrier.error().to_string();
    application_result.reset();
    for (std::size_t iteration = 0U; iteration < 1024U && !application_result.has_value();
         ++iteration) {
      ASSERT_TRUE(transport->poll_once(std::chrono::milliseconds{100}).is_ok())
          << transport->failure().to_string();
      auto next = transport->take_completed();
      if (!next.has_value()) {
        ASSERT_EQ(next.error().code(), common::StatusCode::kUnavailable);
        continue;
      }
      if (next->submission_sequence == *barrier)
        application_result.emplace(std::move(*next));
    }
    ASSERT_TRUE(application_result.has_value());
    EXPECT_EQ(application_result->origin, RaftTransportRuntimeResultOrigin::kApplication);
    ASSERT_TRUE(application_result->result.status.is_ok())
        << application_result->result.status.to_string();
    ASSERT_TRUE(application_result->result.transition.has_value());
    ASSERT_TRUE(application_result->result.transition->read_barrier_ready.has_value());
    EXPECT_EQ(application_result->result.transition->read_barrier_ready->group_id, group());
    ASSERT_TRUE(application_result->observation.has_value());
    EXPECT_LT(application_result->observation->applied_index,
              application_result->result.transition->read_barrier_ready->barrier.read_index);
    EXPECT_EQ(transport->metrics().application_results, 2U);
    EXPECT_EQ(transport->metrics().pending_application_requests, 0U);
  }
  ASSERT_TRUE(durable->shutdown().is_ok());
}

TEST(RaftTransportRuntimeTest, RoutesDurableInboundResponseThroughUnifiedPollOwner) {
  TemporaryDirectory directory1;
  TemporaryDirectory directory2;
  auto durable1 = raft::AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory1.path().string()}, {{group(), {1U, 2U}}});
  auto durable2 = raft::AsyncDurableMultiRaftRuntime::create_new(
      2U, {.directory_path = directory2.path().string()}, {{group(), {1U, 2U}}});
  ASSERT_TRUE(durable1.has_value());
  ASSERT_TRUE(durable2.has_value());
  Authorizer authorizer;
  Authenticator node1_inbound_authenticator;
  node1_inbound_authenticator.principal = 800U;
  Authenticator node2_inbound_authenticator;
  node2_inbound_authenticator.principal = 700U;
  auto receiver1 = RaftTransportReceiver::create(
      {.local_node_id = 1U, .authorizer = &authorizer, .runtime = &*durable1});
  auto receiver2 = RaftTransportReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .runtime = &*durable2});
  ASSERT_TRUE(receiver1.has_value());
  ASSERT_TRUE(receiver2.has_value());
  auto server1 = RaftTransportTcpServer::start(
      {.tls = {.certificate_chain_file = fixture("server.pem").string(),
               .private_key_file = fixture("server-key.pem").string(),
               .trust_store_file = fixture("ca.pem").string()},
       .authenticator = &node1_inbound_authenticator,
       .receiver = &*receiver1,
       .maximum_connections = 2U});
  auto server2 = RaftTransportTcpServer::start(
      {.tls = {.certificate_chain_file = fixture("server.pem").string(),
               .private_key_file = fixture("server-key.pem").string(),
               .trust_store_file = fixture("ca.pem").string()},
       .authenticator = &node2_inbound_authenticator,
       .receiver = &*receiver2,
       .maximum_connections = 2U});
  ASSERT_TRUE(server1.has_value());
  ASSERT_TRUE(server2.has_value());
  auto client_context = network::TlsClientContext::create(client_tls_config());
  ASSERT_TRUE(client_context.has_value());
  Authenticator node1_server_authenticator;
  node1_server_authenticator.principal = 700U;
  auto outbound2 = RaftTransportPeerManager::create(
      {.local_node_id = 2U,
       .peers = {{.connector = {.remote_endpoint = server1->bound_endpoint(),
                                .tls_context = &*client_context,
                                .carrier = {.local_node_id = 2U,
                                            .peer_node_id = 1U,
                                            .authenticator = &node1_server_authenticator,
                                            .node_authorizer = &authorizer,
                                            .peer_ipv4_address = server1->bound_endpoint().address,
                                            .limits = {.maximum_queued_frames = 4U,
                                                       .maximum_queued_bytes = 4096U,
                                                       .handshake_timeout =
                                                           std::chrono::milliseconds{100},
                                                       .frame_write_timeout =
                                                           std::chrono::milliseconds{100}}},
                                .connect_timeout = std::chrono::milliseconds{100}},
                  .limits = {.initial_backoff = std::chrono::milliseconds{5},
                             .maximum_backoff = std::chrono::milliseconds{20}}}},
       .pool = {.maximum_peers = 1U}});
  ASSERT_TRUE(outbound2.has_value());
  DeadlineSource deadlines;
  deadlines.delay = std::chrono::seconds{5};
  auto timers2 = raft::RaftTimerDriver::create(
      {.runtime = &*durable2,
       .election_deadlines = &deadlines,
       .limits = {.maximum_inflight_actions = 2U,
                  .maximum_completed_actions = 2U,
                  .timers = {.maximum_groups = 1U,
                             .maximum_actions_per_poll = 1U,
                             .heartbeat_interval = std::chrono::milliseconds{20}}}});
  ASSERT_TRUE(timers2.has_value());

  {
    auto transport2 = RaftTransportRuntime::create(&*durable2, std::move(*timers2),
                                                   std::move(*server2), std::move(*outbound2),
                                                   {.maximum_pending_results = 4U,
                                                    .maximum_results_per_poll = 4U,
                                                    .maximum_poll_descriptors = 8U});
    ASSERT_TRUE(transport2.has_value()) << transport2.error().to_string();
    const auto start = std::chrono::steady_clock::now();
    ASSERT_TRUE(transport2
                    ->add_group({.group_id = group(),
                                 .node_id = 2U,
                                 .role = raft::Role::kFollower,
                                 .current_term = 0U},
                                start)
                    .is_ok());
    Authenticator node2_server_authenticator;
    node2_server_authenticator.principal = 800U;
    std::vector<std::vector<std::byte>> frames{
        raft::encode_raft_transport_envelope_v1(
            {.group_id = group(),
             .source = 1U,
             .destination = 2U,
             .message = raft::RequestVoteRequest{1U, 1U, 0U, 0U}})
            .value()};
    auto connector1 = RaftTransportTcpConnector::begin(
        std::move(frames),
        {.remote_endpoint = transport2->bound_endpoint(),
         .tls_context = &*client_context,
         .carrier = {.local_node_id = 1U,
                     .peer_node_id = 2U,
                     .authenticator = &node2_server_authenticator,
                     .node_authorizer = &authorizer,
                     .peer_ipv4_address = transport2->bound_endpoint().address,
                     .limits = {.maximum_queued_frames = 2U,
                                .maximum_queued_bytes = 4096U,
                                .handshake_timeout = std::chrono::milliseconds{100},
                                .frame_write_timeout = std::chrono::milliseconds{100}}},
         .connect_timeout = std::chrono::milliseconds{100}},
        start);
    ASSERT_TRUE(connector1.has_value());
    for (std::size_t iteration = 0U;
         iteration < 1024U && connector1->state() == RaftTransportTcpConnectorState::kConnecting;
         ++iteration) {
      ASSERT_TRUE(transport2->poll_once(std::chrono::milliseconds{0}).is_ok());
      ASSERT_TRUE(connector1->on_ready(true, std::chrono::steady_clock::now()).is_ok());
      ASSERT_TRUE(server1->poll_once(std::chrono::milliseconds{0}).is_ok());
    }
    ASSERT_EQ(connector1->state(), RaftTransportTcpConnectorState::kCarrierReady);
    auto node1_peer = connector1->take_connected_peer();
    ASSERT_TRUE(node1_peer.has_value());

    std::optional<RaftTransportRuntimeResult> node2_result;
    std::optional<RaftTransportCompletedReceive> node1_response;
    for (std::size_t iteration = 0U;
         iteration < 16'384U && (!node2_result.has_value() || !node1_response.has_value());
         ++iteration) {
      ASSERT_TRUE(
          node1_peer->carrier.on_ready(true, true, std::chrono::steady_clock::now()).is_ok());
      ASSERT_TRUE(transport2->poll_once(std::chrono::milliseconds{0}).is_ok())
          << transport2->failure().to_string();
      ASSERT_TRUE(server1->poll_once(std::chrono::milliseconds{0}).is_ok());
      if (!node2_result.has_value()) {
        auto next = transport2->take_completed();
        if (next.has_value())
          node2_result = std::move(*next);
        else
          ASSERT_EQ(next.error().code(), common::StatusCode::kUnavailable);
      }
      if (!node1_response.has_value()) {
        auto next = server1->take_completed();
        ASSERT_TRUE(next.has_value()) << next.error().to_string();
        if (next->has_value())
          node1_response = std::move(**next);
      }
    }
    ASSERT_TRUE(node2_result.has_value());
    EXPECT_EQ(node2_result->origin, RaftTransportRuntimeResultOrigin::kInbound);
    EXPECT_EQ(node2_result->remote_source_node_id, 1U);
    ASSERT_TRUE(node2_result->observation.has_value());
    EXPECT_EQ(node2_result->observation->current_term, 1U);
    ASSERT_TRUE(node1_response.has_value());
    EXPECT_EQ(node1_response->source_node_id, 2U);
    EXPECT_TRUE(node1_response->result.status.is_ok());
    EXPECT_GE(transport2->metrics().routed_results, 1U);
  }
  ASSERT_TRUE(server1->shutdown().is_ok());
  ASSERT_TRUE(durable1->shutdown().is_ok());
  ASSERT_TRUE(durable2->shutdown().is_ok());
}

} // namespace
} // namespace chronos::cluster
