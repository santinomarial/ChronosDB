#include "chronos/cluster/raft_transport_tcp_connector.hpp"
#include "chronos/cluster/raft_transport_tcp_server.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {
class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-raft-tcp-server-XXXXXX").string();
    if (char* created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  const std::filesystem::path& path() const noexcept {
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
  authenticate(const network::PeerAuthenticationRequest& request) override {
    return network::PeerAuthenticationResult{
        .authorized = request.peer_certificate_sha256.has_value(), .principal_id = principal};
  }
  std::uint64_t principal{};
};
class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal,
                                      const raft::NodeId node) const override {
    return (principal == 700U && node == 1U) || (principal == 800U && node == 2U);
  }
};

TEST(RaftTransportTcpServerTest, AcceptsPersistentTlsAndPublishesPostSyncResult) {
  TemporaryDirectory directory;
  Authorizer authorizer;
  Authenticator server_authenticator;
  server_authenticator.principal = 700U;
  Authenticator client_authenticator;
  client_authenticator.principal = 800U;
  auto runtime = raft::AsyncDurableMultiRaftRuntime::create_new(
      2U, {.directory_path = directory.path().string()}, {{group(), {1U, 2U}}});
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  auto receiver = RaftTransportReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .runtime = &*runtime});
  ASSERT_TRUE(receiver.has_value());
  auto server = RaftTransportTcpServer::start(
      {.tls = {.certificate_chain_file = fixture("server.pem").string(),
               .private_key_file = fixture("server-key.pem").string(),
               .trust_store_file = fixture("ca.pem").string()},
       .authenticator = &server_authenticator,
       .receiver = &*receiver,
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{100},
                          .frame_read_timeout = std::chrono::milliseconds{100}},
       .maximum_connections = 2U,
       .maximum_accepts_per_poll = 2U});
  ASSERT_TRUE(server.has_value()) << server.error().to_string();
  EXPECT_FALSE(server->next_deadline().has_value());
  EXPECT_GE(server->listener_descriptor(), 0);
  ASSERT_TRUE(server->interests().has_value());
  EXPECT_TRUE(server->interests()->empty());
  auto client_context =
      network::TlsClientContext::create({.certificate_chain_file = fixture("client.pem").string(),
                                         .private_key_file = fixture("client-key.pem").string(),
                                         .trust_store_file = fixture("ca.pem").string(),
                                         .expected_server_identity = "127.0.0.1"});
  ASSERT_TRUE(client_context.has_value());
  std::vector<std::vector<std::byte>> frames{
      raft::encode_raft_transport_envelope_v1({.group_id = group(),
                                               .source = 1U,
                                               .destination = 2U,
                                               .message = raft::RequestVoteRequest{1U, 1U, 0U, 0U}})
          .value()};
  const auto start = RaftTransportTcpConnector::TimePoint{};
  auto connector = RaftTransportTcpConnector::begin(
      std::move(frames),
      {.remote_endpoint = server->bound_endpoint(),
       .tls_context = &*client_context,
       .carrier = {.local_node_id = 1U,
                   .peer_node_id = 2U,
                   .authenticator = &client_authenticator,
                   .node_authorizer = &authorizer,
                   .peer_ipv4_address = server->bound_endpoint().address,
                   .limits = {.maximum_queued_frames = 2U,
                              .maximum_queued_bytes = 4096U,
                              .handshake_timeout = std::chrono::milliseconds{100},
                              .frame_write_timeout = std::chrono::milliseconds{100}}}},
      start);
  ASSERT_TRUE(connector.has_value());
  for (std::size_t iteration = 0U;
       iteration < 1024U && connector->state() == RaftTransportTcpConnectorState::kConnecting;
       ++iteration) {
    const auto server_now = std::chrono::steady_clock::now();
    ASSERT_TRUE(server->accept_ready(server_now).is_ok());
    auto inbound = server->interests();
    ASSERT_TRUE(inbound.has_value());
    for (const RaftTransportTcpServerInterest& interest : *inbound)
      ASSERT_TRUE(server->on_ready(interest.connection_id, true, true, server_now).is_ok());
    ASSERT_TRUE(server->drive(server_now).is_ok());
    ASSERT_TRUE(connector->on_ready(true, start + std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(connector->state(), RaftTransportTcpConnectorState::kCarrierReady);
  auto server_interests = server->interests();
  ASSERT_TRUE(server_interests.has_value());
  for (std::size_t iteration = 0U; iteration < 1024U && server_interests->empty(); ++iteration) {
    ASSERT_TRUE(server->accept_ready(std::chrono::steady_clock::now()).is_ok());
    server_interests = server->interests();
    ASSERT_TRUE(server_interests.has_value());
  }
  ASSERT_EQ(server_interests->size(), 1U);
  EXPECT_NE(server_interests->front().connection_id, 0U);
  EXPECT_GE(server_interests->front().descriptor, 0);
  auto peer = connector->take_connected_peer();
  ASSERT_TRUE(peer.has_value());
  std::optional<RaftTransportCompletedReceive> completed;
  for (std::size_t iteration = 0U; iteration < 8192U && !completed.has_value(); ++iteration) {
    ASSERT_TRUE(peer->carrier.on_ready(true, true, start + std::chrono::milliseconds{2}).is_ok());
    const auto server_now = std::chrono::steady_clock::now();
    auto inbound = server->interests();
    ASSERT_TRUE(inbound.has_value());
    for (const RaftTransportTcpServerInterest& interest : *inbound)
      ASSERT_TRUE(server->on_ready(interest.connection_id, true, true, server_now).is_ok());
    ASSERT_TRUE(server->drive(server_now).is_ok());
    auto next = server->take_completed();
    ASSERT_TRUE(next.has_value()) << next.error().to_string();
    if (next->has_value())
      completed = std::move(**next);
  }
  ASSERT_TRUE(completed.has_value());
  EXPECT_EQ(completed->submission_sequence, 1U);
  EXPECT_EQ(completed->group_id, group());
  EXPECT_EQ(completed->source_node_id, 1U);
  ASSERT_TRUE(completed->result.status.is_ok());
  ASSERT_TRUE(completed->result.transition.has_value());
  ASSERT_TRUE(completed->observation.has_value());
  EXPECT_EQ(completed->observation->group_id, group());
  EXPECT_EQ(completed->observation->current_term, 1U);
  ASSERT_EQ(completed->result.transition->outbound.size(), 1U);
  EXPECT_EQ(completed->result.transition->outbound.front().outbound.destination, 1U);
  EXPECT_EQ(server->metrics().completed_results, 1U);
  EXPECT_EQ(server->metrics().active_connections, 1U);
  EXPECT_TRUE(server->next_deadline().has_value());
  ASSERT_TRUE(server->drive(std::chrono::steady_clock::now()).is_ok());
  ASSERT_TRUE(server->on_transport_closed(server_interests->front().connection_id).is_ok());
  EXPECT_EQ(server->metrics().active_connections, 0U);
  ASSERT_TRUE(server->shutdown().is_ok());
  ASSERT_TRUE(runtime->shutdown().is_ok());
}

} // namespace
} // namespace chronos::cluster
