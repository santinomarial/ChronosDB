#include "chronos/cluster/raft_read_authority_tcp_client.hpp"
#include "chronos/service/replicated_distributed_mutable_query_control_tcp_server.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <poll.h>
#include <string>
#include <system_error>
#include <unistd.h>

namespace chronos::service {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-query-control-service-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
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

[[nodiscard]] network::TlsServerConfig server_tls() {
  return {.certificate_chain_file = fixture("server.pem").string(),
          .private_key_file = fixture("server-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string()};
}

[[nodiscard]] network::TlsClientConfig client_tls() {
  return {.certificate_chain_file = fixture("client.pem").string(),
          .private_key_file = fixture("client-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string(),
          .expected_server_identity = "127.0.0.1"};
}

[[nodiscard]] raft::GroupId group() {
  common::Uuid::Bytes bytes{};
  bytes.fill(std::byte{0x47U});
  return raft::GroupId{bytes};
}

class Authenticator final : public network::ConnectionAuthenticator {
public:
  explicit Authenticator(const std::uint64_t principal) : principal_(principal) {}

  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest& request) override {
    saw_fingerprint = request.peer_certificate_sha256.has_value();
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = principal_};
  }

  bool saw_fingerprint{};

private:
  std::uint64_t principal_{};
};

class NodeAuthorizer final : public cluster::ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal,
                                      const raft::NodeId node) const override {
    return (principal == 91U && node == 1U) || (principal == 92U && node == 2U);
  }
};

class UnusedContextProvider final
    : public ReplicatedDistributedMutableVectorQueryWorkerContextProvider {
public:
  common::Result<ReplicatedDistributedMutableVectorQueryWorkerContext>
  acquire(const query::DistributedMutableVectorFragment&) override {
    ++calls;
    return common::make_unexpected(
        common::Status{common::StatusCode::kUnavailable, "unexpected mutable worker call"});
  }

  std::size_t calls{};
};

TEST(ReplicatedDistributedMutableQueryControlTcpServerTest,
     ServesOneReplicatedGroupAuthorityThroughSharedEndpoint) {
  EXPECT_EQ(ReplicatedDistributedMutableQueryControlTcpServer::start({}).error().code(),
            common::StatusCode::kInvalidArgument);

  TemporaryDirectory directory;
  auto runtime = raft::AsyncDurableMultiRaftRuntime::create_new(
      2U, {.directory_path = directory.path().string()}, {{group(), {2U}}});
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  auto election = runtime->try_submit({{group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value());
  ASSERT_TRUE(election->wait().has_value());
  auto barrier = ReplicatedReadBarrier::create_local(&*runtime, {group()});
  ASSERT_TRUE(barrier.has_value()) << barrier.error().to_string();

  UnusedContextProvider provider;
  NodeAuthorizer authorizer;
  Authenticator client_authenticator{91U};
  auto client_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(client_context.has_value()) << client_context.error().to_string();
  const std::array result_contexts{cluster::DistributedQueryNodeTlsContext{1U, &*client_context}};
  auto server = ReplicatedDistributedMutableQueryControlTcpServer::start(
      {.worker = {.local_node_id = 2U, .context_provider = &provider},
       .read_barrier = &*barrier,
       .listener = {},
       .tls = server_tls(),
       .authenticator = &client_authenticator,
       .node_authorizer = &authorizer,
       .grouped_shuffle_jobs =
           cluster::DistributedVectorGroupedAggregateShuffleJobServiceConfig{
               .local_node_id = 2U,
               .shuffle_tls = server_tls(),
               .shuffle_authenticator = &client_authenticator,
               .result_authenticator = &client_authenticator,
               .node_authorizer = &authorizer,
               .result_tls_contexts = result_contexts},
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000},
                          .maximum_mutable_response_frames = 2U,
                          .maximum_mutable_response_bytes = 1024U},
       .maximum_connections = 2U,
       .maximum_accepts_per_poll = 2U});
  if (!server.has_value() && server.error().code() == common::StatusCode::kIoError)
    GTEST_SKIP() << "workspace does not permit loopback listener creation";
  ASSERT_TRUE(server.has_value()) << server.error().to_string();
  Authenticator server_authenticator{92U};
  auto client = cluster::RaftReadAuthorityTcpClient::begin(
      {.remote_endpoint = server->bound_endpoint(),
       .tls_context = &*client_context,
       .carrier = {.authenticator = &server_authenticator,
                   .node_authorizer = &authorizer,
                   .peer_ipv4_address = server->bound_endpoint().address,
                   .request = {.source_node_id = 1U,
                               .target_node_id = 2U,
                               .group_id = group(),
                               .correlation_id = 47U},
                   .limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                              .exchange_timeout = std::chrono::milliseconds{1000}}},
       .connect_timeout = std::chrono::milliseconds{1000}},
      std::chrono::steady_clock::now());
  ASSERT_TRUE(client.has_value()) << client.error().to_string();

  for (std::size_t iteration = 0U;
       iteration < 4096U && client->state() != cluster::RaftReadAuthorityTcpClientState::kComplete;
       ++iteration) {
    const auto interest = client->interest();
    pollfd descriptor{.fd = client->descriptor(),
                      .events = static_cast<short>((interest.want_read ? POLLIN : 0) |
                                                   (interest.want_write ? POLLOUT : 0))};
    ASSERT_GE(::poll(&descriptor, 1U, 1), 0);
    const auto progress =
        client->on_ready((descriptor.revents & POLLIN) != 0, (descriptor.revents & POLLOUT) != 0,
                         std::chrono::steady_clock::now());
    ASSERT_TRUE(progress.is_ok()) << progress.to_string();
    ASSERT_TRUE(server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }

  ASSERT_EQ(client->state(), cluster::RaftReadAuthorityTcpClientState::kComplete)
      << client->failure().to_string();
  ASSERT_TRUE(client->result().has_value());
  EXPECT_EQ(client->result()->barrier.group_id, group());
  EXPECT_EQ(client->result()->observation.group_id, group());
  EXPECT_EQ(client->result()->observation.node_id, 2U);
  EXPECT_EQ(client->result()->observation.role, raft::Role::kLeader);
  EXPECT_EQ(provider.calls, 0U);
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
  EXPECT_TRUE(server_authenticator.saw_fingerprint);
  EXPECT_EQ(server->metrics().completed_read_authorities, 1U);
  EXPECT_EQ(server->metrics().completed_mutable_queries, 0U);
  EXPECT_TRUE(server->shutdown().is_ok());
  EXPECT_TRUE(barrier->shutdown().is_ok());
  EXPECT_TRUE(runtime->shutdown().is_ok());
}

} // namespace
} // namespace chronos::service
