#include "chronos/cluster/distributed_mutable_vector_query_tcp.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <poll.h>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

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

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

template <typename Id> [[nodiscard]] Id id(const std::uint8_t seed) {
  return Id::from_uuid(uuid(seed)).value();
}

[[nodiscard]] query::DistributedVectorResultSchema result_schema() {
  return {
      .columns = {
          {"value", schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(), false}}};
}

[[nodiscard]] query::DistributedMutableVectorFragment fragment() {
  return {.query_id = uuid(1U),
          .database_id = id<manifest::DatabaseId>(2U),
          .table_id = id<schema::TableId>(3U),
          .tablet_id = id<schema::TabletId>(4U),
          .destination_schema_id = id<schema::SchemaId>(5U),
          .raft_group_id = uuid(6U),
          .serving_node = 2U,
          .applied_position = 10U,
          .observed_leader_commit_position = 10U,
          .placement_epoch = 7U,
          .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
          .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
          .destination_column_ordinals = {0U},
          .plan = {.mode = query::DistributedVectorPlanMode::kRows, .row_output_indices = {0U}},
          .result_schema = result_schema()};
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

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal,
                                      const raft::NodeId node) const override {
    return (principal == 91U && node == 1U) || (principal == 92U && node == 2U);
  }
};

class Worker final : public DistributedMutableVectorQueryWorkerService {
public:
  common::Result<std::vector<DistributedVectorResultExchangeMessage>>
  execute(const query::DistributedMutableVectorFragment& received) override {
    ++calls;
    return std::vector<DistributedVectorResultExchangeMessage>{{.query_id = received.query_id,
                                                                .tablet_id = received.tablet_id,
                                                                .sequence = 1U,
                                                                .terminal = true}};
  }

  std::size_t calls{};
};

[[nodiscard]] DistributedMutableVectorQueryTlsLimits limits() {
  return {.handshake_timeout = std::chrono::milliseconds{1000},
          .exchange_timeout = std::chrono::milliseconds{1000},
          .maximum_response_frames = 2U,
          .maximum_response_bytes = 1024U};
}

TEST(DistributedMutableVectorQueryTcpTest, OwnsRealConnectListenMutualTlsAndCompleteResponse) {
  Authorizer authorizer;
  Worker worker;
  auto receiver = DistributedMutableVectorQueryReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
  ASSERT_TRUE(receiver.has_value());
  Authenticator client_authenticator{91U};
  auto server =
      DistributedMutableVectorQueryTcpServer::start({.listener = {},
                                                     .tls = server_tls(),
                                                     .authenticator = &client_authenticator,
                                                     .receiver = &*receiver,
                                                     .carrier_limits = limits(),
                                                     .maximum_connections = 8U,
                                                     .maximum_accepts_per_poll = 8U});
  ASSERT_TRUE(server.has_value()) << server.error().to_string();

  auto client_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(client_context.has_value());
  auto sender = DistributedMutableVectorQuerySender::create(1U, fragment());
  ASSERT_TRUE(sender.has_value());
  auto attempt = sender->begin_attempt({});
  ASSERT_TRUE(attempt.has_value());
  Authenticator server_authenticator{92U};
  auto client = DistributedMutableVectorQueryTcpClient::begin(
      std::move(*attempt),
      {.remote_endpoint = server->bound_endpoint(),
       .tls_context = &*client_context,
       .carrier = {.authenticator = &server_authenticator,
                   .node_authorizer = &authorizer,
                   .peer_ipv4_address = {127U, 0U, 0U, 1U},
                   .limits = limits()},
       .connect_timeout = std::chrono::milliseconds{1000}},
      DistributedMutableVectorQueryTcpClient::TimePoint::clock::now());
  ASSERT_TRUE(client.has_value()) << client.error().to_string();

  for (std::size_t iteration = 0U; iteration < 4096U; ++iteration) {
    const auto interest = client->interest();
    pollfd descriptor{.fd = client->descriptor(),
                      .events = static_cast<short>((interest.want_read ? POLLIN : 0) |
                                                   (interest.want_write ? POLLOUT : 0)),
                      .revents = 0};
    ASSERT_GE(::poll(&descriptor, 1U, 1), 0);
    ASSERT_TRUE(client
                    ->on_ready((descriptor.revents & POLLIN) != 0,
                               (descriptor.revents & POLLOUT) != 0,
                               DistributedMutableVectorQueryTcpClient::TimePoint::clock::now())
                    .is_ok())
        << client->failure().to_string();
    ASSERT_TRUE(server->poll_once(std::chrono::milliseconds{1}).is_ok());
    if (client->state() == DistributedMutableVectorQueryTcpClientState::kComplete)
      break;
  }

  ASSERT_EQ(client->state(), DistributedMutableVectorQueryTcpClientState::kComplete);
  const auto responses = client->responses();
  ASSERT_TRUE(responses.has_value());
  ASSERT_EQ(responses->size(), 1U);
  const auto& response_payload = responses->front().payload;
  if (!response_payload.has_value()) {
    FAIL() << "successful mutable query response produced no payload";
  }
  EXPECT_TRUE(response_payload->terminal);
  EXPECT_EQ(worker.calls, 1U);
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
  EXPECT_TRUE(server_authenticator.saw_fingerprint);
  EXPECT_EQ(server->metrics().accepted_connections, 1U);
  EXPECT_EQ(server->metrics().completed_connections, 1U);
  EXPECT_EQ(server->metrics().active_connections, 0U);
  EXPECT_TRUE(server->shutdown().is_ok());
  EXPECT_TRUE(server->shutdown().is_ok());
  EXPECT_FALSE(server->is_running());
}

TEST(DistributedMutableVectorQueryTcpTest, ValidatesBoundsAndExpiresConnectExactly) {
  Authorizer authorizer;
  Authenticator authenticator{92U};
  auto listener = network::TcpListener::bind();
  auto context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(listener.has_value());
  ASSERT_TRUE(context.has_value());
  auto sender = DistributedMutableVectorQuerySender::create(1U, fragment());
  ASSERT_TRUE(sender.has_value());
  auto attempt = sender->begin_attempt({});
  ASSERT_TRUE(attempt.has_value());
  const auto start = DistributedMutableVectorQueryTcpClient::TimePoint{};
  auto client = DistributedMutableVectorQueryTcpClient::begin(
      std::move(*attempt),
      {.remote_endpoint = listener->bound_endpoint(),
       .tls_context = &*context,
       .carrier = {.authenticator = &authenticator,
                   .node_authorizer = &authorizer,
                   .peer_ipv4_address = {127U, 0U, 0U, 1U},
                   .limits = limits()},
       .connect_timeout = std::chrono::milliseconds{5}},
      start);
  ASSERT_TRUE(client.has_value());
  EXPECT_TRUE(client->on_ready(false, false, start + std::chrono::milliseconds{4}).is_ok());
  const auto expired = client->on_ready(false, false, start + std::chrono::milliseconds{5});
  EXPECT_EQ(expired.code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(client->state(), DistributedMutableVectorQueryTcpClientState::kFailed);
  EXPECT_EQ(client->descriptor(), -1);
  EXPECT_EQ(client->on_ready(true, true, start + std::chrono::milliseconds{6}), expired);
}

} // namespace
} // namespace chronos::cluster
