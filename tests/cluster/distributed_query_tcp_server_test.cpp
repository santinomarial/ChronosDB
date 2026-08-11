#include "chronos/cluster/distributed_query_tcp_server.hpp"
#include "chronos/cluster/distributed_query_tls_client.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <poll.h>
#include <utility>

namespace chronos::cluster {
namespace {

[[nodiscard]] std::filesystem::path fixture(const char* name) {
  return std::filesystem::path{CHRONOS_NETWORK_FIXTURE_DIR} / "tls" / name;
}

[[nodiscard]] network::TlsServerConfig tls_server_config() {
  return {.certificate_chain_file = fixture("server.pem").string(),
          .private_key_file = fixture("server-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string()};
}

[[nodiscard]] network::TlsClientConfig tls_client_config() {
  return {.certificate_chain_file = fixture("client.pem").string(),
          .private_key_file = fixture("client-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string(),
          .expected_server_identity = "127.0.0.1"};
}

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

template <typename Id> [[nodiscard]] Id id(const std::uint8_t seed) {
  return Id::from_uuid(uuid(seed)).value();
}

[[nodiscard]] query::DistributedAggregateFragmentDispatch dispatch() {
  return {.raft_group_id = uuid(9U),
          .fragment = {
              .query_id = uuid(1U),
              .database_id = id<manifest::DatabaseId>(2U),
              .table_id = id<schema::TableId>(3U),
              .tablet_id = id<schema::TabletId>(4U),
              .destination_schema_id = id<schema::SchemaId>(5U),
              .snapshot_generation = 6U,
              .serving_node = 2U,
              .applied_position = 10U,
              .observed_leader_commit_position = 10U,
              .placement_epoch = 8U,
              .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable,
                              .maximum_staleness_positions = std::nullopt},
              .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
              .destination_column_ordinals = {1U},
              .aggregate_input_index = 0U,
              .event_time_predicate = std::nullopt}};
}

class Authenticator final : public network::ConnectionAuthenticator {
public:
  explicit Authenticator(const std::uint64_t principal_id) : principal_id_(principal_id) {}
  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest& request) override {
    saw_fingerprint = request.peer_certificate_sha256.has_value();
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = principal_id_};
  }
  bool saw_fingerprint{};

private:
  std::uint64_t principal_id_{};
};

class NodeAuthorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal_id,
                                      const raft::NodeId node_id) const override {
    return (principal_id == 91U && node_id == 1U) || (principal_id == 92U && node_id == 2U);
  }
};

class Worker final : public DistributedQueryWorkerService {
public:
  common::Result<query::ExchangeMessage>
  execute(const query::DistributedAggregateFragmentDispatch& received) override {
    ++calls;
    query::MergeableAggregateState partial;
    const common::Status added = partial.add(11.0);
    if (!added.is_ok())
      return common::make_unexpected(added);
    return query::ExchangeMessage{.query_id = received.fragment.query_id,
                                  .tablet_id = received.fragment.tablet_id,
                                  .sequence = 1U,
                                  .partial = partial,
                                  .terminal = true};
  }
  std::size_t calls{};
};

[[nodiscard]] DistributedQueryTcpServerConfig server_config(Authenticator& authenticator,
                                                            DistributedQueryReceiver& receiver) {
  return {.listener = {},
          .tls = tls_server_config(),
          .authenticator = &authenticator,
          .receiver = &receiver,
          .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                             .exchange_timeout = std::chrono::milliseconds{1000}},
          .maximum_connections = 8U,
          .maximum_accepts_per_poll = 8U};
}

void finish_connect(network::TcpSocket& socket, DistributedQueryTcpServer& server) {
  for (std::size_t iteration = 0U;
       iteration < 64U && socket.connect_state() != network::TcpConnectState::kConnected;
       ++iteration) {
    ASSERT_TRUE(server.poll_once(std::chrono::milliseconds{1}).is_ok());
    pollfd writable{.fd = socket.descriptor(), .events = POLLOUT};
    ASSERT_GE(::poll(&writable, 1U, 10), 0);
    if ((writable.revents & (POLLOUT | POLLERR | POLLHUP)) != 0) {
      auto completion = socket.finish_connect();
      ASSERT_TRUE(completion.has_value()) << completion.error().message();
    }
  }
  ASSERT_EQ(socket.connect_state(), network::TcpConnectState::kConnected);
  ASSERT_TRUE(server.poll_once(std::chrono::milliseconds{1}).is_ok());
}

TEST(DistributedQueryTcpServerTest, ServesRealTcpMutualTlsQuery) {
  NodeAuthorizer node_authorizer;
  Worker worker;
  auto receiver = DistributedQueryReceiver::create(
      {.local_node_id = 2U, .authorizer = &node_authorizer, .worker = &worker});
  ASSERT_TRUE(receiver.has_value());
  Authenticator client_authenticator{91U};
  auto server = DistributedQueryTcpServer::start(server_config(client_authenticator, *receiver));
  ASSERT_TRUE(server.has_value()) << server.error().message();
  EXPECT_TRUE(server->is_running());

  auto tcp_client = network::TcpSocket::begin_connect(server->bound_endpoint());
  ASSERT_TRUE(tcp_client.has_value()) << tcp_client.error().message();
  finish_connect(*tcp_client, *server);
  auto tls_context = network::TlsClientContext::create(tls_client_config());
  ASSERT_TRUE(tls_context.has_value());
  auto tls_client = network::TlsSocket::connect(*tls_context, tcp_client->descriptor());
  ASSERT_TRUE(tls_client.has_value());
  auto sender = DistributedQuerySender::create(1U, dispatch());
  ASSERT_TRUE(sender.has_value());
  const auto start = DistributedQuerySender::TimePoint::clock::now();
  auto attempt = sender->begin_attempt(start);
  ASSERT_TRUE(attempt.has_value());
  Authenticator server_authenticator{92U};
  auto client = DistributedQueryTlsClient::create(
      std::move(*tls_client), std::move(*attempt),
      {.authenticator = &server_authenticator,
       .node_authorizer = &node_authorizer,
       .peer_ipv4_address = {127U, 0U, 0U, 1U},
       .limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                  .exchange_timeout = std::chrono::milliseconds{1000}}},
      start);
  ASSERT_TRUE(client.has_value());

  for (std::size_t iteration = 0U; iteration < 1024U; ++iteration) {
    const auto interest = client->interest();
    pollfd descriptor{.fd = tcp_client->descriptor(),
                      .events = static_cast<short>((interest.want_read ? POLLIN : 0) |
                                                   (interest.want_write ? POLLOUT : 0))};
    ASSERT_GE(::poll(&descriptor, 1U, 1), 0);
    ASSERT_TRUE(client
                    ->on_ready((descriptor.revents & POLLIN) != 0,
                               (descriptor.revents & POLLOUT) != 0,
                               DistributedQuerySender::TimePoint::clock::now())
                    .is_ok())
        << client->failure().message();
    ASSERT_TRUE(server->poll_once(std::chrono::milliseconds{1}).is_ok());
    if (client->state() == DistributedQueryTlsClientState::kComplete)
      break;
  }

  ASSERT_EQ(client->state(), DistributedQueryTlsClientState::kComplete);
  auto response = client->response_bytes();
  ASSERT_TRUE(response.has_value());
  ASSERT_TRUE(
      sender->accept_response(*response, DistributedQuerySender::TimePoint::clock::now()).is_ok());
  ASSERT_TRUE(sender->result().has_value());
  EXPECT_EQ(sender->result()->partial.sum, 11.0);
  EXPECT_EQ(worker.calls, 1U);
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
  EXPECT_TRUE(server_authenticator.saw_fingerprint);
  const auto metrics = server->metrics();
  EXPECT_EQ(metrics.accepted_connections, 1U);
  EXPECT_EQ(metrics.completed_connections, 1U);
  EXPECT_EQ(metrics.active_connections, 0U);
  EXPECT_TRUE(server->shutdown().is_ok());
  EXPECT_FALSE(server->is_running());
}

TEST(DistributedQueryTcpServerTest, BoundsAdmissionAndValidatesConfiguration) {
  NodeAuthorizer node_authorizer;
  Worker worker;
  auto receiver = DistributedQueryReceiver::create(
      {.local_node_id = 2U, .authorizer = &node_authorizer, .worker = &worker});
  ASSERT_TRUE(receiver.has_value());
  Authenticator authenticator{91U};
  auto invalid = server_config(authenticator, *receiver);
  invalid.maximum_connections = 0U;
  EXPECT_EQ(DistributedQueryTcpServer::start(std::move(invalid)).error().code(),
            common::StatusCode::kInvalidArgument);

  auto config = server_config(authenticator, *receiver);
  config.maximum_connections = 1U;
  auto server = DistributedQueryTcpServer::start(std::move(config));
  ASSERT_TRUE(server.has_value());
  auto first = network::TcpSocket::begin_connect(server->bound_endpoint());
  auto second = network::TcpSocket::begin_connect(server->bound_endpoint());
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  for (std::size_t iteration = 0U; iteration < 64U && server->metrics().rejected_connections == 0U;
       ++iteration) {
    ASSERT_TRUE(server->poll_once(std::chrono::milliseconds{1}).is_ok());
    for (network::TcpSocket* socket : {&*first, &*second}) {
      if (socket->valid() && socket->connect_state() == network::TcpConnectState::kInProgress) {
        pollfd descriptor{.fd = socket->descriptor(), .events = POLLOUT};
        if (::poll(&descriptor, 1U, 0) > 0)
          (void)socket->finish_connect();
      }
    }
  }
  const auto metrics = server->metrics();
  EXPECT_EQ(metrics.accepted_connections, 1U);
  EXPECT_EQ(metrics.rejected_connections, 1U);
  EXPECT_EQ(metrics.active_connections, 1U);
  EXPECT_TRUE(server->shutdown().is_ok());
  EXPECT_EQ(server->metrics().active_connections, 0U);
}

} // namespace
} // namespace chronos::cluster
