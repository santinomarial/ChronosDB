#include "chronos/cluster/distributed_query_tls_client.hpp"
#include "chronos/cluster/distributed_query_tls_server.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>

namespace chronos::cluster {
namespace {

struct SocketPair {
  std::array<int, 2> sockets{-1, -1};
  ~SocketPair() {
    for (const int socket : sockets) {
      if (socket >= 0)
        ::close(socket);
    }
  }
};

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

[[nodiscard]] SocketPair nonblocking_socket_pair() {
  SocketPair pair;
  EXPECT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, pair.sockets.data()), 0);
  for (const int socket : pair.sockets) {
    const int flags = ::fcntl(socket, F_GETFL, 0);
    EXPECT_GE(flags, 0);
    EXPECT_EQ(::fcntl(socket, F_SETFL, flags | O_NONBLOCK), 0);
  }
  return pair;
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
    return network::PeerAuthenticationResult{.authorized = allow,
                                             .principal_id = allow ? principal_id_ : 0U};
  }

  bool allow{true};
  bool saw_fingerprint{};

private:
  std::uint64_t principal_id_{};
};

class NodeAuthorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal_id,
                                      const raft::NodeId claimed_node_id) const override {
    return (principal_id == 91U && claimed_node_id == 1U) ||
           (principal_id == 92U && claimed_node_id == 2U);
  }
};

class Worker final : public DistributedQueryWorkerService {
public:
  common::Result<query::ExchangeMessage>
  execute(const query::DistributedAggregateFragmentDispatch& received) override {
    ++calls;
    query::MergeableAggregateState partial;
    const common::Status added = partial.add(7.5);
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

[[nodiscard]] DistributedQueryTlsClientConfig client_carrier_config(Authenticator& authenticator,
                                                                    NodeAuthorizer& authorizer) {
  return {.authenticator = &authenticator,
          .node_authorizer = &authorizer,
          .peer_ipv4_address = {127U, 0U, 0U, 1U},
          .limits = {.handshake_timeout = std::chrono::milliseconds{100},
                     .exchange_timeout = std::chrono::milliseconds{100}}};
}

[[nodiscard]] DistributedQueryTlsServerConfig
server_carrier_config(Authenticator& authenticator, DistributedQueryReceiver& receiver) {
  return {.authenticator = &authenticator,
          .receiver = &receiver,
          .peer_ipv4_address = {127U, 0U, 0U, 1U},
          .limits = {.handshake_timeout = std::chrono::milliseconds{100},
                     .exchange_timeout = std::chrono::milliseconds{100}}};
}

TEST(DistributedQueryTlsServerTest, ServesOneAuthenticatedEndToEndQueryAttempt) {
  NodeAuthorizer authorizer;
  Worker worker;
  auto receiver = DistributedQueryReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
  ASSERT_TRUE(receiver.has_value());
  auto sender = DistributedQuerySender::create(1U, dispatch());
  ASSERT_TRUE(sender.has_value());
  const auto start = DistributedQuerySender::TimePoint{};
  auto attempt = sender->begin_attempt(start);
  ASSERT_TRUE(attempt.has_value());

  auto server_context = network::TlsServerContext::create(tls_server_config());
  auto client_context = network::TlsClientContext::create(tls_client_config());
  ASSERT_TRUE(server_context.has_value());
  ASSERT_TRUE(client_context.has_value());
  SocketPair sockets = nonblocking_socket_pair();
  auto server_socket = network::TlsSocket::accept(*server_context, sockets.sockets[0]);
  auto client_socket = network::TlsSocket::connect(*client_context, sockets.sockets[1]);
  ASSERT_TRUE(server_socket.has_value());
  ASSERT_TRUE(client_socket.has_value());

  Authenticator client_authenticator{91U};
  Authenticator server_authenticator{92U};
  auto server = DistributedQueryTlsServer::create(
      std::move(*server_socket), server_carrier_config(client_authenticator, *receiver), start);
  auto client = DistributedQueryTlsClient::create(
      std::move(*client_socket), std::move(*attempt),
      client_carrier_config(server_authenticator, authorizer), start);
  ASSERT_TRUE(server.has_value());
  ASSERT_TRUE(client.has_value());

  for (std::size_t iteration = 0U; iteration < 1024U; ++iteration) {
    ASSERT_TRUE(client->on_ready(true, true, start + std::chrono::milliseconds{1}).is_ok())
        << client->failure().message();
    ASSERT_TRUE(server->on_ready(true, true, start + std::chrono::milliseconds{1}).is_ok())
        << server->failure().message();
    if (client->state() == DistributedQueryTlsClientState::kComplete &&
        server->state() == DistributedQueryTlsServerState::kComplete) {
      break;
    }
  }

  ASSERT_EQ(client->state(), DistributedQueryTlsClientState::kComplete);
  ASSERT_EQ(server->state(), DistributedQueryTlsServerState::kComplete);
  EXPECT_EQ(worker.calls, 1U);
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
  EXPECT_TRUE(server_authenticator.saw_fingerprint);
  auto response = client->response_bytes();
  ASSERT_TRUE(response.has_value());
  ASSERT_TRUE(sender->accept_response(*response, start + std::chrono::milliseconds{2}).is_ok());
  ASSERT_TRUE(sender->result().has_value());
  EXPECT_EQ(sender->result()->partial.sum, 7.5);
}

TEST(DistributedQueryTlsServerTest, RejectsClientPrincipalBeforeRequestDispatch) {
  NodeAuthorizer authorizer;
  Worker worker;
  auto receiver = DistributedQueryReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
  auto sender = DistributedQuerySender::create(1U, dispatch());
  const auto start = DistributedQuerySender::TimePoint{};
  auto attempt = sender->begin_attempt(start);
  auto server_context = network::TlsServerContext::create(tls_server_config());
  auto client_context = network::TlsClientContext::create(tls_client_config());
  ASSERT_TRUE(receiver.has_value());
  ASSERT_TRUE(attempt.has_value());
  ASSERT_TRUE(server_context.has_value());
  ASSERT_TRUE(client_context.has_value());
  SocketPair sockets = nonblocking_socket_pair();
  auto server_socket = network::TlsSocket::accept(*server_context, sockets.sockets[0]);
  auto client_socket = network::TlsSocket::connect(*client_context, sockets.sockets[1]);
  ASSERT_TRUE(server_socket.has_value());
  ASSERT_TRUE(client_socket.has_value());
  Authenticator client_authenticator{91U};
  client_authenticator.allow = false;
  Authenticator server_authenticator{92U};
  auto server = DistributedQueryTlsServer::create(
      std::move(*server_socket), server_carrier_config(client_authenticator, *receiver), start);
  auto client = DistributedQueryTlsClient::create(
      std::move(*client_socket), std::move(*attempt),
      client_carrier_config(server_authenticator, authorizer), start);
  ASSERT_TRUE(server.has_value());
  ASSERT_TRUE(client.has_value());

  common::Status server_progress = common::Status::ok();
  for (std::size_t iteration = 0U; iteration < 1024U && server_progress.is_ok(); ++iteration) {
    (void)client->on_ready(true, true, start + std::chrono::milliseconds{1});
    server_progress = server->on_ready(true, true, start + std::chrono::milliseconds{1});
  }
  EXPECT_EQ(server_progress.code(), common::StatusCode::kUnauthenticated);
  EXPECT_EQ(server->state(), DistributedQueryTlsServerState::kFailed);
  EXPECT_EQ(worker.calls, 0U);
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
}

TEST(DistributedQueryTlsServerTest, ConfigurationAndExactHandshakeDeadlineFailClosed) {
  NodeAuthorizer authorizer;
  Worker worker;
  auto receiver = DistributedQueryReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
  ASSERT_TRUE(receiver.has_value());
  Authenticator authenticator{91U};
  auto config = server_carrier_config(authenticator, *receiver);
  config.limits.handshake_timeout = std::chrono::milliseconds{0};
  EXPECT_EQ(DistributedQueryTlsServer::create(network::TlsSocket{}, config, {}).error().code(),
            common::StatusCode::kInvalidArgument);

  config.limits.handshake_timeout = std::chrono::milliseconds{5};
  auto server = DistributedQueryTlsServer::create(network::TlsSocket{}, config, {});
  ASSERT_TRUE(server.has_value());
  const auto start = DistributedQueryTlsServer::TimePoint{};
  EXPECT_TRUE(server->on_ready(false, false, start + std::chrono::milliseconds{4}).is_ok());
  const common::Status timed_out =
      server->on_ready(false, false, start + std::chrono::milliseconds{5});
  EXPECT_EQ(timed_out.code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(server->state(), DistributedQueryTlsServerState::kFailed);
  EXPECT_EQ(server->on_ready(true, true, start + std::chrono::milliseconds{6}), timed_out);
}

} // namespace
} // namespace chronos::cluster
