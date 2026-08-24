#include "chronos/cluster/distributed_mutable_vector_query_tls.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <filesystem>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>
#include <vector>

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

[[nodiscard]] SocketPair socket_pair() {
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
          .linearizable_barrier = raft::ReadBarrier{.term = 2U, .context = 3U, .read_index = 10U},
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

TEST(DistributedMutableVectorQueryTlsTest,
     CarriesExactFragmentOverAuthenticatedMutualTlsBeforeWorkerExecution) {
  Authorizer authorizer;
  Worker worker;
  auto receiver = DistributedMutableVectorQueryReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
  ASSERT_TRUE(receiver.has_value());
  auto sender = DistributedMutableVectorQuerySender::create(1U, fragment());
  ASSERT_TRUE(sender.has_value());
  auto attempt = sender->begin_attempt({});
  ASSERT_TRUE(attempt.has_value());

  auto server_context = network::TlsServerContext::create(server_tls());
  auto client_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(server_context.has_value());
  ASSERT_TRUE(client_context.has_value());
  SocketPair sockets = socket_pair();
  auto server_socket = network::TlsSocket::accept(*server_context, sockets.sockets[0]);
  auto client_socket = network::TlsSocket::connect(*client_context, sockets.sockets[1]);
  ASSERT_TRUE(server_socket.has_value());
  ASSERT_TRUE(client_socket.has_value());

  Authenticator client_authenticator{91U};
  Authenticator server_authenticator{92U};
  const auto start = DistributedMutableVectorQueryTlsClient::TimePoint{};
  const DistributedMutableVectorQueryTlsLimits limits{
      .handshake_timeout = std::chrono::milliseconds{100},
      .exchange_timeout = std::chrono::milliseconds{100},
      .maximum_response_frames = 2U,
      .maximum_response_bytes = 1024U};
  auto server =
      DistributedMutableVectorQueryTlsServer::create(std::move(*server_socket),
                                                     {.authenticator = &client_authenticator,
                                                      .receiver = &*receiver,
                                                      .peer_ipv4_address = {127U, 0U, 0U, 1U},
                                                      .limits = limits},
                                                     start);
  auto client =
      DistributedMutableVectorQueryTlsClient::create(std::move(*client_socket), std::move(*attempt),
                                                     {.authenticator = &server_authenticator,
                                                      .node_authorizer = &authorizer,
                                                      .peer_ipv4_address = {127U, 0U, 0U, 1U},
                                                      .limits = limits},
                                                     start);
  ASSERT_TRUE(server.has_value()) << server.error().to_string();
  ASSERT_TRUE(client.has_value()) << client.error().to_string();

  for (std::size_t iteration = 0U; iteration < 4096U; ++iteration) {
    ASSERT_TRUE(client->on_ready(true, true, start + std::chrono::milliseconds{1}).is_ok())
        << client->failure().to_string();
    ASSERT_TRUE(server->on_ready(true, true, start + std::chrono::milliseconds{1}).is_ok())
        << server->failure().to_string();
    if (client->state() == DistributedMutableVectorQueryTlsState::kComplete &&
        server->state() == DistributedMutableVectorQueryTlsState::kComplete) {
      break;
    }
  }
  ASSERT_EQ(client->state(), DistributedMutableVectorQueryTlsState::kComplete);
  ASSERT_EQ(server->state(), DistributedMutableVectorQueryTlsState::kComplete);
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
  EXPECT_TRUE(server_authenticator.saw_fingerprint);
  EXPECT_EQ(worker.calls, 1U);
  const auto responses = client->responses();
  ASSERT_TRUE(responses.has_value());
  ASSERT_EQ(responses->size(), 1U);
  EXPECT_EQ(responses->front().status_code, common::StatusCode::kOk);
  ASSERT_TRUE(responses->front().payload.has_value());
  EXPECT_TRUE(responses->front().payload->terminal);
}

TEST(DistributedMutableVectorQueryTlsTest, RejectsMismatchedTargetAndExpiresExactly) {
  Authenticator authenticator{92U};
  Authorizer authorizer;
  auto request = encode_distributed_mutable_vector_query_request({1U, 2U, fragment()}).value();
  const DistributedMutableVectorQueryTlsClientConfig config{
      .authenticator = &authenticator,
      .node_authorizer = &authorizer,
      .limits = {.handshake_timeout = std::chrono::milliseconds{5},
                 .exchange_timeout = std::chrono::milliseconds{5},
                 .maximum_response_frames = 2U,
                 .maximum_response_bytes = 200U}};
  auto client = DistributedMutableVectorQueryTlsClient::create(
      network::TlsSocket{}, {1U, 2U, std::move(request)}, config, {});
  ASSERT_TRUE(client.has_value());
  const auto start = DistributedMutableVectorQueryTlsClient::TimePoint{};
  EXPECT_TRUE(client->on_ready(false, false, start + std::chrono::milliseconds{4}).is_ok());
  const common::Status expired =
      client->on_ready(false, false, start + std::chrono::milliseconds{5});
  EXPECT_EQ(expired.code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(client->state(), DistributedMutableVectorQueryTlsState::kFailed);
  EXPECT_EQ(client->on_ready(true, true, start + std::chrono::milliseconds{6}), expired);

  auto mismatched = encode_distributed_mutable_vector_query_request({1U, 2U, fragment()}).value();
  EXPECT_EQ(DistributedMutableVectorQueryTlsClient::create(
                network::TlsSocket{}, {1U, 3U, std::move(mismatched)}, config, {})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedMutableVectorQueryTlsTest, RejectsServerPrincipalBeforeWritingRequest) {
  Authorizer authorizer;
  Worker worker;
  auto receiver = DistributedMutableVectorQueryReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
  ASSERT_TRUE(receiver.has_value());
  auto sender = DistributedMutableVectorQuerySender::create(1U, fragment());
  ASSERT_TRUE(sender.has_value());
  auto attempt = sender->begin_attempt({});
  ASSERT_TRUE(attempt.has_value());

  auto server_context = network::TlsServerContext::create(server_tls());
  auto client_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(server_context.has_value());
  ASSERT_TRUE(client_context.has_value());
  SocketPair sockets = socket_pair();
  auto server_socket = network::TlsSocket::accept(*server_context, sockets.sockets[0]);
  auto client_socket = network::TlsSocket::connect(*client_context, sockets.sockets[1]);
  ASSERT_TRUE(server_socket.has_value());
  ASSERT_TRUE(client_socket.has_value());

  Authenticator client_authenticator{91U};
  Authenticator foreign_server{93U};
  const auto start = DistributedMutableVectorQueryTlsClient::TimePoint{};
  const DistributedMutableVectorQueryTlsLimits limits{
      .handshake_timeout = std::chrono::milliseconds{100},
      .exchange_timeout = std::chrono::milliseconds{100},
      .maximum_response_frames = 2U,
      .maximum_response_bytes = 1024U};
  auto server =
      DistributedMutableVectorQueryTlsServer::create(std::move(*server_socket),
                                                     {.authenticator = &client_authenticator,
                                                      .receiver = &*receiver,
                                                      .peer_ipv4_address = {127U, 0U, 0U, 1U},
                                                      .limits = limits},
                                                     start);
  auto client =
      DistributedMutableVectorQueryTlsClient::create(std::move(*client_socket), std::move(*attempt),
                                                     {.authenticator = &foreign_server,
                                                      .node_authorizer = &authorizer,
                                                      .peer_ipv4_address = {127U, 0U, 0U, 1U},
                                                      .limits = limits},
                                                     start);
  ASSERT_TRUE(server.has_value());
  ASSERT_TRUE(client.has_value());

  for (std::size_t iteration = 0U; iteration < 4096U; ++iteration) {
    const common::Status client_status =
        client->on_ready(true, true, start + std::chrono::milliseconds{1});
    ASSERT_TRUE(server->on_ready(true, true, start + std::chrono::milliseconds{1}).is_ok())
        << server->failure().to_string();
    if (!client_status.is_ok())
      break;
  }
  EXPECT_EQ(client->state(), DistributedMutableVectorQueryTlsState::kFailed);
  EXPECT_EQ(client->failure().code(), common::StatusCode::kUnauthenticated);
  EXPECT_TRUE(foreign_server.saw_fingerprint);
  EXPECT_EQ(worker.calls, 0U);
  EXPECT_FALSE(client->responses().has_value());
}

} // namespace
} // namespace chronos::cluster
