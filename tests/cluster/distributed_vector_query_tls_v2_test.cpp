#include "chronos/cluster/distributed_vector_query_tls_v2.hpp"

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
          {"key", schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(), false},
          {"total", schema::LogicalType::create(schema::LogicalTypeKind::kFloat64).value(), true}}};
}

[[nodiscard]] query::DistributedVectorFragmentDispatchV2 dispatch_v2() {
  return {.dispatch = {.query_id = uuid(1U),
                       .database_id = id<manifest::DatabaseId>(2U),
                       .table_id = id<schema::TableId>(3U),
                       .tablet_id = id<schema::TabletId>(4U),
                       .destination_schema_id = id<schema::SchemaId>(5U),
                       .raft_group_id = uuid(9U),
                       .snapshot_generation = 6U,
                       .serving_node = 2U,
                       .applied_position = 10U,
                       .observed_leader_commit_position = 10U,
                       .placement_epoch = 8U,
                       .read_policy = {.consistency =
                                           query::DistributedReadConsistency::kLeaderLinearizable},
                       .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
                       .destination_column_ordinals = {0U, 1U},
                       .plan = {.mode = query::DistributedVectorPlanMode::kGroupedAggregate,
                                .group_key_input_indices = {0U},
                                .aggregates = {{.operation = query::VectorAggregateOperation::kSum,
                                                .input_index = 1U}}}},
          .result_schema = result_schema()};
}

[[nodiscard]] std::vector<std::byte> zero_row_batch() {
  const query::DistributedVectorResultSchema schema_value = result_schema();
  const std::array<network::QueryResultColumn, 2U> columns{
      network::QueryResultColumn{schema_value.columns[0].name, schema_value.columns[0].type,
                                 schema_value.columns[0].nullable},
      network::QueryResultColumn{schema_value.columns[1].name, schema_value.columns[1].type,
                                 schema_value.columns[1].nullable}};
  return network::encode_query_result_batch(0U, columns, {}).value();
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

class Worker final : public DistributedVectorQueryWorkerServiceV2 {
public:
  common::Result<std::vector<DistributedVectorResultExchangeMessage>>
  execute(const query::DistributedVectorFragmentDispatchV2& received) override {
    ++calls;
    if (terminal_only) {
      return std::vector<DistributedVectorResultExchangeMessage>{
          {.query_id = received.dispatch.query_id,
           .tablet_id = received.dispatch.tablet_id,
           .sequence = 1U,
           .terminal = true}};
    }
    return std::vector<DistributedVectorResultExchangeMessage>{
        {.query_id = received.dispatch.query_id,
         .tablet_id = received.dispatch.tablet_id,
         .sequence = 1U,
         .terminal = false,
         .encoded_result_batch = zero_row_batch()},
        {.query_id = received.dispatch.query_id,
         .tablet_id = received.dispatch.tablet_id,
         .sequence = 2U,
         .terminal = true,
         .encoded_result_batch = zero_row_batch()}};
  }

  std::size_t calls{};
  bool terminal_only{};
};

TEST(DistributedVectorQueryTlsV2Test, CarriesCompleteAuthenticatedSchemaBoundResponseStream) {
  Authorizer authorizer;
  Worker worker;
  auto receiver = DistributedVectorQueryReceiverV2::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
  ASSERT_TRUE(receiver.has_value());
  auto request = encode_distributed_vector_query_request_v2({1U, 2U, dispatch_v2()});
  ASSERT_TRUE(request.has_value());
  DistributedVectorQueryAttemptV2 attempt{1U, 2U, std::move(*request)};

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
  const auto start = DistributedVectorQueryTlsClientV2::TimePoint{};
  const DistributedVectorQueryTlsLimitsV2 limits{
      .handshake_timeout = std::chrono::milliseconds{100},
      .exchange_timeout = std::chrono::milliseconds{100},
      .maximum_response_frames = 4U,
      .maximum_response_bytes = std::size_t{1024U} * 1024U};
  auto server = DistributedVectorQueryTlsServerV2::create(std::move(*server_socket),
                                                          {.authenticator = &client_authenticator,
                                                           .receiver = &*receiver,
                                                           .peer_ipv4_address = {127U, 0U, 0U, 1U},
                                                           .limits = limits},
                                                          start);
  auto client =
      DistributedVectorQueryTlsClientV2::create(std::move(*client_socket), std::move(attempt),
                                                {.authenticator = &server_authenticator,
                                                 .node_authorizer = &authorizer,
                                                 .peer_ipv4_address = {127U, 0U, 0U, 1U},
                                                 .limits = limits},
                                                start);
  ASSERT_TRUE(server.has_value()) << server.error().to_string();
  ASSERT_TRUE(client.has_value()) << client.error().to_string();
  EXPECT_FALSE(client->responses().has_value());

  for (std::size_t iteration = 0U; iteration < 4096U; ++iteration) {
    ASSERT_TRUE(client->on_ready(true, true, start + std::chrono::milliseconds{1}).is_ok())
        << client->failure().to_string();
    ASSERT_TRUE(server->on_ready(true, true, start + std::chrono::milliseconds{1}).is_ok())
        << server->failure().to_string();
    if (client->state() == DistributedVectorQueryTlsStateV2::kComplete &&
        server->state() == DistributedVectorQueryTlsStateV2::kComplete) {
      break;
    }
  }
  ASSERT_EQ(client->state(), DistributedVectorQueryTlsStateV2::kComplete);
  ASSERT_EQ(server->state(), DistributedVectorQueryTlsStateV2::kComplete);
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
  EXPECT_TRUE(server_authenticator.saw_fingerprint);
  EXPECT_EQ(worker.calls, 1U);
  const auto responses = client->responses();
  ASSERT_TRUE(responses.has_value());
  ASSERT_EQ(responses->size(), 2U);
  ASSERT_TRUE((*responses)[0].payload.has_value());
  ASSERT_TRUE((*responses)[1].payload.has_value());
  EXPECT_EQ((*responses)[0].payload->sequence, 1U);
  EXPECT_FALSE((*responses)[0].payload->terminal);
  EXPECT_EQ((*responses)[1].payload->sequence, 2U);
  EXPECT_TRUE((*responses)[1].payload->terminal);
}

TEST(DistributedVectorQueryTlsV2Test, RejectsInvalidBoundsTargetAndExpiresExactly) {
  Authenticator authenticator{92U};
  Authorizer authorizer;
  auto request = encode_distributed_vector_query_request_v2({1U, 2U, dispatch_v2()}).value();
  DistributedVectorQueryAttemptV2 attempt{1U, 2U, std::move(request)};
  DistributedVectorQueryTlsClientConfigV2 config{
      .authenticator = &authenticator,
      .node_authorizer = &authorizer,
      .limits = {.handshake_timeout = std::chrono::milliseconds{5},
                 .exchange_timeout = std::chrono::milliseconds{5},
                 .maximum_response_frames = 2U,
                 .maximum_response_bytes = 200U}};
  auto client = DistributedVectorQueryTlsClientV2::create(network::TlsSocket{}, std::move(attempt),
                                                          config, {});
  ASSERT_TRUE(client.has_value());
  const auto start = DistributedVectorQueryTlsClientV2::TimePoint{};
  EXPECT_TRUE(client->on_ready(false, false, start + std::chrono::milliseconds{4}).is_ok());
  const auto expired = client->on_ready(false, false, start + std::chrono::milliseconds{5});
  EXPECT_EQ(expired.code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(client->state(), DistributedVectorQueryTlsStateV2::kFailed);
  EXPECT_EQ(client->on_ready(true, true, start + std::chrono::milliseconds{6}), expired);

  config.limits.maximum_response_bytes = 115U;
  auto second = encode_distributed_vector_query_request_v2({1U, 2U, dispatch_v2()}).value();
  EXPECT_EQ(DistributedVectorQueryTlsClientV2::create(network::TlsSocket{},
                                                      {1U, 2U, std::move(second)}, config, {})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  auto mismatched = encode_distributed_vector_query_request_v2({1U, 2U, dispatch_v2()}).value();
  config.limits.maximum_response_bytes = 200U;
  EXPECT_EQ(DistributedVectorQueryTlsClientV2::create(network::TlsSocket{},
                                                      {1U, 3U, std::move(mismatched)}, config, {})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedVectorQueryTlsV2Test, RejectsResponseAboveClientByteBudget) {
  Authorizer authorizer;
  Worker worker;
  worker.terminal_only = true;
  auto receiver = DistributedVectorQueryReceiverV2::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
  ASSERT_TRUE(receiver.has_value());
  auto request = encode_distributed_vector_query_request_v2({1U, 2U, dispatch_v2()});
  ASSERT_TRUE(request.has_value());

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
  const auto start = DistributedVectorQueryTlsClientV2::TimePoint{};
  const DistributedVectorQueryTlsLimitsV2 server_limits{
      .handshake_timeout = std::chrono::milliseconds{100},
      .exchange_timeout = std::chrono::milliseconds{100},
      .maximum_response_frames = 2U,
      .maximum_response_bytes = 1024U};
  auto server = DistributedVectorQueryTlsServerV2::create(std::move(*server_socket),
                                                          {.authenticator = &client_authenticator,
                                                           .receiver = &*receiver,
                                                           .peer_ipv4_address = {127U, 0U, 0U, 1U},
                                                           .limits = server_limits},
                                                          start);
  auto client = DistributedVectorQueryTlsClientV2::create(
      std::move(*client_socket), {1U, 2U, std::move(*request)},
      {.authenticator = &server_authenticator,
       .node_authorizer = &authorizer,
       .peer_ipv4_address = {127U, 0U, 0U, 1U},
       .limits = {.handshake_timeout = std::chrono::milliseconds{100},
                  .exchange_timeout = std::chrono::milliseconds{100},
                  .maximum_response_frames = 2U,
                  .maximum_response_bytes = 199U}},
      start);
  ASSERT_TRUE(server.has_value());
  ASSERT_TRUE(client.has_value());

  for (std::size_t iteration = 0U; iteration < 4096U; ++iteration) {
    const common::Status client_status =
        client->on_ready(true, true, start + std::chrono::milliseconds{1});
    const common::Status server_status =
        server->on_ready(true, true, start + std::chrono::milliseconds{1});
    ASSERT_TRUE(server_status.is_ok()) << server->failure().to_string();
    if (!client_status.is_ok())
      break;
  }
  EXPECT_EQ(client->state(), DistributedVectorQueryTlsStateV2::kFailed);
  EXPECT_EQ(client->failure().code(), common::StatusCode::kResourceExhausted);
  EXPECT_FALSE(client->responses().has_value());
  EXPECT_EQ(worker.calls, 1U);
}

} // namespace
} // namespace chronos::cluster
