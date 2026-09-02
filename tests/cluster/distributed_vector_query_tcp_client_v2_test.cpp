#include "chronos/cluster/distributed_vector_query_tcp_client_v2.hpp"
#include "chronos/cluster/distributed_vector_query_tcp_server_v2.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
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
  const auto schema_value = result_schema();
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
};

[[nodiscard]] DistributedVectorQueryTlsLimitsV2 limits() {
  return {.handshake_timeout = std::chrono::milliseconds{1000},
          .exchange_timeout = std::chrono::milliseconds{1000},
          .maximum_response_frames = 4U,
          .maximum_response_bytes = std::size_t{1024U} * 1024U};
}

[[nodiscard]] DistributedVectorQueryTcpServerConfigV2
server_config(Authenticator& authenticator, DistributedVectorQueryReceiverV2& receiver) {
  return {.listener = {},
          .tls = server_tls(),
          .authenticator = &authenticator,
          .receiver = &receiver,
          .carrier_limits = limits(),
          .maximum_connections = 8U,
          .maximum_accepts_per_poll = 8U};
}

TEST(DistributedVectorQueryTcpClientV2Test, OwnsConnectAndCompleteMutualTlsStream) {
  Authorizer authorizer;
  Worker worker;
  auto receiver = DistributedVectorQueryReceiverV2::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
  auto listener = network::TcpListener::bind();
  auto server_context = network::TlsServerContext::create(server_tls());
  auto client_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(receiver.has_value());
  ASSERT_TRUE(listener.has_value());
  ASSERT_TRUE(server_context.has_value());
  ASSERT_TRUE(client_context.has_value());

  auto request = encode_distributed_vector_query_request_v2({1U, 2U, dispatch_v2()});
  ASSERT_TRUE(request.has_value());
  Authenticator server_authenticator{92U};
  const auto start = DistributedVectorQueryTcpClientV2::TimePoint::clock::now();
  auto client =
      DistributedVectorQueryTcpClientV2::begin({1U, 2U, std::move(*request)},
                                               {.remote_endpoint = listener->bound_endpoint(),
                                                .tls_context = &*client_context,
                                                .carrier = {.authenticator = &server_authenticator,
                                                            .node_authorizer = &authorizer,
                                                            .peer_ipv4_address = {127U, 0U, 0U, 1U},
                                                            .limits = limits()},
                                                .connect_timeout = std::chrono::milliseconds{1000}},
                                               start);
  ASSERT_TRUE(client.has_value()) << client.error().to_string();
  EXPECT_FALSE(client->responses().has_value());

  Authenticator client_authenticator{91U};
  std::optional<network::TcpSocket> accepted_socket;
  std::optional<DistributedVectorQueryTlsServerV2> server;
  for (std::size_t iteration = 0U; iteration < 4096U; ++iteration) {
    if (!accepted_socket.has_value()) {
      auto accepted = listener->accept_one();
      ASSERT_TRUE(accepted.has_value());
      if (accepted->has_value()) {
        // Guarded by both result and nested optional checks above.
        // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
        accepted_socket.emplace(std::move(accepted->value()));
        auto tls = network::TlsSocket::accept(*server_context, accepted_socket->descriptor());
        ASSERT_TRUE(tls.has_value());
        auto carrier = DistributedVectorQueryTlsServerV2::create(
            std::move(*tls),
            {.authenticator = &client_authenticator,
             .receiver = &*receiver,
             .peer_ipv4_address = accepted_socket->peer_endpoint().value().address,
             .limits = limits()},
            DistributedVectorQueryTlsServerV2::TimePoint::clock::now());
        ASSERT_TRUE(carrier.has_value());
        server.emplace(std::move(*carrier));
      }
    }

    std::array<pollfd, 2U> descriptors{};
    std::size_t count = 0U;
    const auto client_interest = client->interest();
    descriptors[count++] = {.fd = client->descriptor(),
                            .events =
                                static_cast<short>((client_interest.want_read ? POLLIN : 0) |
                                                   (client_interest.want_write ? POLLOUT : 0)),
                            .revents = 0};
    if (server.has_value()) {
      const auto server_interest = server->interest();
      // The server is created only after the accepted socket is emplaced.
      // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
      descriptors[count++] = {.fd = accepted_socket.value().descriptor(),
                              .events =
                                  static_cast<short>((server_interest.want_read ? POLLIN : 0) |
                                                     (server_interest.want_write ? POLLOUT : 0)),
                              .revents = 0};
    }
    ASSERT_GE(::poll(descriptors.data(), static_cast<nfds_t>(count), 1), 0);
    const auto now = DistributedVectorQueryTcpClientV2::TimePoint::clock::now();
    ASSERT_TRUE(client
                    ->on_ready((descriptors[0].revents & POLLIN) != 0,
                               (descriptors[0].revents & POLLOUT) != 0, now)
                    .is_ok())
        << client->failure().to_string();
    if (server.has_value()) {
      ASSERT_TRUE(server
                      ->on_ready((descriptors[1].revents & POLLIN) != 0,
                                 (descriptors[1].revents & POLLOUT) != 0, now)
                      .is_ok())
          << server->failure().to_string();
    }
    if (client->state() == DistributedVectorQueryTcpClientStateV2::kComplete &&
        server.has_value() && server->state() == DistributedVectorQueryTlsStateV2::kComplete) {
      break;
    }
  }

  ASSERT_EQ(client->state(), DistributedVectorQueryTcpClientStateV2::kComplete);
  ASSERT_TRUE(server.has_value());
  // Guarded by the assertion above.
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  ASSERT_EQ(server.value().state(), DistributedVectorQueryTlsStateV2::kComplete);
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
  EXPECT_TRUE(server_authenticator.saw_fingerprint);
  EXPECT_EQ(worker.calls, 1U);
  const auto responses = client->responses();
  ASSERT_TRUE(responses.has_value());
  ASSERT_EQ(responses->size(), 2U);
  ASSERT_TRUE((*responses)[0].payload.has_value());
  ASSERT_TRUE((*responses)[1].payload.has_value());
  // Guarded by the payload assertions above.
  // NOLINTBEGIN(bugprone-unchecked-optional-access)
  EXPECT_EQ((*responses)[0].payload.value().sequence, 1U);
  EXPECT_EQ((*responses)[1].payload.value().sequence, 2U);
  EXPECT_TRUE((*responses)[1].payload.value().terminal);
  // NOLINTEND(bugprone-unchecked-optional-access)
}

TEST(DistributedVectorQueryTcpClientV2Test, ValidatesBeforeConnectAndExpiresExactly) {
  Authorizer authorizer;
  Authenticator authenticator{92U};
  auto listener = network::TcpListener::bind();
  auto tls_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(listener.has_value());
  ASSERT_TRUE(tls_context.has_value());
  auto request = encode_distributed_vector_query_request_v2({1U, 2U, dispatch_v2()});
  ASSERT_TRUE(request.has_value());
  const auto start = DistributedVectorQueryTcpClientV2::TimePoint{};
  auto config =
      DistributedVectorQueryTcpClientConfigV2{.remote_endpoint = listener->bound_endpoint(),
                                              .tls_context = &*tls_context,
                                              .carrier = {.authenticator = &authenticator,
                                                          .node_authorizer = &authorizer,
                                                          .peer_ipv4_address = {127U, 0U, 0U, 1U},
                                                          .limits = limits()},
                                              .connect_timeout = std::chrono::milliseconds{5}};
  auto client =
      DistributedVectorQueryTcpClientV2::begin({1U, 2U, std::move(*request)}, config, start);
  ASSERT_TRUE(client.has_value());
  EXPECT_TRUE(client->on_ready(false, false, start + std::chrono::milliseconds{4}).is_ok());
  const auto expired = client->on_ready(false, false, start + std::chrono::milliseconds{5});
  EXPECT_EQ(expired.code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(client->state(), DistributedVectorQueryTcpClientStateV2::kFailed);
  EXPECT_EQ(client->descriptor(), -1);
  EXPECT_EQ(client->on_ready(true, true, start + std::chrono::milliseconds{6}), expired);

  config.carrier.limits.maximum_response_bytes = 115U;
  auto second = encode_distributed_vector_query_request_v2({1U, 2U, dispatch_v2()});
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(DistributedVectorQueryTcpClientV2::begin({1U, 2U, std::move(*second)}, config, start)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedVectorQueryTcpServerV2Test, ServesRealTcpMutualTlsStream) {
  Authorizer authorizer;
  Worker worker;
  auto receiver = DistributedVectorQueryReceiverV2::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
  ASSERT_TRUE(receiver.has_value());
  Authenticator client_authenticator{91U};
  auto server =
      DistributedVectorQueryTcpServerV2::start(server_config(client_authenticator, *receiver));
  ASSERT_TRUE(server.has_value()) << server.error().to_string();

  auto client_context = network::TlsClientContext::create(client_tls());
  auto request = encode_distributed_vector_query_request_v2({1U, 2U, dispatch_v2()});
  ASSERT_TRUE(client_context.has_value());
  ASSERT_TRUE(request.has_value());
  Authenticator server_authenticator{92U};
  const auto start = DistributedVectorQueryTcpClientV2::TimePoint::clock::now();
  auto client =
      DistributedVectorQueryTcpClientV2::begin({1U, 2U, std::move(*request)},
                                               {.remote_endpoint = server->bound_endpoint(),
                                                .tls_context = &*client_context,
                                                .carrier = {.authenticator = &server_authenticator,
                                                            .node_authorizer = &authorizer,
                                                            .peer_ipv4_address = {127U, 0U, 0U, 1U},
                                                            .limits = limits()},
                                                .connect_timeout = std::chrono::milliseconds{1000}},
                                               start);
  ASSERT_TRUE(client.has_value());

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
                               DistributedVectorQueryTcpClientV2::TimePoint::clock::now())
                    .is_ok())
        << client->failure().to_string();
    ASSERT_TRUE(server->poll_once(std::chrono::milliseconds{1}).is_ok());
    if (client->state() == DistributedVectorQueryTcpClientStateV2::kComplete)
      break;
  }

  ASSERT_EQ(client->state(), DistributedVectorQueryTcpClientStateV2::kComplete);
  const auto responses = client->responses();
  ASSERT_TRUE(responses.has_value());
  ASSERT_EQ(responses->size(), 2U);
  ASSERT_TRUE((*responses)[1].payload.has_value());
  // Guarded by the payload assertion above.
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  EXPECT_TRUE((*responses)[1].payload.value().terminal);
  EXPECT_EQ(worker.calls, 1U);
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
  EXPECT_TRUE(server_authenticator.saw_fingerprint);
  const auto server_metrics = server->metrics();
  EXPECT_EQ(server_metrics.accepted_connections, 1U);
  EXPECT_EQ(server_metrics.completed_connections, 1U);
  EXPECT_EQ(server_metrics.failed_connections, 0U);
  EXPECT_EQ(server_metrics.active_connections, 0U);
  EXPECT_TRUE(server->shutdown().is_ok());
  EXPECT_TRUE(server->shutdown().is_ok());
  EXPECT_FALSE(server->is_running());
}

TEST(DistributedVectorQueryTcpServerV2Test, BoundsAdmissionAndValidatesConfiguration) {
  Authorizer authorizer;
  Worker worker;
  auto receiver = DistributedVectorQueryReceiverV2::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
  ASSERT_TRUE(receiver.has_value());
  Authenticator authenticator{91U};
  auto invalid_config = server_config(authenticator, *receiver);
  invalid_config.carrier_limits.maximum_response_bytes = 115U;
  EXPECT_EQ(DistributedVectorQueryTcpServerV2::start(std::move(invalid_config)).error().code(),
            common::StatusCode::kInvalidArgument);

  auto config = server_config(authenticator, *receiver);
  config.maximum_connections = 1U;
  auto server = DistributedVectorQueryTcpServerV2::start(std::move(config));
  ASSERT_TRUE(server.has_value());
  EXPECT_EQ(server->poll_once(std::chrono::milliseconds{-1}).code(),
            common::StatusCode::kInvalidArgument);
  auto first = network::TcpSocket::begin_connect(server->bound_endpoint());
  auto second = network::TcpSocket::begin_connect(server->bound_endpoint());
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  for (std::size_t iteration = 0U; iteration < 64U && server->metrics().rejected_connections == 0U;
       ++iteration) {
    ASSERT_TRUE(server->poll_once(std::chrono::milliseconds{1}).is_ok());
    for (network::TcpSocket* socket : {&*first, &*second}) {
      if (socket->valid() && socket->connect_state() == network::TcpConnectState::kInProgress) {
        pollfd descriptor{.fd = socket->descriptor(), .events = POLLOUT, .revents = 0};
        if (::poll(&descriptor, 1U, 0) > 0) {
          const auto connected = socket->finish_connect();
          ASSERT_TRUE(connected.has_value());
        }
      }
    }
  }
  const auto server_metrics = server->metrics();
  EXPECT_EQ(server_metrics.accepted_connections, 1U);
  EXPECT_EQ(server_metrics.rejected_connections, 1U);
  EXPECT_EQ(server_metrics.active_connections, 1U);
  EXPECT_TRUE(server->shutdown().is_ok());
  EXPECT_EQ(server->metrics().active_connections, 0U);
  EXPECT_FALSE(server->is_running());
  EXPECT_EQ(server->poll_once(std::chrono::milliseconds{0}).code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::cluster
