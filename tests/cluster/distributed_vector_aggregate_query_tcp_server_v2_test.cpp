#include "chronos/cluster/distributed_vector_aggregate_query_tcp_client_v2.hpp"
#include "chronos/cluster/distributed_vector_aggregate_query_tcp_server_v2.hpp"

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

[[nodiscard]] std::vector<query::VectorAggregateDefinition> definitions() {
  return {{.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt},
          {.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
}

[[nodiscard]] query::DistributedVectorFragmentDispatchV2 dispatch_v2() {
  const auto int64 = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  return {
      .dispatch =
          {.query_id = uuid(1U),
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
           .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
           .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
           .destination_column_ordinals = {0U},
           .plan = {.mode = query::DistributedVectorPlanMode::kUngroupedAggregate,
                    .aggregates = {{.operation = query::VectorAggregateOperation::kCountStar},
                                   {.operation = query::VectorAggregateOperation::kCountStar}}}},
      .result_schema = {.columns = {{"first", int64, false}, {"second", int64, false}}}};
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

class Worker final : public DistributedVectorAggregateQueryWorkerServiceV2 {
public:
  common::Result<std::vector<query::VectorAggregateDefinition>>
  bind_definitions(const query::DistributedVectorFragmentDispatchV2&) override {
    ++bind_calls;
    return definitions();
  }

  common::Result<query::DistributedVectorAggregateWorkerResultV2>
  execute(const query::DistributedVectorFragmentDispatchV2& received) override {
    ++execute_calls;
    auto expected = definitions();
    query::DistributedVectorAggregateWorkerResultV2 result{.definitions = expected,
                                                           .input_rows = 3U};
    for (std::size_t ordinal = 0U; ordinal < expected.size(); ++ordinal) {
      auto state = query::MergeableVectorAggregateState::create(expected[ordinal]).value();
      for (std::size_t count = 0U; count <= ordinal; ++count)
        EXPECT_TRUE(state.accumulate_count_star().has_value());
      result.messages.emplace_back(
          query::DistributedVectorAggregateExchangePosition{
              .query_id = received.dispatch.query_id,
              .tablet_id = received.dispatch.tablet_id,
              .sequence = ordinal + 1U,
              .aggregate_ordinal = static_cast<std::uint32_t>(ordinal),
              .terminal = ordinal + 1U == expected.size()},
          std::move(state));
    }
    return result;
  }

  std::size_t bind_calls{};
  std::size_t execute_calls{};
};

[[nodiscard]] DistributedVectorAggregateQueryTlsLimitsV2 limits() {
  return {.handshake_timeout = std::chrono::milliseconds{1000},
          .exchange_timeout = std::chrono::milliseconds{1000},
          .maximum_response_frames = 2U,
          .maximum_response_bytes = std::size_t{1024U} * 1024U};
}

[[nodiscard]] DistributedVectorAggregateQueryTcpServerConfigV2
server_config(Authenticator& authenticator, DistributedVectorAggregateQueryReceiverV2& receiver) {
  return {.listener = {},
          .tls = server_tls(),
          .authenticator = &authenticator,
          .receiver = &receiver,
          .carrier_limits = limits(),
          .maximum_connections = 8U,
          .maximum_accepts_per_poll = 8U};
}

// Optional values below are asserted present before access.
// NOLINTBEGIN(bugprone-unchecked-optional-access)
TEST(DistributedVectorAggregateQueryTcpServerV2Test, ServesRealDefinitionBoundTcpMutualTlsStream) {
  Authorizer authorizer;
  Worker worker;
  auto receiver = DistributedVectorAggregateQueryReceiverV2::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
  ASSERT_TRUE(receiver.has_value());
  Authenticator client_authenticator{91U};
  auto server = DistributedVectorAggregateQueryTcpServerV2::start(
      server_config(client_authenticator, *receiver));
  ASSERT_TRUE(server.has_value()) << server.error().to_string();

  auto client_context = network::TlsClientContext::create(client_tls());
  auto request = encode_distributed_vector_query_request_v2({1U, 2U, dispatch_v2()});
  auto resources = query::QueryResourceContext::create(1U << 20U);
  ASSERT_TRUE(client_context.has_value());
  ASSERT_TRUE(request.has_value());
  ASSERT_TRUE(resources.has_value());
  Authenticator server_authenticator{92U};
  const auto start = DistributedVectorAggregateQueryTcpClientV2::TimePoint::clock::now();
  auto client = DistributedVectorAggregateQueryTcpClientV2::begin(
      {1U, 2U, std::move(*request)}, definitions(), std::move(*resources),
      {.remote_endpoint = server->bound_endpoint(),
       .tls_context = &*client_context,
       .carrier = {.authenticator = &server_authenticator,
                   .node_authorizer = &authorizer,
                   .peer_ipv4_address = {127U, 0U, 0U, 1U},
                   .limits = limits()},
       .connect_timeout = std::chrono::milliseconds{1000}},
      start);
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
                               DistributedVectorAggregateQueryTcpClientV2::TimePoint::clock::now())
                    .is_ok())
        << client->failure().to_string();
    ASSERT_TRUE(server->poll_once(std::chrono::milliseconds{1}).is_ok());
    if (client->state() == DistributedVectorAggregateQueryTcpClientStateV2::kComplete)
      break;
  }

  ASSERT_EQ(client->state(), DistributedVectorAggregateQueryTcpClientStateV2::kComplete);
  const auto responses = client->responses();
  ASSERT_TRUE(responses.has_value());
  ASSERT_EQ(responses->size(), 2U);
  ASSERT_TRUE((*responses)[1].payload.has_value());
  EXPECT_TRUE((*responses)[1].payload->terminal);
  EXPECT_EQ(worker.bind_calls, 1U);
  EXPECT_EQ(worker.execute_calls, 1U);
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

TEST(DistributedVectorAggregateQueryTcpServerV2Test, BoundsAdmissionAndValidatesConfiguration) {
  Authorizer authorizer;
  Worker worker;
  auto receiver = DistributedVectorAggregateQueryReceiverV2::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
  ASSERT_TRUE(receiver.has_value());
  Authenticator authenticator{91U};
  auto invalid_config = server_config(authenticator, *receiver);
  invalid_config.carrier_limits.maximum_response_bytes = 115U;
  EXPECT_EQ(
      DistributedVectorAggregateQueryTcpServerV2::start(std::move(invalid_config)).error().code(),
      common::StatusCode::kInvalidArgument);

  auto config = server_config(authenticator, *receiver);
  config.maximum_connections = 1U;
  auto server = DistributedVectorAggregateQueryTcpServerV2::start(std::move(config));
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
// NOLINTEND(bugprone-unchecked-optional-access)

} // namespace
} // namespace chronos::cluster
