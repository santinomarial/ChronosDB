#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_control_tcp_acquisition.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_coordinator_execution.hpp"
#include "support/failing_allocator.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

class Authenticator final : public network::ConnectionAuthenticator {
public:
  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest&) override {
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = 1U};
  }
};

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(std::uint64_t, raft::NodeId) const override {
    return true;
  }
};

[[nodiscard]] std::filesystem::path fixture(const char* name) {
  return std::filesystem::path{CHRONOS_NETWORK_FIXTURE_DIR} / "tls" / name;
}

[[nodiscard]] network::TlsClientConfig client_tls() {
  return {.certificate_chain_file = fixture("client.pem").string(),
          .private_key_file = fixture("client-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string(),
          .expected_server_identity = "127.0.0.1"};
}

[[nodiscard]] network::TlsServerConfig server_tls() {
  return {.certificate_chain_file = fixture("server.pem").string(),
          .private_key_file = fixture("server-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string()};
}

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleJobPrepare prepare() {
  const auto string = schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const auto count = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  return {.coordinator_node_id = 9U,
          .target_node_id = 3U,
          .coordinator_result_endpoint = {{127U, 0U, 0U, 1U}, 8137U},
          .execution_timeout = std::chrono::milliseconds{30'000},
          .authority = DistributedVectorGroupedAggregateShuffleAuthority::create(
                           uuid(1U), {{schema::TabletId::from_uuid(uuid(2U)).value(), 3U}},
                           {{0U, 3U}}, {{0U, string, false}},
                           {{query::VectorAggregateOperation::kCountStar, std::nullopt}})
                           .value(),
          .result_schema = {.columns = {{"region", string, false}, {"count", count, false}}}};
}

template <typename Id> [[nodiscard]] Id id(const std::uint8_t seed) {
  return Id::from_uuid(uuid(seed)).value();
}

[[nodiscard]] query::DistributedMutableVectorFragment fragment() {
  const auto string = schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const auto count = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  return {.query_id = uuid(1U),
          .database_id = id<manifest::DatabaseId>(8U),
          .table_id = id<schema::TableId>(9U),
          .tablet_id = id<schema::TabletId>(2U),
          .destination_schema_id = id<schema::SchemaId>(10U),
          .raft_group_id = uuid(11U),
          .serving_node = 3U,
          .applied_position = 10U,
          .observed_leader_commit_position = 10U,
          .placement_epoch = 3U,
          .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
          .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
          .destination_column_ordinals = {0U},
          .plan = {.mode = query::DistributedVectorPlanMode::kGroupedAggregate,
                   .group_key_input_indices = {0U},
                   .aggregates = {{.operation = query::VectorAggregateOperation::kCountStar}}},
          .result_schema = {.columns = {{"region", string, false}, {"count", count, false}}}};
}

TEST(DistributedVectorGroupedAggregateShuffleJobControlTlsAllocationFailureTest,
     ClassifiesClientConstructionAllocationSweep) {
  Authenticator authenticator;
  Authorizer authorizer;
  bool saw_failure{};
  bool saw_success{};
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    DistributedVectorGroupedAggregateShuffleJobControlRequest request{prepare()};
    std::optional<common::Result<DistributedVectorGroupedAggregateShuffleJobControlTlsClient>>
        client;
    {
      ::chronos::test::ScopedAllocationFailure failure{fail_after};
      client.emplace(DistributedVectorGroupedAggregateShuffleJobControlTlsClient::create(
          network::TlsSocket{},
          {.authenticator = &authenticator,
           .node_authorizer = &authorizer,
           .request = std::move(request)},
          {}));
      failure.disable();
    }
    if (!client->has_value()) {
      saw_failure = true;
      EXPECT_EQ(client->error().code(), common::StatusCode::kResourceExhausted)
          << client->error().to_string();
      continue;
    }
    saw_success = true;
    break;
  }
  EXPECT_TRUE(saw_failure);
  EXPECT_TRUE(saw_success);
}

TEST(DistributedVectorGroupedAggregateShuffleJobControlTlsAllocationFailureTest,
     ClassifiesServerOwnerAllocation) {
  Authenticator authenticator;
  Authorizer authorizer;
  auto context = network::TlsClientContext::create(client_tls()).value();
  const std::array contexts{DistributedQueryNodeTlsContext{9U, &context}};
  auto service = DistributedVectorGroupedAggregateShuffleJobService::create(
                     {.local_node_id = 3U,
                      .shuffle_authenticator = &authenticator,
                      .result_authenticator = &authenticator,
                      .node_authorizer = &authorizer,
                      .result_tls_contexts = contexts})
                     .value();
  ::chronos::test::ScopedAllocationFailure failure{0U};
  auto server = DistributedVectorGroupedAggregateShuffleJobControlTlsServer::create(
      network::TlsSocket{}, {.authenticator = &authenticator, .service = &service}, {});
  failure.disable();
  ASSERT_FALSE(server.has_value());
  EXPECT_EQ(server.error().code(), common::StatusCode::kResourceExhausted);
}

TEST(DistributedVectorGroupedAggregateShuffleJobControlTlsAllocationFailureTest,
     ClassifiesTcpPreconnectRequestAllocation) {
  Authenticator authenticator;
  Authorizer authorizer;
  auto context = network::TlsClientContext::create(client_tls()).value();
  DistributedVectorGroupedAggregateShuffleJobControlRequest request{prepare()};
  ::chronos::test::ScopedAllocationFailure failure{0U};
  auto client = DistributedVectorGroupedAggregateShuffleJobControlTcpClient::begin(
      {.remote_endpoint = {{127U, 0U, 0U, 1U}, 9U},
       .tls_context = &context,
       .carrier = {.authenticator = &authenticator,
                   .node_authorizer = &authorizer,
                   .peer_ipv4_address = {127U, 0U, 0U, 1U},
                   .request = std::move(request)}},
      {});
  failure.disable();
  ASSERT_FALSE(client.has_value());
  EXPECT_EQ(client.error().code(), common::StatusCode::kResourceExhausted);
}

TEST(DistributedVectorGroupedAggregateShuffleJobControlTlsAllocationFailureTest,
     ClassifiesRetryOwnerRequestAllocation) {
  Authenticator authenticator;
  Authorizer authorizer;
  auto context = network::TlsClientContext::create(client_tls()).value();
  auto config = DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionConfig{
      .route = {.node_id = 3U, .endpoints = {{{127U, 0U, 0U, 1U}, 9U}}, .tls_context = &context},
      .authenticator = &authenticator,
      .node_authorizer = &authorizer,
      .request = DistributedVectorGroupedAggregateShuffleJobControlRequest{prepare()}};
  ::chronos::test::ScopedAllocationFailure failure{0U};
  auto acquisition =
      DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition::create(std::move(config));
  failure.disable();
  ASSERT_FALSE(acquisition.has_value());
  EXPECT_EQ(acquisition.error().code(), common::StatusCode::kResourceExhausted);
}

TEST(DistributedVectorGroupedAggregateShuffleJobControlTlsAllocationFailureTest,
     ClassifiesCoordinatorOwnershipAllocation) {
  Authenticator authenticator;
  Authorizer authorizer;
  auto context = network::TlsClientContext::create(client_tls()).value();
  auto prepared = prepare();
  auto authority = std::move(prepared.authority);
  const std::vector fragments{fragment()};
  auto finalization =
      DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2::create(authority, fragments)
          .value();
  auto config = DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionConfig{
      .coordinator_node_id = 9U,
      .reducer_control_routes = {{.node_id = 3U,
                                  .endpoints = {{{127U, 0U, 0U, 1U}, 9U}},
                                  .tls_context = &context}},
      .authenticator = &authenticator,
      .node_authorizer = &authorizer,
      .execution_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{1},
      .result = {.tls = server_tls(),
                 .authenticator = &authenticator,
                 .node_authorizer = &authorizer,
                 .coordinator_node_id = 9U}};
  ::chronos::test::ScopedAllocationFailure failure{0U};
  auto coordinator = DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution::create(
      authority, finalization, std::move(config));
  failure.disable();
  ASSERT_FALSE(coordinator.has_value());
  EXPECT_EQ(coordinator.error().code(), common::StatusCode::kResourceExhausted);
}

} // namespace
} // namespace chronos::cluster
