#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_shuffle_job_execution.hpp"
#include "support/failing_allocator.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

template <typename Operation>
[[nodiscard]] auto run_failure(const std::size_t fail_after, Operation&& operation) {
  using Result = decltype(operation());
  std::optional<Result> result;
  {
    test::ScopedAllocationFailure failure{fail_after};
    try {
      result.emplace(operation());
    } catch (...) {
      failure.disable();
      throw;
    }
    failure.disable();
  }
  return std::move(*result);
}

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
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  return Identifier::from_uuid(uuid(seed)).value();
}

[[nodiscard]] schema::LogicalType string_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
}

[[nodiscard]] std::vector<query::VectorGroupKeyDefinition> keys() {
  return {{.column_ordinal = 0U, .type = string_type(), .nullable = false}};
}

[[nodiscard]] std::vector<query::VectorAggregateDefinition> aggregates() {
  return {{.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
}

[[nodiscard]] query::DistributedMutableVectorFragment fragment() {
  const auto int64 = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  return {
      .query_id = uuid(1U),
      .database_id = id<manifest::DatabaseId>(2U),
      .table_id = id<schema::TableId>(3U),
      .tablet_id = id<schema::TabletId>(4U),
      .destination_schema_id = id<schema::SchemaId>(5U),
      .raft_group_id = uuid(6U),
      .serving_node = 2U,
      .applied_position = 10U,
      .observed_leader_commit_position = 10U,
      .placement_epoch = 3U,
      .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
      .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
      .destination_column_ordinals = {0U},
      .plan = {.mode = query::DistributedVectorPlanMode::kGroupedAggregate,
               .group_key_input_indices = {0U},
               .aggregates = {{.operation = query::VectorAggregateOperation::kCountStar}}},
      .result_schema = {.columns = {{"region", string_type(), false}, {"count", int64, false}}}};
}

class Authenticator final : public network::ConnectionAuthenticator {
public:
  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest&) override {
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = 9U};
  }
};

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(std::uint64_t, raft::NodeId) const override {
    return true;
  }
};

[[nodiscard]] DistributedMutableVectorGroupedAggregateShuffleJobExecutionConfig
config(network::TlsClientContext& tls, Authenticator& authenticator, const Authorizer& authorizer) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
  DistributedMutableVectorGroupedAggregateShuffleJobExecutionConfig value;
  value.worker_execution.sender.retry.maximum_attempts = 1U;
  value.worker_execution.sender.maximum_response_frames = 1U;
  value.worker_execution.coordinator.messages.maximum_messages_per_fragment = 1U;
  value.worker_execution.coordinator.messages.maximum_total_messages = 1U;
  value.worker_transport.authenticator = &authenticator;
  value.worker_transport.node_authorizer = &authorizer;
  value.worker_transport.routes = {
      {.node_id = 2U, .endpoints = {{{127U, 0U, 0U, 1U}, 2002U}}, .tls_context = &tls}};
  value.worker_transport.execution_deadline = deadline;
  value.worker_transport.maximum_rebindings = 0U;
  value.reducers.coordinator_node_id = 9U;
  value.reducers.reducer_control_routes = {
      {.node_id = 2U, .endpoints = {{{127U, 0U, 0U, 1U}, 2002U}}, .tls_context = &tls}};
  value.reducers.authenticator = &authenticator;
  value.reducers.node_authorizer = &authorizer;
  value.reducers.prepare_retry.maximum_attempts = 1U;
  value.reducers.route_install_retry.maximum_attempts = 1U;
  value.reducers.seal_retry.maximum_attempts = 1U;
  value.reducers.execution_deadline = deadline;
  value.reducers.result.tls = server_tls();
  value.reducers.result.authenticator = &authenticator;
  value.reducers.result.node_authorizer = &authorizer;
  value.reducers.result.coordinator_node_id = 9U;
  value.reducers.result.maximum_retained_server_streams = 1U;
  value.reducers.result.maximum_accepts_per_poll = 1U;
  return value;
}

TEST(DistributedMutableVectorGroupedAggregateShuffleJobExecutionAllocationFailureTest,
     ClassifiesEveryObservedConstructionAllocation) {
  Authenticator authenticator;
  Authorizer authorizer;
  auto tls = network::TlsClientContext::create(client_tls()).value();
  bool succeeded{};
  for (std::size_t fail_after = 0U; fail_after < 4096U; ++fail_after) {
    auto fragments = std::vector{fragment()};
    auto key_definitions = keys();
    auto aggregate_definitions = aggregates();
    auto execution_config = config(tls, authenticator, authorizer);
    common::Result<DistributedMutableVectorGroupedAggregateShuffleJobExecution> result =
        common::make_unexpected(
            common::Status{common::StatusCode::kInternal, "allocation test did not run"});
    try {
      result = run_failure(fail_after, [&] {
        return DistributedMutableVectorGroupedAggregateShuffleJobExecution::create(
            9U, std::move(fragments), std::move(key_definitions), std::move(aggregate_definitions),
            std::move(execution_config));
      });
    } catch (const std::bad_alloc&) {
      ADD_FAILURE() << "construction leaked std::bad_alloc at allocation " << fail_after;
      break;
    }
    if (result.has_value()) {
      succeeded = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted)
        << result.error().to_string();
  }
  EXPECT_TRUE(succeeded);
}

} // namespace
} // namespace chronos::cluster
