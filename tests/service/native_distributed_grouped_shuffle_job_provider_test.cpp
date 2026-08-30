#include "chronos/service/native_distributed_grouped_shuffle_job_provider.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <span>
#include <vector>

namespace chronos::service {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  return Identifier::from_uuid(uuid(seed)).value();
}

[[nodiscard]] schema::LogicalType int64_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
}

[[nodiscard]] query::DistributedMutableVectorFragment fragment(const std::uint8_t tablet_seed,
                                                               const raft::NodeId node_id) {
  return {.query_id = uuid(9U),
          .database_id = id<manifest::DatabaseId>(10U),
          .table_id = id<schema::TableId>(11U),
          .tablet_id = id<schema::TabletId>(tablet_seed),
          .destination_schema_id = id<schema::SchemaId>(12U),
          .raft_group_id = uuid(static_cast<std::uint8_t>(20U + tablet_seed)),
          .serving_node = node_id,
          .applied_position = 7U,
          .observed_leader_commit_position = 7U,
          .placement_epoch = 3U,
          .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
          .linearizable_barrier = raft::ReadBarrier{2U, 3U, 7U},
          .destination_column_ordinals = {0U},
          .plan = {.mode = query::DistributedVectorPlanMode::kRows, .row_output_indices = {0U}},
          .result_schema = {.columns = {{"value", int64_type(), false}}}};
}

[[nodiscard]] std::vector<query::VectorGroupKeyDefinition> keys() {
  return {{.column_ordinal = 0U, .type = int64_type(), .nullable = false}};
}

[[nodiscard]] std::vector<query::VectorAggregateDefinition> aggregates() {
  return {{.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
}

class TestAuthenticator final : public network::ConnectionAuthenticator {
public:
  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest&) override {
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = 9U};
  }
};

class TestAuthorizer final : public cluster::ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal,
                                      const raft::NodeId node) const override {
    return principal == node;
  }
};

struct Security {
  TestAuthenticator authenticator;
  TestAuthorizer authorizer;
};

[[nodiscard]] NativeDistributedGroupedShuffleJobProvider provider(Security& security) {
  auto created =
      NativeDistributedGroupedShuffleJobProvider::create({.coordinator_node_id = 9U,
                                                          .authenticator = &security.authenticator,
                                                          .node_authorizer = &security.authorizer});
  return std::move(created).value();
}

TEST(NativeDistributedGroupedShuffleJobProviderTest,
     SelectsGatewayLifecycleFromExactCanonicalQueryRoutes) {
  Security security;
  auto configured = provider(security);
  network::TlsClientContext node_two_tls;
  network::TlsClientContext node_three_tls;
  const std::vector fragments{fragment(1U, 3U), fragment(2U, 2U), fragment(3U, 3U)};
  const std::vector<cluster::DistributedQueryNodeRoute> routes{
      {.node_id = 2U, .endpoints = {{{127U, 0U, 0U, 1U}, 2002U}}, .tls_context = &node_two_tls},
      {.node_id = 3U, .endpoints = {{{127U, 0U, 0U, 1U}, 2003U}}, .tls_context = &node_three_tls}};
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};

  auto plan = configured.prepare(fragments, keys(), aggregates(), routes, deadline);
  ASSERT_TRUE(plan.has_value()) << plan.error().to_string();
  ASSERT_TRUE(plan->selected);
  ASSERT_TRUE(plan->reducer_jobs.has_value());
  const auto& reducers = *plan->reducer_jobs;
  EXPECT_EQ(reducers.coordinator_node_id, 9U);
  ASSERT_EQ(reducers.reducer_control_routes.size(), 2U);
  EXPECT_EQ(reducers.reducer_control_routes[0].node_id, 2U);
  EXPECT_EQ(reducers.reducer_control_routes[1].node_id, 3U);
  EXPECT_EQ(reducers.reducer_control_routes[0].tls_context, &node_two_tls);
  EXPECT_EQ(reducers.execution_deadline, deadline);
  EXPECT_EQ(reducers.result.execution_deadline, deadline);
  EXPECT_EQ(reducers.result.coordinator_node_id, 9U);
  EXPECT_EQ(reducers.result.authenticator, &security.authenticator);
  EXPECT_EQ(reducers.result.node_authorizer, &security.authorizer);
  EXPECT_GT(reducers.reducer_execution_timeout.count(), 0);
  EXPECT_LE(reducers.reducer_execution_timeout, std::chrono::seconds{5});
}

TEST(NativeDistributedGroupedShuffleJobProviderTest,
     LeavesAnyCoordinatorLocalQueryOnDirectGroupedLifecycle) {
  Security security;
  auto configured = provider(security);
  const std::vector fragments{fragment(1U, 2U), fragment(2U, 9U)};
  auto plan = configured.prepare(fragments, keys(), aggregates(), {},
                                 std::chrono::steady_clock::now() + std::chrono::seconds{1});
  ASSERT_TRUE(plan.has_value()) << plan.error().to_string();
  EXPECT_FALSE(plan->selected);
  EXPECT_FALSE(plan->reducer_jobs.has_value());
}

TEST(NativeDistributedGroupedShuffleJobProviderTest,
     RejectsExpiredIncompleteAndNoncanonicalGatewayAuthority) {
  Security security;
  auto configured = provider(security);
  network::TlsClientContext tls;
  const std::vector fragments{fragment(1U, 2U), fragment(2U, 3U)};
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{1};
  const std::vector<cluster::DistributedQueryNodeRoute> incomplete{
      {.node_id = 2U, .endpoints = {{{127U, 0U, 0U, 1U}, 2002U}}, .tls_context = &tls}};
  EXPECT_EQ(
      configured.prepare(fragments, keys(), aggregates(), incomplete, deadline).error().code(),
      common::StatusCode::kInvalidArgument);

  const std::vector<cluster::DistributedQueryNodeRoute> noncanonical{
      {.node_id = 3U, .endpoints = {{{127U, 0U, 0U, 1U}, 2003U}}, .tls_context = &tls},
      {.node_id = 2U, .endpoints = {{{127U, 0U, 0U, 1U}, 2002U}}, .tls_context = &tls}};
  EXPECT_EQ(
      configured.prepare(fragments, keys(), aggregates(), noncanonical, deadline).error().code(),
      common::StatusCode::kInvalidArgument);
  EXPECT_EQ(configured
                .prepare(fragments, keys(), aggregates(), incomplete,
                         std::chrono::steady_clock::now() - std::chrono::milliseconds{1})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

TEST(NativeDistributedGroupedShuffleJobProviderTest, RejectsInvalidConstructionAndMovedFromUse) {
  Security security;
  EXPECT_EQ(
      NativeDistributedGroupedShuffleJobProvider::create({.coordinator_node_id = 0U,
                                                          .authenticator = &security.authenticator,
                                                          .node_authorizer = &security.authorizer})
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);

  auto configured = provider(security);
  auto moved = std::move(configured);
  const std::vector fragments{fragment(1U, 2U)};
  EXPECT_EQ(configured
                .prepare(fragments, keys(), aggregates(), {},
                         std::chrono::steady_clock::now() + std::chrono::seconds{1})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  static_cast<void>(moved);
}

} // namespace
} // namespace chronos::service
