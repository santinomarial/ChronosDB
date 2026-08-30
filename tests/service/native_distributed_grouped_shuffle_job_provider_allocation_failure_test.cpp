#include "chronos/service/native_distributed_grouped_shuffle_job_provider.hpp"
#include "support/failing_allocator.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <utility>
#include <vector>

namespace chronos::service {
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

class TestAuthenticator final : public network::ConnectionAuthenticator {
public:
  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest&) override {
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = 9U};
  }
};

class TestAuthorizer final : public cluster::ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(std::uint64_t, raft::NodeId) const override {
    return true;
  }
};

TEST(NativeDistributedGroupedShuffleJobProviderAllocationFailureTest,
     ConstructionAndGatewayPreparationFailAtomically) {
  TestAuthenticator authenticator;
  TestAuthorizer authorizer;
  const NativeDistributedGroupedShuffleJobProviderConfig config{
      .coordinator_node_id = 9U, .authenticator = &authenticator, .node_authorizer = &authorizer};
  bool constructed{};
  for (std::size_t fail_after = 0U; fail_after < 32U; ++fail_after) {
    auto result = run_failure(
        fail_after, [&] { return NativeDistributedGroupedShuffleJobProvider::create(config); });
    if (result.has_value()) {
      constructed = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  ASSERT_TRUE(constructed);

  auto provider = NativeDistributedGroupedShuffleJobProvider::create(config).value();
  network::TlsClientContext tls;
  const std::vector fragments{fragment(1U, 2U), fragment(2U, 3U)};
  const std::vector<query::VectorGroupKeyDefinition> keys{
      {.column_ordinal = 0U, .type = int64_type(), .nullable = false}};
  const std::vector<query::VectorAggregateDefinition> aggregates{
      {.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
  const std::vector<cluster::DistributedQueryNodeRoute> routes{
      {.node_id = 2U, .endpoints = {{{127U, 0U, 0U, 1U}, 2002U}}, .tls_context = &tls},
      {.node_id = 3U, .endpoints = {{{127U, 0U, 0U, 1U}, 2003U}}, .tls_context = &tls}};
  bool prepared{};
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      return provider.prepare(fragments, keys, aggregates, routes,
                              std::chrono::steady_clock::now() + std::chrono::seconds{5});
    });
    if (result.has_value()) {
      prepared = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(prepared);
}

} // namespace
} // namespace chronos::service
