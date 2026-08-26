#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_coordinator_execution.hpp"
#include "support/failing_allocator.hpp"

#include <array>
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
    ::chronos::test::ScopedAllocationFailure failure{fail_after};
    result.emplace(operation());
    failure.disable();
  }
  return std::move(*result);
}

[[nodiscard]] std::filesystem::path fixture_path(const char* name) {
  return std::filesystem::path{CHRONOS_NETWORK_FIXTURE_DIR} / "tls" / name;
}

[[nodiscard]] network::TlsServerConfig server_tls() {
  return {.certificate_chain_file = fixture_path("server.pem").string(),
          .private_key_file = fixture_path("server-key.pem").string(),
          .trust_store_file = fixture_path("ca.pem").string()};
}

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

class Authenticator final : public network::ConnectionAuthenticator {
public:
  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest&) override {
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = 91U};
  }
};

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(std::uint64_t, raft::NodeId) const override {
    return common::Result<bool>{true};
  }
};

TEST(DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionAllocationFailureTest,
     ClassifiesCollectorListenerTlsAndCoordinatorConstructionAllocations) {
  const auto tablet = schema::TabletId::from_uuid(uuid(2U)).value();
  const auto type = schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const std::vector<query::VectorGroupKeyDefinition> keys{
      {.column_ordinal = 0U, .type = type, .nullable = false}};
  const query::DistributedMutableVectorFragment fragment{
      .query_id = uuid(1U),
      .database_id = manifest::DatabaseId::from_uuid(uuid(8U)).value(),
      .table_id = schema::TableId::from_uuid(uuid(9U)).value(),
      .tablet_id = tablet,
      .destination_schema_id = schema::SchemaId::from_uuid(uuid(10U)).value(),
      .raft_group_id = uuid(22U),
      .serving_node = 2U,
      .applied_position = 10U,
      .observed_leader_commit_position = 10U,
      .placement_epoch = 3U,
      .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
      .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
      .destination_column_ordinals = {0U},
      .plan = {.mode = query::DistributedVectorPlanMode::kGroupedAggregate,
               .group_key_input_indices = {0U}},
      .result_schema = {.columns = {{"region", type, false}}}};
  const std::array fragments{fragment};
  auto authority = DistributedVectorGroupedAggregateShuffleAuthority::create_from_mutable_fragments(
                       fragments, keys, {})
                       .value();
  auto finalization =
      DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2::create(authority, fragments)
          .value();
  Authenticator authenticator;
  Authorizer authorizer;
  bool saw_failure{};
  bool saw_success{};
  for (std::size_t fail_after = 0U; fail_after < 512U; ++fail_after) {
    SCOPED_TRACE(testing::Message{} << "fail_after=" << fail_after);
    DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionConfig config{
        .listener = {},
        .tls = server_tls(),
        .authenticator = &authenticator,
        .node_authorizer = &authorizer,
        .coordinator_node_id = 9U,
        .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{100},
                           .exchange_timeout = std::chrono::milliseconds{100}},
        .maximum_retained_server_streams = 8U,
        .maximum_accepts_per_poll = 2U};
    auto result = run_failure(fail_after, [&] {
      return DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution::create(
          authority, finalization, std::move(config));
    });
    if (!result.has_value()) {
      saw_failure = true;
      EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted)
          << result.error().to_string();
      continue;
    }
    saw_success = true;
    EXPECT_TRUE(result->cancel().code() == common::StatusCode::kCancelled);
    break;
  }
  EXPECT_TRUE(saw_failure);
  EXPECT_TRUE(saw_success);
}

} // namespace
} // namespace chronos::cluster
