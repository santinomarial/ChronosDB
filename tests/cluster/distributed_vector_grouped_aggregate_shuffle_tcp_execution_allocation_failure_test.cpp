#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_tcp_execution.hpp"
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
    ::chronos::test::ScopedAllocationFailure failure{fail_after};
    result.emplace(operation());
    failure.disable();
  }
  return std::move(*result);
}

[[nodiscard]] std::filesystem::path fixture(const char* name) {
  return std::filesystem::path{CHRONOS_NETWORK_FIXTURE_DIR} / "tls" / name;
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

[[nodiscard]] schema::TabletId tablet() {
  return schema::TabletId::from_uuid(uuid(2U)).value();
}

class Authenticator final : public network::ConnectionAuthenticator {
public:
  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest&) override {
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = 92U};
  }
};

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(std::uint64_t, raft::NodeId) const override {
    return common::Result<bool>{true};
  }
};

TEST(DistributedVectorGroupedAggregateShuffleTcpExecutionAllocationFailureTest,
     ClassifiesEveryPreallocatedSchedulerConstructionAllocation) {
  const auto type = schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const std::vector<query::VectorGroupKeyDefinition> keys{
      {.column_ordinal = 0U, .type = type, .nullable = false}};
  const std::vector<query::VectorAggregateDefinition> aggregates{
      {.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
  auto authority = DistributedVectorGroupedAggregateShuffleAuthority::create(
                       uuid(1U), {{.tablet_id = tablet(), .node_id = 2U}},
                       {{.partition_id = 0U, .node_id = 3U}}, keys, aggregates)
                       .value();
  auto resources = query::QueryResourceContext::create(4U << 20U).value();
  auto state = query::MergeableVectorAggregateState::create(aggregates.front()).value();
  EXPECT_TRUE(state.accumulate_count_star().has_value());
  std::vector<query::ScalarValue> values;
  values.push_back(query::ScalarValue::text(type, "allocation-remote").value());
  std::vector<query::MergeableVectorAggregateState> states;
  states.push_back(std::move(state));
  auto client_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(client_context.has_value());
  Authenticator authenticator;
  Authorizer authorizer;

  bool succeeded{};
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      try {
        std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> encoded;
        auto message = query::encode_distributed_vector_grouped_aggregate_exchange_message(
            {.query_id = uuid(1U),
             .tablet_id = tablet(),
             .sequence = 1U,
             .group_ordinal = 0U,
             .group_count = 1U,
             .terminal = true,
             .empty = false},
            values, states, keys, aggregates);
        if (!message.has_value()) {
          return common::Result<DistributedVectorGroupedAggregateShuffleTcpExecution>{
              common::make_unexpected(message.error())};
        }
        encoded.push_back(std::move(*message));
        auto prepared_retry = DistributedVectorGroupedAggregateShuffleRetry::create(
            authority,
            {.tablet_id = tablet(),
             .partition_id = 0U,
             .source_node_id = 2U,
             .target_node_id = 3U,
             .hash_version = authority.hash_version()},
            std::move(encoded), resources);
        if (!prepared_retry.has_value()) {
          return common::Result<DistributedVectorGroupedAggregateShuffleTcpExecution>{
              common::make_unexpected(prepared_retry.error())};
        }
        std::vector<DistributedVectorGroupedAggregateShuffleRetry> retries;
        retries.push_back(std::move(*prepared_retry));
        DistributedVectorGroupedAggregateShuffleTcpExecutionConfig config{
            .authenticator = &authenticator,
            .node_authorizer = &authorizer,
            .routes = {{.node_id = 3U,
                        .endpoints = {{{127U, 0U, 0U, 1U}, 9U}},
                        .tls_context = &*client_context}},
            .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                               .exchange_timeout = std::chrono::milliseconds{1000},
                               .stream = {.maximum_frames = 1U,
                                          .maximum_encoded_bytes = 1U << 20U}},
            .connect_timeout = std::chrono::milliseconds{1000}};
        return DistributedVectorGroupedAggregateShuffleTcpExecution::create(
            authority, std::move(retries), std::move(config));
      } catch (const std::bad_alloc&) {
        return common::Result<DistributedVectorGroupedAggregateShuffleTcpExecution>{
            common::make_unexpected(
                common::Status{common::StatusCode::kResourceExhausted,
                               "grouped shuffle TCP allocation test setup failed"})};
      }
    });
    if (result.has_value()) {
      EXPECT_EQ(result->metrics().total_edges, 1U);
      succeeded = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(succeeded);
}

} // namespace
} // namespace chronos::cluster
