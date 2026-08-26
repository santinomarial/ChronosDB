#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_tcp_execution.hpp"
#include "support/failing_allocator.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <span>
#include <string>
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

[[nodiscard]] network::TlsClientConfig client_tls() {
  return {.certificate_chain_file = fixture_path("client.pem").string(),
          .private_key_file = fixture_path("client-key.pem").string(),
          .trust_store_file = fixture_path("ca.pem").string(),
          .expected_server_identity = "127.0.0.1"};
}

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

struct Fixture {
  DistributedVectorGroupedAggregateShuffleAuthority authority;
  query::DistributedVectorResultSchema schema;
  schema::LogicalType text_type;
  schema::LogicalType count_type;
};

[[nodiscard]] Fixture fixture() {
  const auto text = schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const auto count = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  const auto tablet = schema::TabletId::from_uuid(uuid(2U)).value();
  auto authority = DistributedVectorGroupedAggregateShuffleAuthority::create(
                       uuid(1U), {{tablet, 2U}}, {{0U, 3U}}, {{0U, text, false}},
                       {{query::VectorAggregateOperation::kCountStar, std::nullopt}})
                       .value();
  return {std::move(authority),
          {.columns = {{"region", text, false}, {"count", count, false}}},
          text,
          count};
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

TEST(DistributedVectorGroupedAggregateShuffleResultTcpExecutionAllocationFailureTest,
     ClassifiesRetryRouteAndSchedulerConstructionAllocations) {
  auto value = fixture();
  auto client_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(client_context.has_value());
  Authenticator authenticator;
  Authorizer authorizer;

  bool succeeded{};
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      try {
        const std::string text{"allocation-result-larger-than-SSO"};
        const std::array<std::byte, sizeof(std::int64_t)> count{std::byte{1U}};
        const std::array columns{network::QueryResultColumn{"region", value.text_type, false},
                                 network::QueryResultColumn{"count", value.count_type, false}};
        const std::array cells{network::QueryResultCell{.value = std::as_bytes(std::span{text})},
                               network::QueryResultCell{.value = count}};
        auto encoded = network::encode_query_result_batch(1U, columns, cells);
        if (!encoded.has_value()) {
          return common::Result<DistributedVectorGroupedAggregateShuffleResultTcpExecution>{
              common::make_unexpected(encoded.error())};
        }
        std::vector<std::vector<std::byte>> batches;
        batches.push_back(std::move(*encoded));
        auto prepared = DistributedVectorGroupedAggregateShuffleResultRetry::create(
            value.authority, value.schema,
            {.partition_id = 0U, .source_node_id = 3U, .coordinator_node_id = 9U},
            std::move(batches),
            {.retry = {.maximum_attempts = 2U,
                       .initial_backoff = std::chrono::milliseconds{1},
                       .maximum_backoff = std::chrono::milliseconds{1}},
             .stream = {.maximum_frames = 1U, .maximum_encoded_bytes = 1U << 20U}});
        if (!prepared.has_value()) {
          return common::Result<DistributedVectorGroupedAggregateShuffleResultTcpExecution>{
              common::make_unexpected(prepared.error())};
        }
        std::vector<DistributedVectorGroupedAggregateShuffleResultRetry> retries;
        retries.push_back(std::move(*prepared));
        return DistributedVectorGroupedAggregateShuffleResultTcpExecution::create(
            value.authority, value.schema, std::move(retries),
            {.authenticator = &authenticator,
             .node_authorizer = &authorizer,
             .routes = {{.node_id = 9U,
                         .endpoints = {{{127U, 0U, 0U, 1U}, 9U}},
                         .tls_context = &*client_context}},
             .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                                .exchange_timeout = std::chrono::milliseconds{1000},
                                .stream = {.maximum_frames = 1U,
                                           .maximum_encoded_bytes = 1U << 20U}},
             .connect_timeout = std::chrono::milliseconds{1000}});
      } catch (const std::bad_alloc&) {
        return common::Result<DistributedVectorGroupedAggregateShuffleResultTcpExecution>{
            common::make_unexpected(
                common::Status{common::StatusCode::kResourceExhausted,
                               "grouped shuffle result TCP allocation test setup failed"})};
      }
    });
    if (result.has_value()) {
      EXPECT_EQ(result->metrics().total_partitions, 1U);
      succeeded = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(succeeded);
}

} // namespace
} // namespace chronos::cluster
