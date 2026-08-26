#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_service.hpp"
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
          .coordinator_result_endpoint = {{127U, 0U, 0U, 1U}, 9123U},
          .execution_timeout = std::chrono::seconds{5},
          .authority = DistributedVectorGroupedAggregateShuffleAuthority::create(
                           uuid(1U), {{schema::TabletId::from_uuid(uuid(2U)).value(), 2U}},
                           {{0U, 3U}}, {{0U, string, false}},
                           {{query::VectorAggregateOperation::kCountStar, std::nullopt}})
                           .value(),
          .result_schema = {.columns = {{"region", string, false}, {"count", count, false}}}};
}

TEST(DistributedVectorGroupedAggregateShuffleJobServiceAllocationFailureTest,
     ClassifiesServiceConstructionAllocation) {
  Authenticator authenticator;
  Authorizer authorizer;
  auto context = network::TlsClientContext::create(client_tls()).value();
  const std::array contexts{DistributedQueryNodeTlsContext{9U, &context}};
  ::chronos::test::ScopedAllocationFailure failure{0U};
  auto service = DistributedVectorGroupedAggregateShuffleJobService::create(
      {.local_node_id = 3U,
       .shuffle_authenticator = &authenticator,
       .result_authenticator = &authenticator,
       .node_authorizer = &authorizer,
       .result_tls_contexts = contexts});
  failure.disable();
  ASSERT_FALSE(service.has_value());
  EXPECT_EQ(service.error().code(), common::StatusCode::kResourceExhausted);
}

TEST(DistributedVectorGroupedAggregateShuffleJobServiceAllocationFailureTest,
     PreservesServiceAndReturnsExhaustionWhenPrepareOwnershipCannotAllocate) {
  Authenticator authenticator;
  Authorizer authorizer;
  auto context = network::TlsClientContext::create(client_tls()).value();
  const std::array contexts{DistributedQueryNodeTlsContext{9U, &context}};
  auto service = DistributedVectorGroupedAggregateShuffleJobService::create(
                     {.local_node_id = 3U,
                      .shuffle_tls = server_tls(),
                      .shuffle_authenticator = &authenticator,
                      .result_authenticator = &authenticator,
                      .node_authorizer = &authorizer,
                      .result_tls_contexts = contexts})
                     .value();
  DistributedVectorGroupedAggregateShuffleJobControlRequest request{prepare()};
  const network::PeerAuthenticationResult peer{.authorized = true, .principal_id = 1U};
  ::chronos::test::ScopedAllocationFailure failure{0U};
  auto response = service.receive(std::move(request), peer, std::chrono::steady_clock::now());
  failure.disable();
  ASSERT_TRUE(response.has_value()) << response.error().to_string();
  EXPECT_EQ(response->status_code, common::StatusCode::kResourceExhausted);
  EXPECT_EQ(service.metrics().active_jobs, 0U);
}

} // namespace
} // namespace chronos::cluster
