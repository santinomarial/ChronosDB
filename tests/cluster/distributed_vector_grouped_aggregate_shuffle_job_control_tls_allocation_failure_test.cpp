#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_control_tls.hpp"
#include "support/failing_allocator.hpp"

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
  auto service = DistributedVectorGroupedAggregateShuffleJobService::create(
                     {.local_node_id = 3U,
                      .shuffle_authenticator = &authenticator,
                      .result_authenticator = &authenticator,
                      .node_authorizer = &authorizer,
                      .result_tls_context = &context})
                     .value();
  ::chronos::test::ScopedAllocationFailure failure{0U};
  auto server = DistributedVectorGroupedAggregateShuffleJobControlTlsServer::create(
      network::TlsSocket{}, {.authenticator = &authenticator, .service = &service}, {});
  failure.disable();
  ASSERT_FALSE(server.has_value());
  EXPECT_EQ(server.error().code(), common::StatusCode::kResourceExhausted);
}

} // namespace
} // namespace chronos::cluster
