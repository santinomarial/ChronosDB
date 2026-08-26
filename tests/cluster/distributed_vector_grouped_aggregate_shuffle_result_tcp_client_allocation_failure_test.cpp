#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_tcp_client.hpp"
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

TEST(DistributedVectorGroupedAggregateShuffleResultTcpClientAllocationFailureTest,
     ClassifiesOwnerAllocationAndClosesTheDescriptor) {
  const auto tablet = schema::TabletId::from_uuid(uuid(2U)).value();
  const auto type = schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const std::vector<query::VectorGroupKeyDefinition> keys{
      {.column_ordinal = 0U, .type = type, .nullable = false}};
  const std::vector<query::VectorAggregateDefinition> aggregates{
      {.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
  auto authority = DistributedVectorGroupedAggregateShuffleAuthority::create(
                       uuid(1U), {{.tablet_id = tablet, .node_id = 2U}},
                       {{.partition_id = 0U, .node_id = 3U}}, keys, aggregates)
                       .value();
  query::DistributedVectorResultSchema schema{.columns = {{"region", type, false}}};
  const std::string value{"allocation-result-larger-than-SSO"};
  const std::array columns{network::QueryResultColumn{"region", type, false}};
  const std::array cells{network::QueryResultCell{.value = std::as_bytes(std::span{value})}};
  std::vector<std::vector<std::byte>> batches;
  batches.push_back(network::encode_query_result_batch(1U, columns, cells).value());
  auto stream = DistributedVectorGroupedAggregateShuffleResultStreamSender::create(
                    authority, schema, 0U, 3U, 9U, batches)
                    .value();
  auto listener = network::TcpListener::bind();
  auto context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(listener.has_value());
  ASSERT_TRUE(context.has_value());
  Authenticator authenticator;
  Authorizer authorizer;
  auto result = run_failure(0U, [&] {
    return DistributedVectorGroupedAggregateShuffleResultTcpClient::begin(
        {.attempt_number = 1U, .target_node_id = 9U, .stream = std::move(stream)}, authority,
        schema,
        {.remote_endpoint = listener->bound_endpoint(),
         .tls_context = &*context,
         .carrier = {.authenticator = &authenticator,
                     .node_authorizer = &authorizer,
                     .peer_ipv4_address = {127U, 0U, 0U, 1U}},
         .connect_timeout = std::chrono::milliseconds{1000}},
        {});
  });
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
}

} // namespace
} // namespace chronos::cluster
