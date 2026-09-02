#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_tls.hpp"
#include "support/failing_allocator.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
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
    test::ScopedAllocationFailure failure{fail_after};
    result.emplace(operation());
    failure.disable();
  }
  return std::move(*result);
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

TEST(DistributedVectorGroupedAggregateShuffleResultTlsAllocationFailureTest,
     ClassifiesClientAndServerOwnerAllocations) {
  const auto string = schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const auto tablet = schema::TabletId::from_uuid(uuid(2U)).value();
  auto authority = DistributedVectorGroupedAggregateShuffleAuthority::create(
                       uuid(1U), {{tablet, 2U}}, {{0U, 3U}}, {{0U, string, false}},
                       {{query::VectorAggregateOperation::kCountStar, std::nullopt}})
                       .value();
  const query::DistributedVectorResultSchema schema{.columns = {{"region", string, false}}};
  const std::string label = "result";
  const std::array columns{network::QueryResultColumn{"region", string, false}};
  const std::array cells{network::QueryResultCell{.value = std::as_bytes(std::span{label})}};
  const std::vector<std::vector<std::byte>> batches{
      network::encode_query_result_batch(1U, columns, cells).value()};
  Authenticator authenticator;
  Authorizer authorizer;
  const DistributedVectorGroupedAggregateShuffleResultTlsClientConfig client_config{
      .authenticator = &authenticator, .node_authorizer = &authorizer, .limits = {}};
  const DistributedVectorGroupedAggregateShuffleResultTlsServerConfig server_config{
      .authenticator = &authenticator,
      .node_authorizer = &authorizer,
      .authority = &authority,
      .result_schema = &schema,
      .coordinator_node_id = 9U,
      .limits = {}};

  bool client_success{};
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      auto sender = DistributedVectorGroupedAggregateShuffleResultStreamSender::create(
          authority, schema, 0U, 3U, 9U, batches);
      if (!sender.has_value()) {
        return common::Result<DistributedVectorGroupedAggregateShuffleResultTlsClient>{
            common::make_unexpected(sender.error())};
      }
      return DistributedVectorGroupedAggregateShuffleResultTlsClient::create(
          network::TlsSocket{}, std::move(*sender), authority, schema, 9U, client_config, {});
    });
    if (result.has_value()) {
      client_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(client_success);

  bool server_success{};
  for (std::size_t fail_after = 0U; fail_after < 16U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      return DistributedVectorGroupedAggregateShuffleResultTlsServer::create(network::TlsSocket{},
                                                                             server_config, {});
    });
    if (result.has_value()) {
      server_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(server_success);
}

} // namespace
} // namespace chronos::cluster
