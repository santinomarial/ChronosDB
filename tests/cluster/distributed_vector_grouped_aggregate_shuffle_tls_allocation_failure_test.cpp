#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_tls.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <cstdint>
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

TEST(DistributedVectorGroupedAggregateShuffleTlsAllocationFailureTest,
     ClassifiesClientAndServerOwnerAllocation) {
  const auto tablet = schema::TabletId::from_uuid(uuid(2U)).value();
  const auto string = schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const std::vector<query::VectorGroupKeyDefinition> keys{
      {.column_ordinal = 0U, .type = string, .nullable = false}};
  auto authority = DistributedVectorGroupedAggregateShuffleAuthority::create(
                       uuid(1U), {{.tablet_id = tablet, .node_id = 2U}},
                       {{.partition_id = 0U, .node_id = 3U}}, keys, {})
                       .value();
  query::DistributedVectorGroupedAggregateExchangeMessage empty{{.query_id = uuid(1U),
                                                                 .tablet_id = tablet,
                                                                 .sequence = 1U,
                                                                 .group_ordinal = 0U,
                                                                 .group_count = 0U,
                                                                 .terminal = true,
                                                                 .empty = true},
                                                                {},
                                                                {}};
  std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> messages;
  messages.push_back(
      query::encode_distributed_vector_grouped_aggregate_exchange_message(empty, keys, {}).value());
  const DistributedVectorGroupedAggregateShuffleEdge edge{.tablet_id = tablet,
                                                          .partition_id = 0U,
                                                          .source_node_id = 2U,
                                                          .target_node_id = 3U,
                                                          .hash_version = authority.hash_version()};
  query::QueryResourceContext resources = query::QueryResourceContext::create(4U << 20U).value();
  Authenticator authenticator;
  Authorizer authorizer;
  const DistributedVectorGroupedAggregateShuffleTlsClientConfig client_config{
      .authenticator = &authenticator, .node_authorizer = &authorizer};
  const DistributedVectorGroupedAggregateShuffleTlsServerConfig server_config{
      .authenticator = &authenticator,
      .node_authorizer = &authorizer,
      .authority = &authority,
      .local_node_id = 3U};

  bool client_success{};
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      auto sender = DistributedVectorGroupedAggregateShuffleStreamSender::create(
          authority, edge, messages, resources);
      if (!sender.has_value())
        return common::Result<DistributedVectorGroupedAggregateShuffleTlsClient>{
            common::make_unexpected(sender.error())};
      return DistributedVectorGroupedAggregateShuffleTlsClient::create(
          network::TlsSocket{}, std::move(*sender), authority, client_config, {});
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
      return DistributedVectorGroupedAggregateShuffleTlsServer::create(
          network::TlsSocket{}, resources, server_config, {});
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
