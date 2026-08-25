#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_tcp_client.hpp"
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

TEST(DistributedVectorGroupedAggregateShuffleTcpClientAllocationFailureTest,
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
  const DistributedVectorGroupedAggregateShuffleEdge edge{.tablet_id = tablet,
                                                          .partition_id = 0U,
                                                          .source_node_id = 2U,
                                                          .target_node_id = 3U,
                                                          .hash_version = authority.hash_version()};
  auto state = query::MergeableVectorAggregateState::create(aggregates.front()).value();
  ASSERT_TRUE(state.accumulate_count_star().has_value());
  std::vector<query::ScalarValue> values;
  values.push_back(query::ScalarValue::text(type, "allocation-key-larger-than-SSO").value());
  std::vector<query::MergeableVectorAggregateState> states;
  states.push_back(std::move(state));
  query::DistributedVectorGroupedAggregateExchangeMessage message{{.query_id = uuid(1U),
                                                                   .tablet_id = tablet,
                                                                   .sequence = 1U,
                                                                   .group_ordinal = 0U,
                                                                   .group_count = 1U,
                                                                   .terminal = true,
                                                                   .empty = false},
                                                                  std::move(values),
                                                                  std::move(states)};
  std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> encoded;
  encoded.push_back(
      query::encode_distributed_vector_grouped_aggregate_exchange_message(message, keys, aggregates)
          .value());
  auto resources = query::QueryResourceContext::create(4U << 20U).value();
  auto stream = DistributedVectorGroupedAggregateShuffleStreamSender::create(authority, edge,
                                                                             encoded, resources)
                    .value();
  auto listener = network::TcpListener::bind();
  auto context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(listener.has_value());
  ASSERT_TRUE(context.has_value());
  Authenticator authenticator;
  Authorizer authorizer;
  auto result = run_failure(0U, [&] {
    return DistributedVectorGroupedAggregateShuffleTcpClient::begin(
        {.attempt_number = 1U, .target_node_id = 3U, .stream = std::move(stream)}, authority,
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
