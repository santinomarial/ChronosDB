#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_destination_execution.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_retry.hpp"
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

[[nodiscard]] network::TlsServerConfig server_tls() {
  return {.certificate_chain_file = fixture("server.pem").string(),
          .private_key_file = fixture("server-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string()};
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

[[nodiscard]] schema::LogicalType string_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
}

[[nodiscard]] std::vector<query::VectorGroupKeyDefinition> keys() {
  return {{.column_ordinal = 0U, .type = string_type(), .nullable = false}};
}

[[nodiscard]] std::vector<query::VectorAggregateDefinition> aggregates() {
  return {{.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleAuthority
authority(const raft::NodeId source_node) {
  return DistributedVectorGroupedAggregateShuffleAuthority::create(
             uuid(1U), {{.tablet_id = tablet(), .node_id = source_node}},
             {{.partition_id = 0U, .node_id = 3U}}, keys(), aggregates())
      .value();
}

[[nodiscard]] std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> input() {
  auto state = query::MergeableVectorAggregateState::create(aggregates().front()).value();
  EXPECT_TRUE(state.accumulate_count_star().has_value());
  std::vector<query::ScalarValue> values;
  values.push_back(query::ScalarValue::text(string_type(), "allocation-key").value());
  std::vector<query::MergeableVectorAggregateState> states;
  states.push_back(std::move(state));
  std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> encoded;
  encoded.push_back(query::encode_distributed_vector_grouped_aggregate_exchange_message(
                        {.query_id = uuid(1U),
                         .tablet_id = tablet(),
                         .sequence = 1U,
                         .group_ordinal = 0U,
                         .group_count = 1U,
                         .terminal = true,
                         .empty = false},
                        values, states, keys(), aggregates())
                        .value());
  return encoded;
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleTlsLimits carrier_limits() {
  return {.handshake_timeout = std::chrono::milliseconds{1000},
          .exchange_timeout = std::chrono::milliseconds{1000},
          .stream = {.maximum_frames = 1U, .maximum_encoded_bytes = 1U << 20U}};
}

class Authenticator final : public network::ConnectionAuthenticator {
public:
  explicit Authenticator(const std::uint64_t principal) : principal_(principal) {}

  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest&) override {
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = principal_};
  }

private:
  std::uint64_t principal_{};
};

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal,
                                      const raft::NodeId node) const override {
    return common::Result<bool>{(principal == 91U && node == 2U) ||
                                (principal == 92U && node == 3U)};
  }
};

TEST(DistributedVectorGroupedAggregateShuffleDestinationExecutionAllocationFailureTest,
     ClassifiesConstructionAndRetainsAcknowledgedStreamAcrossAdmissionFailure) {
  auto local = authority(3U);
  bool create_success{};
  for (std::size_t fail_after = 0U; fail_after < 512U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      return DistributedVectorGroupedAggregateShuffleDestinationExecution::start(
          local, {.local_node_id = 3U});
    });
    if (result.has_value()) {
      create_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(create_success);

  auto expected = authority(2U);
  auto resources = query::QueryResourceContext::create(8U << 20U).value();
  Authenticator client_authenticator{91U};
  Authenticator server_authenticator{92U};
  Authorizer authorizer;
  auto destination = DistributedVectorGroupedAggregateShuffleDestinationExecution::start(
      expected, {.local_node_id = 3U,
                 .listener = {},
                 .tls = server_tls(),
                 .authenticator = &client_authenticator,
                 .node_authorizer = &authorizer,
                 .resources = resources,
                 .carrier_limits = carrier_limits(),
                 .maximum_retained_streams = 1U,
                 .maximum_accepts_per_poll = 1U,
                 .maximum_reducer_admissions_per_poll = 1U});
  auto client_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(destination.has_value()) << destination.error().to_string();
  ASSERT_TRUE(client_context.has_value()) << client_context.error().to_string();

  auto prepared = DistributedVectorGroupedAggregateShuffleRetry::create(
      expected,
      {.tablet_id = tablet(),
       .partition_id = 0U,
       .source_node_id = 2U,
       .target_node_id = 3U,
       .hash_version = expected.hash_version()},
      input(), resources);
  ASSERT_TRUE(prepared.has_value()) << prepared.error().to_string();
  std::vector<DistributedVectorGroupedAggregateShuffleRetry> retries;
  retries.push_back(std::move(*prepared));
  auto sender = DistributedVectorGroupedAggregateShuffleTcpExecution::create(
      expected, std::move(retries),
      {.authenticator = &server_authenticator,
       .node_authorizer = &authorizer,
       .routes = {{.node_id = 3U,
                   .endpoints = {destination->bound_endpoint()},
                   .tls_context = &*client_context}},
       .carrier_limits = carrier_limits(),
       .connect_timeout = std::chrono::milliseconds{1000},
       .execution_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5}});
  ASSERT_TRUE(sender.has_value()) << sender.error().to_string();

  for (std::size_t iteration = 0U; iteration < 4096U; ++iteration) {
    ASSERT_TRUE(sender->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(destination->poll_once(std::chrono::milliseconds{1}).is_ok());
    if (destination->transport_metrics().retained_streams == 1U) {
      break;
    }
  }
  ASSERT_EQ(destination->transport_metrics().retained_streams, 1U);

  const common::Status failed =
      run_failure(0U, [&] { return destination->poll_once(std::chrono::milliseconds{0}); });
  EXPECT_EQ(failed.code(), common::StatusCode::kResourceExhausted) << failed.to_string();
  EXPECT_EQ(destination->metrics().pending_remote_streams, 1U);
  EXPECT_EQ(destination->transport_metrics().retained_streams, 0U);
  EXPECT_EQ(destination->state(),
            DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kReceiving);

  EXPECT_TRUE(destination->poll_once(std::chrono::milliseconds{0}).is_ok());
  EXPECT_EQ(destination->metrics().pending_remote_streams, 0U);
  EXPECT_EQ(destination->metrics().remote_stream_deliveries, 1U);
  EXPECT_EQ(destination->state(),
            DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kReady);
  for (std::size_t iteration = 0U;
       iteration < 4096U &&
       sender->state() != DistributedVectorGroupedAggregateShuffleTcpExecutionState::kComplete;
       ++iteration) {
    ASSERT_TRUE(sender->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(sender->state(), DistributedVectorGroupedAggregateShuffleTcpExecutionState::kComplete);
  EXPECT_TRUE(destination->seal_transport().is_ok());
}

} // namespace
} // namespace chronos::cluster
