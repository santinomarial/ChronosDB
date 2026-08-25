#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_destination_execution.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_source_plan.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_tcp_execution.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::cluster {
namespace {

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

[[nodiscard]] schema::TabletId tablet(const std::uint8_t seed) {
  return schema::TabletId::from_uuid(uuid(seed)).value();
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

[[nodiscard]] DistributedVectorGroupedAggregateShuffleAuthority authority() {
  return DistributedVectorGroupedAggregateShuffleAuthority::create(
             uuid(1U),
             {{.tablet_id = tablet(2U), .node_id = 2U}, {.tablet_id = tablet(3U), .node_id = 3U}},
             {{.partition_id = 0U, .node_id = 3U}}, keys(), aggregates())
      .value();
}

[[nodiscard]] std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage>
input(const schema::TabletId& tablet_id) {
  auto state = query::MergeableVectorAggregateState::create(aggregates().front()).value();
  EXPECT_TRUE(state.accumulate_count_star().has_value());
  std::vector<query::ScalarValue> values;
  values.push_back(query::ScalarValue::text(string_type(), "shared-key").value());
  std::vector<query::MergeableVectorAggregateState> states;
  states.push_back(std::move(state));
  std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> encoded;
  encoded.push_back(query::encode_distributed_vector_grouped_aggregate_exchange_message(
                        {.query_id = uuid(1U),
                         .tablet_id = tablet_id,
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
  authenticate(const network::PeerAuthenticationRequest& request) override {
    saw_fingerprint = request.peer_certificate_sha256.has_value();
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = principal_};
  }

  bool saw_fingerprint{};

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

[[nodiscard]] query::ScalarValue cell(const query::VectorChunk& chunk, const std::size_t column) {
  const columnar::PhysicalColumnView* physical = chunk.column(column);
  EXPECT_NE(physical, nullptr);
  return query::ScalarValue::from_column_cell(
             physical->type(), chunk.cell({.column_ordinal = column, .selected_row = 0U}).value())
      .value();
}

[[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleTcpExecution>
remote_execution(const DistributedVectorGroupedAggregateShuffleAuthority& expected,
                 const query::QueryResourceContext& resources,
                 network::ConnectionAuthenticator& authenticator,
                 const ClusterNodePrincipalAuthorizer& authorizer,
                 network::TlsClientContext& tls_context, const network::Ipv4Endpoint endpoint) {
  const auto messages = input(tablet(2U));
  auto plan = DistributedVectorGroupedAggregateShuffleSourcePlan::create(expected, tablet(2U),
                                                                         messages, resources);
  if (!plan.has_value())
    return common::make_unexpected(plan.error());
  return DistributedVectorGroupedAggregateShuffleTcpExecution::create(
      expected, plan->take_remote_retries(),
      {.authenticator = &authenticator,
       .node_authorizer = &authorizer,
       .routes = {{.node_id = 3U, .endpoints = {endpoint}, .tls_context = &tls_context}},
       .carrier_limits = carrier_limits(),
       .connect_timeout = std::chrono::milliseconds{1000},
       .execution_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5}});
}

void drive(DistributedVectorGroupedAggregateShuffleTcpExecution& sender,
           DistributedVectorGroupedAggregateShuffleDestinationExecution& destination) {
  for (std::size_t iteration = 0U; iteration < 4096U; ++iteration) {
    ASSERT_TRUE(sender.poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(destination.poll_once(std::chrono::milliseconds{1}).is_ok());
    if (sender.state() == DistributedVectorGroupedAggregateShuffleTcpExecutionState::kComplete &&
        destination.transport_metrics().retained_streams == 0U) {
      return;
    }
  }
  FAIL() << "grouped shuffle sender and destination did not quiesce";
}

TEST(DistributedVectorGroupedAggregateShuffleDestinationExecutionTest,
     DrainsRemoteAndLocalSourcesAndAcknowledgesLateExactRetryAfterOutput) {
  auto expected = authority();
  auto resources = query::QueryResourceContext::create(16U << 20U).value();
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
                 .maximum_retained_streams = 4U,
                 .maximum_accepts_per_poll = 2U,
                 .maximum_reducer_admissions_per_poll = 2U});
  auto client_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(destination.has_value()) << destination.error().to_string();
  ASSERT_TRUE(client_context.has_value()) << client_context.error().to_string();

  const auto local_messages = input(tablet(3U));
  auto local_plan = DistributedVectorGroupedAggregateShuffleSourcePlan::create(
      expected, tablet(3U), local_messages, resources);
  ASSERT_TRUE(local_plan.has_value()) << local_plan.error().to_string();
  auto local_streams = local_plan->take_local_streams();
  ASSERT_EQ(local_streams.size(), 1U);
  EXPECT_TRUE(destination->accept_local_stream(local_streams.front()).is_ok());
  EXPECT_EQ(destination->state(),
            DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kReceiving);

  auto sender = remote_execution(expected, resources, server_authenticator, authorizer,
                                 *client_context, destination->bound_endpoint());
  ASSERT_TRUE(sender.has_value()) << sender.error().to_string();
  drive(*sender, *destination);
  EXPECT_EQ(destination->state(),
            DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kReady);
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
  EXPECT_TRUE(server_authenticator.saw_fingerprint);

  auto output = destination->next(0U);
  ASSERT_TRUE(output.has_value()) << output.error().to_string();
  ASSERT_EQ(output->kind(), query::PhysicalOperatorStepKind::kChunk);
  EXPECT_EQ(std::get<std::string>(cell(output->chunk()->chunk(), 0U).storage()), "shared-key");
  EXPECT_EQ(std::get<std::int64_t>(cell(output->chunk()->chunk(), 1U).storage()), 2);
  EXPECT_EQ(destination->next(0U)->kind(), query::PhysicalOperatorStepKind::kEnd);

  auto duplicate = remote_execution(expected, resources, server_authenticator, authorizer,
                                    *client_context, destination->bound_endpoint());
  ASSERT_TRUE(duplicate.has_value()) << duplicate.error().to_string();
  drive(*duplicate, *destination);
  auto reducer_metrics = destination->reducer_metrics(0U);
  ASSERT_TRUE(reducer_metrics.has_value());
  EXPECT_EQ(reducer_metrics->accepted_sources, 2U);
  EXPECT_EQ(reducer_metrics->duplicate_streams, 1U);
  EXPECT_EQ(destination->metrics().local_stream_deliveries, 1U);
  EXPECT_EQ(destination->metrics().remote_stream_deliveries, 2U);
  EXPECT_EQ(destination->metrics().ready_partitions, 1U);

  EXPECT_TRUE(destination->seal_transport().is_ok());
  EXPECT_EQ(destination->state(),
            DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kComplete);
}

TEST(DistributedVectorGroupedAggregateShuffleDestinationExecutionTest,
     CompletesLocalOnlyAuthorityWithoutTransportAndRejectsForeignNode) {
  auto local_authority = DistributedVectorGroupedAggregateShuffleAuthority::create(
                             uuid(1U), {{.tablet_id = tablet(3U), .node_id = 3U}},
                             {{.partition_id = 0U, .node_id = 3U}}, keys(), aggregates())
                             .value();
  auto resources = query::QueryResourceContext::create(4U << 20U).value();
  auto destination = DistributedVectorGroupedAggregateShuffleDestinationExecution::start(
      local_authority, {.local_node_id = 3U});
  ASSERT_TRUE(destination.has_value()) << destination.error().to_string();
  EXPECT_EQ(destination->bound_endpoint().port, 0U);
  EXPECT_EQ(DistributedVectorGroupedAggregateShuffleDestinationExecution::start(
                local_authority, {.local_node_id = 4U})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  const auto messages = input(tablet(3U));
  auto plan = DistributedVectorGroupedAggregateShuffleSourcePlan::create(
      local_authority, tablet(3U), messages, resources);
  ASSERT_TRUE(plan.has_value());
  auto streams = plan->take_local_streams();
  ASSERT_EQ(streams.size(), 1U);
  EXPECT_TRUE(destination->accept_local_stream(streams.front()).is_ok());
  EXPECT_EQ(destination->state(),
            DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kComplete);
  EXPECT_TRUE(destination->seal_transport().is_ok());
}

} // namespace
} // namespace chronos::cluster
