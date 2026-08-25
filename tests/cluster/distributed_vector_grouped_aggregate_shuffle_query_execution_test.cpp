#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_query_execution.hpp"
#include "chronos/common/byte_reader.hpp"
#include "chronos/network/messages.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

template <typename Id> [[nodiscard]] Id id(const std::uint8_t seed) {
  return Id::from_uuid(uuid(seed)).value();
}

[[nodiscard]] schema::TabletId tablet() {
  return id<schema::TabletId>(2U);
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

[[nodiscard]] schema::LogicalType string_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
}

[[nodiscard]] std::vector<query::VectorGroupKeyDefinition> keys() {
  return {{.column_ordinal = 0U, .type = string_type(), .nullable = false}};
}

[[nodiscard]] std::vector<query::VectorAggregateDefinition> aggregates() {
  return {{.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
}

[[nodiscard]] query::DistributedMutableVectorFragment fragment() {
  const auto int64 = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  return {
      .query_id = uuid(1U),
      .database_id = id<manifest::DatabaseId>(8U),
      .table_id = id<schema::TableId>(9U),
      .tablet_id = tablet(),
      .destination_schema_id = id<schema::SchemaId>(10U),
      .raft_group_id = uuid(11U),
      .serving_node = 2U,
      .applied_position = 10U,
      .observed_leader_commit_position = 10U,
      .placement_epoch = 3U,
      .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
      .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
      .destination_column_ordinals = {0U},
      .plan = {.mode = query::DistributedVectorPlanMode::kGroupedAggregate,
               .group_key_input_indices = {0U},
               .aggregates = {{.operation = query::VectorAggregateOperation::kCountStar}},
               .limit = 1U},
      .result_schema = {.columns = {{"region", string_type(), false}, {"count", int64, false}}}};
}

[[nodiscard]] query::DistributedMutableVectorFragment fragment(const schema::TabletId& tablet_id,
                                                               const raft::NodeId node) {
  auto value = fragment();
  value.tablet_id = tablet_id;
  value.raft_group_id = uuid(static_cast<std::uint8_t>(node + 20U));
  value.serving_node = node;
  value.linearizable_barrier = raft::ReadBarrier{node, 3U, 10U};
  value.plan.limit = std::nullopt;
  return value;
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleAuthority authority() {
  std::vector fragments{fragment()};
  return DistributedVectorGroupedAggregateShuffleAuthority::create_from_mutable_fragments(
             fragments, keys(), aggregates())
      .value();
}

[[nodiscard]] std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage>
messages() {
  auto state = query::MergeableVectorAggregateState::create(aggregates().front()).value();
  EXPECT_TRUE(state.accumulate_count_star().has_value());
  EXPECT_TRUE(state.accumulate_count_star().has_value());
  std::vector<query::ScalarValue> values;
  values.push_back(query::ScalarValue::text(string_type(), "east").value());
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

[[nodiscard]] std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage>
messages(const schema::TabletId& tablet_id, std::string label) {
  auto state = query::MergeableVectorAggregateState::create(aggregates().front()).value();
  EXPECT_TRUE(state.accumulate_count_star().has_value());
  std::vector<query::ScalarValue> values;
  values.push_back(query::ScalarValue::text(string_type(), std::move(label)).value());
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
    saw_fingerprint = saw_fingerprint || request.peer_certificate_sha256.has_value();
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
    return common::Result<bool>{(principal == 91U || principal == 92U) &&
                                (node == 2U || node == 3U)};
  }
};

[[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleQueryExecution>
create_execution() {
  std::vector fragments{fragment()};
  std::vector<DistributedVectorGroupedAggregateShuffleSourceInput> sources;
  sources.push_back({.tablet_id = tablet(), .messages = messages()});
  DistributedVectorGroupedAggregateShuffleQueryExecutionConfig config;
  config.destinations.push_back({.local_node_id = 2U});
  return DistributedVectorGroupedAggregateShuffleQueryExecution::create(
      authority(), std::move(fragments), std::move(sources), std::move(config));
}

TEST(DistributedVectorGroupedAggregateShuffleQueryExecutionTest,
     OwnsLocalFanoutGatherAndAtomicNativeFinalization) {
  auto execution = create_execution();
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();
  EXPECT_EQ(execution->state(),
            DistributedVectorGroupedAggregateShuffleQueryExecutionState::kRunning);
  EXPECT_FALSE(execution->result().has_value());
  ASSERT_TRUE(execution->poll_once(std::chrono::milliseconds{0}).is_ok());
  EXPECT_EQ(execution->state(),
            DistributedVectorGroupedAggregateShuffleQueryExecutionState::kComplete);
  ASSERT_TRUE(execution->result().has_value());
  EXPECT_EQ(execution->result()->row_count, 1U);
  ASSERT_EQ(execution->result()->encoded_batches.size(), 1U);
  auto decoded = network::decode_query_result_batch(execution->result()->encoded_batches.front());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  ASSERT_EQ(decoded->row_count(), 1U);
  ASSERT_EQ(decoded->columns().size(), 2U);
  ASSERT_NE(decoded->cell(0U, 0U), nullptr);
  auto region =
      query::decode_canonical_scalar_value(string_type(), false, decoded->cell(0U, 0U)->value);
  ASSERT_TRUE(region.has_value()) << region.error().to_string();
  EXPECT_EQ(std::get<std::string>(region->storage()), "east");
  common::ByteReader count{decoded->cell(0U, 1U)->value};
  EXPECT_EQ(count.read_i64_le().value(), 2);
  const auto metrics = execution->metrics();
  EXPECT_EQ(metrics.source_tablets, 1U);
  EXPECT_EQ(metrics.destination_nodes, 1U);
  EXPECT_EQ(metrics.local_edges, 1U);
  EXPECT_EQ(metrics.remote_edges, 0U);
  EXPECT_EQ(metrics.ready_destinations, 1U);
  EXPECT_EQ(metrics.result.emitted_chunks, 1U);
}

TEST(DistributedVectorGroupedAggregateShuffleQueryExecutionTest,
     RejectsIncompleteCoverageAndOwnsCancellation) {
  DistributedVectorGroupedAggregateShuffleQueryExecutionConfig config;
  config.destinations.push_back({.local_node_id = 2U});
  auto missing_source = DistributedVectorGroupedAggregateShuffleQueryExecution::create(
      authority(), std::vector{fragment()}, {}, config);
  ASSERT_FALSE(missing_source.has_value());
  EXPECT_EQ(missing_source.error().code(), common::StatusCode::kInvalidArgument);

  auto execution = create_execution();
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();
  EXPECT_EQ(execution->cancel().code(), common::StatusCode::kCancelled);
  EXPECT_EQ(execution->state(),
            DistributedVectorGroupedAggregateShuffleQueryExecutionState::kCancelled);
  EXPECT_FALSE(execution->result().has_value());
}

TEST(DistributedVectorGroupedAggregateShuffleQueryExecutionTest,
     ComposesBidirectionalRemoteEdgesThroughMutualTlsReceipts) {
  std::vector fragments{fragment(id<schema::TabletId>(2U), 2U),
                        fragment(id<schema::TabletId>(3U), 3U)};
  auto expected = DistributedVectorGroupedAggregateShuffleAuthority::create_from_mutable_fragments(
      fragments, keys(), aggregates());
  ASSERT_TRUE(expected.has_value()) << expected.error().to_string();

  auto node2_reservation = network::TcpListener::bind();
  auto node3_reservation = network::TcpListener::bind();
  ASSERT_TRUE(node2_reservation.has_value());
  ASSERT_TRUE(node3_reservation.has_value());
  const network::Ipv4Endpoint node2_endpoint = node2_reservation->bound_endpoint();
  const network::Ipv4Endpoint node3_endpoint = node3_reservation->bound_endpoint();
  ASSERT_TRUE(node2_reservation->close().is_ok());
  ASSERT_TRUE(node3_reservation->close().is_ok());

  auto resources = query::QueryResourceContext::create(16U << 20U).value();
  auto tls_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(tls_context.has_value());
  Authenticator client_authenticator{91U};
  Authenticator server_authenticator{92U};
  Authorizer authorizer;
  DistributedVectorGroupedAggregateShuffleQueryExecutionConfig config;
  config.destinations.push_back({.local_node_id = 2U,
                                 .listener = {.bind_endpoint = node2_endpoint},
                                 .tls = server_tls(),
                                 .authenticator = &client_authenticator,
                                 .node_authorizer = &authorizer,
                                 .resources = resources,
                                 .carrier_limits = carrier_limits()});
  config.destinations.push_back({.local_node_id = 3U,
                                 .listener = {.bind_endpoint = node3_endpoint},
                                 .tls = server_tls(),
                                 .authenticator = &client_authenticator,
                                 .node_authorizer = &authorizer,
                                 .resources = resources,
                                 .carrier_limits = carrier_limits()});
  config.transport = DistributedVectorGroupedAggregateShuffleTcpExecutionConfig{
      .authenticator = &server_authenticator,
      .node_authorizer = &authorizer,
      .routes = {{.node_id = 2U, .endpoints = {node2_endpoint}, .tls_context = &*tls_context},
                 {.node_id = 3U, .endpoints = {node3_endpoint}, .tls_context = &*tls_context}},
      .carrier_limits = carrier_limits(),
      .connect_timeout = std::chrono::milliseconds{1000},
      .execution_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5}};
  std::vector<DistributedVectorGroupedAggregateShuffleSourceInput> sources;
  sources.push_back(
      {.tablet_id = fragments[0].tablet_id, .messages = messages(fragments[0].tablet_id, "east")});
  sources.push_back(
      {.tablet_id = fragments[1].tablet_id, .messages = messages(fragments[1].tablet_id, "west")});
  auto execution = DistributedVectorGroupedAggregateShuffleQueryExecution::create(
      std::move(*expected), std::move(fragments), std::move(sources), std::move(config));
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();

  for (std::size_t iteration = 0U; iteration < 256U; ++iteration) {
    const common::Status driven = execution->poll_once(std::chrono::milliseconds{1});
    ASSERT_TRUE(driven.is_ok()) << driven.to_string();
    if (execution->state() ==
        DistributedVectorGroupedAggregateShuffleQueryExecutionState::kComplete) {
      break;
    }
  }
  ASSERT_EQ(execution->state(),
            DistributedVectorGroupedAggregateShuffleQueryExecutionState::kComplete);
  ASSERT_TRUE(execution->result().has_value());
  EXPECT_EQ(execution->result()->row_count, 2U);
  const auto metrics = execution->metrics();
  EXPECT_EQ(metrics.source_tablets, 2U);
  EXPECT_EQ(metrics.destination_nodes, 2U);
  EXPECT_EQ(metrics.local_edges, 2U);
  EXPECT_EQ(metrics.remote_edges, 2U);
  EXPECT_EQ(metrics.transport.succeeded_edges, 2U);
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
  EXPECT_TRUE(server_authenticator.saw_fingerprint);
}

} // namespace
} // namespace chronos::cluster
