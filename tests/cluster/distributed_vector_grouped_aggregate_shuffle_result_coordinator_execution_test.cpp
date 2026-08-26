#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_coordinator_execution.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_tcp_execution.hpp"

#include <algorithm>
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

[[nodiscard]] std::filesystem::path fixture_path(const char* name) {
  return std::filesystem::path{CHRONOS_NETWORK_FIXTURE_DIR} / "tls" / name;
}

[[nodiscard]] network::TlsServerConfig server_tls() {
  return {.certificate_chain_file = fixture_path("server.pem").string(),
          .private_key_file = fixture_path("server-key.pem").string(),
          .trust_store_file = fixture_path("ca.pem").string()};
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

template <typename Id> [[nodiscard]] Id id(const std::uint8_t seed) {
  return Id::from_uuid(uuid(seed)).value();
}

[[nodiscard]] schema::LogicalType string_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
}

[[nodiscard]] schema::LogicalType i64_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
}

[[nodiscard]] query::DistributedVectorResultSchema result_schema() {
  return {.columns = {{"region", string_type(), false}, {"count", i64_type(), false}}};
}

[[nodiscard]] query::DistributedMutableVectorFragment fragment(const std::uint8_t tablet_seed,
                                                               const raft::NodeId node_id) {
  return {.query_id = uuid(1U),
          .database_id = id<manifest::DatabaseId>(8U),
          .table_id = id<schema::TableId>(9U),
          .tablet_id = id<schema::TabletId>(tablet_seed),
          .destination_schema_id = id<schema::SchemaId>(10U),
          .raft_group_id = uuid(static_cast<std::uint8_t>(tablet_seed + 20U)),
          .serving_node = node_id,
          .applied_position = 10U,
          .observed_leader_commit_position = 10U,
          .placement_epoch = 3U,
          .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
          .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
          .destination_column_ordinals = {0U},
          .plan = {.mode = query::DistributedVectorPlanMode::kGroupedAggregate,
                   .group_key_input_indices = {0U},
                   .aggregates = {{.operation = query::VectorAggregateOperation::kCountStar}},
                   .order_keys = {{.output_index = 1U,
                                   .direction = query::PhysicalSortDirection::kDescending}},
                   .limit = 1U},
          .result_schema = result_schema()};
}

struct Proofs {
  std::vector<query::DistributedMutableVectorFragment> fragments;
  DistributedVectorGroupedAggregateShuffleAuthority authority;
  DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2 finalization;

  explicit Proofs(std::vector<query::DistributedMutableVectorFragment> input)
      : fragments(std::move(input)),
        authority(*DistributedVectorGroupedAggregateShuffleAuthority::create_from_mutable_fragments(
            fragments, std::array{query::VectorGroupKeyDefinition{0U, string_type(), false}},
            std::array{query::VectorAggregateDefinition{query::VectorAggregateOperation::kCountStar,
                                                        std::nullopt}})),
        finalization(*DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2::create(
            authority, fragments)) {}
};

[[nodiscard]] Proofs proofs() {
  return Proofs{{fragment(2U, 3U), fragment(3U, 4U)}};
}

[[nodiscard]] std::array<std::byte, sizeof(std::uint64_t)> encoded_u64(std::uint64_t value) {
  std::array<std::byte, sizeof(std::uint64_t)> bytes{};
  for (std::size_t index = 0U; index < bytes.size(); ++index)
    bytes[index] = static_cast<std::byte>((value >> (index * 8U)) & std::uint64_t{0xffU});
  return bytes;
}

[[nodiscard]] std::vector<std::byte> batch(const std::string& value, const std::uint64_t count) {
  const std::array columns{network::QueryResultColumn{"region", string_type(), false},
                           network::QueryResultColumn{"count", i64_type(), false}};
  const auto encoded_count = encoded_u64(count);
  const std::array cells{network::QueryResultCell{.value = std::as_bytes(std::span{value})},
                         network::QueryResultCell{.value = encoded_count}};
  return network::encode_query_result_batch(1U, columns, cells).value();
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleResultTlsLimits carrier_limits() {
  return {.handshake_timeout = std::chrono::milliseconds{1000},
          .exchange_timeout = std::chrono::milliseconds{1000},
          .stream = {.maximum_frames = 1U, .maximum_encoded_bytes = 1U << 20U}};
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleResultRetry
retry(const DistributedVectorGroupedAggregateShuffleAuthority& authority,
      const query::DistributedVectorResultSchema& schema, const std::uint32_t partition_id,
      const std::string& value, const std::uint64_t count) {
  const auto source = authority.destination_node(partition_id).value();
  std::vector<std::vector<std::byte>> batches;
  batches.push_back(batch(value, count));
  return DistributedVectorGroupedAggregateShuffleResultRetry::create(
             authority, schema,
             {.partition_id = partition_id, .source_node_id = source, .coordinator_node_id = 9U},
             std::move(batches),
             {.retry = {.maximum_attempts = 2U,
                        .initial_backoff = std::chrono::milliseconds{1},
                        .maximum_backoff = std::chrono::milliseconds{1}},
              .stream = carrier_limits().stream})
      .value();
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
  // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
  common::Result<bool> authorize_node(const std::uint64_t principal,
                                      const raft::NodeId node) const override {
    return common::Result<bool>{(principal == 91U && (node == 3U || node == 4U)) ||
                                (principal == 92U && node == 9U)};
  }
};

TEST(DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionTest,
     CollectsTlsResultsAndAtomicallyFinalizesGlobalOrderAndLimit) {
  auto proof = proofs();
  Authenticator reducer_authenticator{91U};
  Authenticator coordinator_authenticator{92U};
  Authorizer authorizer;
  auto coordinator = DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution::create(
      proof.authority, proof.finalization,
      {.listener = {},
       .tls = server_tls(),
       .authenticator = &reducer_authenticator,
       .node_authorizer = &authorizer,
       .coordinator_node_id = 9U,
       .carrier_limits = carrier_limits(),
       .maximum_retained_server_streams = 2U,
       .maximum_accepts_per_poll = 2U,
       .execution_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5}});
  ASSERT_TRUE(coordinator.has_value()) << coordinator.error().to_string();
  auto client_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(client_context.has_value()) << client_context.error().to_string();

  std::vector<DistributedVectorGroupedAggregateShuffleResultRetry> retries;
  retries.push_back(retry(proof.authority, proof.finalization.result_schema(), 0U, "east", 1U));
  retries.push_back(retry(proof.authority, proof.finalization.result_schema(), 1U, "west", 2U));
  auto reducers = DistributedVectorGroupedAggregateShuffleResultTcpExecution::create(
      proof.authority, proof.finalization.result_schema(), std::move(retries),
      {.authenticator = &coordinator_authenticator,
       .node_authorizer = &authorizer,
       .routes = {{.node_id = 9U,
                   .endpoints = {coordinator->bound_endpoint()},
                   .tls_context = &*client_context}},
       .carrier_limits = carrier_limits(),
       .connect_timeout = std::chrono::milliseconds{1000},
       .execution_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5}});
  ASSERT_TRUE(reducers.has_value()) << reducers.error().to_string();

  for (std::size_t iteration = 0U; iteration < 4096U; ++iteration) {
    const common::Status sent = reducers->poll_once(std::chrono::milliseconds{1});
    ASSERT_TRUE(sent.is_ok()) << sent.to_string();
    const common::Status received = coordinator->poll_once(std::chrono::milliseconds{1});
    ASSERT_TRUE(received.is_ok()) << received.to_string();
    if (reducers->state() ==
            DistributedVectorGroupedAggregateShuffleResultTcpExecutionState::kComplete &&
        coordinator->state() ==
            DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionState::kComplete) {
      break;
    }
  }

  ASSERT_EQ(reducers->state(),
            DistributedVectorGroupedAggregateShuffleResultTcpExecutionState::kComplete);
  ASSERT_EQ(coordinator->state(),
            DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionState::kComplete);
  const auto metrics = coordinator->metrics();
  EXPECT_EQ(metrics.collector.accepted_partitions, 2U);
  EXPECT_EQ(metrics.collector.retained_encoded_bytes, 0U);
  EXPECT_EQ(metrics.finalization_attempts, 1U);
  EXPECT_EQ(metrics.finalized_rows, 1U);
  EXPECT_GT(metrics.finalized_encoded_bytes, 0U);
  EXPECT_TRUE(reducer_authenticator.saw_fingerprint);
  EXPECT_TRUE(coordinator_authenticator.saw_fingerprint);

  auto result = coordinator->take_result();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_EQ(result->encoded_batches.size(), 1U);
  auto decoded = network::decode_query_result_batch(result->encoded_batches.front());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  ASSERT_EQ(decoded->row_count(), 1U);
  const auto* region = decoded->cell(0U, 0U);
  const auto* count = decoded->cell(0U, 1U);
  ASSERT_NE(region, nullptr);
  ASSERT_NE(count, nullptr);
  const std::string wanted{"west"};
  const auto wanted_bytes = std::as_bytes(std::span{wanted});
  EXPECT_TRUE(std::equal(region->value.begin(), region->value.end(), wanted_bytes.begin(),
                         wanted_bytes.end()));
  ASSERT_EQ(count->value.size(), sizeof(std::uint64_t));
  EXPECT_EQ(count->value.front(), std::byte{2U});
  EXPECT_EQ(coordinator->state(),
            DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionState::kResultTaken);
  EXPECT_EQ(coordinator->take_result().error().code(), common::StatusCode::kUnavailable);
}

TEST(DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionTest,
     RejectsDifferentProofObjectAndCancelsBeforeAccept) {
  auto proof = proofs();
  Authenticator authenticator{91U};
  Authorizer authorizer;
  auto copied_authority =
      DistributedVectorGroupedAggregateShuffleAuthority::create_from_mutable_fragments(
          proof.fragments, std::array{query::VectorGroupKeyDefinition{0U, string_type(), false}},
          std::array{query::VectorAggregateDefinition{query::VectorAggregateOperation::kCountStar,
                                                      std::nullopt}})
          .value();
  EXPECT_EQ(DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution::create(
                copied_authority, proof.finalization,
                {.tls = server_tls(),
                 .authenticator = &authenticator,
                 .node_authorizer = &authorizer,
                 .coordinator_node_id = 9U})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  auto coordinator = DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution::create(
      proof.authority, proof.finalization,
      {.tls = server_tls(),
       .authenticator = &authenticator,
       .node_authorizer = &authorizer,
       .coordinator_node_id = 9U});
  ASSERT_TRUE(coordinator.has_value()) << coordinator.error().to_string();
  EXPECT_EQ(coordinator->cancel().code(), common::StatusCode::kCancelled);
  EXPECT_EQ(coordinator->state(),
            DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionState::kCancelled);
  EXPECT_EQ(coordinator->metrics().server.accepted_connections, 0U);
  EXPECT_EQ(coordinator->take_result().error().code(), common::StatusCode::kUnavailable);

  auto expired = DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution::create(
      proof.authority, proof.finalization,
      {.tls = server_tls(),
       .authenticator = &authenticator,
       .node_authorizer = &authorizer,
       .coordinator_node_id = 9U,
       .execution_deadline = std::chrono::steady_clock::now() - std::chrono::milliseconds{1}});
  ASSERT_TRUE(expired.has_value()) << expired.error().to_string();
  EXPECT_EQ(expired->poll_once(std::chrono::milliseconds{1}).code(),
            common::StatusCode::kCancelled);
  EXPECT_EQ(expired->state(),
            DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionState::kCancelled);
  EXPECT_EQ(expired->metrics().server.accepted_connections, 0U);
}

} // namespace
} // namespace chronos::cluster
