#include "chronos/cluster/distributed_vector_aggregate_query_tcp_server_v2.hpp"
#include "chronos/cluster/raft_observation_tcp_server.hpp"
#include "chronos/common/byte_reader.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/service/replicated_distributed_query.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::service {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-replicated-query-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

template <typename Id> [[nodiscard]] Id id(const std::uint8_t seed) {
  return Id::from_uuid(uuid(seed)).value();
}

[[nodiscard]] schema::TableSchema make_schema() {
  const schema::ColumnId event = id<schema::ColumnId>(5U);
  const schema::ColumnId value = id<schema::ColumnId>(6U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(
                        event, "event_time",
                        schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(),
                        false)
                        .value());
  columns.push_back(schema::ColumnDefinition::create(
                        value, "value",
                        schema::LogicalType::create(schema::LogicalTypeKind::kFloat64).value(),
                        true)
                        .value());
  return schema::TableSchema::create(id<schema::TableId>(2U), id<schema::SchemaId>(4U),
                                     schema::SchemaVersion::initial(), std::nullopt,
                                     std::move(columns),
                                     {.event_time_column = event,
                                      .physical_ordering_key = {event},
                                      .partition_columns = {event},
                                      .shard_key = {event},
                                      .deduplication_key = {event}})
      .value();
}

void write_file(const std::filesystem::path& path, const common::ByteView bytes) {
  std::ofstream output{path, std::ios::binary};
  ASSERT_TRUE(output.good());
  for (const std::byte value : bytes)
    output.put(static_cast<char>(std::to_integer<unsigned char>(value)));
  output.close();
  ASSERT_TRUE(output.good());
}

[[nodiscard]] common::Result<manifest::TemporalDatabaseStoragePublisher>
make_publisher(const std::filesystem::path& root, const schema::SchemaLineage& lineage,
               const schema::TabletId& tablet_id, const raft::GroupId& group_id,
               const raft::LogIndex durable_position) {
  const schema::TableSchema& current = *lineage.current();
  const manifest::TemporalTabletDescriptor tablet{.table_id = current.table_id(),
                                                  .tablet_id = tablet_id,
                                                  .recovery_schema_id = current.schema_id(),
                                                  .recovery_schema_version = current.version(),
                                                  .source_id = group_id,
                                                  .durable_position = durable_position,
                                                  .reclaim_position = 0U,
                                                  .first_part_index = 0U,
                                                  .part_count = 0U,
                                                  .durable_version_count = 0U,
                                                  .commit_source =
                                                      manifest::ManifestCommitSource::kRaft};
  auto encoded =
      manifest::encode_manifest_v2_temporal({.generation = 1U,
                                             .database_id = id<manifest::DatabaseId>(1U),
                                             .wal_reclaim_checkpoint = std::nullopt,
                                             .tablets = std::span{std::addressof(tablet), 1U},
                                             .parts = {},
                                             .retries = {}});
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  if (!std::filesystem::create_directories(root / manifest::kPartsDirectoryName) ||
      !std::filesystem::create_directories(root / manifest::kManifestDirectoryName)) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "cannot create query manifest fixture"});
  }
  write_file(root / manifest::kManifestDirectoryName / manifest::kManifestLockFileName, {});
  write_file(root / manifest::kManifestDirectoryName / *manifest::manifest_file_name(1U),
             encoded->bytes());
  auto storage = manifest::ManifestStorage::open_existing({.database_root = root.string()});
  if (!storage.has_value())
    return common::make_unexpected(storage.error());
  const std::array schema_bindings{
      manifest::TabletSchemaBinding{.tablet_id = tablet_id, .lineage = std::cref(lineage)}};
  const std::array source_bindings{
      manifest::TemporalTabletSourceBinding{.tablet_id = tablet_id,
                                            .commit_source = manifest::ManifestCommitSource::kRaft,
                                            .source_id = group_id}};
  auto loaded = storage->load_selected_temporal_manifest(
      {.expected_database_id = id<manifest::DatabaseId>(1U),
       .schema_bindings = schema_bindings,
       .source_bindings = source_bindings});
  if (!loaded.has_value())
    return common::make_unexpected(loaded.error());
  auto selected =
      std::make_shared<const manifest::LoadedTemporalManifestGeneration>(std::move(*loaded));
  return manifest::TemporalDatabaseStoragePublisher::create(selected, schema_bindings);
}

[[nodiscard]] std::filesystem::path tls_fixture(const char* name) {
  return std::filesystem::path{CHRONOS_NETWORK_FIXTURE_DIR} / "tls" / name;
}

// NOLINTBEGIN(readability-convert-member-functions-to-static)
class QueryAuthenticator final : public network::ConnectionAuthenticator {
public:
  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest&) override {
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = 91U};
  }
};
// NOLINTEND(readability-convert-member-functions-to-static)

class QueryNodeAuthorizer final : public cluster::ClusterNodePrincipalAuthorizer {
public:
  // NOLINTNEXTLINE(readability-convert-member-functions-to-static,bugprone-easily-swappable-parameters)
  common::Result<bool> authorize_node(const std::uint64_t principal_id,
                                      const raft::NodeId node_id) const override {
    return common::Result<bool>{principal_id == 91U &&
                                (node_id == 1U || node_id == 11U || node_id == 12U)};
  }
};

class ObservationService final : public cluster::RaftObservationService {
public:
  ObservationService(const raft::NodeId node, const raft::Role role, const raft::LogIndex position)
      : node_(node), role_(role), position_(position) {}

  common::Result<raft::RaftGroupObservation> observe(const raft::GroupId& group_id) override {
    ++calls;
    return raft::RaftGroupObservation{.group_id = group_id,
                                      .node_id = node_,
                                      .role = role_,
                                      .current_term = 1U,
                                      .leader_id = 11U,
                                      .last_log_index = position_,
                                      .commit_index = position_,
                                      .applied_index = position_,
                                      .voters = {11U, 12U},
                                      .committed_voters = {11U, 12U}};
  }

  std::size_t calls{};

private:
  raft::NodeId node_{};
  raft::Role role_{raft::Role::kFollower};
  raft::LogIndex position_{};
};

[[nodiscard]] network::TlsServerConfig observation_server_tls_config() {
  return {.certificate_chain_file = tls_fixture("server.pem").string(),
          .private_key_file = tls_fixture("server-key.pem").string(),
          .trust_store_file = tls_fixture("ca.pem").string()};
}

[[nodiscard]] std::string endpoint_text(const network::Ipv4Endpoint& endpoint) {
  return std::to_string(endpoint.address[0]) + "." + std::to_string(endpoint.address[1]) + "." +
         std::to_string(endpoint.address[2]) + "." + std::to_string(endpoint.address[3]) + ":" +
         std::to_string(endpoint.port);
}

[[nodiscard]] query::DistributedAggregatePlan make_plan(const schema::TabletId& tablet_id,
                                                        const raft::LogIndex applied_position) {
  return {.query_id = uuid(7U),
          .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
          .fragments = {{.tablet_id = tablet_id,
                         .minimum_event_time = 0,
                         .maximum_event_time = 100,
                         .leader_node = 11U,
                         .local_applied_position = applied_position,
                         .known_leader_commit_position = applied_position}}};
}

[[nodiscard]] query::DistributedAggregatePlan
make_follower_plan(const schema::TabletId& tablet_id, const raft::LogIndex applied_position) {
  return {.query_id = uuid(17U),
          .read_policy = {.consistency = query::DistributedReadConsistency::kFollowerBoundedStale,
                          .maximum_staleness_positions = 1U},
          .fragments = {{.tablet_id = tablet_id,
                         .minimum_event_time = 0,
                         .maximum_event_time = 100,
                         .leader_node = 11U,
                         .local_applied_position = applied_position,
                         .known_leader_commit_position = applied_position}}};
}

[[nodiscard]] query::DistributedVectorQueryPlan
make_vector_aggregate_plan(const schema::TabletId& tablet_id,
                           const raft::LogIndex applied_position) {
  const query::DistributedAggregatePlan aggregate = make_plan(tablet_id, applied_position);
  return {.query_id = aggregate.query_id,
          .read_policy = aggregate.read_policy,
          .fragments = aggregate.fragments,
          .intent = {.mode = query::DistributedVectorPlanMode::kUngroupedAggregate,
                     .aggregates = {{.operation = query::VectorAggregateOperation::kAverage,
                                     .input_index = 1U}}}};
}

[[nodiscard]] query::DistributedVectorQueryPlan
make_follower_vector_aggregate_plan(const schema::TabletId& tablet_id,
                                    const raft::LogIndex applied_position) {
  query::DistributedVectorQueryPlan plan = make_vector_aggregate_plan(tablet_id, applied_position);
  plan.query_id = uuid(17U);
  plan.read_policy = make_follower_plan(tablet_id, applied_position).read_policy;
  return plan;
}

[[nodiscard]] query::DistributedVectorQueryPlan
make_follower_vector_count_plan(const schema::TabletId& tablet_id,
                                const raft::LogIndex applied_position) {
  query::DistributedVectorQueryPlan plan =
      make_follower_vector_aggregate_plan(tablet_id, applied_position);
  plan.query_id = uuid(27U);
  plan.intent.aggregates = {{.operation = query::VectorAggregateOperation::kCountStar}};
  return plan;
}

[[nodiscard]] query::DistributedVectorQueryPlan
make_vector_grouped_aggregate_plan(const schema::TabletId& tablet_id,
                                   const raft::LogIndex applied_position) {
  const query::DistributedAggregatePlan aggregate = make_plan(tablet_id, applied_position);
  return {.query_id = aggregate.query_id,
          .read_policy = aggregate.read_policy,
          .fragments = aggregate.fragments,
          .intent = {.mode = query::DistributedVectorPlanMode::kGroupedAggregate,
                     .group_key_input_indices = {1U},
                     .aggregates = {{.operation = query::VectorAggregateOperation::kCountStar}},
                     .order_keys = {{.output_index = 0U,
                                     .direction = query::PhysicalSortDirection::kDescending,
                                     .null_placement = query::ScalarNullPlacement::kLast}},
                     .limit = 1U}};
}

[[nodiscard]] query::DistributedVectorQueryPlan
make_follower_vector_grouped_aggregate_plan(const schema::TabletId& tablet_id,
                                            const raft::LogIndex applied_position) {
  query::DistributedVectorQueryPlan plan =
      make_vector_grouped_aggregate_plan(tablet_id, applied_position);
  plan.query_id = uuid(37U);
  plan.read_policy = make_follower_plan(tablet_id, applied_position).read_policy;
  return plan;
}

class CountStarVectorAggregateWorker final
    : public cluster::DistributedVectorAggregateQueryWorkerServiceV2 {
public:
  common::Result<std::vector<query::VectorAggregateDefinition>>
  bind_definitions(const query::DistributedVectorFragmentDispatchV2& dispatch) override {
    ++bind_calls;
    return definitions(dispatch);
  }

  common::Result<query::DistributedVectorAggregateWorkerResultV2>
  execute(const query::DistributedVectorFragmentDispatchV2& dispatch) override {
    ++execute_calls;
    auto bound = definitions(dispatch);
    if (!bound.has_value())
      return common::make_unexpected(bound.error());
    auto state = query::MergeableVectorAggregateState::create(bound->front());
    if (!state.has_value())
      return common::make_unexpected(state.error());
    for (std::size_t row = 0U; row < 3U; ++row) {
      auto accumulated = state->accumulate_count_star();
      if (!accumulated.has_value())
        return common::make_unexpected(accumulated.error());
    }
    query::DistributedVectorAggregateWorkerResultV2 result{.definitions = std::move(*bound),
                                                           .input_rows = 3U};
    result.messages.emplace_back(
        query::DistributedVectorAggregateExchangePosition{.query_id = dispatch.dispatch.query_id,
                                                          .tablet_id = dispatch.dispatch.tablet_id,
                                                          .sequence = 1U,
                                                          .aggregate_ordinal = 0U,
                                                          .terminal = true},
        std::move(*state));
    return result;
  }

  std::size_t bind_calls{};
  std::size_t execute_calls{};

private:
  [[nodiscard]] static common::Result<std::vector<query::VectorAggregateDefinition>>
  definitions(const query::DistributedVectorFragmentDispatchV2& dispatch) {
    if (dispatch.dispatch.plan.mode != query::DistributedVectorPlanMode::kUngroupedAggregate ||
        dispatch.dispatch.plan.aggregates.size() != 1U ||
        dispatch.dispatch.plan.aggregates.front().operation !=
            query::VectorAggregateOperation::kCountStar ||
        dispatch.dispatch.plan.aggregates.front().input_index.has_value()) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kInvalidArgument,
                         "count-star test worker received a different aggregate plan"});
    }
    return std::vector<query::VectorAggregateDefinition>{
        {.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
  }
};

TEST(ReplicatedDistributedQueryTest, ConstructsOneAuthorityBoundTcpLifecycleOwner) {
  ReplicatedFollowerDistributedAggregateQuery empty_scalar_owner;
  EXPECT_EQ(empty_scalar_owner.state(), ReplicatedFollowerDistributedAggregateQueryState::kFailed);
  EXPECT_FALSE(empty_scalar_owner.metrics().execution.has_value());
  EXPECT_EQ(empty_scalar_owner.poll_once(std::chrono::milliseconds{0}).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(empty_scalar_owner.cancel().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(empty_scalar_owner.result().error().code(), common::StatusCode::kInvalidArgument);

  ReplicatedFollowerDistributedGroupedFloat64Query empty_grouped_owner;
  EXPECT_EQ(empty_grouped_owner.state(),
            ReplicatedFollowerDistributedGroupedFloat64QueryState::kFailed);
  EXPECT_FALSE(empty_grouped_owner.metrics().execution.has_value());
  EXPECT_EQ(empty_grouped_owner.poll_once(std::chrono::milliseconds{0}).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(empty_grouped_owner.cancel().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(empty_grouped_owner.result().error().code(), common::StatusCode::kInvalidArgument);

  TemporaryDirectory directory;
  ASSERT_TRUE(std::filesystem::create_directory(directory.path() / "raft"));
  const raft::GroupId metadata_group = uuid(1U);
  const raft::GroupId tablet_group = uuid(8U);
  auto runtime = raft::AsyncDurableMultiRaftRuntime::create_new(
      11U, {.directory_path = (directory.path() / "raft").string()},
      {{metadata_group, {11U}}, {tablet_group, {11U}}});
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  for (const raft::GroupId group_id : {metadata_group, tablet_group}) {
    auto election = runtime->try_submit({{group_id, raft::StartElectionOperation{}}});
    ASSERT_TRUE(election.has_value()) << election.error().to_string();
    ASSERT_TRUE(election->wait().has_value());
  }
  auto barrier =
      ReplicatedReadBarrier::create_local(std::addressof(*runtime), {tablet_group, metadata_group});
  ASSERT_TRUE(barrier.has_value()) << barrier.error().to_string();
  auto missing_tablet_barrier =
      ReplicatedReadBarrier::create_local(std::addressof(*runtime), {metadata_group});
  ASSERT_TRUE(missing_tablet_barrier.has_value()) << missing_tablet_barrier.error().to_string();
  auto inspected_authority = barrier->await_authority();
  ASSERT_TRUE(inspected_authority.has_value()) << inspected_authority.error().to_string();
  for (const ReplicatedReadAuthority& authority : *inspected_authority) {
    auto applied = runtime->try_submit(
        {{authority.observation.group_id,
          raft::MarkAppliedOperation{.index = authority.barrier.barrier.read_index}}});
    ASSERT_TRUE(applied.has_value()) << applied.error().to_string();
    auto applied_result = applied->wait();
    ASSERT_TRUE(applied_result.has_value()) << applied_result.error().to_string();
    ASSERT_EQ(applied_result->size(), 1U);
    ASSERT_TRUE(applied_result->front().status.is_ok())
        << applied_result->front().status.to_string();
  }
  const auto inspected_tablet = std::ranges::find(
      *inspected_authority, tablet_group,
      [](const ReplicatedReadAuthority& authority) { return authority.observation.group_id; });
  const auto inspected_metadata = std::ranges::find(
      *inspected_authority, metadata_group,
      [](const ReplicatedReadAuthority& authority) { return authority.observation.group_id; });
  ASSERT_NE(inspected_tablet, inspected_authority->end());
  ASSERT_NE(inspected_metadata, inspected_authority->end());
  const raft::LogIndex applied_position = inspected_tablet->barrier.barrier.read_index;
  const raft::LogIndex metadata_applied_position = inspected_metadata->barrier.barrier.read_index;

  const schema::TableSchema schema_value = make_schema();
  const schema::SchemaLineage lineage = schema::SchemaLineage::create(schema_value).value();
  const schema::TabletId tablet_id = id<schema::TabletId>(3U);
  auto publisher = make_publisher(directory.path() / "database", lineage, tablet_id, tablet_group,
                                  applied_position);
  ASSERT_TRUE(publisher.has_value()) << publisher.error().to_string();
  const raft::MetadataCatalogSnapshot catalog{
      .applied_index = metadata_applied_position,
      .cluster_nodes = {{11U, "127.0.0.1:1"}},
      .schema_definitions = {{"metrics", false,
                              std::make_shared<const schema::TableSchema>(schema_value)}},
      .active_schemas = {{schema_value.table_id(), schema_value.schema_id()}},
      .tablet_placements =
          {{schema_value.table_id(), tablet_id, 1U, {11U}, std::optional<raft::NodeId>{11U}}},
      .tablet_group_bindings = {{tablet_id, tablet_group}}};
  auto tls_context = network::TlsClientContext::create(
      {.certificate_chain_file = tls_fixture("client.pem").string(),
       .private_key_file = tls_fixture("client-key.pem").string(),
       .trust_store_file = tls_fixture("ca.pem").string(),
       .expected_server_identity = "127.0.0.1"});
  ASSERT_TRUE(tls_context.has_value()) << tls_context.error().to_string();
  const std::array tls_contexts{
      cluster::DistributedQueryNodeTlsContext{11U, std::addressof(*tls_context)}};
  const std::array<std::uint32_t, 2U> projection{0U, 1U};
  QueryAuthenticator authenticator;
  QueryNodeAuthorizer authorizer;

  auto snapshot = publisher->snapshot();
  ASSERT_TRUE(snapshot.has_value()) << snapshot.error().to_string();
  const ReplicatedDistributedAggregateQueryConfig config{
      .source_node_id = 1U,
      .read_barrier = std::addressof(*barrier),
      .metadata_group_id = metadata_group,
      .catalog = std::cref(catalog),
      .table_id = schema_value.table_id(),
      .destination_column_ordinals = projection,
      .aggregate_input_index = 1U,
      .tls_contexts = tls_contexts,
      .authenticator = std::addressof(authenticator),
      .node_authorizer = std::addressof(authorizer),
      .binding_limits = {.maximum_fragments = 1U,
                         .maximum_total_projection_ordinals = projection.size()}};
  auto execution = create_replicated_distributed_aggregate_query(
      make_plan(tablet_id, applied_position), std::move(*snapshot), config);
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();
  EXPECT_EQ(execution->state(), cluster::DistributedQueryTcpExecutionState::kRunning);
  ASSERT_EQ(execution->snapshot().dispatches().size(), 1U);
  EXPECT_EQ(execution->snapshot().dispatches().front().raft_group_id, tablet_group);
  EXPECT_EQ(execution->snapshot().snapshot().generation(), 1U);

  const ReplicatedDistributedVectorAggregateQueryConfigV2 vector_config{
      .source_node_id = 1U,
      .read_barrier = std::addressof(*barrier),
      .metadata_group_id = metadata_group,
      .catalog = std::cref(catalog),
      .table_id = schema_value.table_id(),
      .destination_column_ordinals = projection,
      .tls_contexts = tls_contexts,
      .authenticator = std::addressof(authenticator),
      .node_authorizer = std::addressof(authorizer),
      .binding_limits = {.maximum_fragments = 1U,
                         .maximum_total_projection_ordinals = projection.size()}};
  auto vector_snapshot = publisher->snapshot();
  ASSERT_TRUE(vector_snapshot.has_value()) << vector_snapshot.error().to_string();
  auto vector_execution = create_replicated_distributed_vector_aggregate_query_v2(
      make_vector_aggregate_plan(tablet_id, applied_position), std::move(*vector_snapshot),
      query::DistributedVectorResultSchema{
          .columns = {{"average", schema_value.columns()[1].type(), true}}},
      vector_config);
  ASSERT_TRUE(vector_execution.has_value()) << vector_execution.error().to_string();
  EXPECT_EQ(vector_execution->state(),
            cluster::DistributedVectorAggregateQueryTcpExecutionStateV2::kRunning);
  ASSERT_EQ(vector_execution->snapshot().dispatches().size(), 1U);
  EXPECT_EQ(vector_execution->snapshot().dispatches().front().raft_group_id, tablet_group);
  EXPECT_EQ(vector_execution->snapshot().snapshot().generation(), 1U);
  EXPECT_EQ(vector_execution->snapshot().result_schema().columns.front().name, "average");
  ASSERT_EQ(vector_execution->snapshot().aggregate_definitions().size(), 1U);
  EXPECT_EQ(vector_execution->snapshot().aggregate_definitions().front().operation,
            query::VectorAggregateOperation::kAverage);
  ASSERT_TRUE(vector_execution->snapshot().aggregate_definitions().front().input.has_value());
  // Guarded by the assertion above.
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  EXPECT_EQ(vector_execution->snapshot().aggregate_definitions().front().input->type,
            schema_value.columns()[1].type());

  const ReplicatedDistributedVectorGroupedAggregateQueryConfigV2 vector_grouped_config{
      .source_node_id = 1U,
      .read_barrier = std::addressof(*barrier),
      .metadata_group_id = metadata_group,
      .catalog = std::cref(catalog),
      .table_id = schema_value.table_id(),
      .destination_column_ordinals = projection,
      .tls_contexts = tls_contexts,
      .authenticator = std::addressof(authenticator),
      .node_authorizer = std::addressof(authorizer),
      .binding_limits = {.maximum_fragments = 1U,
                         .maximum_total_projection_ordinals = projection.size()}};
  auto vector_grouped_snapshot = publisher->snapshot();
  ASSERT_TRUE(vector_grouped_snapshot.has_value()) << vector_grouped_snapshot.error().to_string();
  auto vector_grouped_execution = create_replicated_distributed_vector_grouped_aggregate_query_v2(
      make_vector_grouped_aggregate_plan(tablet_id, applied_position),
      std::move(*vector_grouped_snapshot),
      query::DistributedVectorResultSchema{
          .columns = {{"value", schema_value.columns()[1].type(), true},
                      {"row_count",
                       schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(),
                       false}}},
      vector_grouped_config);
  ASSERT_TRUE(vector_grouped_execution.has_value()) << vector_grouped_execution.error().to_string();
  EXPECT_EQ(vector_grouped_execution->state(),
            cluster::DistributedVectorGroupedAggregateQueryTcpExecutionStateV2::kRunning);
  ASSERT_EQ(vector_grouped_execution->snapshot().dispatches().size(), 1U);
  EXPECT_EQ(vector_grouped_execution->snapshot().dispatches().front().raft_group_id, tablet_group);
  ASSERT_EQ(vector_grouped_execution->snapshot().grouped_key_definitions().size(), 1U);
  EXPECT_EQ(vector_grouped_execution->snapshot().grouped_key_definitions().front().type,
            schema_value.columns()[1].type());
  ASSERT_EQ(vector_grouped_execution->snapshot().grouped_aggregate_definitions().size(), 1U);
  EXPECT_EQ(vector_grouped_execution->snapshot().grouped_aggregate_definitions().front().operation,
            query::VectorAggregateOperation::kCountStar);

  auto row_snapshot = publisher->snapshot();
  ASSERT_TRUE(row_snapshot.has_value());
  auto row_plan = make_vector_aggregate_plan(tablet_id, applied_position);
  row_plan.intent = {.mode = query::DistributedVectorPlanMode::kRows, .row_output_indices = {1U}};
  EXPECT_EQ(create_replicated_distributed_vector_aggregate_query_v2(
                row_plan, std::move(*row_snapshot),
                query::DistributedVectorResultSchema{
                    .columns = {{"value", schema_value.columns()[1].type(), true}}},
                vector_config)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  const ReplicatedDistributedGroupedFloat64QueryConfig grouped_config{
      .source_node_id = 1U,
      .read_barrier = std::addressof(*barrier),
      .metadata_group_id = metadata_group,
      .catalog = std::cref(catalog),
      .table_id = schema_value.table_id(),
      .destination_column_ordinals = projection,
      .aggregate_input_index = 1U,
      .group_key_input_index = 1U,
      .tls_contexts = tls_contexts,
      .authenticator = std::addressof(authenticator),
      .node_authorizer = std::addressof(authorizer),
      .binding_limits = {.maximum_fragments = 1U,
                         .maximum_total_projection_ordinals = projection.size()}};
  auto grouped_snapshot = publisher->snapshot();
  ASSERT_TRUE(grouped_snapshot.has_value()) << grouped_snapshot.error().to_string();
  auto grouped_execution = create_replicated_distributed_grouped_float64_query(
      make_plan(tablet_id, applied_position), std::move(*grouped_snapshot), grouped_config);
  ASSERT_TRUE(grouped_execution.has_value()) << grouped_execution.error().to_string();
  EXPECT_EQ(grouped_execution->state(),
            cluster::DistributedGroupedQueryTcpExecutionState::kRunning);
  ASSERT_EQ(grouped_execution->snapshot().dispatches().size(), 1U);
  EXPECT_EQ(grouped_execution->snapshot().dispatches().front().raft_group_id, tablet_group);
  EXPECT_EQ(grouped_execution->snapshot().dispatches().front().fragment.group_key_input_index, 1U);
  EXPECT_EQ(grouped_execution->snapshot().snapshot().generation(), 1U);

  auto unsupported_grouped_snapshot = publisher->snapshot();
  ASSERT_TRUE(unsupported_grouped_snapshot.has_value());
  ReplicatedDistributedGroupedFloat64QueryConfig unsupported_grouped_config = grouped_config;
  unsupported_grouped_config.group_key_input_index = 0U;
  EXPECT_EQ(create_replicated_distributed_grouped_float64_query(
                make_plan(tablet_id, applied_position), std::move(*unsupported_grouped_snapshot),
                unsupported_grouped_config)
                .error()
                .code(),
            common::StatusCode::kNotSupported);

  auto stale_snapshot = publisher->snapshot();
  ASSERT_TRUE(stale_snapshot.has_value()) << stale_snapshot.error().to_string();
  raft::MetadataCatalogSnapshot stale_catalog = catalog;
  stale_catalog.applied_index = 0U;
  ReplicatedDistributedAggregateQueryConfig stale_config = config;
  stale_config.catalog = std::cref(stale_catalog);
  auto stale = create_replicated_distributed_aggregate_query(
      make_plan(tablet_id, applied_position), std::move(*stale_snapshot), stale_config);
  ASSERT_FALSE(stale.has_value());
  EXPECT_EQ(stale.error().code(), common::StatusCode::kUnavailable);

  auto missing_snapshot = publisher->snapshot();
  ASSERT_TRUE(missing_snapshot.has_value()) << missing_snapshot.error().to_string();
  ReplicatedDistributedAggregateQueryConfig missing_config = config;
  missing_config.read_barrier = std::addressof(*missing_tablet_barrier);
  auto missing = create_replicated_distributed_aggregate_query(
      make_plan(tablet_id, applied_position), std::move(*missing_snapshot), missing_config);
  ASSERT_FALSE(missing.has_value());
  EXPECT_EQ(missing.error().code(), common::StatusCode::kUnavailable);

  auto follower_snapshot = publisher->snapshot();
  ASSERT_TRUE(follower_snapshot.has_value()) << follower_snapshot.error().to_string();
  raft::MetadataCatalogSnapshot follower_catalog = catalog;
  follower_catalog.cluster_nodes.push_back({12U, "127.0.0.1:2"});
  follower_catalog.tablet_placements.front().replicas = {11U, 12U};
  const std::array follower_tls_contexts{
      cluster::DistributedQueryNodeTlsContext{12U, std::addressof(*tls_context)}};
  const std::array follower_authorities{query::DistributedAggregateFollowerReadAuthority{
      .leader_observation = {.group_id = tablet_group,
                             .node_id = 11U,
                             .role = raft::Role::kLeader,
                             .current_term = 1U,
                             .leader_id = 11U,
                             .last_log_index = applied_position,
                             .commit_index = applied_position,
                             .applied_index = applied_position,
                             .voters = {11U, 12U},
                             .committed_voters = {11U, 12U}},
      .follower_observation = {.group_id = tablet_group,
                               .node_id = 12U,
                               .role = raft::Role::kFollower,
                               .current_term = 1U,
                               .leader_id = 11U,
                               .last_log_index = applied_position,
                               .commit_index = applied_position,
                               .applied_index = applied_position,
                               .voters = {11U, 12U},
                               .committed_voters = {11U, 12U}}}};
  ReplicatedDistributedAggregateQueryConfig follower_config = config;
  follower_config.read_barrier = std::addressof(*missing_tablet_barrier);
  follower_config.catalog = std::cref(follower_catalog);
  follower_config.tls_contexts = follower_tls_contexts;
  auto follower_execution = create_replicated_follower_distributed_aggregate_query(
      make_follower_plan(tablet_id, applied_position), std::move(*follower_snapshot),
      follower_authorities, follower_config);
  ASSERT_TRUE(follower_execution.has_value()) << follower_execution.error().to_string();
  EXPECT_EQ(follower_execution->state(), cluster::DistributedQueryTcpExecutionState::kRunning);
  EXPECT_EQ(follower_execution->snapshot().dispatches().front().fragment.serving_node, 12U);

  ReplicatedDistributedVectorAggregateQueryConfigV2 follower_vector_config = vector_config;
  follower_vector_config.read_barrier = std::addressof(*missing_tablet_barrier);
  follower_vector_config.catalog = std::cref(follower_catalog);
  follower_vector_config.tls_contexts = follower_tls_contexts;
  auto follower_vector_snapshot = publisher->snapshot();
  ASSERT_TRUE(follower_vector_snapshot.has_value());
  const auto follower_vector_plan =
      make_follower_vector_aggregate_plan(tablet_id, applied_position);
  auto follower_vector_execution = create_replicated_follower_distributed_vector_aggregate_query_v2(
      follower_vector_plan, std::move(*follower_vector_snapshot),
      query::DistributedVectorResultSchema{
          .columns = {{"average", schema_value.columns()[1].type(), true}}},
      follower_authorities, follower_vector_config);
  ASSERT_TRUE(follower_vector_execution.has_value())
      << follower_vector_execution.error().to_string();
  EXPECT_EQ(follower_vector_execution->state(),
            cluster::DistributedVectorAggregateQueryTcpExecutionStateV2::kRunning);
  EXPECT_EQ(follower_vector_execution->snapshot().dispatches().front().serving_node, 12U);
  EXPECT_EQ(follower_vector_execution->snapshot().dispatches().front().raft_group_id, tablet_group);
  ASSERT_EQ(follower_vector_execution->snapshot().aggregate_definitions().size(), 1U);
  EXPECT_EQ(follower_vector_execution->snapshot().aggregate_definitions().front().operation,
            query::VectorAggregateOperation::kAverage);

  ReplicatedDistributedVectorGroupedAggregateQueryConfigV2 follower_vector_grouped_config =
      vector_grouped_config;
  follower_vector_grouped_config.read_barrier = std::addressof(*missing_tablet_barrier);
  follower_vector_grouped_config.catalog = std::cref(follower_catalog);
  follower_vector_grouped_config.tls_contexts = follower_tls_contexts;
  auto follower_vector_grouped_snapshot = publisher->snapshot();
  ASSERT_TRUE(follower_vector_grouped_snapshot.has_value());
  auto follower_vector_grouped_execution =
      create_replicated_follower_distributed_vector_grouped_aggregate_query_v2(
          make_follower_vector_grouped_aggregate_plan(tablet_id, applied_position),
          std::move(*follower_vector_grouped_snapshot),
          query::DistributedVectorResultSchema{
              .columns = {{"value", schema_value.columns()[1].type(), true},
                          {"row_count",
                           schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(),
                           false}}},
          follower_authorities, follower_vector_grouped_config);
  ASSERT_TRUE(follower_vector_grouped_execution.has_value())
      << follower_vector_grouped_execution.error().to_string();
  EXPECT_EQ(follower_vector_grouped_execution->state(),
            cluster::DistributedVectorGroupedAggregateQueryTcpExecutionStateV2::kRunning);
  EXPECT_EQ(follower_vector_grouped_execution->snapshot().dispatches().front().serving_node, 12U);
  EXPECT_EQ(follower_vector_grouped_execution->snapshot().dispatches().front().raft_group_id,
            tablet_group);
  ASSERT_EQ(follower_vector_grouped_execution->snapshot().grouped_key_definitions().size(), 1U);
  ASSERT_EQ(follower_vector_grouped_execution->snapshot().grouped_aggregate_definitions().size(),
            1U);

  ReplicatedDistributedGroupedFloat64QueryConfig follower_grouped_config = grouped_config;
  follower_grouped_config.read_barrier = std::addressof(*missing_tablet_barrier);
  follower_grouped_config.catalog = std::cref(follower_catalog);
  follower_grouped_config.tls_contexts = follower_tls_contexts;
  auto follower_grouped_snapshot = publisher->snapshot();
  ASSERT_TRUE(follower_grouped_snapshot.has_value());
  auto follower_grouped_execution = create_replicated_follower_distributed_grouped_float64_query(
      make_follower_plan(tablet_id, applied_position), std::move(*follower_grouped_snapshot),
      follower_authorities, follower_grouped_config);
  ASSERT_TRUE(follower_grouped_execution.has_value())
      << follower_grouped_execution.error().to_string();
  EXPECT_EQ(follower_grouped_execution->state(),
            cluster::DistributedGroupedQueryTcpExecutionState::kRunning);
  EXPECT_EQ(
      follower_grouped_execution->snapshot().dispatches().front().fragment.aggregate.serving_node,
      12U);
  EXPECT_EQ(
      follower_grouped_execution->snapshot().dispatches().front().fragment.group_key_input_index,
      1U);

  ObservationService leader_service{11U, raft::Role::kLeader, applied_position};
  ObservationService follower_service{12U, raft::Role::kFollower, applied_position};
  auto leader_receiver = cluster::RaftObservationReceiver::create(
      {.local_node_id = 11U, .authorizer = &authorizer, .service = &leader_service});
  auto follower_receiver = cluster::RaftObservationReceiver::create(
      {.local_node_id = 12U, .authorizer = &authorizer, .service = &follower_service});
  ASSERT_TRUE(leader_receiver.has_value());
  ASSERT_TRUE(follower_receiver.has_value());
  auto server_config = [&](cluster::RaftObservationReceiver& receiver) {
    return cluster::RaftObservationTcpServerConfig{
        .listener = {},
        .tls = observation_server_tls_config(),
        .authenticator = &authenticator,
        .receiver = &receiver,
        .session_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                           .exchange_timeout = std::chrono::milliseconds{1000}},
        .maximum_connections = 4U,
        .maximum_accepts_per_poll = 4U};
  };
  auto leader_server = cluster::RaftObservationTcpServer::start(server_config(*leader_receiver));
  auto follower_server =
      cluster::RaftObservationTcpServer::start(server_config(*follower_receiver));
  ASSERT_TRUE(leader_server.has_value()) << leader_server.error().to_string();
  ASSERT_TRUE(follower_server.has_value()) << follower_server.error().to_string();

  raft::MetadataCatalogSnapshot lifecycle_catalog = follower_catalog;
  lifecycle_catalog.cluster_nodes = {{11U, endpoint_text(leader_server->bound_endpoint())},
                                     {12U, endpoint_text(follower_server->bound_endpoint())}};
  ReplicatedDistributedAggregateQueryConfig lifecycle_query_config = follower_config;
  lifecycle_query_config.catalog = std::cref(lifecycle_catalog);
  const std::array observation_tls_contexts{
      cluster::RaftObservationNodeTlsContext{11U, std::addressof(*tls_context)},
      cluster::RaftObservationNodeTlsContext{12U, std::addressof(*tls_context)}};

  auto acquiring_cancel_snapshot = publisher->snapshot();
  ASSERT_TRUE(acquiring_cancel_snapshot.has_value());
  auto acquiring_cancel = ReplicatedFollowerDistributedAggregateQuery::create(
      make_follower_plan(tablet_id, applied_position), std::move(*acquiring_cancel_snapshot),
      {.source_node_id = 1U,
       .first_correlation_id = 61U,
       .tls_contexts = observation_tls_contexts,
       .authenticator = &authenticator,
       .node_authorizer = &authorizer,
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000}},
       .connect_timeout = std::chrono::milliseconds{1000},
       .retry = {.maximum_attempts = 1U,
                 .initial_backoff = std::chrono::milliseconds{1},
                 .maximum_backoff = std::chrono::milliseconds{1}},
       .maximum_pairs = 1U},
      lifecycle_query_config);
  ASSERT_TRUE(acquiring_cancel.has_value()) << acquiring_cancel.error().to_string();
  ASSERT_TRUE(acquiring_cancel->poll_once(std::chrono::milliseconds{0}).is_ok());
  EXPECT_EQ(acquiring_cancel->state(),
            ReplicatedFollowerDistributedAggregateQueryState::kAcquiringAuthority);
  EXPECT_EQ(acquiring_cancel->metrics().authority.active_pairs, 1U);
  EXPECT_FALSE(acquiring_cancel->metrics().execution.has_value());
  const common::Status authority_cancelled = acquiring_cancel->cancel();
  EXPECT_EQ(authority_cancelled.code(), common::StatusCode::kCancelled);
  EXPECT_EQ(acquiring_cancel->state(),
            ReplicatedFollowerDistributedAggregateQueryState::kCancelled);
  EXPECT_EQ(acquiring_cancel->metrics().authority.active_pairs, 0U);
  EXPECT_FALSE(acquiring_cancel->metrics().execution.has_value());
  EXPECT_EQ(acquiring_cancel->result().error(), authority_cancelled);
  EXPECT_EQ(acquiring_cancel->poll_once(std::chrono::milliseconds{0}), authority_cancelled);
  EXPECT_EQ(acquiring_cancel->cancel(), authority_cancelled);

  auto lifecycle_snapshot = publisher->snapshot();
  ASSERT_TRUE(lifecycle_snapshot.has_value());
  auto lifecycle = ReplicatedFollowerDistributedAggregateQuery::create(
      make_follower_plan(tablet_id, applied_position), std::move(*lifecycle_snapshot),
      {.source_node_id = 1U,
       .first_correlation_id = 71U,
       .tls_contexts = observation_tls_contexts,
       .authenticator = &authenticator,
       .node_authorizer = &authorizer,
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000}},
       .connect_timeout = std::chrono::milliseconds{1000},
       .retry = {.maximum_attempts = 1U,
                 .initial_backoff = std::chrono::milliseconds{1},
                 .maximum_backoff = std::chrono::milliseconds{1}},
       .maximum_pairs = 1U},
      lifecycle_query_config);
  ASSERT_TRUE(lifecycle.has_value()) << lifecycle.error().to_string();
  EXPECT_EQ(lifecycle->state(),
            ReplicatedFollowerDistributedAggregateQueryState::kAcquiringAuthority);
  EXPECT_EQ(lifecycle->result().error().code(), common::StatusCode::kInvalidArgument);
  for (std::size_t iteration = 0U;
       iteration < 4096U &&
       lifecycle->state() == ReplicatedFollowerDistributedAggregateQueryState::kAcquiringAuthority;
       ++iteration) {
    ASSERT_TRUE(lifecycle->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(leader_server->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(follower_server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(lifecycle->state(), ReplicatedFollowerDistributedAggregateQueryState::kExecuting)
      << lifecycle->failure().to_string();
  EXPECT_EQ(leader_service.calls, 1U);
  EXPECT_EQ(follower_service.calls, 1U);
  EXPECT_EQ(lifecycle->metrics().authority.completed_pairs, 1U);
  EXPECT_TRUE(lifecycle->metrics().execution.has_value());
  ASSERT_TRUE(lifecycle->poll_once(std::chrono::milliseconds{0}).is_ok());
  ASSERT_TRUE(lifecycle->metrics().execution.has_value());
  // Guarded by the execution-metrics assertion above.
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  EXPECT_EQ(lifecycle->metrics().execution->active_attempts, 1U);
  const common::Status lifecycle_cancelled = lifecycle->cancel();
  EXPECT_EQ(lifecycle_cancelled.code(), common::StatusCode::kCancelled);
  EXPECT_EQ(lifecycle->state(), ReplicatedFollowerDistributedAggregateQueryState::kCancelled);
  ASSERT_TRUE(lifecycle->metrics().execution.has_value());
  // Guarded by the execution-metrics assertion above.
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  EXPECT_EQ(lifecycle->metrics().execution->active_attempts, 0U);
  EXPECT_EQ(lifecycle->result().error(), lifecycle_cancelled);
  EXPECT_EQ(lifecycle->poll_once(std::chrono::milliseconds{0}), lifecycle_cancelled);
  EXPECT_EQ(lifecycle->cancel(), lifecycle_cancelled);

  ReplicatedDistributedAggregateQueryConfig deadline_query_config = lifecycle_query_config;
  deadline_query_config.execution_deadline = std::chrono::steady_clock::time_point{};
  auto deadline_snapshot = publisher->snapshot();
  ASSERT_TRUE(deadline_snapshot.has_value());
  auto deadline_lifecycle = ReplicatedFollowerDistributedAggregateQuery::create(
      make_follower_plan(tablet_id, applied_position), std::move(*deadline_snapshot),
      {.source_node_id = 1U,
       .first_correlation_id = 75U,
       .tls_contexts = observation_tls_contexts,
       .authenticator = &authenticator,
       .node_authorizer = &authorizer,
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000}},
       .connect_timeout = std::chrono::milliseconds{1000},
       .retry = {.maximum_attempts = 1U,
                 .initial_backoff = std::chrono::milliseconds{1},
                 .maximum_backoff = std::chrono::milliseconds{1}},
       .maximum_pairs = 1U},
      deadline_query_config);
  ASSERT_TRUE(deadline_lifecycle.has_value()) << deadline_lifecycle.error().to_string();
  for (std::size_t iteration = 0U;
       iteration < 4096U &&
       deadline_lifecycle->state() ==
           ReplicatedFollowerDistributedAggregateQueryState::kAcquiringAuthority;
       ++iteration) {
    ASSERT_TRUE(deadline_lifecycle->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(leader_server->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(follower_server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(deadline_lifecycle->state(),
            ReplicatedFollowerDistributedAggregateQueryState::kExecuting)
      << deadline_lifecycle->failure().to_string();
  EXPECT_EQ(leader_service.calls, 2U);
  EXPECT_EQ(follower_service.calls, 2U);
  const common::Status deadline_cancelled =
      deadline_lifecycle->poll_once(std::chrono::milliseconds{0});
  EXPECT_EQ(deadline_cancelled.code(), common::StatusCode::kCancelled);
  EXPECT_EQ(deadline_lifecycle->state(),
            ReplicatedFollowerDistributedAggregateQueryState::kCancelled);
  ASSERT_TRUE(deadline_lifecycle->metrics().execution.has_value());
  // Guarded by the execution-metrics assertion above.
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  EXPECT_EQ(deadline_lifecycle->metrics().execution->attempts_started, 0U);
  EXPECT_EQ(deadline_lifecycle->result().error(), deadline_cancelled);
  EXPECT_EQ(deadline_lifecycle->poll_once(std::chrono::milliseconds{0}), deadline_cancelled);
  EXPECT_EQ(deadline_lifecycle->cancel(), deadline_cancelled);

  ReplicatedDistributedVectorAggregateQueryConfigV2 vector_lifecycle_query_config =
      follower_vector_config;
  vector_lifecycle_query_config.catalog = std::cref(lifecycle_catalog);
  auto vector_lifecycle_snapshot = publisher->snapshot();
  ASSERT_TRUE(vector_lifecycle_snapshot.has_value());
  auto vector_lifecycle = ReplicatedFollowerDistributedVectorAggregateQueryV2::create(
      make_follower_vector_aggregate_plan(tablet_id, applied_position),
      std::move(*vector_lifecycle_snapshot),
      query::DistributedVectorResultSchema{
          .columns = {{"average", schema_value.columns()[1].type(), true}}},
      {.source_node_id = 1U,
       .first_correlation_id = 81U,
       .tls_contexts = observation_tls_contexts,
       .authenticator = &authenticator,
       .node_authorizer = &authorizer,
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000}},
       .connect_timeout = std::chrono::milliseconds{1000},
       .retry = {.maximum_attempts = 1U,
                 .initial_backoff = std::chrono::milliseconds{1},
                 .maximum_backoff = std::chrono::milliseconds{1}},
       .maximum_pairs = 1U},
      vector_lifecycle_query_config);
  ASSERT_TRUE(vector_lifecycle.has_value()) << vector_lifecycle.error().to_string();
  EXPECT_EQ(vector_lifecycle->state(),
            ReplicatedFollowerDistributedVectorAggregateQueryStateV2::kAcquiringAuthority);
  EXPECT_EQ(vector_lifecycle->result().error().code(), common::StatusCode::kInvalidArgument);
  for (std::size_t iteration = 0U;
       iteration < 4096U &&
       vector_lifecycle->state() ==
           ReplicatedFollowerDistributedVectorAggregateQueryStateV2::kAcquiringAuthority;
       ++iteration) {
    ASSERT_TRUE(vector_lifecycle->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(leader_server->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(follower_server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(vector_lifecycle->state(),
            ReplicatedFollowerDistributedVectorAggregateQueryStateV2::kExecuting)
      << vector_lifecycle->failure().to_string();
  EXPECT_EQ(leader_service.calls, 3U);
  EXPECT_EQ(follower_service.calls, 3U);
  EXPECT_EQ(vector_lifecycle->metrics().authority.completed_pairs, 1U);
  EXPECT_TRUE(vector_lifecycle->metrics().execution.has_value());
  const common::Status vector_lifecycle_cancelled = vector_lifecycle->cancel();
  EXPECT_EQ(vector_lifecycle_cancelled.code(), common::StatusCode::kCancelled);
  EXPECT_EQ(vector_lifecycle->state(),
            ReplicatedFollowerDistributedVectorAggregateQueryStateV2::kCancelled);
  EXPECT_EQ(vector_lifecycle->result().error(), vector_lifecycle_cancelled);

  auto vector_acquiring_cancel_snapshot = publisher->snapshot();
  ASSERT_TRUE(vector_acquiring_cancel_snapshot.has_value());
  auto vector_acquiring_cancel = ReplicatedFollowerDistributedVectorAggregateQueryV2::create(
      make_follower_vector_aggregate_plan(tablet_id, applied_position),
      std::move(*vector_acquiring_cancel_snapshot),
      query::DistributedVectorResultSchema{
          .columns = {{"average", schema_value.columns()[1].type(), true}}},
      {.source_node_id = 1U,
       .first_correlation_id = 91U,
       .tls_contexts = observation_tls_contexts,
       .authenticator = &authenticator,
       .node_authorizer = &authorizer,
       .maximum_pairs = 1U},
      vector_lifecycle_query_config);
  ASSERT_TRUE(vector_acquiring_cancel.has_value()) << vector_acquiring_cancel.error().to_string();
  EXPECT_EQ(vector_acquiring_cancel->cancel().code(), common::StatusCode::kCancelled);
  EXPECT_EQ(vector_acquiring_cancel->state(),
            ReplicatedFollowerDistributedVectorAggregateQueryStateV2::kCancelled);
  EXPECT_EQ(vector_acquiring_cancel->metrics().authority.active_pairs, 0U);

  auto invalid_schema_snapshot = publisher->snapshot();
  ASSERT_TRUE(invalid_schema_snapshot.has_value());
  EXPECT_EQ(ReplicatedFollowerDistributedVectorAggregateQueryV2::create(
                make_follower_vector_aggregate_plan(tablet_id, applied_position),
                std::move(*invalid_schema_snapshot), query::DistributedVectorResultSchema{},
                {.source_node_id = 1U,
                 .first_correlation_id = 101U,
                 .tls_contexts = observation_tls_contexts,
                 .authenticator = &authenticator,
                 .node_authorizer = &authorizer,
                 .maximum_pairs = 1U},
                vector_lifecycle_query_config)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  ReplicatedDistributedGroupedFloat64QueryConfig grouped_lifecycle_query_config =
      follower_grouped_config;
  grouped_lifecycle_query_config.catalog = std::cref(lifecycle_catalog);
  auto grouped_lifecycle_snapshot = publisher->snapshot();
  ASSERT_TRUE(grouped_lifecycle_snapshot.has_value());
  auto grouped_lifecycle = ReplicatedFollowerDistributedGroupedFloat64Query::create(
      make_follower_plan(tablet_id, applied_position), std::move(*grouped_lifecycle_snapshot),
      {.source_node_id = 1U,
       .first_correlation_id = 111U,
       .tls_contexts = observation_tls_contexts,
       .authenticator = &authenticator,
       .node_authorizer = &authorizer,
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000}},
       .connect_timeout = std::chrono::milliseconds{1000},
       .retry = {.maximum_attempts = 1U,
                 .initial_backoff = std::chrono::milliseconds{1},
                 .maximum_backoff = std::chrono::milliseconds{1}},
       .maximum_pairs = 1U},
      grouped_lifecycle_query_config);
  ASSERT_TRUE(grouped_lifecycle.has_value()) << grouped_lifecycle.error().to_string();
  EXPECT_EQ(grouped_lifecycle->state(),
            ReplicatedFollowerDistributedGroupedFloat64QueryState::kAcquiringAuthority);
  EXPECT_EQ(grouped_lifecycle->result().error().code(), common::StatusCode::kInvalidArgument);
  for (std::size_t iteration = 0U;
       iteration < 4096U &&
       grouped_lifecycle->state() ==
           ReplicatedFollowerDistributedGroupedFloat64QueryState::kAcquiringAuthority;
       ++iteration) {
    ASSERT_TRUE(grouped_lifecycle->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(leader_server->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(follower_server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(grouped_lifecycle->state(),
            ReplicatedFollowerDistributedGroupedFloat64QueryState::kExecuting)
      << grouped_lifecycle->failure().to_string();
  EXPECT_EQ(leader_service.calls, 4U);
  EXPECT_EQ(follower_service.calls, 4U);
  EXPECT_EQ(grouped_lifecycle->metrics().authority.completed_pairs, 1U);
  EXPECT_TRUE(grouped_lifecycle->metrics().execution.has_value());
  const common::Status grouped_lifecycle_cancelled = grouped_lifecycle->cancel();
  EXPECT_EQ(grouped_lifecycle_cancelled.code(), common::StatusCode::kCancelled);
  EXPECT_EQ(grouped_lifecycle->state(),
            ReplicatedFollowerDistributedGroupedFloat64QueryState::kCancelled);
  EXPECT_EQ(grouped_lifecycle->result().error(), grouped_lifecycle_cancelled);

  auto successful_snapshot = publisher->snapshot();
  ASSERT_TRUE(successful_snapshot.has_value());
  auto successful_lifecycle = ReplicatedFollowerDistributedVectorAggregateQueryV2::create(
      make_follower_vector_count_plan(tablet_id, applied_position), std::move(*successful_snapshot),
      query::DistributedVectorResultSchema{
          .columns = {{"count",
                       schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(),
                       false}}},
      {.source_node_id = 1U,
       .first_correlation_id = 121U,
       .tls_contexts = observation_tls_contexts,
       .authenticator = &authenticator,
       .node_authorizer = &authorizer,
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000}},
       .connect_timeout = std::chrono::milliseconds{1000},
       .retry = {.maximum_attempts = 1U,
                 .initial_backoff = std::chrono::milliseconds{1},
                 .maximum_backoff = std::chrono::milliseconds{1}},
       .maximum_pairs = 1U},
      vector_lifecycle_query_config);
  ASSERT_TRUE(successful_lifecycle.has_value()) << successful_lifecycle.error().to_string();
  for (std::size_t iteration = 0U;
       iteration < 4096U &&
       successful_lifecycle->state() ==
           ReplicatedFollowerDistributedVectorAggregateQueryStateV2::kAcquiringAuthority;
       ++iteration) {
    ASSERT_TRUE(successful_lifecycle->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(leader_server->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(follower_server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(successful_lifecycle->state(),
            ReplicatedFollowerDistributedVectorAggregateQueryStateV2::kExecuting)
      << successful_lifecycle->failure().to_string();
  EXPECT_EQ(leader_service.calls, 5U);
  EXPECT_EQ(follower_service.calls, 5U);

  const network::Ipv4Endpoint follower_query_endpoint = follower_server->bound_endpoint();
  ASSERT_TRUE(follower_server->shutdown().is_ok());
  CountStarVectorAggregateWorker aggregate_worker;
  auto aggregate_receiver = cluster::DistributedVectorAggregateQueryReceiverV2::create(
      {.local_node_id = 12U,
       .authorizer = &authorizer,
       .worker = &aggregate_worker,
       .maximum_response_frames = 1U,
       .maximum_response_bytes = std::size_t{1024U} * 1024U});
  ASSERT_TRUE(aggregate_receiver.has_value()) << aggregate_receiver.error().to_string();
  auto aggregate_server = cluster::DistributedVectorAggregateQueryTcpServerV2::start(
      {.listener = {.bind_endpoint = follower_query_endpoint},
       .tls = observation_server_tls_config(),
       .authenticator = &authenticator,
       .receiver = &*aggregate_receiver,
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000},
                          .maximum_response_frames = 1U,
                          .maximum_response_bytes = std::size_t{1024U} * 1024U},
       .maximum_connections = 4U,
       .maximum_accepts_per_poll = 4U});
  ASSERT_TRUE(aggregate_server.has_value()) << aggregate_server.error().to_string();
  ASSERT_EQ(aggregate_server->bound_endpoint(), follower_query_endpoint);

  for (std::size_t iteration = 0U;
       iteration < 4096U &&
       successful_lifecycle->state() ==
           ReplicatedFollowerDistributedVectorAggregateQueryStateV2::kExecuting;
       ++iteration) {
    ASSERT_TRUE(successful_lifecycle->poll_once(std::chrono::milliseconds{1}).is_ok())
        << successful_lifecycle->failure().to_string();
    ASSERT_TRUE(aggregate_server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(successful_lifecycle->state(),
            ReplicatedFollowerDistributedVectorAggregateQueryStateV2::kComplete)
      << successful_lifecycle->failure().to_string();
  auto successful_result = successful_lifecycle->result();
  ASSERT_TRUE(successful_result.has_value()) << successful_result.error().to_string();
  // Guarded by the completed owner state and result assertion above.
  // NOLINTBEGIN(bugprone-unchecked-optional-access)
  const auto* const retained_result = std::addressof(successful_result->get());
  ASSERT_EQ(retained_result->row_count, 1U);
  ASSERT_EQ(retained_result->result_schema.columns.size(), 1U);
  EXPECT_EQ(retained_result->result_schema.columns.front().name, "count");
  auto decoded_result = network::decode_query_result_batch(retained_result->encoded_batch);
  ASSERT_TRUE(decoded_result.has_value()) << decoded_result.error().to_string();
  ASSERT_EQ(decoded_result->row_count(), 1U);
  ASSERT_EQ(decoded_result->columns().size(), 1U);
  const network::QueryResultCell* count_cell = decoded_result->cell(0U, 0U);
  ASSERT_NE(count_cell, nullptr);
  common::ByteReader count_reader{count_cell->value};
  auto count = count_reader.read_i64_le();
  ASSERT_TRUE(count.has_value());
  EXPECT_EQ(*count, 3);
  EXPECT_TRUE(count_reader.empty());
  auto retained_again = successful_lifecycle->result();
  ASSERT_TRUE(retained_again.has_value());
  EXPECT_EQ(std::addressof(retained_again->get()), retained_result);
  // NOLINTEND(bugprone-unchecked-optional-access)
  EXPECT_TRUE(successful_lifecycle->poll_once(std::chrono::milliseconds{0}).is_ok());
  EXPECT_EQ(successful_lifecycle->cancel().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(aggregate_worker.bind_calls, 1U);
  EXPECT_EQ(aggregate_worker.execute_calls, 1U);
  const auto successful_metrics = successful_lifecycle->metrics();
  EXPECT_EQ(successful_metrics.authority.completed_pairs, 1U);
  ASSERT_TRUE(successful_metrics.execution.has_value());
  // Guarded by the execution-metrics assertion above.
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  EXPECT_EQ(successful_metrics.execution->transport_completed_attempts, 1U);
  EXPECT_EQ(aggregate_server->metrics().completed_connections, 1U);
  EXPECT_TRUE(aggregate_server->shutdown().is_ok());
  EXPECT_TRUE(leader_server->shutdown().is_ok());

  EXPECT_TRUE(barrier->shutdown().is_ok());
  EXPECT_TRUE(missing_tablet_barrier->shutdown().is_ok());
  EXPECT_TRUE(runtime->shutdown().is_ok());
}

} // namespace
} // namespace chronos::service
