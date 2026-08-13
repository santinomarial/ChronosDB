#include "chronos/cluster/distributed_grouped_query_execution.hpp"
#include "chronos/cluster/distributed_grouped_query_tcp_execution.hpp"
#include "chronos/cluster/distributed_grouped_query_tcp_server.hpp"
#include "chronos/cluster/distributed_query_execution.hpp"
#include "chronos/cluster/distributed_query_tcp_execution.hpp"
#include "chronos/cluster/distributed_query_tcp_server.hpp"
#include "chronos/cluster/distributed_vector_aggregate_query_execution_v2.hpp"
#include "chronos/cluster/distributed_vector_aggregate_query_tcp_execution_v2.hpp"
#include "chronos/cluster/distributed_vector_aggregate_query_tcp_server_v2.hpp"
#include "chronos/cluster/distributed_vector_query_execution_v2.hpp"
#include "chronos/cluster/distributed_vector_query_tcp_execution_v2.hpp"
#include "chronos/cluster/distributed_vector_query_tcp_server_v2.hpp"
#include "chronos/common/byte_reader.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/raft/rebalancing.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/schema_lineage.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <gtest/gtest.h>
#include <memory>
#include <new>
#include <optional>
#include <ranges>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-query-execution-XXXXXX").string();
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

void write_file(const std::filesystem::path& path, const common::ByteView bytes) {
  std::ofstream output{path, std::ios::binary};
  for (const std::byte value : bytes)
    output.put(static_cast<char>(std::to_integer<unsigned char>(value)));
}

[[nodiscard]] schema::TableSchema schema_value() {
  const auto event = id<schema::ColumnId>(5U);
  const auto value = id<schema::ColumnId>(6U);
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

struct ExecutionInput {
  query::DistributedAggregatePlan plan;
  std::vector<query::DistributedReadAdmission> admissions;
  query::CompatibleDistributedAggregateSnapshot snapshot;
  query::CompatibleDistributedGroupedFloat64Snapshot grouped_snapshot;
  query::CompatibleDistributedVectorSnapshotV2 vector_snapshot;
  query::CompatibleDistributedVectorSnapshotV2 vector_aggregate_snapshot;
};

[[nodiscard]] common::Result<ExecutionInput>
make_input(const TemporaryDirectory& directory, const std::uint8_t query_seed = 7U,
           const std::array<raft::NodeId, 2U> serving_nodes = {11U, 12U},
           const std::array<std::uint64_t, 2U> placement_epochs = {12U, 13U}) {
  const schema::TableSchema schema = schema_value();
  const schema::SchemaLineage lineage = schema::SchemaLineage::create(schema).value();
  const std::array tablets{id<schema::TabletId>(3U), id<schema::TabletId>(9U)};
  const std::array groups{uuid(8U), uuid(10U)};
  const std::array positions{10U, 20U};
  std::vector<manifest::TemporalTabletDescriptor> descriptors;
  for (std::size_t index = 0U; index < tablets.size(); ++index) {
    descriptors.push_back({.table_id = schema.table_id(),
                           .tablet_id = tablets[index],
                           .recovery_schema_id = schema.schema_id(),
                           .recovery_schema_version = schema.version(),
                           .source_id = groups[index],
                           .durable_position = positions[index],
                           .reclaim_position = 0U,
                           .first_part_index = 0U,
                           .part_count = 0U,
                           .durable_version_count = 0U,
                           .commit_source = manifest::ManifestCommitSource::kRaft});
  }
  auto encoded = manifest::encode_manifest_v2_temporal({.generation = 1U,
                                                        .database_id = id<manifest::DatabaseId>(1U),
                                                        .wal_reclaim_checkpoint = std::nullopt,
                                                        .tablets = descriptors,
                                                        .parts = {},
                                                        .retries = {}});
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  if (!std::filesystem::create_directory(directory.path() / manifest::kPartsDirectoryName) ||
      !std::filesystem::create_directory(directory.path() / manifest::kManifestDirectoryName)) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "cannot create execution fixture"});
  }
  write_file(directory.path() / manifest::kManifestDirectoryName / manifest::kManifestLockFileName,
             {});
  write_file(directory.path() / manifest::kManifestDirectoryName /
                 *manifest::manifest_file_name(1U),
             encoded->bytes());
  auto storage =
      manifest::ManifestStorage::open_existing({.database_root = directory.path().string()});
  if (!storage.has_value())
    return common::make_unexpected(storage.error());
  std::vector<manifest::TabletSchemaBinding> schema_bindings;
  std::vector<manifest::TemporalTabletSourceBinding> source_bindings;
  for (std::size_t index = 0U; index < tablets.size(); ++index) {
    schema_bindings.push_back({tablets[index], std::cref(lineage)});
    source_bindings.push_back(
        {tablets[index], manifest::ManifestCommitSource::kRaft, groups[index]});
  }
  auto loaded = storage->load_selected_temporal_manifest(
      {.expected_database_id = id<manifest::DatabaseId>(1U),
       .schema_bindings = schema_bindings,
       .source_bindings = source_bindings});
  if (!loaded.has_value())
    return common::make_unexpected(loaded.error());
  auto owner =
      std::make_shared<const manifest::LoadedTemporalManifestGeneration>(std::move(*loaded));
  auto publisher = manifest::TemporalDatabaseStoragePublisher::create(owner, schema_bindings);
  if (!publisher.has_value())
    return common::make_unexpected(publisher.error());
  auto snapshot = publisher->snapshot();
  if (!snapshot.has_value())
    return common::make_unexpected(snapshot.error());

  query::DistributedAggregatePlan plan{
      .query_id = uuid(query_seed),
      .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
      .fragments = {{tablets[0], 0, 100, serving_nodes[0], 10U, 10U},
                    {tablets[1], 101, 200, serving_nodes[1], 20U, 20U}}};
  std::vector<query::DistributedReadAdmission> admissions{
      {tablets[0], serving_nodes[0], 10U, 10U, raft::ReadBarrier{2U, 3U, 10U}},
      {tablets[1], serving_nodes[1], 20U, 20U, raft::ReadBarrier{2U, 4U, 20U}}};
  const std::array placements{
      raft::TabletPlacementMetadata{schema.table_id(),
                                    tablets[0],
                                    placement_epochs[0],
                                    {serving_nodes[0], serving_nodes[0] + 2U},
                                    serving_nodes[0]},
      raft::TabletPlacementMetadata{schema.table_id(),
                                    tablets[1],
                                    placement_epochs[1],
                                    {serving_nodes[1], serving_nodes[1] + 2U},
                                    serving_nodes[1]}};
  const std::array<std::uint32_t, 2U> projection{0U, 1U};
  const std::array bindings{query::DistributedAggregateSnapshotFragmentBinding{
                                std::cref(admissions[0]), std::cref(schema), groups[0],
                                std::cref(placements[0]), projection, 1U, std::nullopt},
                            query::DistributedAggregateSnapshotFragmentBinding{
                                std::cref(admissions[1]), std::cref(schema), groups[1],
                                std::cref(placements[1]), projection, 1U, std::nullopt}};
  auto grouped = query::bind_compatible_distributed_grouped_float64_snapshot(
      plan, *snapshot, bindings, 1U,
      {.maximum_fragments = 2U, .maximum_total_projection_ordinals = 4U});
  if (!grouped.has_value())
    return common::make_unexpected(grouped.error());
  const query::DistributedVectorQueryPlan vector_plan{
      .query_id = plan.query_id,
      .read_policy = plan.read_policy,
      .fragments = plan.fragments,
      .intent = {.mode = query::DistributedVectorPlanMode::kRows,
                 .row_output_indices = {1U},
                 .order_keys = {{.output_index = 0U,
                                 .direction = query::PhysicalSortDirection::kDescending,
                                 .null_placement = query::ScalarNullPlacement::kLast}},
                 .limit = 1U}};
  const std::array vector_bindings{query::DistributedVectorSnapshotFragmentBinding{
                                       std::cref(admissions[0]), std::cref(schema), groups[0],
                                       std::cref(placements[0]), projection, std::nullopt},
                                   query::DistributedVectorSnapshotFragmentBinding{
                                       std::cref(admissions[1]), std::cref(schema), groups[1],
                                       std::cref(placements[1]), projection, std::nullopt}};
  auto vector = query::bind_compatible_distributed_vector_snapshot_v2(
      vector_plan, *snapshot, vector_bindings,
      query::DistributedVectorResultSchema{
          .columns = {{"value", schema.columns()[1].type(), true}}},
      {.maximum_fragments = 2U, .maximum_total_projection_ordinals = 4U});
  if (!vector.has_value())
    return common::make_unexpected(vector.error());
  const query::DistributedVectorQueryPlan vector_aggregate_plan{
      .query_id = plan.query_id,
      .read_policy = plan.read_policy,
      .fragments = plan.fragments,
      .intent = {.mode = query::DistributedVectorPlanMode::kUngroupedAggregate,
                 .aggregates = {{.operation = query::VectorAggregateOperation::kCountStar},
                                {.operation = query::VectorAggregateOperation::kCountStar}}}};
  const auto int64 = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  auto vector_aggregate = query::bind_compatible_distributed_vector_snapshot_v2(
      vector_aggregate_plan, *snapshot, vector_bindings,
      query::DistributedVectorResultSchema{
          .columns = {{"first_count", int64, false}, {"second_count", int64, false}}},
      {.maximum_fragments = 2U, .maximum_total_projection_ordinals = 4U});
  if (!vector_aggregate.has_value())
    return common::make_unexpected(vector_aggregate.error());
  auto compatible = query::bind_compatible_distributed_aggregate_snapshot(
      plan, std::move(*snapshot), bindings,
      {.maximum_fragments = 2U, .maximum_total_projection_ordinals = 4U});
  if (!compatible.has_value())
    return common::make_unexpected(compatible.error());
  return ExecutionInput{std::move(plan),     std::move(admissions), std::move(*compatible),
                        std::move(*grouped), std::move(*vector),    std::move(*vector_aggregate)};
}

[[nodiscard]] query::ExchangeMessage message(const schema::TabletId& tablet, const double value) {
  query::MergeableAggregateState partial;
  EXPECT_TRUE(partial.add(value).is_ok());
  return {uuid(7U), tablet, 1U, partial, true};
}

[[nodiscard]] DistributedVectorQueryResponseV2
vector_response(const query::CompatibleDistributedVectorSnapshotV2& snapshot,
                const std::size_t index) {
  const query::DistributedVectorFragmentDispatch& dispatch = snapshot.dispatches()[index];
  std::vector<network::QueryResultColumn> columns;
  columns.reserve(snapshot.result_schema().columns.size());
  for (const query::DistributedVectorResultColumn& column : snapshot.result_schema().columns)
    columns.push_back({column.name, column.type, column.nullable});
  const auto batch = network::encode_query_result_batch(0U, columns, {});
  EXPECT_TRUE(batch.has_value());
  return {.source_node_id = dispatch.serving_node,
          .target_node_id = 1U,
          .query_id = dispatch.query_id,
          .tablet_id = dispatch.tablet_id,
          .status_code = common::StatusCode::kOk,
          .payload = DistributedVectorResultExchangeMessage{.query_id = dispatch.query_id,
                                                            .tablet_id = dispatch.tablet_id,
                                                            .sequence = 1U,
                                                            .terminal = true,
                                                            .encoded_result_batch = *batch}};
}

[[nodiscard]] std::vector<DistributedVectorAggregateQueryResponseV2>
vector_aggregate_responses(const query::CompatibleDistributedVectorSnapshotV2& snapshot,
                           const std::size_t tablet_index) {
  const query::DistributedVectorFragmentDispatch& dispatch = snapshot.dispatches()[tablet_index];
  const auto definitions = snapshot.aggregate_definitions();
  std::vector<DistributedVectorAggregateQueryResponseV2> responses;
  responses.reserve(definitions.size());
  for (std::size_t ordinal = 0U; ordinal < definitions.size(); ++ordinal) {
    auto state = query::MergeableVectorAggregateState::create(definitions[ordinal]).value();
    for (std::size_t count = 0U; count < tablet_index + ordinal + 1U; ++count)
      EXPECT_TRUE(state.accumulate_count_star().has_value());
    responses.push_back({.source_node_id = dispatch.serving_node,
                         .target_node_id = 1U,
                         .query_id = dispatch.query_id,
                         .tablet_id = dispatch.tablet_id,
                         .status_code = common::StatusCode::kOk,
                         .payload = query::DistributedVectorAggregateExchangeMessage{
                             query::DistributedVectorAggregateExchangePosition{
                                 .query_id = dispatch.query_id,
                                 .tablet_id = dispatch.tablet_id,
                                 .sequence = ordinal + 1U,
                                 .aggregate_ordinal = static_cast<std::uint32_t>(ordinal),
                                 .terminal = ordinal + 1U == definitions.size()},
                             std::move(state)}});
  }
  return responses;
}

[[nodiscard]] std::filesystem::path tls_fixture(const char* name) {
  return std::filesystem::path{CHRONOS_NETWORK_FIXTURE_DIR} / "tls" / name;
}

[[nodiscard]] network::TlsServerConfig execution_tls_server_config() {
  return {.certificate_chain_file = tls_fixture("server.pem").string(),
          .private_key_file = tls_fixture("server-key.pem").string(),
          .trust_store_file = tls_fixture("ca.pem").string()};
}

[[nodiscard]] network::TlsClientConfig execution_tls_client_config() {
  return {.certificate_chain_file = tls_fixture("client.pem").string(),
          .private_key_file = tls_fixture("client-key.pem").string(),
          .trust_store_file = tls_fixture("ca.pem").string(),
          .expected_server_identity = "127.0.0.1"};
}

class ExecutionAuthenticator final : public network::ConnectionAuthenticator {
public:
  explicit ExecutionAuthenticator(const std::uint64_t principal_id) : principal_id_(principal_id) {}

  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest&) override {
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = principal_id_};
  }

private:
  std::uint64_t principal_id_{};
};

class ExecutionNodeAuthorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal_id,
                                      const raft::NodeId node_id) const override {
    return (principal_id == 91U && node_id == 1U) ||
           (principal_id == 92U && (node_id == 11U || node_id == 12U || node_id == 13U));
  }
};

class ExecutionLeaderHintProvider final : public DistributedQueryLeaderHintProvider {
public:
  common::Result<std::optional<DistributedQueryLeaderHint>>
  current_leader_hint(const schema::TabletId&, const raft::GroupId&) const override {
    ++calls;
    return DistributedQueryLeaderHint{13U, 14U};
  }

  mutable std::size_t calls{};
};

class ExecutionWorker final : public DistributedQueryWorkerService {
public:
  ExecutionWorker(const double value, const bool fail_first) noexcept
      : value_(value), fail_first_(fail_first) {}

  common::Result<query::ExchangeMessage>
  execute(const query::DistributedAggregateFragmentDispatch& dispatch) override {
    ++calls;
    if (fail_first_ && calls == 1U) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kUnavailable, "injected retryable worker failure"});
    }
    query::MergeableAggregateState partial;
    const common::Status added = partial.add(value_);
    if (!added.is_ok())
      return common::make_unexpected(added);
    return query::ExchangeMessage{dispatch.fragment.query_id, dispatch.fragment.tablet_id, 1U,
                                  partial, true};
  }

  std::size_t calls{};

private:
  double value_{};
  bool fail_first_{};
};

class GroupedExecutionWorker final : public DistributedGroupedQueryWorkerService {
public:
  explicit GroupedExecutionWorker(const double value, const bool fail_first = false) noexcept
      : value_(value), fail_first_(fail_first) {}

  common::Result<query::DistributedGroupedFloat64WorkerResult>
  execute(const query::DistributedGroupedFloat64FragmentDispatch& dispatch) override {
    ++calls;
    if (fail_first_ && calls == 1U) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kUnavailable, "injected grouped worker failure"});
    }
    query::MergeableAggregateState partial;
    const common::Status added = partial.add(value_);
    if (!added.is_ok())
      return common::make_unexpected(added);
    return query::DistributedGroupedFloat64WorkerResult{
        std::vector<query::GroupedFloat64ExchangeMessage>{{
            .query_id = dispatch.fragment.aggregate.query_id,
            .tablet_id = dispatch.fragment.aggregate.tablet_id,
            .sequence = 1U,
            .group_key = 5.0,
            .partial = partial,
            .terminal = true,
        }}};
  }

  std::size_t calls{};

private:
  double value_{};
  bool fail_first_{};
};

class VectorExecutionWorkerV2 final : public DistributedVectorQueryWorkerServiceV2 {
public:
  common::Result<std::vector<DistributedVectorResultExchangeMessage>>
  execute(const query::DistributedVectorFragmentDispatchV2& dispatch) override {
    ++calls;
    std::vector<network::QueryResultColumn> columns;
    try {
      columns.reserve(dispatch.result_schema.columns.size());
      for (const query::DistributedVectorResultColumn& column : dispatch.result_schema.columns)
        columns.push_back({column.name, column.type, column.nullable});
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                    "vector execution test allocation failed"});
    }
    auto batch = network::encode_query_result_batch(0U, columns, {});
    if (!batch.has_value())
      return common::make_unexpected(batch.error());
    return std::vector<DistributedVectorResultExchangeMessage>{{
        .query_id = dispatch.dispatch.query_id,
        .tablet_id = dispatch.dispatch.tablet_id,
        .sequence = 1U,
        .terminal = true,
        .encoded_result_batch = std::move(*batch),
    }};
  }

  std::size_t calls{};
};

class VectorAggregateExecutionWorkerV2 final
    : public DistributedVectorAggregateQueryWorkerServiceV2 {
public:
  explicit VectorAggregateExecutionWorkerV2(const std::size_t tablet_index) noexcept
      : tablet_index_(tablet_index) {}

  common::Result<std::vector<query::VectorAggregateDefinition>>
  bind_definitions(const query::DistributedVectorFragmentDispatchV2& dispatch) override {
    ++bind_calls;
    return definitions(dispatch);
  }

  common::Result<query::DistributedVectorAggregateWorkerResultV2>
  execute(const query::DistributedVectorFragmentDispatchV2& dispatch) override {
    ++execute_calls;
    auto expected = definitions(dispatch);
    if (!expected.has_value())
      return common::make_unexpected(expected.error());
    try {
      query::DistributedVectorAggregateWorkerResultV2 result{.definitions = *expected,
                                                             .input_rows = tablet_index_ + 1U};
      result.messages.reserve(expected->size());
      for (std::size_t ordinal = 0U; ordinal < expected->size(); ++ordinal) {
        auto state = query::MergeableVectorAggregateState::create((*expected)[ordinal]);
        if (!state.has_value())
          return common::make_unexpected(state.error());
        for (std::size_t count = 0U; count < tablet_index_ + ordinal + 1U; ++count) {
          auto accumulated = state->accumulate_count_star();
          if (!accumulated.has_value())
            return common::make_unexpected(accumulated.error());
        }
        result.messages.emplace_back(
            query::DistributedVectorAggregateExchangePosition{
                .query_id = dispatch.dispatch.query_id,
                .tablet_id = dispatch.dispatch.tablet_id,
                .sequence = ordinal + 1U,
                .aggregate_ordinal = static_cast<std::uint32_t>(ordinal),
                .terminal = ordinal + 1U == expected->size()},
            std::move(*state));
      }
      return result;
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kResourceExhausted,
                         "vector aggregate execution test worker allocation failed"});
    }
  }

  std::size_t bind_calls{};
  std::size_t execute_calls{};

private:
  [[nodiscard]] static common::Result<std::vector<query::VectorAggregateDefinition>>
  definitions(const query::DistributedVectorFragmentDispatchV2& dispatch) {
    try {
      std::vector<query::VectorAggregateDefinition> result;
      result.reserve(dispatch.dispatch.plan.aggregates.size());
      for (const query::DistributedVectorAggregateIntent& aggregate :
           dispatch.dispatch.plan.aggregates) {
        if (aggregate.operation != query::VectorAggregateOperation::kCountStar) {
          return common::make_unexpected(
              common::Status{common::StatusCode::kInvalidArgument,
                             "vector aggregate execution test worker supports only COUNT(*)"});
        }
        result.push_back({.operation = aggregate.operation, .input = std::nullopt});
      }
      return result;
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kResourceExhausted,
                         "vector aggregate execution test definition allocation failed"});
    }
  }

  std::size_t tablet_index_{};
};

[[nodiscard]] DistributedQueryTcpServerConfig
execution_server_config(ExecutionAuthenticator& authenticator, DistributedQueryReceiver& receiver) {
  return {.listener = {},
          .tls = execution_tls_server_config(),
          .authenticator = &authenticator,
          .receiver = &receiver,
          .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                             .exchange_timeout = std::chrono::milliseconds{1000}},
          .maximum_connections = 8U,
          .maximum_accepts_per_poll = 8U};
}

[[nodiscard]] DistributedGroupedQueryTcpServerConfig
grouped_execution_server_config(ExecutionAuthenticator& authenticator,
                                DistributedGroupedQueryReceiver& receiver) {
  return {.listener = {},
          .tls = execution_tls_server_config(),
          .authenticator = &authenticator,
          .receiver = &receiver,
          .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                             .exchange_timeout = std::chrono::milliseconds{1000},
                             .maximum_response_frames = 4U},
          .maximum_connections = 8U,
          .maximum_accepts_per_poll = 8U};
}

[[nodiscard]] DistributedVectorQueryTcpServerConfigV2
vector_execution_server_config(ExecutionAuthenticator& authenticator,
                               DistributedVectorQueryReceiverV2& receiver) {
  return {.listener = {},
          .tls = execution_tls_server_config(),
          .authenticator = &authenticator,
          .receiver = &receiver,
          .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                             .exchange_timeout = std::chrono::milliseconds{1000},
                             .maximum_response_frames = 4U,
                             .maximum_response_bytes = std::size_t{1024U} * 1024U},
          .maximum_connections = 8U,
          .maximum_accepts_per_poll = 8U};
}

[[nodiscard]] DistributedVectorAggregateQueryTcpServerConfigV2
vector_aggregate_execution_server_config(ExecutionAuthenticator& authenticator,
                                         DistributedVectorAggregateQueryReceiverV2& receiver) {
  return {.listener = {},
          .tls = execution_tls_server_config(),
          .authenticator = &authenticator,
          .receiver = &receiver,
          .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                             .exchange_timeout = std::chrono::milliseconds{1000},
                             .maximum_response_frames = 2U,
                             .maximum_response_bytes = std::size_t{1024U} * 1024U},
          .maximum_connections = 8U,
          .maximum_accepts_per_poll = 8U};
}

TEST(DistributedQueryExecutionTest, DeliversEveryTerminalResultExactlyOnceAndFinishes) {
  TemporaryDirectory directory;
  auto input = make_input(directory);
  ASSERT_TRUE(input.has_value()) << input.error().to_string();
  const auto tablets =
      std::array{input->plan.fragments[0].tablet_id, input->plan.fragments[1].tablet_id};
  auto execution = DistributedQueryExecution::create_from_bound_snapshot(
      1U, std::move(input->plan), std::move(input->snapshot));
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();
  EXPECT_EQ(execution->snapshot().snapshot().generation(), 1U);
  EXPECT_EQ(execution->finish().error().code(), common::StatusCode::kUnavailable);
  const auto now = DistributedQueryExecution::TimePoint{};
  ASSERT_TRUE(execution->begin_attempt(tablets[0], now).has_value());
  ASSERT_TRUE(execution->begin_attempt(tablets[1], now).has_value());

  const auto first =
      encode_distributed_query_response_v1({11U, 1U, uuid(7U), tablets[0], common::StatusCode::kOk,
                                            message(tablets[0], 2.5), std::nullopt})
          .value();
  ASSERT_TRUE(execution->accept_response(tablets[0], first, now).is_ok());
  EXPECT_EQ(execution->finish().error().code(), common::StatusCode::kUnavailable);
  const auto second =
      encode_distributed_query_response_v1({12U, 1U, uuid(7U), tablets[1], common::StatusCode::kOk,
                                            message(tablets[1], 3.5), std::nullopt})
          .value();
  ASSERT_TRUE(execution->accept_response(tablets[1], second, now).is_ok());
  const auto result = execution->finish();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_EQ(result->count, 2U);
  EXPECT_EQ(result->sum, 6.0);
  EXPECT_EQ(execution->accept_response(tablets[1], second, now).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(execution->begin_attempt(id<schema::TabletId>(99U), now).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedQueryExecutionTest, RetryBackoffDoesNotFailUntilSenderBecomesTerminal) {
  TemporaryDirectory directory;
  auto input = make_input(directory);
  ASSERT_TRUE(input.has_value()) << input.error().to_string();
  const schema::TabletId tablet = input->plan.fragments[0].tablet_id;
  auto execution = DistributedQueryExecution::create(
      1U, std::move(input->plan), std::move(input->admissions), std::move(input->snapshot),
      {.coordinator = {},
       .retry = {.maximum_attempts = 2U,
                 .initial_backoff = std::chrono::milliseconds{10},
                 .maximum_backoff = std::chrono::milliseconds{10}}});
  ASSERT_TRUE(execution.has_value());
  const auto now = DistributedQueryExecution::TimePoint{};
  ASSERT_TRUE(execution->begin_attempt(tablet, now).has_value());
  const auto retry = encode_distributed_query_response_v1(
                         {11U, 1U, uuid(7U), tablet, common::StatusCode::kUnavailable, std::nullopt,
                          DistributedQueryLeaderHint{13U, 14U}})
                         .value();
  ASSERT_TRUE(execution->accept_response(tablet, retry, now).is_ok());
  EXPECT_EQ(*execution->sender_state(tablet), DistributedQuerySenderState::kBackoff);
  EXPECT_EQ(*execution->next_attempt_not_before(tablet), now + std::chrono::milliseconds{10});
  EXPECT_EQ(*execution->suggested_leader(tablet), DistributedQueryLeaderHint(13U, 14U));
  ASSERT_TRUE(execution->begin_attempt(tablet, now + std::chrono::milliseconds{10}).has_value());
  ASSERT_TRUE(execution
                  ->record_transport_failure(tablet, common::StatusCode::kIoError,
                                             now + std::chrono::milliseconds{10})
                  .is_ok());
  EXPECT_EQ(*execution->sender_state(tablet), DistributedQuerySenderState::kFailed);
  EXPECT_EQ(execution->finish().error().code(), common::StatusCode::kIoError);
}

TEST(DistributedVectorQueryExecutionV2Test,
     RetainsPinnedPlanAndPublishesOnlyCompleteSchemaBoundStreams) {
  TemporaryDirectory directory;
  auto input = make_input(directory);
  ASSERT_TRUE(input.has_value()) << input.error().to_string();
  const auto dispatches = input->vector_snapshot.dispatches();
  ASSERT_EQ(dispatches.size(), 2U);
  const std::array tablets{dispatches[0].tablet_id, dispatches[1].tablet_id};
  const query::DistributedVectorPlanIntent expected_plan = dispatches.front().plan;
  const auto first_response = vector_response(input->vector_snapshot, 0U);
  const auto second_response = vector_response(input->vector_snapshot, 1U);

  auto execution = DistributedVectorQueryExecutionV2::create(
      1U, std::move(input->vector_snapshot),
      {.coordinator = {
           .messages = {.maximum_messages_per_fragment = 2U, .maximum_total_messages = 4U}}});
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();
  EXPECT_EQ(execution->snapshot().snapshot().generation(), 1U);
  EXPECT_EQ(execution->finish().error().code(), common::StatusCode::kUnavailable);
  const auto now = DistributedVectorQueryExecutionV2::TimePoint{};
  ASSERT_TRUE(execution->begin_attempt(tablets[0], now).has_value());
  ASSERT_TRUE(execution->begin_attempt(tablets[1], now).has_value());
  EXPECT_EQ(execution->begin_attempt(id<schema::TabletId>(99U), now).error().code(),
            common::StatusCode::kInvalidArgument);

  ASSERT_TRUE(execution->accept_responses(tablets[0], std::span{&first_response, 1U}, now).is_ok());
  EXPECT_EQ(execution->finish().error().code(), common::StatusCode::kUnavailable);
  ASSERT_TRUE(
      execution->accept_responses(tablets[1], std::span{&second_response, 1U}, now).is_ok());
  auto result = execution->finish();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_EQ(result->plan, expected_plan);
  ASSERT_EQ(result->result.result_schema.columns.size(), 1U);
  EXPECT_EQ(result->result.result_schema.columns.front().name, "value");
  ASSERT_EQ(result->result.messages.size(), 2U);
  EXPECT_EQ(result->result.messages[0].tablet_id, tablets[0]);
  EXPECT_EQ(result->result.messages[1].tablet_id, tablets[1]);
  EXPECT_TRUE(result->result.messages[0].terminal);
  EXPECT_TRUE(result->result.messages[1].terminal);
  EXPECT_EQ(execution->finish().error().code(), common::StatusCode::kInvalidArgument);
}

TEST(DistributedVectorQueryExecutionV2Test, RetryBackoffPoisonsOnlyAtTerminalFailure) {
  TemporaryDirectory directory;
  auto input = make_input(directory);
  ASSERT_TRUE(input.has_value()) << input.error().to_string();
  const schema::TabletId tablet = input->vector_snapshot.dispatches().front().tablet_id;
  auto execution = DistributedVectorQueryExecutionV2::create(
      1U, std::move(input->vector_snapshot),
      {.sender = {.retry = {.maximum_attempts = 2U,
                            .initial_backoff = std::chrono::milliseconds{10},
                            .maximum_backoff = std::chrono::milliseconds{10}}}});
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();
  const auto now = DistributedVectorQueryExecutionV2::TimePoint{};
  ASSERT_TRUE(execution->begin_attempt(tablet, now).has_value());
  ASSERT_TRUE(
      execution->record_transport_failure(tablet, common::StatusCode::kIoError, now).is_ok());
  EXPECT_EQ(*execution->sender_state(tablet), DistributedQuerySenderState::kBackoff);
  EXPECT_EQ(*execution->next_attempt_not_before(tablet), now + std::chrono::milliseconds{10});
  EXPECT_EQ(execution->finish().error().code(), common::StatusCode::kUnavailable);
  ASSERT_TRUE(execution->begin_attempt(tablet, now + std::chrono::milliseconds{10}).has_value());
  ASSERT_TRUE(execution
                  ->record_transport_failure(tablet, common::StatusCode::kIoError,
                                             now + std::chrono::milliseconds{10})
                  .is_ok());
  EXPECT_EQ(*execution->sender_state(tablet), DistributedQuerySenderState::kFailed);
  EXPECT_EQ(execution->finish().error().code(), common::StatusCode::kIoError);
}

TEST(DistributedVectorQueryExecutionV2Test, CoordinatorAdmissionFailurePoisonsCompletion) {
  TemporaryDirectory directory;
  auto input = make_input(directory);
  ASSERT_TRUE(input.has_value()) << input.error().to_string();
  const schema::TabletId tablet = input->vector_snapshot.dispatches().front().tablet_id;
  auto first = vector_response(input->vector_snapshot, 0U);
  ASSERT_TRUE(first.payload.has_value());
  auto second = first;
  ASSERT_TRUE(second.payload.has_value());
  // Guarded by the payload assertions above.
  // NOLINTBEGIN(bugprone-unchecked-optional-access)
  first.payload->terminal = false;
  second.payload->sequence = 2U;
  // NOLINTEND(bugprone-unchecked-optional-access)
  const std::array responses{first, second};
  auto execution = DistributedVectorQueryExecutionV2::create(
      1U, std::move(input->vector_snapshot),
      {.sender = {.maximum_response_frames = 2U},
       .coordinator = {
           .messages = {.maximum_messages_per_fragment = 1U, .maximum_total_messages = 2U}}});
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();
  const auto now = DistributedVectorQueryExecutionV2::TimePoint{};
  ASSERT_TRUE(execution->begin_attempt(tablet, now).has_value());
  EXPECT_EQ(execution->accept_responses(tablet, responses, now).code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(execution->finish().error().code(), common::StatusCode::kResourceExhausted);
}

TEST(DistributedVectorAggregateQueryExecutionV2Test,
     RetainsPinnedDefinitionsAndPublishesOnlyTheMergedCompleteResult) {
  TemporaryDirectory directory;
  auto input = make_input(directory);
  ASSERT_TRUE(input.has_value()) << input.error().to_string();
  const auto dispatches = input->vector_aggregate_snapshot.dispatches();
  ASSERT_EQ(dispatches.size(), 2U);
  const std::array tablets{dispatches[0].tablet_id, dispatches[1].tablet_id};
  const query::DistributedVectorPlanIntent expected_plan = dispatches.front().plan;
  auto first = vector_aggregate_responses(input->vector_aggregate_snapshot, 0U);
  auto second = vector_aggregate_responses(input->vector_aggregate_snapshot, 1U);

  auto execution = DistributedVectorAggregateQueryExecutionV2::create(
      1U, std::move(input->vector_aggregate_snapshot),
      {.sender = {.maximum_response_frames = 2U, .maximum_response_bytes = 4096U},
       .coordinator = {
           .messages = {.maximum_messages_per_fragment = 2U, .maximum_total_messages = 4U},
           .maximum_total_encoded_bytes = std::size_t{1024U} * 1024U}});
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();
  EXPECT_EQ(execution->snapshot().snapshot().generation(), 1U);
  EXPECT_EQ(execution->definitions().size(), 2U);
  EXPECT_EQ(execution->resources().maximum_memory_bytes(),
            kDefaultDistributedVectorAggregateQueryExecutionMemoryBytesV2);
  EXPECT_EQ(execution->finish().error().code(), common::StatusCode::kUnavailable);
  const auto now = DistributedVectorAggregateQueryExecutionV2::TimePoint{};
  ASSERT_TRUE(execution->begin_attempt(tablets[0], now).has_value());
  ASSERT_TRUE(execution->begin_attempt(tablets[1], now).has_value());
  EXPECT_EQ(execution->begin_attempt(id<schema::TabletId>(99U), now).error().code(),
            common::StatusCode::kInvalidArgument);

  ASSERT_TRUE(execution->accept_responses(tablets[0], first, now).is_ok());
  EXPECT_EQ(execution->finish().error().code(), common::StatusCode::kUnavailable);
  ASSERT_TRUE(execution->accept_responses(tablets[1], second, now).is_ok());
  auto result = execution->finish();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_EQ(result->plan, expected_plan);
  ASSERT_EQ(result->result.definitions.size(), 2U);
  ASSERT_EQ(result->result.values.size(), 2U);
  EXPECT_EQ(std::get<std::int64_t>(result->result.values[0].storage()), 3);
  EXPECT_EQ(std::get<std::int64_t>(result->result.values[1].storage()), 5);
  EXPECT_EQ(result->result.result_schema.columns[0].name, "first_count");
  EXPECT_EQ(execution->finish().error().code(), common::StatusCode::kInvalidArgument);
}

TEST(DistributedVectorAggregateQueryExecutionV2Test, RetryBackoffPoisonsOnlyAtTerminalFailure) {
  TemporaryDirectory directory;
  auto input = make_input(directory);
  ASSERT_TRUE(input.has_value()) << input.error().to_string();
  const schema::TabletId tablet = input->vector_aggregate_snapshot.dispatches().front().tablet_id;
  auto execution = DistributedVectorAggregateQueryExecutionV2::create(
      1U, std::move(input->vector_aggregate_snapshot),
      {.sender = {.retry = {.maximum_attempts = 2U,
                            .initial_backoff = std::chrono::milliseconds{10},
                            .maximum_backoff = std::chrono::milliseconds{10}},
                  .maximum_response_frames = 2U}});
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();
  const auto now = DistributedVectorAggregateQueryExecutionV2::TimePoint{};
  ASSERT_TRUE(execution->begin_attempt(tablet, now).has_value());
  ASSERT_TRUE(
      execution->record_transport_failure(tablet, common::StatusCode::kIoError, now).is_ok());
  EXPECT_EQ(*execution->sender_state(tablet), DistributedQuerySenderState::kBackoff);
  EXPECT_EQ(*execution->next_attempt_not_before(tablet), now + std::chrono::milliseconds{10});
  EXPECT_EQ(execution->finish().error().code(), common::StatusCode::kUnavailable);
  ASSERT_TRUE(execution->begin_attempt(tablet, now + std::chrono::milliseconds{10}).has_value());
  ASSERT_TRUE(execution
                  ->record_transport_failure(tablet, common::StatusCode::kIoError,
                                             now + std::chrono::milliseconds{10})
                  .is_ok());
  EXPECT_EQ(*execution->sender_state(tablet), DistributedQuerySenderState::kFailed);
  EXPECT_EQ(execution->finish().error().code(), common::StatusCode::kIoError);
  EXPECT_EQ(execution->finish().error().code(), common::StatusCode::kInvalidArgument);
}

TEST(DistributedVectorAggregateQueryExecutionV2Test, RejectsRowModeBeforeSenderConstruction) {
  TemporaryDirectory directory;
  auto input = make_input(directory);
  ASSERT_TRUE(input.has_value()) << input.error().to_string();
  EXPECT_EQ(
      DistributedVectorAggregateQueryExecutionV2::create(1U, std::move(input->vector_snapshot))
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);
}

TEST(DistributedVectorAggregateQueryTcpExecutionV2Test,
     SchedulesDefinitionBoundTabletsAndFinalizesOneNativeResult) {
  TemporaryDirectory directory;
  auto input = make_input(directory);
  ASSERT_TRUE(input.has_value()) << input.error().to_string();
  auto execution = DistributedVectorAggregateQueryExecutionV2::create(
      1U, std::move(input->vector_aggregate_snapshot),
      {.sender = {.retry = {.maximum_attempts = 2U,
                            .initial_backoff = std::chrono::milliseconds{1},
                            .maximum_backoff = std::chrono::milliseconds{1}},
                  .maximum_response_frames = 2U,
                  .maximum_response_bytes = std::size_t{1024U} * 1024U},
       .coordinator = {
           .messages = {.maximum_messages_per_fragment = 2U, .maximum_total_messages = 4U},
           .maximum_total_encoded_bytes = std::size_t{2U} * 1024U * 1024U}});
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();

  auto refused_listener = network::TcpListener::bind();
  ASSERT_TRUE(refused_listener.has_value()) << refused_listener.error().to_string();
  const network::Ipv4Endpoint refused_endpoint = refused_listener->bound_endpoint();
  ASSERT_TRUE(refused_listener->close().is_ok());

  ExecutionNodeAuthorizer authorizer;
  VectorAggregateExecutionWorkerV2 first_worker{0U};
  VectorAggregateExecutionWorkerV2 second_worker{1U};
  auto first_receiver = DistributedVectorAggregateQueryReceiverV2::create(
      {.local_node_id = 11U, .authorizer = &authorizer, .worker = &first_worker});
  auto second_receiver = DistributedVectorAggregateQueryReceiverV2::create(
      {.local_node_id = 12U, .authorizer = &authorizer, .worker = &second_worker});
  ASSERT_TRUE(first_receiver.has_value()) << first_receiver.error().to_string();
  ASSERT_TRUE(second_receiver.has_value()) << second_receiver.error().to_string();
  ExecutionAuthenticator client_authenticator{91U};
  auto first_server = DistributedVectorAggregateQueryTcpServerV2::start(
      vector_aggregate_execution_server_config(client_authenticator, *first_receiver));
  auto second_server = DistributedVectorAggregateQueryTcpServerV2::start(
      vector_aggregate_execution_server_config(client_authenticator, *second_receiver));
  ASSERT_TRUE(first_server.has_value()) << first_server.error().to_string();
  ASSERT_TRUE(second_server.has_value()) << second_server.error().to_string();

  auto tls_context = network::TlsClientContext::create(execution_tls_client_config());
  ASSERT_TRUE(tls_context.has_value()) << tls_context.error().to_string();
  ExecutionAuthenticator server_authenticator{92U};
  auto scheduled = DistributedVectorAggregateQueryTcpExecutionV2::create(
      std::move(*execution),
      {.authenticator = &server_authenticator,
       .node_authorizer = &authorizer,
       .routes = {{.node_id = 11U,
                   .endpoints = {refused_endpoint, first_server->bound_endpoint()},
                   .tls_context = std::addressof(*tls_context)},
                  {.node_id = 12U,
                   .endpoints = {second_server->bound_endpoint()},
                   .tls_context = std::addressof(*tls_context)}},
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000},
                          .maximum_response_frames = 2U,
                          .maximum_response_bytes = std::size_t{1024U} * 1024U},
       .connect_timeout = std::chrono::milliseconds{1000},
       .execution_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5}});
  ASSERT_TRUE(scheduled.has_value()) << scheduled.error().to_string();
  for (std::size_t iteration = 0U;
       iteration < 4096U &&
       scheduled->state() == DistributedVectorAggregateQueryTcpExecutionStateV2::kRunning;
       ++iteration) {
    ASSERT_TRUE(scheduled->poll_once(std::chrono::milliseconds{1}).is_ok())
        << scheduled->failure().to_string();
    ASSERT_TRUE(first_server->poll_once(std::chrono::milliseconds{0}).is_ok());
    ASSERT_TRUE(second_server->poll_once(std::chrono::milliseconds{0}).is_ok());
  }
  ASSERT_EQ(scheduled->state(), DistributedVectorAggregateQueryTcpExecutionStateV2::kComplete)
      << scheduled->failure().to_string();
  ASSERT_TRUE(scheduled->result().has_value());
  // Guarded by the completion state and result assertion above.
  // NOLINTBEGIN(bugprone-unchecked-optional-access)
  EXPECT_EQ(scheduled->result()->row_count, 1U);
  EXPECT_EQ(scheduled->result()->result_schema.columns[0].name, "first_count");
  auto decoded = network::decode_query_result_batch(scheduled->result()->encoded_batch);
  // NOLINTEND(bugprone-unchecked-optional-access)
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  ASSERT_EQ(decoded->row_count(), 1U);
  ASSERT_EQ(decoded->columns().size(), 2U);
  const network::QueryResultCell* first = decoded->cell(0U, 0U);
  const network::QueryResultCell* second = decoded->cell(0U, 1U);
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  common::ByteReader first_reader{first->value};
  common::ByteReader second_reader{second->value};
  auto first_value = first_reader.read_i64_le();
  auto second_value = second_reader.read_i64_le();
  ASSERT_TRUE(first_value.has_value());
  ASSERT_TRUE(second_value.has_value());
  EXPECT_EQ(*first_value, 3);
  EXPECT_EQ(*second_value, 5);
  EXPECT_TRUE(first_reader.empty());
  EXPECT_TRUE(second_reader.empty());
  EXPECT_TRUE(scheduled->poll_once(std::chrono::milliseconds{0}).is_ok());
  EXPECT_EQ(first_worker.bind_calls, 1U);
  EXPECT_EQ(first_worker.execute_calls, 1U);
  EXPECT_EQ(second_worker.bind_calls, 1U);
  EXPECT_EQ(second_worker.execute_calls, 1U);
  const auto metrics = scheduled->metrics();
  EXPECT_EQ(metrics.attempts_started, 3U);
  EXPECT_EQ(metrics.retries_started, 1U);
  EXPECT_EQ(metrics.transport_completed_attempts, 2U);
  EXPECT_EQ(metrics.transport_failed_attempts, 1U);
  EXPECT_EQ(metrics.active_attempts, 0U);
  EXPECT_TRUE(first_server->shutdown().is_ok());
  EXPECT_TRUE(second_server->shutdown().is_ok());
}

TEST(DistributedVectorAggregateQueryTcpExecutionV2Test,
     RejectsIncompleteRoutesAndOwnsDeadlineAndExplicitCancellation) {
  TemporaryDirectory missing_directory;
  auto missing_input = make_input(missing_directory);
  ASSERT_TRUE(missing_input.has_value()) << missing_input.error().to_string();
  auto missing_execution = DistributedVectorAggregateQueryExecutionV2::create(
      1U, std::move(missing_input->vector_aggregate_snapshot));
  ASSERT_TRUE(missing_execution.has_value());
  ExecutionNodeAuthorizer authorizer;
  ExecutionAuthenticator authenticator{92U};
  auto tls_context = network::TlsClientContext::create(execution_tls_client_config());
  ASSERT_TRUE(tls_context.has_value()) << tls_context.error().to_string();
  auto listener = network::TcpListener::bind();
  ASSERT_TRUE(listener.has_value()) << listener.error().to_string();
  const DistributedQueryNodeRoute first_route{.node_id = 11U,
                                              .endpoints = {listener->bound_endpoint()},
                                              .tls_context = std::addressof(*tls_context)};
  EXPECT_EQ(DistributedVectorAggregateQueryTcpExecutionV2::create(std::move(*missing_execution),
                                                                  {.authenticator = &authenticator,
                                                                   .node_authorizer = &authorizer,
                                                                   .routes = {first_route}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  TemporaryDirectory bounded_directory;
  auto bounded_input = make_input(bounded_directory);
  ASSERT_TRUE(bounded_input.has_value()) << bounded_input.error().to_string();
  auto bounded_execution = DistributedVectorAggregateQueryExecutionV2::create(
      1U, std::move(bounded_input->vector_aggregate_snapshot));
  ASSERT_TRUE(bounded_execution.has_value());
  EXPECT_EQ(
      DistributedVectorAggregateQueryTcpExecutionV2::create(
          std::move(*bounded_execution), {.authenticator = &authenticator,
                                          .node_authorizer = &authorizer,
                                          .routes = {first_route,
                                                     {.node_id = 12U,
                                                      .endpoints = {listener->bound_endpoint()},
                                                      .tls_context = std::addressof(*tls_context)}},
                                          .finalization_limits = {.maximum_working_bytes = 0U}})
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);

  TemporaryDirectory expired_directory;
  auto expired_input = make_input(expired_directory);
  ASSERT_TRUE(expired_input.has_value()) << expired_input.error().to_string();
  auto expired_execution = DistributedVectorAggregateQueryExecutionV2::create(
      1U, std::move(expired_input->vector_aggregate_snapshot));
  ASSERT_TRUE(expired_execution.has_value());
  auto expired = DistributedVectorAggregateQueryTcpExecutionV2::create(
      std::move(*expired_execution),
      {.authenticator = &authenticator,
       .node_authorizer = &authorizer,
       .routes = {first_route,
                  {.node_id = 12U,
                   .endpoints = {listener->bound_endpoint()},
                   .tls_context = std::addressof(*tls_context)}},
       .execution_deadline = std::chrono::steady_clock::now() - std::chrono::milliseconds{1}});
  ASSERT_TRUE(expired.has_value()) << expired.error().to_string();
  EXPECT_EQ(expired->poll_once(std::chrono::milliseconds{0}).code(),
            common::StatusCode::kCancelled);
  EXPECT_EQ(expired->state(), DistributedVectorAggregateQueryTcpExecutionStateV2::kCancelled);
  EXPECT_EQ(expired->metrics().attempts_started, 0U);
  EXPECT_EQ(expired->metrics().active_attempts, 0U);

  TemporaryDirectory cancelled_directory;
  auto cancelled_input = make_input(cancelled_directory);
  ASSERT_TRUE(cancelled_input.has_value()) << cancelled_input.error().to_string();
  auto cancelled_execution = DistributedVectorAggregateQueryExecutionV2::create(
      1U, std::move(cancelled_input->vector_aggregate_snapshot));
  ASSERT_TRUE(cancelled_execution.has_value());
  auto cancelled = DistributedVectorAggregateQueryTcpExecutionV2::create(
      std::move(*cancelled_execution), {.authenticator = &authenticator,
                                        .node_authorizer = &authorizer,
                                        .routes = {first_route,
                                                   {.node_id = 12U,
                                                    .endpoints = {listener->bound_endpoint()},
                                                    .tls_context = std::addressof(*tls_context)}}});
  ASSERT_TRUE(cancelled.has_value()) << cancelled.error().to_string();
  ASSERT_TRUE(cancelled->poll_once(std::chrono::milliseconds{0}).is_ok());
  EXPECT_EQ(cancelled->state(), DistributedVectorAggregateQueryTcpExecutionStateV2::kRunning);
  EXPECT_GT(cancelled->metrics().active_attempts, 0U);
  EXPECT_EQ(cancelled->cancel().code(), common::StatusCode::kCancelled);
  EXPECT_EQ(cancelled->state(), DistributedVectorAggregateQueryTcpExecutionStateV2::kCancelled);
  EXPECT_EQ(cancelled->metrics().active_attempts, 0U);
  EXPECT_TRUE(listener->close().is_ok());
}

TEST(DistributedVectorQueryTcpExecutionV2Test,
     SchedulesAllTabletsAndRotatesOnlyPrevalidatedAddresses) {
  TemporaryDirectory directory;
  auto input = make_input(directory);
  ASSERT_TRUE(input.has_value()) << input.error().to_string();
  auto execution = DistributedVectorQueryExecutionV2::create(
      1U, std::move(input->vector_snapshot),
      {.sender = {.retry = {.maximum_attempts = 2U,
                            .initial_backoff = std::chrono::milliseconds{1},
                            .maximum_backoff = std::chrono::milliseconds{1}},
                  .maximum_response_frames = 4U,
                  .maximum_response_bytes = std::size_t{1024U} * 1024U},
       .coordinator = {
           .messages = {.maximum_messages_per_fragment = 4U, .maximum_total_messages = 8U},
           .maximum_total_encoded_bytes = std::size_t{2U} * 1024U * 1024U}});
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();

  auto refused_listener = network::TcpListener::bind();
  ASSERT_TRUE(refused_listener.has_value()) << refused_listener.error().to_string();
  const network::Ipv4Endpoint refused_endpoint = refused_listener->bound_endpoint();
  ASSERT_TRUE(refused_listener->close().is_ok());

  ExecutionNodeAuthorizer authorizer;
  VectorExecutionWorkerV2 first_worker;
  VectorExecutionWorkerV2 second_worker;
  auto first_receiver = DistributedVectorQueryReceiverV2::create(
      {.local_node_id = 11U, .authorizer = &authorizer, .worker = &first_worker});
  auto second_receiver = DistributedVectorQueryReceiverV2::create(
      {.local_node_id = 12U, .authorizer = &authorizer, .worker = &second_worker});
  ASSERT_TRUE(first_receiver.has_value()) << first_receiver.error().to_string();
  ASSERT_TRUE(second_receiver.has_value()) << second_receiver.error().to_string();
  ExecutionAuthenticator client_authenticator{91U};
  auto first_server = DistributedVectorQueryTcpServerV2::start(
      vector_execution_server_config(client_authenticator, *first_receiver));
  auto second_server = DistributedVectorQueryTcpServerV2::start(
      vector_execution_server_config(client_authenticator, *second_receiver));
  ASSERT_TRUE(first_server.has_value()) << first_server.error().to_string();
  ASSERT_TRUE(second_server.has_value()) << second_server.error().to_string();
  auto tls_context = network::TlsClientContext::create(execution_tls_client_config());
  ASSERT_TRUE(tls_context.has_value()) << tls_context.error().to_string();
  ExecutionAuthenticator server_authenticator{92U};
  auto scheduled = DistributedVectorQueryTcpExecutionV2::create(
      std::move(*execution),
      {.authenticator = &server_authenticator,
       .node_authorizer = &authorizer,
       .routes = {{.node_id = 11U,
                   .endpoints = {refused_endpoint, first_server->bound_endpoint()},
                   .tls_context = std::addressof(*tls_context)},
                  {.node_id = 12U,
                   .endpoints = {second_server->bound_endpoint()},
                   .tls_context = std::addressof(*tls_context)}},
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000},
                          .maximum_response_frames = 4U,
                          .maximum_response_bytes = std::size_t{1024U} * 1024U},
       .connect_timeout = std::chrono::milliseconds{1000},
       .execution_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5}});
  ASSERT_TRUE(scheduled.has_value()) << scheduled.error().to_string();
  for (std::size_t iteration = 0U;
       iteration < 4096U &&
       scheduled->state() == DistributedVectorQueryTcpExecutionStateV2::kRunning;
       ++iteration) {
    ASSERT_TRUE(scheduled->poll_once(std::chrono::milliseconds{1}).is_ok())
        << scheduled->failure().to_string();
    ASSERT_TRUE(first_server->poll_once(std::chrono::milliseconds{0}).is_ok());
    ASSERT_TRUE(second_server->poll_once(std::chrono::milliseconds{0}).is_ok());
  }
  ASSERT_EQ(scheduled->state(), DistributedVectorQueryTcpExecutionStateV2::kComplete)
      << scheduled->failure().to_string();
  ASSERT_TRUE(scheduled->result().has_value());
  // Guarded by the completion state and result assertion above.
  // NOLINTBEGIN(bugprone-unchecked-optional-access)
  EXPECT_EQ(scheduled->result()->plan.mode, query::DistributedVectorPlanMode::kRows);
  EXPECT_EQ(scheduled->result()->result.messages.size(), 2U);
  // NOLINTEND(bugprone-unchecked-optional-access)
  EXPECT_EQ(first_worker.calls, 1U);
  EXPECT_EQ(second_worker.calls, 1U);
  const auto metrics = scheduled->metrics();
  EXPECT_EQ(metrics.attempts_started, 3U);
  EXPECT_EQ(metrics.retries_started, 1U);
  EXPECT_EQ(metrics.transport_completed_attempts, 2U);
  EXPECT_EQ(metrics.transport_failed_attempts, 1U);
  EXPECT_EQ(metrics.active_attempts, 0U);
  EXPECT_TRUE(first_server->shutdown().is_ok());
  EXPECT_TRUE(second_server->shutdown().is_ok());
}

TEST(DistributedVectorQueryTcpExecutionV2Test,
     RejectsIncompleteRoutesAndOwnsDeadlineAndExplicitCancellation) {
  TemporaryDirectory missing_directory;
  auto missing_input = make_input(missing_directory);
  ASSERT_TRUE(missing_input.has_value()) << missing_input.error().to_string();
  auto missing_execution =
      DistributedVectorQueryExecutionV2::create(1U, std::move(missing_input->vector_snapshot));
  ASSERT_TRUE(missing_execution.has_value());
  ExecutionNodeAuthorizer authorizer;
  ExecutionAuthenticator authenticator{92U};
  auto tls_context = network::TlsClientContext::create(execution_tls_client_config());
  ASSERT_TRUE(tls_context.has_value()) << tls_context.error().to_string();
  auto listener = network::TcpListener::bind();
  ASSERT_TRUE(listener.has_value()) << listener.error().to_string();
  const DistributedQueryNodeRoute first_route{.node_id = 11U,
                                              .endpoints = {listener->bound_endpoint()},
                                              .tls_context = std::addressof(*tls_context)};
  EXPECT_EQ(DistributedVectorQueryTcpExecutionV2::create(std::move(*missing_execution),
                                                         {.authenticator = &authenticator,
                                                          .node_authorizer = &authorizer,
                                                          .routes = {first_route}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  TemporaryDirectory expired_directory;
  auto expired_input = make_input(expired_directory);
  ASSERT_TRUE(expired_input.has_value()) << expired_input.error().to_string();
  auto expired_execution =
      DistributedVectorQueryExecutionV2::create(1U, std::move(expired_input->vector_snapshot));
  ASSERT_TRUE(expired_execution.has_value());
  auto expired = DistributedVectorQueryTcpExecutionV2::create(
      std::move(*expired_execution),
      {.authenticator = &authenticator,
       .node_authorizer = &authorizer,
       .routes = {first_route,
                  {.node_id = 12U,
                   .endpoints = {listener->bound_endpoint()},
                   .tls_context = std::addressof(*tls_context)}},
       .execution_deadline = std::chrono::steady_clock::now() - std::chrono::milliseconds{1}});
  ASSERT_TRUE(expired.has_value()) << expired.error().to_string();
  EXPECT_EQ(expired->poll_once(std::chrono::milliseconds{0}).code(),
            common::StatusCode::kCancelled);
  EXPECT_EQ(expired->state(), DistributedVectorQueryTcpExecutionStateV2::kCancelled);
  EXPECT_EQ(expired->metrics().attempts_started, 0U);
  EXPECT_EQ(expired->metrics().active_attempts, 0U);

  TemporaryDirectory cancelled_directory;
  auto cancelled_input = make_input(cancelled_directory);
  ASSERT_TRUE(cancelled_input.has_value()) << cancelled_input.error().to_string();
  auto cancelled_execution =
      DistributedVectorQueryExecutionV2::create(1U, std::move(cancelled_input->vector_snapshot));
  ASSERT_TRUE(cancelled_execution.has_value());
  auto cancelled = DistributedVectorQueryTcpExecutionV2::create(
      std::move(*cancelled_execution), {.authenticator = &authenticator,
                                        .node_authorizer = &authorizer,
                                        .routes = {first_route,
                                                   {.node_id = 12U,
                                                    .endpoints = {listener->bound_endpoint()},
                                                    .tls_context = std::addressof(*tls_context)}}});
  ASSERT_TRUE(cancelled.has_value()) << cancelled.error().to_string();
  ASSERT_TRUE(cancelled->poll_once(std::chrono::milliseconds{0}).is_ok());
  EXPECT_EQ(cancelled->state(), DistributedVectorQueryTcpExecutionStateV2::kRunning);
  EXPECT_GT(cancelled->metrics().active_attempts, 0U);
  EXPECT_EQ(cancelled->cancel().code(), common::StatusCode::kCancelled);
  EXPECT_EQ(cancelled->state(), DistributedVectorQueryTcpExecutionStateV2::kCancelled);
  EXPECT_EQ(cancelled->metrics().active_attempts, 0U);
  EXPECT_TRUE(listener->close().is_ok());
}

TEST(DistributedGroupedQueryExecutionTest, WithholdsResultsUntilEverySenderCloses) {
  TemporaryDirectory directory;
  auto input = make_input(directory);
  ASSERT_TRUE(input.has_value()) << input.error().to_string();
  const auto dispatches = input->grouped_snapshot.dispatches();
  const std::array tablets{dispatches[0].fragment.aggregate.tablet_id,
                           dispatches[1].fragment.aggregate.tablet_id};
  auto execution = DistributedGroupedQueryExecution::create(1U, std::move(input->grouped_snapshot));
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();
  EXPECT_EQ(execution->snapshot().snapshot().generation(), 1U);
  EXPECT_EQ(execution->finish().error().code(), common::StatusCode::kUnavailable);

  const auto now = DistributedGroupedQueryExecution::TimePoint{};
  ASSERT_TRUE(execution->begin_attempt(tablets[0], now).has_value());
  ASSERT_TRUE(execution->begin_attempt(tablets[1], now).has_value());
  query::MergeableAggregateState partial;
  ASSERT_TRUE(partial.add(2.5).is_ok());
  const std::array first{DistributedGroupedQueryResponse{
      .source_node_id = 11U,
      .target_node_id = 1U,
      .query_id = uuid(7U),
      .tablet_id = tablets[0],
      .status_code = common::StatusCode::kOk,
      .payload = DistributedGroupedQueryResponsePayload{
          query::GroupedFloat64ExchangeMessage{uuid(7U), tablets[0], 1U, 5.0, partial, true}}}};
  ASSERT_TRUE(execution->accept_responses(tablets[0], first, now).is_ok());
  EXPECT_EQ(execution->finish().error().code(), common::StatusCode::kUnavailable);

  const std::array second{DistributedGroupedQueryResponse{
      .source_node_id = 12U,
      .target_node_id = 1U,
      .query_id = uuid(7U),
      .tablet_id = tablets[1],
      .status_code = common::StatusCode::kOk,
      .payload = DistributedGroupedQueryResponsePayload{
          query::GroupedExchangeTerminalMessage{uuid(7U), tablets[1], 1U}}}};
  ASSERT_TRUE(execution->accept_responses(tablets[1], second, now).is_ok());
  auto result = execution->finish();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_EQ(result->size(), 1U);
  EXPECT_EQ(result->front().group_key, 5.0);
  EXPECT_EQ(result->front().aggregate.count, 1U);
  EXPECT_EQ(result->front().aggregate.sum, 2.5);
  EXPECT_EQ(execution->accept_responses(tablets[1], second, now).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(execution->begin_attempt(id<schema::TabletId>(99U), now).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedGroupedQueryExecutionTest, PublishesOnlyTerminalSenderFailure) {
  TemporaryDirectory directory;
  auto input = make_input(directory);
  ASSERT_TRUE(input.has_value()) << input.error().to_string();
  const schema::TabletId tablet =
      input->grouped_snapshot.dispatches().front().fragment.aggregate.tablet_id;
  auto execution = DistributedGroupedQueryExecution::create(
      1U, std::move(input->grouped_snapshot),
      {.coordinator = {},
       .retry = {.maximum_attempts = 2U,
                 .initial_backoff = std::chrono::milliseconds{10},
                 .maximum_backoff = std::chrono::milliseconds{10}}});
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();
  const auto now = DistributedGroupedQueryExecution::TimePoint{};
  ASSERT_TRUE(execution->begin_attempt(tablet, now).has_value());
  ASSERT_TRUE(
      execution->record_transport_failure(tablet, common::StatusCode::kIoError, now).is_ok());
  EXPECT_EQ(*execution->sender_state(tablet), DistributedQuerySenderState::kBackoff);
  EXPECT_EQ(execution->finish().error().code(), common::StatusCode::kUnavailable);
  ASSERT_TRUE(execution->begin_attempt(tablet, now + std::chrono::milliseconds{10}).has_value());
  ASSERT_TRUE(execution
                  ->record_transport_failure(tablet, common::StatusCode::kIoError,
                                             now + std::chrono::milliseconds{10})
                  .is_ok());
  EXPECT_EQ(*execution->sender_state(tablet), DistributedQuerySenderState::kFailed);
  EXPECT_EQ(execution->finish().error().code(), common::StatusCode::kIoError);
}

TEST(DistributedGroupedQueryExecutionTest, CarriesGlobalGroupedOrderAndLimit) {
  TemporaryDirectory directory;
  auto input = make_input(directory);
  ASSERT_TRUE(input.has_value()) << input.error().to_string();
  const auto dispatches = input->grouped_snapshot.dispatches();
  const std::array tablets{dispatches[0].fragment.aggregate.tablet_id,
                           dispatches[1].fragment.aggregate.tablet_id};
  auto execution = DistributedGroupedQueryExecution::create(
      1U, std::move(input->grouped_snapshot),
      {.coordinator = {},
       .retry = {},
       .result = {.direction = query::DistributedGroupedFloat64ResultDirection::kDescending,
                  .null_placement = query::DistributedGroupedFloat64NullPlacement::kLast,
                  .limit = 1U}});
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();
  const auto now = DistributedGroupedQueryExecution::TimePoint{};
  ASSERT_TRUE(execution->begin_attempt(tablets[0], now).has_value());
  ASSERT_TRUE(execution->begin_attempt(tablets[1], now).has_value());
  query::MergeableAggregateState first_partial;
  query::MergeableAggregateState second_partial;
  ASSERT_TRUE(first_partial.add(2.5).is_ok());
  ASSERT_TRUE(second_partial.add(3.5).is_ok());
  const std::array first{DistributedGroupedQueryResponse{
      .source_node_id = 11U,
      .target_node_id = 1U,
      .query_id = uuid(7U),
      .tablet_id = tablets[0],
      .status_code = common::StatusCode::kOk,
      .payload = DistributedGroupedQueryResponsePayload{query::GroupedFloat64ExchangeMessage{
          uuid(7U), tablets[0], 1U, 5.0, first_partial, true}}}};
  const std::array second{DistributedGroupedQueryResponse{
      .source_node_id = 12U,
      .target_node_id = 1U,
      .query_id = uuid(7U),
      .tablet_id = tablets[1],
      .status_code = common::StatusCode::kOk,
      .payload = DistributedGroupedQueryResponsePayload{query::GroupedFloat64ExchangeMessage{
          uuid(7U), tablets[1], 1U, 7.0, second_partial, true}}}};
  ASSERT_TRUE(execution->accept_responses(tablets[0], first, now).is_ok());
  ASSERT_TRUE(execution->accept_responses(tablets[1], second, now).is_ok());
  const auto result = execution->finish();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_EQ(result->size(), 1U);
  EXPECT_EQ(result->front().group_key, 7.0);
  EXPECT_EQ(result->front().aggregate.sum, 3.5);
}

TEST(DistributedGroupedQueryTcpExecutionTest, SchedulesAllTabletsAndRotatesAddressesOnRetry) {
  TemporaryDirectory directory;
  auto input = make_input(directory);
  ASSERT_TRUE(input.has_value()) << input.error().to_string();
  auto execution = DistributedGroupedQueryExecution::create(
      1U, std::move(input->grouped_snapshot),
      {.coordinator = {},
       .retry = {.maximum_attempts = 2U,
                 .initial_backoff = std::chrono::milliseconds{1},
                 .maximum_backoff = std::chrono::milliseconds{1}}});
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();

  auto refused_listener = network::TcpListener::bind();
  ASSERT_TRUE(refused_listener.has_value()) << refused_listener.error().to_string();
  const network::Ipv4Endpoint refused_endpoint = refused_listener->bound_endpoint();
  ASSERT_TRUE(refused_listener->close().is_ok());

  ExecutionNodeAuthorizer authorizer;
  GroupedExecutionWorker first_worker{2.5};
  GroupedExecutionWorker second_worker{3.5};
  auto first_receiver = DistributedGroupedQueryReceiver::create(
      {.local_node_id = 11U, .authorizer = &authorizer, .worker = &first_worker});
  auto second_receiver = DistributedGroupedQueryReceiver::create(
      {.local_node_id = 12U, .authorizer = &authorizer, .worker = &second_worker});
  ASSERT_TRUE(first_receiver.has_value());
  ASSERT_TRUE(second_receiver.has_value());
  ExecutionAuthenticator client_authenticator{91U};
  auto first_server = DistributedGroupedQueryTcpServer::start(
      grouped_execution_server_config(client_authenticator, *first_receiver));
  auto second_server = DistributedGroupedQueryTcpServer::start(
      grouped_execution_server_config(client_authenticator, *second_receiver));
  ASSERT_TRUE(first_server.has_value()) << first_server.error().to_string();
  ASSERT_TRUE(second_server.has_value()) << second_server.error().to_string();

  auto tls_context = network::TlsClientContext::create(execution_tls_client_config());
  ASSERT_TRUE(tls_context.has_value()) << tls_context.error().to_string();
  ExecutionAuthenticator server_authenticator{92U};
  auto scheduled = DistributedGroupedQueryTcpExecution::create(
      std::move(*execution),
      {.authenticator = &server_authenticator,
       .node_authorizer = &authorizer,
       .routes = {{11U, {refused_endpoint, first_server->bound_endpoint()}, &*tls_context},
                  {12U, {second_server->bound_endpoint()}, &*tls_context}},
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000},
                          .maximum_response_frames = 4U},
       .connect_timeout = std::chrono::milliseconds{1000}});
  ASSERT_TRUE(scheduled.has_value()) << scheduled.error().to_string();

  for (std::size_t iteration = 0U;
       iteration < 4096U &&
       scheduled->state() == DistributedGroupedQueryTcpExecutionState::kRunning;
       ++iteration) {
    const common::Status status = scheduled->poll_once(std::chrono::milliseconds{1});
    ASSERT_TRUE(status.is_ok()) << status.to_string();
    ASSERT_TRUE(first_server->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(second_server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(scheduled->state(), DistributedGroupedQueryTcpExecutionState::kComplete)
      << scheduled->failure().to_string();
  auto result = scheduled->result();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_EQ(result->size(), 1U);
  EXPECT_EQ(result->front().group_key, 5.0);
  EXPECT_EQ(result->front().aggregate.count, 2U);
  EXPECT_EQ(result->front().aggregate.sum, 6.0);
  EXPECT_EQ(first_worker.calls, 1U);
  EXPECT_EQ(second_worker.calls, 1U);
  const auto metrics = scheduled->metrics();
  EXPECT_EQ(metrics.attempts_started, 3U);
  EXPECT_EQ(metrics.retries_started, 1U);
  EXPECT_EQ(metrics.transport_completed_attempts, 2U);
  EXPECT_EQ(metrics.transport_failed_attempts, 1U);
  EXPECT_EQ(metrics.active_attempts, 0U);
}

TEST(DistributedGroupedQueryTcpExecutionTest, RejectsIncompleteRoutesAndOwnsDeadlineCancellation) {
  ExecutionNodeAuthorizer authorizer;
  ExecutionAuthenticator authenticator{92U};
  auto tls_context = network::TlsClientContext::create(execution_tls_client_config());
  ASSERT_TRUE(tls_context.has_value());

  TemporaryDirectory incomplete_directory;
  auto incomplete_input = make_input(incomplete_directory);
  ASSERT_TRUE(incomplete_input.has_value());
  auto incomplete_execution =
      DistributedGroupedQueryExecution::create(1U, std::move(incomplete_input->grouped_snapshot));
  ASSERT_TRUE(incomplete_execution.has_value());
  EXPECT_EQ(DistributedGroupedQueryTcpExecution::create(
                std::move(*incomplete_execution),
                {.authenticator = &authenticator,
                 .node_authorizer = &authorizer,
                 .routes = {{11U, {{{127U, 0U, 0U, 1U}, 1U}}, &*tls_context}}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  TemporaryDirectory expired_directory;
  auto expired_input = make_input(expired_directory);
  ASSERT_TRUE(expired_input.has_value());
  auto expired_execution =
      DistributedGroupedQueryExecution::create(1U, std::move(expired_input->grouped_snapshot));
  ASSERT_TRUE(expired_execution.has_value());
  auto expired = DistributedGroupedQueryTcpExecution::create(
      std::move(*expired_execution),
      {.authenticator = &authenticator,
       .node_authorizer = &authorizer,
       .routes = {{11U, {{{127U, 0U, 0U, 1U}, 1U}}, &*tls_context},
                  {12U, {{{127U, 0U, 0U, 1U}, 2U}}, &*tls_context}},
       .execution_deadline = DistributedGroupedQueryExecution::TimePoint{}});
  ASSERT_TRUE(expired.has_value());
  const common::Status deadline = expired->poll_once(std::chrono::milliseconds{100});
  EXPECT_EQ(deadline.code(), common::StatusCode::kCancelled);
  EXPECT_EQ(expired->state(), DistributedGroupedQueryTcpExecutionState::kCancelled);
  EXPECT_EQ(expired->metrics().attempts_started, 0U);
  EXPECT_EQ(expired->metrics().active_attempts, 0U);
  EXPECT_EQ(expired->result().error(), deadline);
  EXPECT_EQ(expired->cancel(), deadline);

  TemporaryDirectory cancelled_directory;
  auto cancelled_input = make_input(cancelled_directory);
  ASSERT_TRUE(cancelled_input.has_value());
  auto cancelled_execution =
      DistributedGroupedQueryExecution::create(1U, std::move(cancelled_input->grouped_snapshot));
  ASSERT_TRUE(cancelled_execution.has_value());
  auto listener = network::TcpListener::bind();
  ASSERT_TRUE(listener.has_value()) << listener.error().to_string();
  auto cancelled = DistributedGroupedQueryTcpExecution::create(
      std::move(*cancelled_execution),
      {.authenticator = &authenticator,
       .node_authorizer = &authorizer,
       .routes = {{11U, {listener->bound_endpoint()}, &*tls_context},
                  {12U, {listener->bound_endpoint()}, &*tls_context}}});
  ASSERT_TRUE(cancelled.has_value());
  ASSERT_TRUE(cancelled->poll_once(std::chrono::milliseconds{0}).is_ok());
  EXPECT_EQ(cancelled->metrics().attempts_started, 2U);
  EXPECT_EQ(cancelled->metrics().active_attempts, 2U);
  const common::Status cancellation = cancelled->cancel();
  EXPECT_EQ(cancellation.code(), common::StatusCode::kCancelled);
  EXPECT_EQ(cancelled->state(), DistributedGroupedQueryTcpExecutionState::kCancelled);
  EXPECT_EQ(cancelled->metrics().active_attempts, 0U);
  EXPECT_EQ(cancelled->poll_once(std::chrono::milliseconds{0}), cancellation);
  EXPECT_EQ(cancelled->result().error(), cancellation);
}

TEST(DistributedGroupedQueryTcpExecutionTest, RebindsWholeQueryAndDiscardsPriorGroupPartials) {
  ExecutionNodeAuthorizer authorizer;
  ExecutionAuthenticator client_authenticator{91U};
  ExecutionAuthenticator server_authenticator{92U};
  auto tls_context = network::TlsClientContext::create(execution_tls_client_config());
  ASSERT_TRUE(tls_context.has_value());

  GroupedExecutionWorker old_first_worker{100.0};
  GroupedExecutionWorker old_second_worker{200.0, true};
  auto old_first_receiver = DistributedGroupedQueryReceiver::create(
      {.local_node_id = 11U, .authorizer = &authorizer, .worker = &old_first_worker});
  auto old_second_receiver = DistributedGroupedQueryReceiver::create(
      {.local_node_id = 12U, .authorizer = &authorizer, .worker = &old_second_worker});
  ASSERT_TRUE(old_first_receiver.has_value());
  ASSERT_TRUE(old_second_receiver.has_value());
  auto old_first_server = DistributedGroupedQueryTcpServer::start(
      grouped_execution_server_config(client_authenticator, *old_first_receiver));
  auto old_second_server = DistributedGroupedQueryTcpServer::start(
      grouped_execution_server_config(client_authenticator, *old_second_receiver));
  ASSERT_TRUE(old_first_server.has_value());
  ASSERT_TRUE(old_second_server.has_value());

  TemporaryDirectory old_directory;
  auto old_input = make_input(old_directory);
  ASSERT_TRUE(old_input.has_value());
  auto old_execution = DistributedGroupedQueryExecution::create(
      1U, std::move(old_input->grouped_snapshot),
      {.coordinator = {},
       .retry = {.maximum_attempts = 1U,
                 .initial_backoff = std::chrono::milliseconds{1},
                 .maximum_backoff = std::chrono::milliseconds{1}}});
  ASSERT_TRUE(old_execution.has_value());
  auto scheduled = DistributedGroupedQueryTcpExecution::create(
      std::move(*old_execution),
      {.authenticator = &server_authenticator,
       .node_authorizer = &authorizer,
       .routes = {{11U, {old_first_server->bound_endpoint()}, &*tls_context},
                  {12U, {old_second_server->bound_endpoint()}, &*tls_context}},
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000},
                          .maximum_response_frames = 4U},
       .connect_timeout = std::chrono::milliseconds{1000},
       .maximum_rebindings = 1U});
  ASSERT_TRUE(scheduled.has_value());

  for (std::size_t iteration = 0U;
       iteration < 1024U && scheduled->metrics().transport_completed_attempts == 0U; ++iteration) {
    ASSERT_TRUE(scheduled->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(old_first_server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(scheduled->metrics().transport_completed_attempts, 1U);
  ASSERT_EQ(old_first_worker.calls, 1U);
  for (std::size_t iteration = 0U;
       iteration < 1024U &&
       scheduled->state() == DistributedGroupedQueryTcpExecutionState::kRunning;
       ++iteration) {
    ASSERT_TRUE(old_second_server->poll_once(std::chrono::milliseconds{1}).is_ok());
    const common::Status status = scheduled->poll_once(std::chrono::milliseconds{1});
    ASSERT_TRUE(status.is_ok() || status.code() == common::StatusCode::kUnavailable);
  }
  ASSERT_EQ(scheduled->state(), DistributedGroupedQueryTcpExecutionState::kFailed);
  EXPECT_EQ(scheduled->failure().code(), common::StatusCode::kUnavailable);

  TemporaryDirectory wrong_directory;
  auto wrong_input = make_input(wrong_directory, 99U);
  ASSERT_TRUE(wrong_input.has_value());
  auto wrong_execution =
      DistributedGroupedQueryExecution::create(1U, std::move(wrong_input->grouped_snapshot));
  ASSERT_TRUE(wrong_execution.has_value());
  EXPECT_EQ(scheduled
                ->rebind(std::move(*wrong_execution),
                         {.authenticator = &server_authenticator,
                          .node_authorizer = &authorizer,
                          .routes = {{11U, {old_first_server->bound_endpoint()}, &*tls_context},
                                     {12U, {old_second_server->bound_endpoint()}, &*tls_context}},
                          .maximum_rebindings = 1U})
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(scheduled->state(), DistributedGroupedQueryTcpExecutionState::kFailed);
  EXPECT_EQ(scheduled->metrics().rebindings_started, 0U);

  GroupedExecutionWorker new_first_worker{2.5};
  GroupedExecutionWorker new_second_worker{3.5};
  auto new_first_receiver = DistributedGroupedQueryReceiver::create(
      {.local_node_id = 11U, .authorizer = &authorizer, .worker = &new_first_worker});
  auto new_second_receiver = DistributedGroupedQueryReceiver::create(
      {.local_node_id = 12U, .authorizer = &authorizer, .worker = &new_second_worker});
  ASSERT_TRUE(new_first_receiver.has_value());
  ASSERT_TRUE(new_second_receiver.has_value());
  auto new_first_server = DistributedGroupedQueryTcpServer::start(
      grouped_execution_server_config(client_authenticator, *new_first_receiver));
  auto new_second_server = DistributedGroupedQueryTcpServer::start(
      grouped_execution_server_config(client_authenticator, *new_second_receiver));
  ASSERT_TRUE(new_first_server.has_value());
  ASSERT_TRUE(new_second_server.has_value());
  TemporaryDirectory new_directory;
  auto new_input = make_input(new_directory);
  ASSERT_TRUE(new_input.has_value());
  auto new_execution =
      DistributedGroupedQueryExecution::create(1U, std::move(new_input->grouped_snapshot));
  ASSERT_TRUE(new_execution.has_value());
  ASSERT_TRUE(scheduled
                  ->rebind(std::move(*new_execution),
                           {.authenticator = &server_authenticator,
                            .node_authorizer = &authorizer,
                            .routes = {{11U, {new_first_server->bound_endpoint()}, &*tls_context},
                                       {12U, {new_second_server->bound_endpoint()}, &*tls_context}},
                            .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                                               .exchange_timeout = std::chrono::milliseconds{1000},
                                               .maximum_response_frames = 4U},
                            .connect_timeout = std::chrono::milliseconds{1000},
                            .maximum_rebindings = 1U})
                  .is_ok());
  EXPECT_EQ(scheduled->state(), DistributedGroupedQueryTcpExecutionState::kRunning);
  EXPECT_EQ(scheduled->metrics().rebindings_started, 1U);

  for (std::size_t iteration = 0U;
       iteration < 2048U &&
       scheduled->state() == DistributedGroupedQueryTcpExecutionState::kRunning;
       ++iteration) {
    ASSERT_TRUE(scheduled->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(new_first_server->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(new_second_server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(scheduled->state(), DistributedGroupedQueryTcpExecutionState::kComplete)
      << scheduled->failure().to_string();
  auto result = scheduled->result();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_EQ(result->size(), 1U);
  EXPECT_EQ(result->front().group_key, 5.0);
  EXPECT_EQ(result->front().aggregate.count, 2U);
  EXPECT_EQ(result->front().aggregate.sum, 6.0);
  EXPECT_EQ(scheduled->metrics().attempts_started, 4U);
  EXPECT_EQ(scheduled->metrics().transport_completed_attempts, 4U);
  EXPECT_EQ(scheduled->metrics().rebindings_started, 1U);
}

TEST(DistributedQueryTcpExecutionTest, ResolvesSelectedRoutesFromCommittedNodeMetadata) {
  TemporaryDirectory directory;
  auto input = make_input(directory);
  ASSERT_TRUE(input.has_value()) << input.error().to_string();
  network::TlsClientContext first_tls;
  network::TlsClientContext second_tls;
  raft::MetadataCatalogSnapshot catalog{.applied_index = 9U,
                                        .cluster_nodes = {{10U, "node-10.example:7410"},
                                                          {11U, "127.0.0.1:7411"},
                                                          {12U, "127.0.0.2:7412"}}};
  const std::array contexts{DistributedQueryNodeTlsContext{11U, &first_tls},
                            DistributedQueryNodeTlsContext{12U, &second_tls}};

  auto routes =
      resolve_distributed_query_node_routes(catalog, input->snapshot.dispatches(), contexts);
  ASSERT_TRUE(routes.has_value()) << routes.error().to_string();
  ASSERT_EQ(routes->size(), 2U);
  EXPECT_EQ((*routes)[0].node_id, 11U);
  EXPECT_EQ((*routes)[0].endpoints,
            (std::vector<network::Ipv4Endpoint>{{{127U, 0U, 0U, 1U}, 7411U}}));
  EXPECT_EQ((*routes)[0].tls_context, &first_tls);
  EXPECT_EQ((*routes)[1].node_id, 12U);
  EXPECT_EQ((*routes)[1].endpoints,
            (std::vector<network::Ipv4Endpoint>{{{127U, 0U, 0U, 2U}, 7412U}}));
  EXPECT_EQ((*routes)[1].tls_context, &second_tls);

  auto vector_routes =
      resolve_distributed_query_node_routes(catalog, input->vector_snapshot.dispatches(), contexts);
  ASSERT_TRUE(vector_routes.has_value()) << vector_routes.error().to_string();
  ASSERT_EQ(vector_routes->size(), routes->size());
  for (std::size_t index = 0U; index < routes->size(); ++index) {
    EXPECT_EQ((*vector_routes)[index].node_id, (*routes)[index].node_id);
    EXPECT_EQ((*vector_routes)[index].endpoints, (*routes)[index].endpoints);
    EXPECT_EQ((*vector_routes)[index].tls_context, (*routes)[index].tls_context);
  }
  ASSERT_EQ(input->vector_snapshot.dispatches().size(), 2U);
  std::array invalid_vector_dispatches{input->vector_snapshot.dispatches()[0],
                                       input->vector_snapshot.dispatches()[1]};
  invalid_vector_dispatches[0].serving_node = 0U;
  EXPECT_EQ(resolve_distributed_query_node_routes(catalog, invalid_vector_dispatches, contexts)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  catalog.cluster_nodes[2].endpoint = "localhost:7412";
  routes = resolve_distributed_query_node_routes(catalog, input->snapshot.dispatches(), contexts);
  ASSERT_TRUE(routes.has_value()) << routes.error().to_string();
  ASSERT_FALSE((*routes)[1].endpoints.empty());
  EXPECT_TRUE(std::ranges::all_of((*routes)[1].endpoints, [](const auto& endpoint) {
    return endpoint.port == 7412U &&
           endpoint.address != std::array<std::uint8_t, 4>{0U, 0U, 0U, 0U};
  }));
  catalog.cluster_nodes[2].endpoint = "127.0.0.2:7412";
  EXPECT_EQ(resolve_distributed_query_node_routes(catalog, input->snapshot.dispatches(),
                                                  std::span{contexts}.first(1U))
                .error()
                .code(),
            common::StatusCode::kUnavailable);
  EXPECT_EQ(resolve_distributed_query_node_routes(catalog, input->snapshot.dispatches(), contexts,
                                                  {.maximum_routes = 1U})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
  catalog.cluster_nodes[2].endpoint = "Node-12.example:7412";
  EXPECT_EQ(resolve_distributed_query_node_routes(catalog, input->snapshot.dispatches(), contexts)
                .error()
                .code(),
            common::StatusCode::kUnavailable);
  catalog.cluster_nodes[2].endpoint = "127.0.0.2:7412";
  std::swap(catalog.cluster_nodes[0], catalog.cluster_nodes[2]);
  EXPECT_EQ(resolve_distributed_query_node_routes(catalog, input->snapshot.dispatches(), contexts)
                .error()
                .code(),
            common::StatusCode::kCorruption);
}

TEST(DistributedQueryTcpExecutionTest, SchedulesPlanOrderedTabletsAndRetriesWithoutRebinding) {
  TemporaryDirectory directory;
  auto input = make_input(directory);
  ASSERT_TRUE(input.has_value()) << input.error().to_string();
  auto execution = DistributedQueryExecution::create(
      1U, std::move(input->plan), std::move(input->admissions), std::move(input->snapshot),
      {.coordinator = {},
       .retry = {.maximum_attempts = 2U,
                 .initial_backoff = std::chrono::milliseconds{1},
                 .maximum_backoff = std::chrono::milliseconds{1}}});
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();

  ExecutionNodeAuthorizer authorizer;
  ExecutionWorker first_worker{2.5, true};
  ExecutionWorker second_worker{3.5, false};
  auto first_receiver = DistributedQueryReceiver::create(
      {.local_node_id = 11U, .authorizer = &authorizer, .worker = &first_worker});
  auto second_receiver = DistributedQueryReceiver::create(
      {.local_node_id = 12U, .authorizer = &authorizer, .worker = &second_worker});
  ASSERT_TRUE(first_receiver.has_value());
  ASSERT_TRUE(second_receiver.has_value());
  ExecutionAuthenticator client_authenticator{91U};
  auto first_server = DistributedQueryTcpServer::start(
      execution_server_config(client_authenticator, *first_receiver));
  auto second_server = DistributedQueryTcpServer::start(
      execution_server_config(client_authenticator, *second_receiver));
  ASSERT_TRUE(first_server.has_value()) << first_server.error().to_string();
  ASSERT_TRUE(second_server.has_value()) << second_server.error().to_string();

  auto tls_context = network::TlsClientContext::create(execution_tls_client_config());
  ASSERT_TRUE(tls_context.has_value()) << tls_context.error().to_string();
  ExecutionAuthenticator server_authenticator{92U};
  auto tcp_execution = DistributedQueryTcpExecution::create(
      std::move(*execution),
      {.authenticator = &server_authenticator,
       .node_authorizer = &authorizer,
       .routes = {{11U, {first_server->bound_endpoint()}, &*tls_context},
                  {12U, {second_server->bound_endpoint()}, &*tls_context}},
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000}},
       .connect_timeout = std::chrono::milliseconds{1000}});
  ASSERT_TRUE(tcp_execution.has_value()) << tcp_execution.error().to_string();
  EXPECT_EQ(tcp_execution->snapshot().snapshot().generation(), 1U);

  for (std::size_t iteration = 0U;
       iteration < 2048U && tcp_execution->state() == DistributedQueryTcpExecutionState::kRunning;
       ++iteration) {
    ASSERT_TRUE(tcp_execution->poll_once(std::chrono::milliseconds{1}).is_ok())
        << tcp_execution->failure().to_string();
    ASSERT_TRUE(first_server->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(second_server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }

  ASSERT_EQ(tcp_execution->state(), DistributedQueryTcpExecutionState::kComplete)
      << tcp_execution->failure().to_string();
  auto result = tcp_execution->result();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_EQ(result->count, 2U);
  EXPECT_EQ(result->sum, 6.0);
  EXPECT_EQ(first_worker.calls, 2U);
  EXPECT_EQ(second_worker.calls, 1U);
  const auto metrics = tcp_execution->metrics();
  EXPECT_EQ(metrics.attempts_started, 3U);
  EXPECT_EQ(metrics.retries_started, 1U);
  EXPECT_EQ(metrics.transport_completed_attempts, 3U);
  EXPECT_EQ(metrics.transport_failed_attempts, 0U);
  EXPECT_EQ(metrics.active_attempts, 0U);
}

TEST(DistributedQueryTcpExecutionTest, RotatesBoundedNodeAddressesAcrossFiniteRetries) {
  TemporaryDirectory directory;
  auto input = make_input(directory);
  ASSERT_TRUE(input.has_value()) << input.error().to_string();
  auto execution = DistributedQueryExecution::create(
      1U, std::move(input->plan), std::move(input->admissions), std::move(input->snapshot),
      {.coordinator = {},
       .retry = {.maximum_attempts = 2U,
                 .initial_backoff = std::chrono::milliseconds{1},
                 .maximum_backoff = std::chrono::milliseconds{1}}});
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();

  auto refused_listener = network::TcpListener::bind();
  ASSERT_TRUE(refused_listener.has_value());
  const network::Ipv4Endpoint refused_endpoint = refused_listener->bound_endpoint();
  ASSERT_TRUE(refused_listener->close().is_ok());

  ExecutionNodeAuthorizer authorizer;
  ExecutionWorker first_worker{2.5, false};
  ExecutionWorker second_worker{3.5, false};
  auto first_receiver = DistributedQueryReceiver::create(
      {.local_node_id = 11U, .authorizer = &authorizer, .worker = &first_worker});
  auto second_receiver = DistributedQueryReceiver::create(
      {.local_node_id = 12U, .authorizer = &authorizer, .worker = &second_worker});
  ASSERT_TRUE(first_receiver.has_value());
  ASSERT_TRUE(second_receiver.has_value());
  ExecutionAuthenticator client_authenticator{91U};
  auto first_server = DistributedQueryTcpServer::start(
      execution_server_config(client_authenticator, *first_receiver));
  auto second_server = DistributedQueryTcpServer::start(
      execution_server_config(client_authenticator, *second_receiver));
  ASSERT_TRUE(first_server.has_value()) << first_server.error().to_string();
  ASSERT_TRUE(second_server.has_value()) << second_server.error().to_string();

  auto tls_context = network::TlsClientContext::create(execution_tls_client_config());
  ASSERT_TRUE(tls_context.has_value()) << tls_context.error().to_string();
  ExecutionAuthenticator server_authenticator{92U};
  auto scheduled = DistributedQueryTcpExecution::create(
      std::move(*execution),
      {.authenticator = &server_authenticator,
       .node_authorizer = &authorizer,
       .routes = {{11U, {refused_endpoint, first_server->bound_endpoint()}, &*tls_context},
                  {12U, {second_server->bound_endpoint()}, &*tls_context}},
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000}},
       .connect_timeout = std::chrono::milliseconds{1000}});
  ASSERT_TRUE(scheduled.has_value()) << scheduled.error().to_string();

  for (std::size_t iteration = 0U;
       iteration < 4096U && scheduled->state() == DistributedQueryTcpExecutionState::kRunning;
       ++iteration) {
    const common::Status status = scheduled->poll_once(std::chrono::milliseconds{1});
    ASSERT_TRUE(status.is_ok()) << status.to_string();
    ASSERT_TRUE(first_server->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(second_server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(scheduled->state(), DistributedQueryTcpExecutionState::kComplete)
      << scheduled->failure().to_string();
  auto result = scheduled->result();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_EQ(result->count, 2U);
  EXPECT_EQ(result->sum, 6.0);
  EXPECT_EQ(first_worker.calls, 1U);
  EXPECT_EQ(second_worker.calls, 1U);
  const auto metrics = scheduled->metrics();
  EXPECT_EQ(metrics.attempts_started, 3U);
  EXPECT_EQ(metrics.retries_started, 1U);
  EXPECT_EQ(metrics.transport_completed_attempts, 2U);
  EXPECT_EQ(metrics.transport_failed_attempts, 1U);
}

TEST(DistributedQueryTcpExecutionTest, RejectsIncompleteRoutesBeforeOpeningAttempts) {
  TemporaryDirectory directory;
  auto input = make_input(directory);
  ASSERT_TRUE(input.has_value()) << input.error().to_string();
  auto execution = DistributedQueryExecution::create(
      1U, std::move(input->plan), std::move(input->admissions), std::move(input->snapshot));
  ASSERT_TRUE(execution.has_value());
  ExecutionNodeAuthorizer authorizer;
  ExecutionAuthenticator authenticator{92U};
  auto tls_context = network::TlsClientContext::create(execution_tls_client_config());
  ASSERT_TRUE(tls_context.has_value());
  EXPECT_EQ(
      DistributedQueryTcpExecution::create(
          std::move(*execution), {.authenticator = &authenticator,
                                  .node_authorizer = &authorizer,
                                  .routes = {{11U, {{{127U, 0U, 0U, 1U}, 1U}}, &*tls_context}}})
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);
}

TEST(DistributedQueryTcpExecutionTest, DeadlineAndCancellationReleaseEveryAttempt) {
  ExecutionNodeAuthorizer authorizer;
  ExecutionAuthenticator authenticator{92U};
  auto tls_context = network::TlsClientContext::create(execution_tls_client_config());
  ASSERT_TRUE(tls_context.has_value());

  TemporaryDirectory expired_directory;
  auto expired_input = make_input(expired_directory);
  ASSERT_TRUE(expired_input.has_value());
  auto expired_execution = DistributedQueryExecution::create(1U, std::move(expired_input->plan),
                                                             std::move(expired_input->admissions),
                                                             std::move(expired_input->snapshot));
  ASSERT_TRUE(expired_execution.has_value());
  auto expired = DistributedQueryTcpExecution::create(
      std::move(*expired_execution),
      {.authenticator = &authenticator,
       .node_authorizer = &authorizer,
       .routes = {{11U, {{{127U, 0U, 0U, 1U}, 1U}}, &*tls_context},
                  {12U, {{{127U, 0U, 0U, 1U}, 2U}}, &*tls_context}},
       .execution_deadline = DistributedQueryExecution::TimePoint{}});
  ASSERT_TRUE(expired.has_value());
  const common::Status deadline = expired->poll_once(std::chrono::milliseconds{100});
  EXPECT_EQ(deadline.code(), common::StatusCode::kCancelled);
  EXPECT_EQ(expired->state(), DistributedQueryTcpExecutionState::kCancelled);
  EXPECT_EQ(expired->metrics().attempts_started, 0U);
  EXPECT_EQ(expired->result().error(), deadline);
  EXPECT_EQ(expired->poll_once(std::chrono::milliseconds{0}), deadline);

  TemporaryDirectory cancelled_directory;
  auto cancelled_input = make_input(cancelled_directory);
  ASSERT_TRUE(cancelled_input.has_value());
  auto cancelled_execution = DistributedQueryExecution::create(
      1U, std::move(cancelled_input->plan), std::move(cancelled_input->admissions),
      std::move(cancelled_input->snapshot));
  ASSERT_TRUE(cancelled_execution.has_value());
  auto listener = network::TcpListener::bind();
  ASSERT_TRUE(listener.has_value()) << listener.error().to_string();
  auto cancelled = DistributedQueryTcpExecution::create(
      std::move(*cancelled_execution),
      {.authenticator = &authenticator,
       .node_authorizer = &authorizer,
       .routes = {{11U, {listener->bound_endpoint()}, &*tls_context},
                  {12U, {listener->bound_endpoint()}, &*tls_context}}});
  ASSERT_TRUE(cancelled.has_value());
  ASSERT_TRUE(cancelled->poll_once(std::chrono::milliseconds{0}).is_ok());
  EXPECT_EQ(cancelled->metrics().attempts_started, 2U);
  EXPECT_EQ(cancelled->metrics().active_attempts, 2U);
  const common::Status cancellation = cancelled->cancel();
  EXPECT_EQ(cancellation.code(), common::StatusCode::kCancelled);
  EXPECT_EQ(cancelled->state(), DistributedQueryTcpExecutionState::kCancelled);
  EXPECT_EQ(cancelled->metrics().active_attempts, 0U);
  EXPECT_EQ(cancelled->cancel(), cancellation);
  EXPECT_EQ(cancelled->poll_once(std::chrono::milliseconds{0}), cancellation);
  EXPECT_EQ(cancelled->result().error(), cancellation);
}

TEST(DistributedQueryTcpExecutionTest, RebindsWholeQueryAndDiscardsPriorEpochPartials) {
  ExecutionNodeAuthorizer authorizer;
  ExecutionAuthenticator client_authenticator{91U};
  ExecutionAuthenticator server_authenticator{92U};
  auto tls_context = network::TlsClientContext::create(execution_tls_client_config());
  ASSERT_TRUE(tls_context.has_value());

  ExecutionWorker old_first_worker{100.0, false};
  ExecutionWorker old_second_worker{200.0, true};
  ExecutionLeaderHintProvider leader_hint_provider;
  auto old_first_receiver = DistributedQueryReceiver::create(
      {.local_node_id = 11U, .authorizer = &authorizer, .worker = &old_first_worker});
  auto old_second_receiver =
      DistributedQueryReceiver::create({.local_node_id = 12U,
                                        .authorizer = &authorizer,
                                        .worker = &old_second_worker,
                                        .leader_hint_provider = &leader_hint_provider});
  ASSERT_TRUE(old_first_receiver.has_value());
  ASSERT_TRUE(old_second_receiver.has_value());
  auto old_first_server = DistributedQueryTcpServer::start(
      execution_server_config(client_authenticator, *old_first_receiver));
  auto old_second_server = DistributedQueryTcpServer::start(
      execution_server_config(client_authenticator, *old_second_receiver));
  ASSERT_TRUE(old_first_server.has_value());
  ASSERT_TRUE(old_second_server.has_value());

  TemporaryDirectory old_directory;
  auto old_input = make_input(old_directory);
  ASSERT_TRUE(old_input.has_value());
  auto old_execution = DistributedQueryExecution::create(
      1U, std::move(old_input->plan), std::move(old_input->admissions),
      std::move(old_input->snapshot),
      {.coordinator = {},
       .retry = {.maximum_attempts = 1U,
                 .initial_backoff = std::chrono::milliseconds{1},
                 .maximum_backoff = std::chrono::milliseconds{1}}});
  ASSERT_TRUE(old_execution.has_value());
  auto scheduled = DistributedQueryTcpExecution::create(
      std::move(*old_execution),
      {.authenticator = &server_authenticator,
       .node_authorizer = &authorizer,
       .routes = {{11U, {old_first_server->bound_endpoint()}, &*tls_context},
                  {12U, {old_second_server->bound_endpoint()}, &*tls_context}},
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000}},
       .connect_timeout = std::chrono::milliseconds{1000},
       .execution_deadline = std::nullopt,
       .maximum_rebindings = 1U});
  ASSERT_TRUE(scheduled.has_value());

  for (std::size_t iteration = 0U;
       iteration < 1024U && scheduled->metrics().transport_completed_attempts == 0U; ++iteration) {
    ASSERT_TRUE(scheduled->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(old_first_server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(scheduled->metrics().transport_completed_attempts, 1U);
  ASSERT_EQ(old_first_worker.calls, 1U);
  for (std::size_t iteration = 0U;
       iteration < 1024U && scheduled->state() == DistributedQueryTcpExecutionState::kRunning;
       ++iteration) {
    ASSERT_TRUE(old_second_server->poll_once(std::chrono::milliseconds{1}).is_ok());
    const common::Status status = scheduled->poll_once(std::chrono::milliseconds{1});
    ASSERT_TRUE(status.is_ok() || status.code() == common::StatusCode::kUnavailable);
  }
  ASSERT_EQ(scheduled->state(), DistributedQueryTcpExecutionState::kFailed);
  EXPECT_EQ(scheduled->failure().code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(leader_hint_provider.calls, 1U);
  const schema::TabletId hinted_tablet = scheduled->snapshot().dispatches()[1].fragment.tablet_id;
  EXPECT_EQ(*scheduled->suggested_leader(hinted_tablet), DistributedQueryLeaderHint(13U, 14U));

  TemporaryDirectory wrong_directory;
  auto wrong_input = make_input(wrong_directory, 99U);
  ASSERT_TRUE(wrong_input.has_value());
  auto wrong_execution = DistributedQueryExecution::create(1U, std::move(wrong_input->plan),
                                                           std::move(wrong_input->admissions),
                                                           std::move(wrong_input->snapshot));
  ASSERT_TRUE(wrong_execution.has_value());
  EXPECT_EQ(scheduled
                ->rebind(std::move(*wrong_execution),
                         {.authenticator = &server_authenticator,
                          .node_authorizer = &authorizer,
                          .routes = {{11U, {old_first_server->bound_endpoint()}, &*tls_context},
                                     {12U, {old_second_server->bound_endpoint()}, &*tls_context}},
                          .maximum_rebindings = 1U})
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(scheduled->state(), DistributedQueryTcpExecutionState::kFailed);
  EXPECT_EQ(scheduled->metrics().rebindings_started, 0U);

  ExecutionWorker new_first_worker{2.5, false};
  ExecutionWorker new_second_worker{3.5, false};
  auto new_first_receiver = DistributedQueryReceiver::create(
      {.local_node_id = 11U, .authorizer = &authorizer, .worker = &new_first_worker});
  auto new_second_receiver = DistributedQueryReceiver::create(
      {.local_node_id = 12U, .authorizer = &authorizer, .worker = &new_second_worker});
  ASSERT_TRUE(new_first_receiver.has_value());
  ASSERT_TRUE(new_second_receiver.has_value());
  auto new_first_server = DistributedQueryTcpServer::start(
      execution_server_config(client_authenticator, *new_first_receiver));
  auto new_second_server = DistributedQueryTcpServer::start(
      execution_server_config(client_authenticator, *new_second_receiver));
  ASSERT_TRUE(new_first_server.has_value());
  ASSERT_TRUE(new_second_server.has_value());
  TemporaryDirectory new_directory;
  auto new_input = make_input(new_directory);
  ASSERT_TRUE(new_input.has_value());
  auto new_execution = DistributedQueryExecution::create(1U, std::move(new_input->plan),
                                                         std::move(new_input->admissions),
                                                         std::move(new_input->snapshot));
  ASSERT_TRUE(new_execution.has_value());
  ASSERT_TRUE(scheduled
                  ->rebind(std::move(*new_execution),
                           {.authenticator = &server_authenticator,
                            .node_authorizer = &authorizer,
                            .routes = {{11U, {new_first_server->bound_endpoint()}, &*tls_context},
                                       {12U, {new_second_server->bound_endpoint()}, &*tls_context}},
                            .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                                               .exchange_timeout = std::chrono::milliseconds{1000}},
                            .connect_timeout = std::chrono::milliseconds{1000},
                            .maximum_rebindings = 1U})
                  .is_ok());
  EXPECT_EQ(scheduled->state(), DistributedQueryTcpExecutionState::kRunning);
  EXPECT_EQ(scheduled->metrics().rebindings_started, 1U);

  for (std::size_t iteration = 0U;
       iteration < 2048U && scheduled->state() == DistributedQueryTcpExecutionState::kRunning;
       ++iteration) {
    ASSERT_TRUE(scheduled->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(new_first_server->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(new_second_server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(scheduled->state(), DistributedQueryTcpExecutionState::kComplete)
      << scheduled->failure().to_string();
  auto result = scheduled->result();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_EQ(result->count, 2U);
  EXPECT_EQ(result->sum, 6.0);
  EXPECT_EQ(scheduled->metrics().attempts_started, 4U);
  EXPECT_EQ(scheduled->metrics().transport_completed_attempts, 4U);
  EXPECT_EQ(scheduled->metrics().rebindings_started, 1U);
}

TEST(DistributedQueryMovementGateTest, QueryResultIsStableAcrossCompletedTabletMovement) {
  ExecutionNodeAuthorizer authorizer;
  ExecutionAuthenticator client_authenticator{91U};
  ExecutionAuthenticator server_authenticator{92U};
  auto tls_context = network::TlsClientContext::create(execution_tls_client_config());
  ASSERT_TRUE(tls_context.has_value());

  ExecutionWorker source_worker{2.5, false};
  ExecutionWorker stable_worker{3.5, false};
  ExecutionWorker target_worker{2.5, false};
  auto source_receiver = DistributedQueryReceiver::create(
      {.local_node_id = 11U, .authorizer = &authorizer, .worker = &source_worker});
  auto stable_receiver = DistributedQueryReceiver::create(
      {.local_node_id = 12U, .authorizer = &authorizer, .worker = &stable_worker});
  auto target_receiver = DistributedQueryReceiver::create(
      {.local_node_id = 13U, .authorizer = &authorizer, .worker = &target_worker});
  ASSERT_TRUE(source_receiver.has_value());
  ASSERT_TRUE(stable_receiver.has_value());
  ASSERT_TRUE(target_receiver.has_value());
  auto source_server = DistributedQueryTcpServer::start(
      execution_server_config(client_authenticator, *source_receiver));
  auto stable_server = DistributedQueryTcpServer::start(
      execution_server_config(client_authenticator, *stable_receiver));
  auto target_server = DistributedQueryTcpServer::start(
      execution_server_config(client_authenticator, *target_receiver));
  ASSERT_TRUE(source_server.has_value());
  ASSERT_TRUE(stable_server.has_value());
  ASSERT_TRUE(target_server.has_value());

  TemporaryDirectory before_directory;
  auto before_input = make_input(before_directory);
  ASSERT_TRUE(before_input.has_value());
  const schema::TabletId moved_tablet = before_input->plan.fragments[0].tablet_id;
  auto before_execution = DistributedQueryExecution::create(1U, std::move(before_input->plan),
                                                            std::move(before_input->admissions),
                                                            std::move(before_input->snapshot));
  ASSERT_TRUE(before_execution.has_value());
  auto before = DistributedQueryTcpExecution::create(
      std::move(*before_execution),
      {.authenticator = &server_authenticator,
       .node_authorizer = &authorizer,
       .routes = {{11U, {source_server->bound_endpoint()}, &*tls_context},
                  {12U, {stable_server->bound_endpoint()}, &*tls_context}}});
  ASSERT_TRUE(before.has_value());
  for (std::size_t iteration = 0U;
       iteration < 2048U && before->state() == DistributedQueryTcpExecutionState::kRunning;
       ++iteration) {
    ASSERT_TRUE(before->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(source_server->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(stable_server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(before->state(), DistributedQueryTcpExecutionState::kComplete);
  const auto before_result = before->result();
  ASSERT_TRUE(before_result.has_value());

  const std::vector<std::byte> snapshot{std::byte{1U}, std::byte{2U}, std::byte{3U}, std::byte{4U}};
  auto movement = raft::TabletMovement::begin(moved_tablet, 12U, 11U, 13U, {11U, 15U});
  ASSERT_TRUE(movement.has_value());
  ASSERT_TRUE(
      movement->begin_snapshot({1U, 10U, 2U, snapshot.size(), common::crc32c(snapshot)}).is_ok());
  const common::ByteView first_half{snapshot.data(), 2U};
  const common::ByteView second_half{snapshot.data() + 2U, 2U};
  ASSERT_TRUE(movement->accept_snapshot_chunk(0U, first_half, common::crc32c(first_half)).is_ok());
  ASSERT_TRUE(
      movement->accept_snapshot_chunk(2U, second_half, common::crc32c(second_half)).is_ok());
  ASSERT_TRUE(movement->finish_snapshot().is_ok());
  ASSERT_TRUE(movement->mark_caught_up(10U).is_ok());
  ASSERT_TRUE(movement->promote_target(12U, 13U).is_ok());
  ASSERT_TRUE(movement->remove_source(13U, 14U).is_ok());
  ASSERT_EQ(movement->record().phase, raft::TabletMovementPhase::kComplete);
  EXPECT_EQ(movement->record().voting_replicas, (std::vector<raft::NodeId>{13U, 15U}));

  TemporaryDirectory after_directory;
  auto after_input = make_input(after_directory, 7U, {13U, 12U}, {14U, 13U});
  ASSERT_TRUE(after_input.has_value());
  auto after_execution = DistributedQueryExecution::create(1U, std::move(after_input->plan),
                                                           std::move(after_input->admissions),
                                                           std::move(after_input->snapshot));
  ASSERT_TRUE(after_execution.has_value());
  auto after = DistributedQueryTcpExecution::create(
      std::move(*after_execution),
      {.authenticator = &server_authenticator,
       .node_authorizer = &authorizer,
       .routes = {{13U, {target_server->bound_endpoint()}, &*tls_context},
                  {12U, {stable_server->bound_endpoint()}, &*tls_context}}});
  ASSERT_TRUE(after.has_value());
  for (std::size_t iteration = 0U;
       iteration < 2048U && after->state() == DistributedQueryTcpExecutionState::kRunning;
       ++iteration) {
    ASSERT_TRUE(after->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(target_server->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(stable_server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(after->state(), DistributedQueryTcpExecutionState::kComplete);
  const auto after_result = after->result();
  ASSERT_TRUE(after_result.has_value());
  EXPECT_EQ(after_result->count, before_result->count);
  EXPECT_EQ(after_result->sum, before_result->sum);
  EXPECT_EQ(after_result->minimum, before_result->minimum);
  EXPECT_EQ(after_result->maximum, before_result->maximum);
  EXPECT_EQ(after_result->mean, before_result->mean);
  EXPECT_EQ(after_result->m2, before_result->m2);
  EXPECT_EQ(source_worker.calls, 1U);
  EXPECT_EQ(target_worker.calls, 1U);
  EXPECT_EQ(stable_worker.calls, 2U);
}

TEST(DistributedQueryExecutionTest, RejectsAdmissionOrderThatDiffersFromPinnedDispatches) {
  TemporaryDirectory directory;
  auto input = make_input(directory);
  ASSERT_TRUE(input.has_value()) << input.error().to_string();
  std::swap(input->admissions[0], input->admissions[1]);
  EXPECT_EQ(DistributedQueryExecution::create(1U, std::move(input->plan),
                                              std::move(input->admissions),
                                              std::move(input->snapshot))
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::cluster
