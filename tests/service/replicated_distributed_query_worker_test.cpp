#include "chronos/cluster/distributed_grouped_query_tcp_client.hpp"
#include "chronos/cluster/distributed_query_tcp_client.hpp"
#include "chronos/cluster/distributed_vector_aggregate_query_tcp_client_v2.hpp"
#include "chronos/cluster/distributed_vector_query_tcp_client_v2.hpp"
#include "chronos/cluster/raft_observation_tcp_server.hpp"
#include "chronos/common/byte_reader.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/manifest/temporal_part_validation.hpp"
#include "chronos/manifest/temporal_validation.hpp"
#include "chronos/raft/rebalancing.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/service/replicated_distributed_grouped_query_receiver.hpp"
#include "chronos/service/replicated_distributed_grouped_query_tcp_server.hpp"
#include "chronos/service/replicated_distributed_query.hpp"
#include "chronos/service/replicated_distributed_query_tcp_server.hpp"
#include "chronos/service/replicated_distributed_query_worker.hpp"
#include "chronos/service/replicated_distributed_vector_aggregate_query_tcp_server_v2.hpp"
#include "chronos/service/replicated_distributed_vector_query_tcp_server_v2.hpp"
#include "cseg/cseg_test_fixture.hpp"

#include <array>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <poll.h>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::service {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

template <typename Id> [[nodiscard]] Id id(const std::uint8_t seed) {
  return Id::from_uuid(uuid(seed)).value();
}

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-query-worker-service-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  TemporaryDirectory(const TemporaryDirectory&) = delete;
  TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

void write_file(const std::filesystem::path& path, const common::ByteView bytes) {
  std::ofstream output{path, std::ios::binary};
  ASSERT_TRUE(output.good());
  for (const std::byte value : bytes)
    output.put(static_cast<char>(std::to_integer<unsigned char>(value)));
  output.close();
  ASSERT_TRUE(output.good());
}

[[nodiscard]] std::filesystem::path tls_fixture(const char* name) {
  return std::filesystem::path{CHRONOS_NETWORK_FIXTURE_DIR} / "tls" / name;
}

[[nodiscard]] network::TlsServerConfig tls_server_config() {
  return {.certificate_chain_file = tls_fixture("server.pem").string(),
          .private_key_file = tls_fixture("server-key.pem").string(),
          .trust_store_file = tls_fixture("ca.pem").string()};
}

[[nodiscard]] network::TlsClientConfig tls_client_config() {
  return {.certificate_chain_file = tls_fixture("client.pem").string(),
          .private_key_file = tls_fixture("client-key.pem").string(),
          .trust_store_file = tls_fixture("ca.pem").string(),
          .expected_server_identity = "127.0.0.1"};
}

class Authenticator final : public network::ConnectionAuthenticator {
public:
  explicit Authenticator(const std::uint64_t principal_id) : principal_id_(principal_id) {}

  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest& request) override {
    saw_fingerprint = request.peer_certificate_sha256.has_value();
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = principal_id_};
  }

  bool saw_fingerprint{};

private:
  std::uint64_t principal_id_{};
};

class NodeAuthorizer final : public cluster::ClusterNodePrincipalAuthorizer {
public:
  // NOLINTNEXTLINE(readability-convert-member-functions-to-static)
  common::Result<bool> authorize_node(const std::uint64_t principal_id,
                                      const raft::NodeId node_id) const override {
    return common::Result<bool>{(principal_id == 91U && node_id == 1U) ||
                                (principal_id == 92U && (node_id == 11U || node_id == 12U ||
                                                         node_id == 13U || node_id == 14U))};
  }
};

class LifecycleObservationService final : public cluster::RaftObservationService {
public:
  struct Config {
    raft::NodeId node_id{};
    raft::Role role{raft::Role::kFollower};
    raft::NodeId leader_id{};
    raft::LogIndex position{};
    std::array<raft::NodeId, 2U> voters{11U, 13U};
  };

  explicit LifecycleObservationService(const Config config)
      : node_id_(config.node_id), role_(config.role), leader_id_(config.leader_id),
        position_(config.position), voters_(config.voters) {}

  common::Result<raft::RaftGroupObservation> observe(const raft::GroupId& group_id) override {
    ++calls;
    return raft::RaftGroupObservation{.group_id = group_id,
                                      .node_id = node_id_,
                                      .role = role_,
                                      .current_term = 3U,
                                      .leader_id = leader_id_,
                                      .last_log_index = position_,
                                      .commit_index = position_,
                                      .applied_index = position_,
                                      .voters = {voters_[0], voters_[1]},
                                      .committed_voters = {voters_[0], voters_[1]}};
  }

  std::size_t calls{};

private:
  raft::NodeId node_id_{};
  raft::Role role_{raft::Role::kFollower};
  raft::NodeId leader_id_{};
  raft::LogIndex position_{};
  std::array<raft::NodeId, 2U> voters_{};
};

[[nodiscard]] std::string endpoint_text(const network::Ipv4Endpoint& endpoint) {
  return std::to_string(endpoint.address[0]) + "." + std::to_string(endpoint.address[1]) + "." +
         std::to_string(endpoint.address[2]) + "." + std::to_string(endpoint.address[3]) + ":" +
         std::to_string(endpoint.port);
}

[[nodiscard]] std::shared_ptr<const schema::SchemaLineage> make_lineage() {
  const schema::ColumnId event_id = id<schema::ColumnId>(5U);
  const schema::ColumnId value_id = id<schema::ColumnId>(6U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(
                        event_id, "event_time",
                        schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(),
                        false)
                        .value());
  columns.push_back(schema::ColumnDefinition::create(
                        value_id, "value",
                        schema::LogicalType::create(schema::LogicalTypeKind::kFloat64).value(),
                        false)
                        .value());
  auto schema_value = schema::TableSchema::create(id<schema::TableId>(2U), id<schema::SchemaId>(4U),
                                                  schema::SchemaVersion::initial(), std::nullopt,
                                                  std::move(columns),
                                                  {.event_time_column = event_id,
                                                   .physical_ordering_key = {event_id},
                                                   .partition_columns = {event_id},
                                                   .shard_key = {event_id},
                                                   .deduplication_key = {event_id}});
  return std::make_shared<const schema::SchemaLineage>(
      schema::SchemaLineage::create(std::move(*schema_value)).value());
}

class ContextProvider final : public ReplicatedDistributedQueryWorkerContextProvider,
                              public ReplicatedDistributedGroupedQueryWorkerContextProvider,
                              public ReplicatedDistributedVectorQueryWorkerContextProviderV2 {
public:
  ContextProvider(manifest::TemporalDatabaseStorageSnapshot snapshot,
                  std::shared_ptr<const schema::SchemaLineage> lineage,
                  raft::TabletPlacementMetadata placement, raft::GroupId raft_group_id,
                  std::optional<raft::ReadBarrier> barrier)
      : snapshot_(std::move(snapshot)), lineage_(std::move(lineage)),
        placement_(std::move(placement)), raft_group_id_(raft_group_id), barrier_(barrier) {}

  common::Result<ReplicatedDistributedQueryWorkerContext>
  acquire(const query::DistributedAggregateFragmentDispatch&) override {
    ++calls;
    return ReplicatedDistributedQueryWorkerContext{.snapshot = snapshot_,
                                                   .lineage = lineage_,
                                                   .placement = placement_,
                                                   .raft_group_id = raft_group_id_,
                                                   .local_linearizable_barrier = barrier_};
  }

  common::Result<ReplicatedDistributedQueryWorkerContext>
  acquire(const query::DistributedGroupedFloat64FragmentDispatch&) override {
    ++grouped_calls;
    return ReplicatedDistributedQueryWorkerContext{.snapshot = snapshot_,
                                                   .lineage = lineage_,
                                                   .placement = placement_,
                                                   .raft_group_id = raft_group_id_,
                                                   .local_linearizable_barrier = barrier_};
  }

  common::Result<ReplicatedDistributedQueryWorkerContext>
  acquire(const query::DistributedVectorFragmentDispatchV2&) override {
    ++vector_calls;
    return ReplicatedDistributedQueryWorkerContext{.snapshot = snapshot_,
                                                   .lineage = lineage_,
                                                   .placement = placement_,
                                                   .raft_group_id = raft_group_id_,
                                                   .local_linearizable_barrier = barrier_};
  }

  void set_placement_epoch(const std::uint64_t epoch) noexcept {
    placement_.placement_epoch = epoch;
  }

  void set_raft_group_id(const raft::GroupId& group_id) noexcept {
    raft_group_id_ = group_id;
  }

  void set_authority(raft::TabletPlacementMetadata placement,
                     std::optional<raft::ReadBarrier> barrier) {
    placement_ = std::move(placement);
    barrier_ = barrier;
  }

  void clear_lineage() noexcept {
    lineage_.reset();
  }

  std::size_t calls{};
  std::size_t grouped_calls{};
  std::size_t vector_calls{};

private:
  manifest::TemporalDatabaseStorageSnapshot snapshot_;
  std::shared_ptr<const schema::SchemaLineage> lineage_;
  raft::TabletPlacementMetadata placement_;
  raft::GroupId raft_group_id_;
  std::optional<raft::ReadBarrier> barrier_;
};

TEST(ReplicatedDistributedQueryWorkerTest, AcquiresFreshAuthorityAndExecutesRealCseg) {
  EXPECT_EQ(ReplicatedDistributedQueryWorker::create({}).error().code(),
            common::StatusCode::kInvalidArgument);

  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  ASSERT_TRUE(std::filesystem::create_directory(directory.path() / manifest::kPartsDirectoryName));
  ASSERT_TRUE(
      std::filesystem::create_directory(directory.path() / manifest::kManifestDirectoryName));
  write_file(directory.path() / manifest::kManifestDirectoryName / manifest::kManifestLockFileName,
             {});

  const auto lineage = make_lineage();
  const auto schema_value = lineage->current();
  ASSERT_NE(schema_value, nullptr);
  const manifest::DatabaseId database_id = id<manifest::DatabaseId>(1U);
  const schema::TabletId tablet_id = id<schema::TabletId>(3U);
  const common::Uuid group_id = uuid(8U);
  const cseg::EncodedCsegPart encoded_part = cseg::test::make_valid_temporal_float64_part(
      cseg::PageCompression::kNone,
      {.commit_source = cseg::temporal_format::CommitSource::kRaft, .source_id = group_id});
  auto part = manifest::describe_manifest_v2_temporal_part_image(
      encoded_part.bytes(), *schema_value, tablet_id, manifest::ManifestCommitSource::kRaft,
      group_id);
  ASSERT_TRUE(part.has_value()) << part.error().to_string();
  write_file(directory.path() / manifest::kPartsDirectoryName /
                 manifest::part_file_name(part->part_id),
             encoded_part.bytes());
  const std::array tablets{
      manifest::TemporalTabletDescriptor{.table_id = schema_value->table_id(),
                                         .tablet_id = tablet_id,
                                         .recovery_schema_id = schema_value->schema_id(),
                                         .recovery_schema_version = schema_value->version(),
                                         .source_id = group_id,
                                         .durable_position = 10U,
                                         .reclaim_position = 0U,
                                         .first_part_index = 0U,
                                         .part_count = 1U,
                                         .durable_version_count = 2U,
                                         .commit_source = manifest::ManifestCommitSource::kRaft}};
  const std::array parts{*part};
  auto encoded_manifest =
      manifest::encode_manifest_v2_temporal({.generation = 1U,
                                             .database_id = database_id,
                                             .wal_reclaim_checkpoint = std::nullopt,
                                             .tablets = tablets,
                                             .parts = parts,
                                             .retries = {}});
  ASSERT_TRUE(encoded_manifest.has_value()) << encoded_manifest.error().to_string();
  write_file(directory.path() / manifest::kManifestDirectoryName /
                 *manifest::manifest_file_name(1U),
             encoded_manifest->bytes());
  auto storage =
      manifest::ManifestStorage::open_existing({.database_root = directory.path().string()});
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  const std::array schema_bindings{
      manifest::TabletSchemaBinding{.tablet_id = tablet_id, .lineage = std::cref(*lineage)}};
  const std::array source_bindings{
      manifest::TemporalTabletSourceBinding{.tablet_id = tablet_id,
                                            .commit_source = manifest::ManifestCommitSource::kRaft,
                                            .source_id = group_id}};
  auto loaded = storage->load_selected_temporal_manifest({.expected_database_id = database_id,
                                                          .schema_bindings = schema_bindings,
                                                          .source_bindings = source_bindings,
                                                          .decode_limits = {},
                                                          .part_validation_limits = {}});
  ASSERT_TRUE(loaded.has_value()) << loaded.error().to_string();
  auto selected =
      std::make_shared<const manifest::LoadedTemporalManifestGeneration>(std::move(*loaded));
  auto publisher = manifest::TemporalDatabaseStoragePublisher::create(selected, schema_bindings);
  ASSERT_TRUE(publisher.has_value()) << publisher.error().to_string();
  auto snapshot = publisher->snapshot();
  ASSERT_TRUE(snapshot.has_value()) << snapshot.error().to_string();

  const query::DistributedAggregateFragmentDispatch dispatch{
      .raft_group_id = group_id,
      .fragment = {
          .query_id = uuid(7U),
          .database_id = database_id,
          .table_id = schema_value->table_id(),
          .tablet_id = tablet_id,
          .destination_schema_id = schema_value->schema_id(),
          .snapshot_generation = 1U,
          .serving_node = 11U,
          .applied_position = 10U,
          .observed_leader_commit_position = 10U,
          .placement_epoch = 12U,
          .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable,
                          .maximum_staleness_positions = std::nullopt},
          .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
          .destination_column_ordinals = {0U, 1U},
          .aggregate_input_index = 1U,
          .event_time_predicate = cseg::EventTimePredicate{.lower = cseg::EventTimeBound{15, true},
                                                           .upper = std::nullopt}}};
  ContextProvider provider{std::move(*snapshot),
                           lineage,
                           {.table_id = schema_value->table_id(),
                            .tablet_id = tablet_id,
                            .placement_epoch = 12U,
                            .replicas = {11U, 12U},
                            .leader_hint = 11U},
                           group_id,
                           raft::ReadBarrier{2U, 3U, 10U}};
  auto worker = ReplicatedDistributedQueryWorker::create(
      {.local_node_id = 11U, .storage = &*storage, .context_provider = &provider});
  ASSERT_TRUE(worker.has_value()) << worker.error().to_string();

  auto result = worker->execute(dispatch);
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_EQ(result->query_id, dispatch.fragment.query_id);
  EXPECT_EQ(result->tablet_id, tablet_id);
  EXPECT_EQ(result->partial.count, 1U);
  EXPECT_EQ(result->partial.sum, 2.5);
  EXPECT_TRUE(result->terminal);
  EXPECT_EQ(provider.calls, 1U);

  EXPECT_EQ(ReplicatedDistributedGroupedQueryWorker::create({}).error().code(),
            common::StatusCode::kInvalidArgument);
  const query::DistributedGroupedFloat64FragmentDispatch grouped_dispatch{
      .raft_group_id = dispatch.raft_group_id,
      .fragment = {.aggregate = dispatch.fragment, .group_key_input_index = 1U}};
  auto grouped_worker = ReplicatedDistributedGroupedQueryWorker::create(
      {.local_node_id = 11U, .storage = &*storage, .context_provider = &provider});
  ASSERT_TRUE(grouped_worker.has_value()) << grouped_worker.error().to_string();
  const auto grouped_result = grouped_worker->execute(grouped_dispatch);
  ASSERT_TRUE(grouped_result.has_value()) << grouped_result.error().to_string();
  const auto* grouped_messages =
      std::get_if<std::vector<query::GroupedFloat64ExchangeMessage>>(&*grouped_result);
  ASSERT_NE(grouped_messages, nullptr);
  ASSERT_EQ(grouped_messages->size(), 1U);
  EXPECT_EQ(grouped_messages->front().group_key, 2.5);
  EXPECT_EQ(grouped_messages->front().partial.sum, 2.5);
  EXPECT_TRUE(grouped_messages->front().terminal);
  EXPECT_EQ(provider.grouped_calls, 1U);

  EXPECT_EQ(ReplicatedDistributedVectorQueryWorkerV2::create({}).error().code(),
            common::StatusCode::kInvalidArgument);
  const query::DistributedVectorFragmentDispatchV2 vector_dispatch{
      .dispatch = {.query_id = dispatch.fragment.query_id,
                   .database_id = dispatch.fragment.database_id,
                   .table_id = dispatch.fragment.table_id,
                   .tablet_id = dispatch.fragment.tablet_id,
                   .destination_schema_id = dispatch.fragment.destination_schema_id,
                   .raft_group_id = dispatch.raft_group_id,
                   .snapshot_generation = dispatch.fragment.snapshot_generation,
                   .serving_node = dispatch.fragment.serving_node,
                   .applied_position = dispatch.fragment.applied_position,
                   .observed_leader_commit_position =
                       dispatch.fragment.observed_leader_commit_position,
                   .placement_epoch = dispatch.fragment.placement_epoch,
                   .read_policy = dispatch.fragment.read_policy,
                   .linearizable_barrier = dispatch.fragment.linearizable_barrier,
                   .destination_column_ordinals = {0U, 1U},
                   .plan = {.mode = query::DistributedVectorPlanMode::kRows,
                            .row_output_indices = {1U},
                            .order_keys = {{.output_index = 0U,
                                            .direction = query::PhysicalSortDirection::kDescending,
                                            .null_placement = query::ScalarNullPlacement::kLast}},
                            .limit = 1U}},
      .result_schema = {.columns = {{"value", schema_value->columns()[1].type(), false}}}};
  auto vector_worker = ReplicatedDistributedVectorQueryWorkerV2::create(
      {.local_node_id = 11U, .storage = &*storage, .context_provider = &provider});
  ASSERT_TRUE(vector_worker.has_value()) << vector_worker.error().to_string();
  const auto vector_result = vector_worker->execute(vector_dispatch);
  ASSERT_TRUE(vector_result.has_value()) << vector_result.error().to_string();
  ASSERT_EQ(vector_result->size(), 1U);
  EXPECT_EQ(vector_result->front().query_id, vector_dispatch.dispatch.query_id);
  EXPECT_EQ(vector_result->front().tablet_id, tablet_id);
  EXPECT_EQ(vector_result->front().sequence, 1U);
  EXPECT_TRUE(vector_result->front().terminal);
  auto vector_batch =
      network::decode_query_result_batch(vector_result->front().encoded_result_batch);
  ASSERT_TRUE(vector_batch.has_value()) << vector_batch.error().to_string();
  EXPECT_EQ(vector_batch->row_count(), 2U);
  ASSERT_EQ(vector_batch->columns().size(), 1U);
  EXPECT_EQ(vector_batch->columns()[0].name, "value");
  const std::array expected_values{1.5, 2.5};
  for (std::uint32_t row = 0U; row < expected_values.size(); ++row) {
    const network::QueryResultCell* cell = vector_batch->cell(row, 0U);
    ASSERT_NE(cell, nullptr);
    ASSERT_FALSE(cell->is_null);
    ASSERT_EQ(cell->value.size(), sizeof(std::uint64_t));
    std::uint64_t bits{};
    for (std::size_t index = 0U; index < cell->value.size(); ++index) {
      bits |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(cell->value[index]))
              << (index * 8U);
    }
    EXPECT_EQ(std::bit_cast<double>(bits), expected_values[row]);
  }
  EXPECT_EQ(provider.vector_calls, 1U);

  auto aggregate_vector = vector_dispatch;
  aggregate_vector.dispatch.plan = {
      .mode = query::DistributedVectorPlanMode::kUngroupedAggregate,
      .aggregates = {{.operation = query::VectorAggregateOperation::kCountStar}}};
  EXPECT_EQ(vector_worker->execute(aggregate_vector).error().code(),
            common::StatusCode::kNotSupported);
  EXPECT_EQ(provider.vector_calls, 2U);

  EXPECT_EQ(ReplicatedDistributedVectorAggregateQueryWorkerV2::create({}).error().code(),
            common::StatusCode::kInvalidArgument);
  aggregate_vector.dispatch.plan = {
      .mode = query::DistributedVectorPlanMode::kUngroupedAggregate,
      .aggregates = {{.operation = query::VectorAggregateOperation::kCountStar},
                     {.operation = query::VectorAggregateOperation::kSum, .input_index = 1U}}};
  aggregate_vector.result_schema = {
      .columns = {{.name = "count",
                   .type = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(),
                   .nullable = false},
                  {.name = "sum", .type = schema_value->columns()[1].type(), .nullable = true}}};
  auto aggregate_vector_worker = ReplicatedDistributedVectorAggregateQueryWorkerV2::create(
      {.local_node_id = 11U, .storage = &*storage, .context_provider = &provider});
  ASSERT_TRUE(aggregate_vector_worker.has_value()) << aggregate_vector_worker.error().to_string();
  auto aggregate_definitions = aggregate_vector_worker->bind_definitions(aggregate_vector);
  ASSERT_TRUE(aggregate_definitions.has_value()) << aggregate_definitions.error().to_string();
  ASSERT_EQ(aggregate_definitions->size(), 2U);
  EXPECT_EQ((*aggregate_definitions)[0].operation, query::VectorAggregateOperation::kCountStar);
  EXPECT_FALSE((*aggregate_definitions)[0].input.has_value());
  EXPECT_EQ((*aggregate_definitions)[1].operation, query::VectorAggregateOperation::kSum);
  ASSERT_TRUE((*aggregate_definitions)[1].input.has_value());
  // Guarded by the input assertion above.
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  EXPECT_EQ(aggregate_definitions->at(1U).input->type, schema_value->columns()[1].type());
  EXPECT_EQ(provider.vector_calls, 3U);

  provider.set_placement_epoch(13U);
  EXPECT_EQ(aggregate_vector_worker->execute(aggregate_vector).error().code(),
            common::StatusCode::kUnavailable);
  EXPECT_EQ(provider.vector_calls, 4U);
  provider.set_placement_epoch(12U);
  auto aggregate_vector_result = aggregate_vector_worker->execute(aggregate_vector);
  ASSERT_TRUE(aggregate_vector_result.has_value()) << aggregate_vector_result.error().to_string();
  EXPECT_EQ(provider.vector_calls, 5U);
  EXPECT_EQ(aggregate_vector_result->definitions, *aggregate_definitions);
  ASSERT_EQ(aggregate_vector_result->messages.size(), 2U);
  EXPECT_EQ(aggregate_vector_result->input_rows, 2U);
  EXPECT_EQ(aggregate_vector_result->messages[0].sequence, 1U);
  EXPECT_FALSE(aggregate_vector_result->messages[0].terminal);
  EXPECT_EQ(aggregate_vector_result->messages[1].sequence, 2U);
  EXPECT_TRUE(aggregate_vector_result->messages[1].terminal);
  auto aggregate_count = std::move(aggregate_vector_result->messages[0].state).take_result();
  ASSERT_TRUE(aggregate_count.has_value());
  EXPECT_EQ(std::get<std::int64_t>(aggregate_count->storage()), 2);
  auto aggregate_sum = std::move(aggregate_vector_result->messages[1].state).take_result();
  ASSERT_TRUE(aggregate_sum.has_value());
  EXPECT_EQ(std::get<double>(aggregate_sum->storage()), 4.0);

  NodeAuthorizer aggregate_vector_authorizer;
  EXPECT_EQ(ReplicatedDistributedVectorAggregateQueryTcpServerV2::start({}).error().code(),
            common::StatusCode::kInvalidArgument);
  Authenticator aggregate_vector_client_authenticator{91U};
  Authenticator aggregate_vector_server_authenticator{92U};
  const std::size_t aggregate_calls_before_tcp = provider.vector_calls;
  auto aggregate_vector_server = ReplicatedDistributedVectorAggregateQueryTcpServerV2::start(
      {.worker = {.local_node_id = 11U, .storage = &*storage, .context_provider = &provider},
       .listener = {},
       .tls = tls_server_config(),
       .authenticator = &aggregate_vector_client_authenticator,
       .node_authorizer = &aggregate_vector_authorizer,
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000},
                          .maximum_response_frames = 4U,
                          .maximum_response_bytes = std::size_t{1024U} * 1024U},
       .maximum_connections = 4U,
       .maximum_accepts_per_poll = 4U});
  ASSERT_TRUE(aggregate_vector_server.has_value()) << aggregate_vector_server.error().to_string();
  auto moved_aggregate_vector_server = std::move(*aggregate_vector_server);
  auto aggregate_vector_tls_context = network::TlsClientContext::create(tls_client_config());
  auto aggregate_vector_request = cluster::encode_distributed_vector_query_request_v2(
      {.source_node_id = 1U, .target_node_id = 11U, .dispatch = aggregate_vector});
  auto aggregate_resources = query::QueryResourceContext::create(1U << 20U);
  ASSERT_TRUE(aggregate_vector_tls_context.has_value())
      << aggregate_vector_tls_context.error().to_string();
  ASSERT_TRUE(aggregate_vector_request.has_value()) << aggregate_vector_request.error().to_string();
  ASSERT_TRUE(aggregate_resources.has_value()) << aggregate_resources.error().to_string();
  auto client_definitions = *aggregate_definitions;
  const auto aggregate_vector_start =
      cluster::DistributedVectorAggregateQueryTcpClientV2::TimePoint::clock::now();
  auto aggregate_vector_client = cluster::DistributedVectorAggregateQueryTcpClientV2::begin(
      {1U, 11U, std::move(*aggregate_vector_request)}, std::move(client_definitions),
      std::move(*aggregate_resources),
      {.remote_endpoint = moved_aggregate_vector_server.bound_endpoint(),
       .tls_context = std::addressof(*aggregate_vector_tls_context),
       .carrier = {.authenticator = &aggregate_vector_server_authenticator,
                   .node_authorizer = &aggregate_vector_authorizer,
                   .peer_ipv4_address = {127U, 0U, 0U, 1U},
                   .limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                              .exchange_timeout = std::chrono::milliseconds{1000},
                              .maximum_response_frames = 4U,
                              .maximum_response_bytes = std::size_t{1024U} * 1024U}},
       .connect_timeout = std::chrono::milliseconds{1000}},
      aggregate_vector_start);
  ASSERT_TRUE(aggregate_vector_client.has_value()) << aggregate_vector_client.error().to_string();
  for (std::size_t iteration = 0U;
       iteration < 4096U && aggregate_vector_client->state() !=
                                cluster::DistributedVectorAggregateQueryTcpClientStateV2::kComplete;
       ++iteration) {
    const auto interest = aggregate_vector_client->interest();
    pollfd descriptor{.fd = aggregate_vector_client->descriptor(),
                      .events = static_cast<short>((interest.want_read ? POLLIN : 0) |
                                                   (interest.want_write ? POLLOUT : 0))};
    ASSERT_GE(::poll(&descriptor, 1U, 1), 0);
    ASSERT_TRUE(
        aggregate_vector_client
            ->on_ready((descriptor.revents & POLLIN) != 0, (descriptor.revents & POLLOUT) != 0,
                       cluster::DistributedVectorAggregateQueryTcpClientV2::TimePoint::clock::now())
            .is_ok())
        << aggregate_vector_client->failure().to_string();
    ASSERT_TRUE(moved_aggregate_vector_server.poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(aggregate_vector_client->state(),
            cluster::DistributedVectorAggregateQueryTcpClientStateV2::kComplete)
      << aggregate_vector_client->failure().to_string();
  const auto aggregate_tcp_responses = aggregate_vector_client->responses();
  ASSERT_TRUE(aggregate_tcp_responses.has_value()) << aggregate_tcp_responses.error().to_string();
  ASSERT_EQ(aggregate_tcp_responses->size(), aggregate_definitions->size());
  for (std::size_t ordinal = 0U; ordinal < aggregate_tcp_responses->size(); ++ordinal) {
    ASSERT_TRUE((*aggregate_tcp_responses)[ordinal].payload.has_value());
    EXPECT_EQ((*aggregate_tcp_responses)[ordinal].status_code, common::StatusCode::kOk);
    // Guarded by the payload assertion above.
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    const auto& payload = *(*aggregate_tcp_responses)[ordinal].payload;
    EXPECT_EQ(payload.aggregate_ordinal, ordinal);
    EXPECT_EQ(payload.state.definition(), (*aggregate_definitions)[ordinal]);
  }
  EXPECT_EQ(provider.vector_calls, aggregate_calls_before_tcp + 2U);
  EXPECT_TRUE(aggregate_vector_client_authenticator.saw_fingerprint);
  EXPECT_TRUE(aggregate_vector_server_authenticator.saw_fingerprint);
  EXPECT_EQ(moved_aggregate_vector_server.metrics().completed_connections, 1U);
  EXPECT_TRUE(moved_aggregate_vector_server.shutdown().is_ok());

  auto empty_vector = vector_dispatch;
  empty_vector.dispatch.event_time_predicate =
      cseg::EventTimePredicate{.lower = cseg::EventTimeBound{30, true}, .upper = std::nullopt};
  const auto empty_vector_result = vector_worker->execute(empty_vector);
  ASSERT_TRUE(empty_vector_result.has_value()) << empty_vector_result.error().to_string();
  ASSERT_EQ(empty_vector_result->size(), 1U);
  EXPECT_TRUE(empty_vector_result->front().terminal);
  EXPECT_TRUE(empty_vector_result->front().encoded_result_batch.empty());

  auto byte_bounded_vector_worker = ReplicatedDistributedVectorQueryWorkerV2::create(
      {.local_node_id = 11U,
       .storage = &*storage,
       .context_provider = &provider,
       .limits = {.maximum_total_encoded_bytes = 84U}});
  ASSERT_TRUE(byte_bounded_vector_worker.has_value());
  EXPECT_EQ(byte_bounded_vector_worker->execute(vector_dispatch).error().code(),
            common::StatusCode::kResourceExhausted);

  auto message_bounded_vector_worker = ReplicatedDistributedVectorQueryWorkerV2::create(
      {.local_node_id = 11U,
       .storage = &*storage,
       .context_provider = &provider,
       .limits = {.rows = {.scan = {.maximum_rows_per_chunk = 1U, .chunk = {.maximum_rows = 1U}},
                           .output = {.maximum_rows = 1U}},
                  .maximum_messages = 1U}});
  ASSERT_TRUE(message_bounded_vector_worker.has_value());
  EXPECT_EQ(message_bounded_vector_worker->execute(vector_dispatch).error().code(),
            common::StatusCode::kResourceExhausted);

  NodeAuthorizer vector_authorizer;
  EXPECT_EQ(ReplicatedDistributedVectorQueryTcpServerV2::start({}).error().code(),
            common::StatusCode::kInvalidArgument);
  Authenticator vector_tcp_client_authenticator{91U};
  Authenticator vector_tcp_server_authenticator{92U};
  const std::size_t vector_calls_before_tcp = provider.vector_calls;
  auto vector_server = ReplicatedDistributedVectorQueryTcpServerV2::start(
      {.worker = {.local_node_id = 11U, .storage = &*storage, .context_provider = &provider},
       .listener = {},
       .tls = tls_server_config(),
       .authenticator = &vector_tcp_client_authenticator,
       .node_authorizer = &vector_authorizer,
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000},
                          .maximum_response_frames = 4U,
                          .maximum_response_bytes = std::size_t{1024U} * 1024U},
       .maximum_connections = 4U,
       .maximum_accepts_per_poll = 4U});
  ASSERT_TRUE(vector_server.has_value()) << vector_server.error().to_string();
  auto moved_vector_server = std::move(*vector_server);
  auto vector_tls_context = network::TlsClientContext::create(tls_client_config());
  auto vector_request = cluster::encode_distributed_vector_query_request_v2(
      {.source_node_id = 1U, .target_node_id = 11U, .dispatch = vector_dispatch});
  ASSERT_TRUE(vector_tls_context.has_value()) << vector_tls_context.error().to_string();
  ASSERT_TRUE(vector_request.has_value()) << vector_request.error().to_string();
  const auto vector_start = cluster::DistributedVectorQueryTcpClientV2::TimePoint::clock::now();
  auto vector_client = cluster::DistributedVectorQueryTcpClientV2::begin(
      {1U, 11U, std::move(*vector_request)},
      {.remote_endpoint = moved_vector_server.bound_endpoint(),
       .tls_context = std::addressof(*vector_tls_context),
       .carrier = {.authenticator = &vector_tcp_server_authenticator,
                   .node_authorizer = &vector_authorizer,
                   .peer_ipv4_address = {127U, 0U, 0U, 1U},
                   .limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                              .exchange_timeout = std::chrono::milliseconds{1000},
                              .maximum_response_frames = 4U,
                              .maximum_response_bytes = std::size_t{1024U} * 1024U}},
       .connect_timeout = std::chrono::milliseconds{1000}},
      vector_start);
  ASSERT_TRUE(vector_client.has_value()) << vector_client.error().to_string();
  for (std::size_t iteration = 0U;
       iteration < 4096U &&
       vector_client->state() != cluster::DistributedVectorQueryTcpClientStateV2::kComplete;
       ++iteration) {
    const auto interest = vector_client->interest();
    pollfd descriptor{.fd = vector_client->descriptor(),
                      .events = static_cast<short>((interest.want_read ? POLLIN : 0) |
                                                   (interest.want_write ? POLLOUT : 0))};
    ASSERT_GE(::poll(&descriptor, 1U, 1), 0);
    ASSERT_TRUE(vector_client
                    ->on_ready((descriptor.revents & POLLIN) != 0,
                               (descriptor.revents & POLLOUT) != 0,
                               cluster::DistributedVectorQueryTcpClientV2::TimePoint::clock::now())
                    .is_ok())
        << vector_client->failure().to_string();
    ASSERT_TRUE(moved_vector_server.poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(vector_client->state(), cluster::DistributedVectorQueryTcpClientStateV2::kComplete)
      << vector_client->failure().to_string();
  const auto vector_tcp_responses = vector_client->responses();
  ASSERT_TRUE(vector_tcp_responses.has_value()) << vector_tcp_responses.error().to_string();
  ASSERT_EQ(vector_tcp_responses->size(), 1U);
  EXPECT_EQ(vector_tcp_responses->front().status_code, common::StatusCode::kOk);
  ASSERT_TRUE(vector_tcp_responses->front().payload.has_value());
  // Guarded by the payload assertion above.
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  const auto& vector_tcp_message = *vector_tcp_responses->front().payload;
  EXPECT_TRUE(vector_tcp_message.terminal);
  const auto vector_tcp_batch =
      network::decode_query_result_batch(vector_tcp_message.encoded_result_batch);
  ASSERT_TRUE(vector_tcp_batch.has_value()) << vector_tcp_batch.error().to_string();
  EXPECT_EQ(vector_tcp_batch->row_count(), 2U);
  EXPECT_EQ(provider.vector_calls, vector_calls_before_tcp + 1U);
  EXPECT_TRUE(vector_tcp_client_authenticator.saw_fingerprint);
  EXPECT_TRUE(vector_tcp_server_authenticator.saw_fingerprint);
  EXPECT_EQ(moved_vector_server.metrics().completed_connections, 1U);
  EXPECT_TRUE(moved_vector_server.shutdown().is_ok());

  NodeAuthorizer grouped_authorizer;
  EXPECT_EQ(ReplicatedDistributedGroupedQueryReceiver::create({}).error().code(),
            common::StatusCode::kInvalidArgument);
  auto grouped_receiver = ReplicatedDistributedGroupedQueryReceiver::create(
      {.worker = {.local_node_id = 11U, .storage = &*storage, .context_provider = &provider},
       .node_authorizer = &grouped_authorizer});
  ASSERT_TRUE(grouped_receiver.has_value()) << grouped_receiver.error().to_string();
  const auto grouped_request = cluster::encode_distributed_grouped_query_request_v1(
      {.source_node_id = 1U, .target_node_id = 11U, .dispatch = grouped_dispatch});
  ASSERT_TRUE(grouped_request.has_value()) << grouped_request.error().to_string();
  const auto grouped_frames =
      grouped_receiver->receive(*grouped_request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(grouped_frames.has_value()) << grouped_frames.error().to_string();
  ASSERT_EQ(grouped_frames->size(), 1U);
  const auto grouped_response =
      cluster::decode_distributed_grouped_query_response_v1(grouped_frames->front());
  ASSERT_TRUE(grouped_response.has_value()) << grouped_response.error().to_string();
  ASSERT_TRUE(grouped_response->payload.has_value());
  // Guarded by the payload assertion above.
  // NOLINTBEGIN(bugprone-unchecked-optional-access)
  const auto* remote_grouped =
      std::get_if<query::GroupedFloat64ExchangeMessage>(&*grouped_response->payload);
  // NOLINTEND(bugprone-unchecked-optional-access)
  ASSERT_NE(remote_grouped, nullptr);
  EXPECT_EQ(remote_grouped->group_key, 2.5);
  EXPECT_EQ(remote_grouped->partial.sum, 2.5);
  EXPECT_EQ(provider.grouped_calls, 2U);

  EXPECT_EQ(ReplicatedDistributedGroupedQueryTcpServer::start({}).error().code(),
            common::StatusCode::kInvalidArgument);
  Authenticator grouped_tcp_client_authenticator{91U};
  Authenticator grouped_tcp_server_authenticator{92U};
  auto grouped_server = ReplicatedDistributedGroupedQueryTcpServer::start(
      {.worker = {.local_node_id = 11U, .storage = &*storage, .context_provider = &provider},
       .listener = {},
       .tls = tls_server_config(),
       .authenticator = &grouped_tcp_client_authenticator,
       .node_authorizer = &grouped_authorizer,
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000},
                          .maximum_response_frames = 4U},
       .maximum_connections = 4U,
       .maximum_accepts_per_poll = 4U});
  ASSERT_TRUE(grouped_server.has_value()) << grouped_server.error().to_string();
  auto grouped_tls_context = network::TlsClientContext::create(tls_client_config());
  ASSERT_TRUE(grouped_tls_context.has_value()) << grouped_tls_context.error().to_string();
  auto grouped_client = cluster::DistributedGroupedQueryTcpClient::begin(
      {1U, 11U, *grouped_request},
      {.remote_endpoint = grouped_server->bound_endpoint(),
       .tls_context = std::addressof(*grouped_tls_context),
       .carrier = {.authenticator = &grouped_tcp_server_authenticator,
                   .node_authorizer = &grouped_authorizer,
                   .peer_ipv4_address = {127U, 0U, 0U, 1U},
                   .limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                              .exchange_timeout = std::chrono::milliseconds{1000},
                              .maximum_response_frames = 4U}},
       .connect_timeout = std::chrono::milliseconds{1000}},
      cluster::DistributedGroupedQueryTcpClient::TimePoint::clock::now());
  ASSERT_TRUE(grouped_client.has_value()) << grouped_client.error().to_string();
  for (std::size_t iteration = 0U;
       iteration < 1024U &&
       grouped_client->state() != cluster::DistributedGroupedQueryTcpClientState::kComplete;
       ++iteration) {
    const auto interest = grouped_client->interest();
    pollfd descriptor{.fd = grouped_client->descriptor(),
                      .events = static_cast<short>((interest.want_read ? POLLIN : 0) |
                                                   (interest.want_write ? POLLOUT : 0))};
    ASSERT_GE(::poll(&descriptor, 1U, 1), 0);
    ASSERT_TRUE(grouped_client
                    ->on_ready((descriptor.revents & POLLIN) != 0,
                               (descriptor.revents & POLLOUT) != 0,
                               cluster::DistributedGroupedQueryTcpClient::TimePoint::clock::now())
                    .is_ok())
        << grouped_client->failure().to_string();
    ASSERT_TRUE(grouped_server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(grouped_client->state(), cluster::DistributedGroupedQueryTcpClientState::kComplete)
      << grouped_client->failure().to_string();
  auto grouped_tcp_responses = grouped_client->responses();
  ASSERT_TRUE(grouped_tcp_responses.has_value()) << grouped_tcp_responses.error().to_string();
  ASSERT_EQ(grouped_tcp_responses->size(), 1U);
  ASSERT_TRUE(grouped_tcp_responses->front().payload.has_value());
  // Guarded by the payload assertion above.
  // NOLINTBEGIN(bugprone-unchecked-optional-access)
  const auto* grouped_tcp_result =
      std::get_if<query::GroupedFloat64ExchangeMessage>(&*grouped_tcp_responses->front().payload);
  // NOLINTEND(bugprone-unchecked-optional-access)
  ASSERT_NE(grouped_tcp_result, nullptr);
  EXPECT_EQ(grouped_tcp_result->group_key, 2.5);
  EXPECT_EQ(grouped_tcp_result->partial.sum, 2.5);
  EXPECT_TRUE(grouped_tcp_result->terminal);
  EXPECT_EQ(provider.grouped_calls, 3U);
  EXPECT_TRUE(grouped_tcp_client_authenticator.saw_fingerprint);
  EXPECT_TRUE(grouped_tcp_server_authenticator.saw_fingerprint);
  EXPECT_EQ(grouped_server->metrics().completed_connections, 1U);
  EXPECT_TRUE(grouped_server->shutdown().is_ok());

  auto movement = raft::TabletMovement::begin(tablet_id, 12U, 11U, 13U, {11U, 12U});
  ASSERT_TRUE(movement.has_value()) << movement.error().to_string();
  ASSERT_TRUE(movement
                  ->begin_snapshot({.manifest_generation = 1U,
                                    .applied_index = 10U,
                                    .applied_term = 3U,
                                    .total_bytes = encoded_part.bytes().size(),
                                    .content_crc32c = common::crc32c(encoded_part.bytes())})
                  .is_ok());
  ASSERT_TRUE(
      movement
          ->accept_snapshot_chunk(0U, encoded_part.bytes(), common::crc32c(encoded_part.bytes()))
          .is_ok());
  ASSERT_TRUE(movement->finish_snapshot().is_ok());
  ASSERT_TRUE(movement->mark_caught_up(10U).is_ok());
  ASSERT_TRUE(movement->promote_target(12U, 13U).is_ok());
  ASSERT_TRUE(movement->remove_source(13U, 14U).is_ok());
  ASSERT_EQ(movement->record().phase, raft::TabletMovementPhase::kComplete);

  TemporaryDirectory target_directory;
  ASSERT_FALSE(target_directory.path().empty());
  ASSERT_TRUE(
      std::filesystem::create_directory(target_directory.path() / manifest::kPartsDirectoryName));
  ASSERT_TRUE(std::filesystem::create_directory(target_directory.path() /
                                                manifest::kManifestDirectoryName));
  write_file(target_directory.path() / manifest::kManifestDirectoryName /
                 manifest::kManifestLockFileName,
             {});
  write_file(target_directory.path() / manifest::kPartsDirectoryName /
                 manifest::part_file_name(part->part_id),
             movement->received_snapshot());
  write_file(target_directory.path() / manifest::kManifestDirectoryName /
                 *manifest::manifest_file_name(1U),
             encoded_manifest->bytes());
  auto target_storage =
      manifest::ManifestStorage::open_existing({.database_root = target_directory.path().string()});
  ASSERT_TRUE(target_storage.has_value()) << target_storage.error().to_string();
  auto target_loaded =
      target_storage->load_selected_temporal_manifest({.expected_database_id = database_id,
                                                       .schema_bindings = schema_bindings,
                                                       .source_bindings = source_bindings,
                                                       .decode_limits = {},
                                                       .part_validation_limits = {}});
  ASSERT_TRUE(target_loaded.has_value()) << target_loaded.error().to_string();
  auto target_selected =
      std::make_shared<const manifest::LoadedTemporalManifestGeneration>(std::move(*target_loaded));
  auto target_publisher =
      manifest::TemporalDatabaseStoragePublisher::create(target_selected, schema_bindings);
  ASSERT_TRUE(target_publisher.has_value()) << target_publisher.error().to_string();
  auto target_snapshot = target_publisher->snapshot();
  ASSERT_TRUE(target_snapshot.has_value()) << target_snapshot.error().to_string();
  ContextProvider target_provider{std::move(*target_snapshot),
                                  lineage,
                                  {.table_id = schema_value->table_id(),
                                   .tablet_id = tablet_id,
                                   .placement_epoch = 14U,
                                   .replicas = {12U, 13U},
                                   .leader_hint = 13U},
                                  group_id,
                                  raft::ReadBarrier{2U, 3U, 10U}};
  Authenticator target_client_authenticator{91U};
  auto target_server = ReplicatedDistributedGroupedQueryTcpServer::start(
      {.worker = {.local_node_id = 13U,
                  .storage = &*target_storage,
                  .context_provider = &target_provider},
       .listener = {},
       .tls = tls_server_config(),
       .authenticator = &target_client_authenticator,
       .node_authorizer = &grouped_authorizer,
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000},
                          .maximum_response_frames = 4U},
       .maximum_connections = 4U,
       .maximum_accepts_per_poll = 4U});
  ASSERT_TRUE(target_server.has_value()) << target_server.error().to_string();
  auto moved_dispatch = grouped_dispatch;
  moved_dispatch.fragment.aggregate.serving_node = 13U;
  moved_dispatch.fragment.aggregate.placement_epoch = 14U;
  const auto moved_request = cluster::encode_distributed_grouped_query_request_v1(
      {.source_node_id = 1U, .target_node_id = 13U, .dispatch = moved_dispatch});
  ASSERT_TRUE(moved_request.has_value()) << moved_request.error().to_string();
  auto moved_client = cluster::DistributedGroupedQueryTcpClient::begin(
      {1U, 13U, *moved_request},
      {.remote_endpoint = target_server->bound_endpoint(),
       .tls_context = std::addressof(*grouped_tls_context),
       .carrier = {.authenticator = &grouped_tcp_server_authenticator,
                   .node_authorizer = &grouped_authorizer,
                   .peer_ipv4_address = {127U, 0U, 0U, 1U},
                   .limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                              .exchange_timeout = std::chrono::milliseconds{1000},
                              .maximum_response_frames = 4U}},
       .connect_timeout = std::chrono::milliseconds{1000}},
      cluster::DistributedGroupedQueryTcpClient::TimePoint::clock::now());
  ASSERT_TRUE(moved_client.has_value()) << moved_client.error().to_string();
  for (std::size_t iteration = 0U;
       iteration < 1024U &&
       moved_client->state() != cluster::DistributedGroupedQueryTcpClientState::kComplete;
       ++iteration) {
    const auto interest = moved_client->interest();
    pollfd descriptor{.fd = moved_client->descriptor(),
                      .events = static_cast<short>((interest.want_read ? POLLIN : 0) |
                                                   (interest.want_write ? POLLOUT : 0))};
    ASSERT_GE(::poll(&descriptor, 1U, 1), 0);
    ASSERT_TRUE(moved_client
                    ->on_ready((descriptor.revents & POLLIN) != 0,
                               (descriptor.revents & POLLOUT) != 0,
                               cluster::DistributedGroupedQueryTcpClient::TimePoint::clock::now())
                    .is_ok())
        << moved_client->failure().to_string();
    ASSERT_TRUE(target_server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(moved_client->state(), cluster::DistributedGroupedQueryTcpClientState::kComplete)
      << moved_client->failure().to_string();
  auto moved_responses = moved_client->responses();
  ASSERT_TRUE(moved_responses.has_value()) << moved_responses.error().to_string();
  ASSERT_EQ(moved_responses->size(), 1U);
  ASSERT_TRUE(moved_responses->front().payload.has_value());
  // Guarded by the payload assertion above.
  // NOLINTBEGIN(bugprone-unchecked-optional-access)
  const auto* moved_result =
      std::get_if<query::GroupedFloat64ExchangeMessage>(&*moved_responses->front().payload);
  // NOLINTEND(bugprone-unchecked-optional-access)
  ASSERT_NE(moved_result, nullptr);
  EXPECT_EQ(moved_result->group_key, grouped_tcp_result->group_key);
  EXPECT_EQ(moved_result->partial.count, grouped_tcp_result->partial.count);
  EXPECT_EQ(moved_result->partial.sum, grouped_tcp_result->partial.sum);
  EXPECT_EQ(moved_result->partial.minimum, grouped_tcp_result->partial.minimum);
  EXPECT_EQ(moved_result->partial.maximum, grouped_tcp_result->partial.maximum);
  EXPECT_EQ(moved_result->partial.mean, grouped_tcp_result->partial.mean);
  EXPECT_EQ(moved_result->partial.m2, grouped_tcp_result->partial.m2);
  EXPECT_EQ(target_provider.grouped_calls, 1U);
  EXPECT_TRUE(target_client_authenticator.saw_fingerprint);
  EXPECT_EQ(target_server->metrics().completed_connections, 1U);
  EXPECT_TRUE(target_server->shutdown().is_ok());

  EXPECT_EQ(ReplicatedDistributedQueryTcpServer::start({}).error().code(),
            common::StatusCode::kInvalidArgument);
  Authenticator client_authenticator{91U};
  Authenticator server_authenticator{92U};
  NodeAuthorizer node_authorizer;
  auto server = ReplicatedDistributedQueryTcpServer::start(
      {.worker = {.local_node_id = 11U, .storage = &*storage, .context_provider = &provider},
       .listener = {},
       .tls = tls_server_config(),
       .authenticator = &client_authenticator,
       .node_authorizer = &node_authorizer,
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000}},
       .maximum_connections = 4U,
       .maximum_accepts_per_poll = 4U});
  ASSERT_TRUE(server.has_value()) << server.error().to_string();
  auto tls_context = network::TlsClientContext::create(tls_client_config());
  ASSERT_TRUE(tls_context.has_value()) << tls_context.error().to_string();
  auto sender = cluster::DistributedQuerySender::create(1U, dispatch);
  ASSERT_TRUE(sender.has_value()) << sender.error().to_string();
  const auto start = cluster::DistributedQuerySender::TimePoint::clock::now();
  auto attempt = sender->begin_attempt(start);
  ASSERT_TRUE(attempt.has_value()) << attempt.error().to_string();
  auto client = cluster::DistributedQueryTcpClient::begin(
      std::move(*attempt),
      {.remote_endpoint = server->bound_endpoint(),
       .tls_context = std::addressof(*tls_context),
       .carrier = {.authenticator = &server_authenticator,
                   .node_authorizer = &node_authorizer,
                   .peer_ipv4_address = {127U, 0U, 0U, 1U},
                   .limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                              .exchange_timeout = std::chrono::milliseconds{1000}}},
       .connect_timeout = std::chrono::milliseconds{1000}},
      start);
  ASSERT_TRUE(client.has_value()) << client.error().to_string();
  for (std::size_t iteration = 0U;
       iteration < 1024U && client->state() != cluster::DistributedQueryTcpClientState::kComplete;
       ++iteration) {
    const auto interest = client->interest();
    pollfd descriptor{.fd = client->descriptor(),
                      .events = static_cast<short>((interest.want_read ? POLLIN : 0) |
                                                   (interest.want_write ? POLLOUT : 0))};
    ASSERT_GE(::poll(&descriptor, 1U, 1), 0);
    ASSERT_TRUE(client
                    ->on_ready((descriptor.revents & POLLIN) != 0,
                               (descriptor.revents & POLLOUT) != 0,
                               cluster::DistributedQuerySender::TimePoint::clock::now())
                    .is_ok())
        << client->failure().to_string();
    ASSERT_TRUE(server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(client->state(), cluster::DistributedQueryTcpClientState::kComplete)
      << client->failure().to_string();
  auto response = client->response_bytes();
  ASSERT_TRUE(response.has_value()) << response.error().to_string();
  ASSERT_TRUE(
      sender->accept_response(*response, cluster::DistributedQuerySender::TimePoint::clock::now())
          .is_ok());
  auto remote_result = sender->result();
  ASSERT_TRUE(remote_result.has_value());
  // Guarded by the local and remote result assertions above.
  // NOLINTBEGIN(bugprone-unchecked-optional-access)
  EXPECT_EQ(remote_result->partial.count, result->partial.count);
  EXPECT_EQ(remote_result->partial.sum, result->partial.sum);
  EXPECT_EQ(remote_result->partial.minimum, result->partial.minimum);
  EXPECT_EQ(remote_result->partial.maximum, result->partial.maximum);
  EXPECT_EQ(remote_result->partial.mean, result->partial.mean);
  EXPECT_EQ(remote_result->partial.m2, result->partial.m2);
  // NOLINTEND(bugprone-unchecked-optional-access)
  EXPECT_EQ(provider.calls, 2U);
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
  EXPECT_TRUE(server_authenticator.saw_fingerprint);
  EXPECT_EQ(server->metrics().completed_connections, 1U);
  EXPECT_TRUE(server->shutdown().is_ok());

  const raft::GroupId metadata_group = uuid(30U);
  ASSERT_TRUE(std::filesystem::create_directory(directory.path() / "metadata-raft"));
  auto metadata_runtime = raft::AsyncDurableMultiRaftRuntime::create_new(
      13U, {.directory_path = (directory.path() / "metadata-raft").string()},
      {{metadata_group, {13U}}});
  ASSERT_TRUE(metadata_runtime.has_value()) << metadata_runtime.error().to_string();
  auto metadata_election =
      metadata_runtime->try_submit({{metadata_group, raft::StartElectionOperation{}}});
  ASSERT_TRUE(metadata_election.has_value()) << metadata_election.error().to_string();
  ASSERT_TRUE(metadata_election->wait().has_value());
  auto metadata_barrier =
      ReplicatedReadBarrier::create_local(std::addressof(*metadata_runtime), {metadata_group});
  ASSERT_TRUE(metadata_barrier.has_value()) << metadata_barrier.error().to_string();
  auto metadata_authority = metadata_barrier->await_authority();
  ASSERT_TRUE(metadata_authority.has_value()) << metadata_authority.error().to_string();
  ASSERT_EQ(metadata_authority->size(), 1U);
  const raft::LogIndex metadata_applied_position =
      metadata_authority->front().barrier.barrier.read_index;
  auto metadata_applied = metadata_runtime->try_submit(
      {{metadata_group, raft::MarkAppliedOperation{.index = metadata_applied_position}}});
  ASSERT_TRUE(metadata_applied.has_value()) << metadata_applied.error().to_string();
  auto metadata_applied_result = metadata_applied->wait();
  ASSERT_TRUE(metadata_applied_result.has_value()) << metadata_applied_result.error().to_string();
  ASSERT_EQ(metadata_applied_result->size(), 1U);
  ASSERT_TRUE(metadata_applied_result->front().status.is_ok())
      << metadata_applied_result->front().status.to_string();

  LifecycleObservationService leader_observation_service{
      {.node_id = 13U, .role = raft::Role::kLeader, .leader_id = 13U, .position = 10U}};
  LifecycleObservationService follower_observation_service{
      {.node_id = 11U, .role = raft::Role::kFollower, .leader_id = 13U, .position = 10U}};
  auto leader_observation_receiver =
      cluster::RaftObservationReceiver::create({.local_node_id = 13U,
                                                .authorizer = &node_authorizer,
                                                .service = &leader_observation_service});
  auto follower_observation_receiver =
      cluster::RaftObservationReceiver::create({.local_node_id = 11U,
                                                .authorizer = &node_authorizer,
                                                .service = &follower_observation_service});
  ASSERT_TRUE(leader_observation_receiver.has_value())
      << leader_observation_receiver.error().to_string();
  ASSERT_TRUE(follower_observation_receiver.has_value())
      << follower_observation_receiver.error().to_string();
  Authenticator lifecycle_inbound_authenticator{91U};
  auto observation_server_config = [&](cluster::RaftObservationReceiver& receiver) {
    return cluster::RaftObservationTcpServerConfig{
        .listener = {},
        .tls = tls_server_config(),
        .authenticator = &lifecycle_inbound_authenticator,
        .receiver = &receiver,
        .session_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                           .exchange_timeout = std::chrono::milliseconds{1000}},
        .maximum_connections = 4U,
        .maximum_accepts_per_poll = 4U};
  };
  auto leader_observation_server = cluster::RaftObservationTcpServer::start(
      observation_server_config(*leader_observation_receiver));
  auto follower_observation_server = cluster::RaftObservationTcpServer::start(
      observation_server_config(*follower_observation_receiver));
  ASSERT_TRUE(leader_observation_server.has_value())
      << leader_observation_server.error().to_string();
  ASSERT_TRUE(follower_observation_server.has_value())
      << follower_observation_server.error().to_string();

  const query::DistributedVectorQueryPlan lifecycle_plan{
      .query_id = uuid(31U),
      .read_policy = {.consistency = query::DistributedReadConsistency::kFollowerBoundedStale,
                      .maximum_staleness_positions = 1U},
      .fragments = {{.tablet_id = tablet_id,
                     .minimum_event_time = 0,
                     .maximum_event_time = 100,
                     .leader_node = 13U,
                     .local_applied_position = 10U,
                     .known_leader_commit_position = 10U}},
      .intent = {
          .mode = query::DistributedVectorPlanMode::kUngroupedAggregate,
          .aggregates = {{.operation = query::VectorAggregateOperation::kCountStar},
                         {.operation = query::VectorAggregateOperation::kSum, .input_index = 1U}}}};
  const raft::MetadataCatalogSnapshot lifecycle_catalog{
      .applied_index = metadata_applied_position,
      .cluster_nodes = {{11U, endpoint_text(follower_observation_server->bound_endpoint())},
                        {13U, endpoint_text(leader_observation_server->bound_endpoint())}},
      .schema_definitions = {{"metrics", false, schema_value}},
      .active_schemas = {{schema_value->table_id(), schema_value->schema_id()}},
      .tablet_placements = {{schema_value->table_id(),
                             tablet_id,
                             12U,
                             {11U, 13U},
                             std::optional<raft::NodeId>{13U}}},
      .tablet_group_bindings = {{tablet_id, group_id}}};
  const std::array<std::uint32_t, 2U> lifecycle_projection{0U, 1U};
  const std::array lifecycle_observation_tls_contexts{
      cluster::RaftObservationNodeTlsContext{11U, std::addressof(*tls_context)},
      cluster::RaftObservationNodeTlsContext{13U, std::addressof(*tls_context)}};
  const std::array lifecycle_query_tls_contexts{
      cluster::DistributedQueryNodeTlsContext{11U, std::addressof(*tls_context)}};
  Authenticator lifecycle_remote_authenticator{92U};
  auto lifecycle_snapshot = publisher->snapshot();
  ASSERT_TRUE(lifecycle_snapshot.has_value()) << lifecycle_snapshot.error().to_string();
  auto lifecycle = ReplicatedFollowerDistributedVectorAggregateQueryV2::create(
      lifecycle_plan, std::move(*lifecycle_snapshot),
      query::DistributedVectorResultSchema{
          .columns = {{.name = "count",
                       .type = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(),
                       .nullable = false},
                      {.name = "sum",
                       .type = schema_value->columns()[1].type(),
                       .nullable = true}}},
      {.source_node_id = 1U,
       .first_correlation_id = 201U,
       .tls_contexts = lifecycle_observation_tls_contexts,
       .authenticator = &lifecycle_remote_authenticator,
       .node_authorizer = &node_authorizer,
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000}},
       .connect_timeout = std::chrono::milliseconds{1000},
       .retry = {.maximum_attempts = 1U,
                 .initial_backoff = std::chrono::milliseconds{1},
                 .maximum_backoff = std::chrono::milliseconds{1}},
       .maximum_pairs = 1U},
      {.source_node_id = 1U,
       .read_barrier = &*metadata_barrier,
       .metadata_group_id = metadata_group,
       .catalog = std::cref(lifecycle_catalog),
       .table_id = schema_value->table_id(),
       .destination_column_ordinals = lifecycle_projection,
       .tls_contexts = lifecycle_query_tls_contexts,
       .authenticator = &lifecycle_remote_authenticator,
       .node_authorizer = &node_authorizer,
       .binding_limits = {.maximum_fragments = 1U,
                          .maximum_total_projection_ordinals = lifecycle_projection.size()},
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000},
                          .maximum_response_frames = 2U,
                          .maximum_response_bytes = std::size_t{1024U} * 1024U},
       .connect_timeout = std::chrono::milliseconds{1000}});
  ASSERT_TRUE(lifecycle.has_value()) << lifecycle.error().to_string();
  for (std::size_t iteration = 0U;
       iteration < 4096U &&
       lifecycle->state() ==
           ReplicatedFollowerDistributedVectorAggregateQueryStateV2::kAcquiringAuthority;
       ++iteration) {
    ASSERT_TRUE(lifecycle->poll_once(std::chrono::milliseconds{1}).is_ok())
        << lifecycle->failure().to_string();
    ASSERT_TRUE(leader_observation_server->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(follower_observation_server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(lifecycle->state(),
            ReplicatedFollowerDistributedVectorAggregateQueryStateV2::kExecuting)
      << lifecycle->failure().to_string();
  EXPECT_EQ(leader_observation_service.calls, 1U);
  EXPECT_EQ(follower_observation_service.calls, 1U);

  const network::Ipv4Endpoint lifecycle_query_endpoint =
      follower_observation_server->bound_endpoint();
  ASSERT_TRUE(follower_observation_server->shutdown().is_ok());
  provider.set_authority({.table_id = schema_value->table_id(),
                          .tablet_id = tablet_id,
                          .placement_epoch = 12U,
                          .replicas = {11U, 13U},
                          .leader_hint = 13U},
                         std::nullopt);
  const std::size_t vector_calls_before_lifecycle = provider.vector_calls;
  auto lifecycle_query_server = ReplicatedDistributedVectorAggregateQueryTcpServerV2::start(
      {.worker = {.local_node_id = 11U, .storage = &*storage, .context_provider = &provider},
       .listener = {.bind_endpoint = lifecycle_query_endpoint},
       .tls = tls_server_config(),
       .authenticator = &lifecycle_inbound_authenticator,
       .node_authorizer = &node_authorizer,
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000},
                          .maximum_response_frames = 2U,
                          .maximum_response_bytes = std::size_t{1024U} * 1024U},
       .maximum_connections = 4U,
       .maximum_accepts_per_poll = 4U});
  ASSERT_TRUE(lifecycle_query_server.has_value()) << lifecycle_query_server.error().to_string();
  ASSERT_EQ(lifecycle_query_server->bound_endpoint(), lifecycle_query_endpoint);
  for (std::size_t iteration = 0U;
       iteration < 4096U &&
       lifecycle->state() == ReplicatedFollowerDistributedVectorAggregateQueryStateV2::kExecuting;
       ++iteration) {
    ASSERT_TRUE(lifecycle->poll_once(std::chrono::milliseconds{1}).is_ok())
        << lifecycle->failure().to_string();
    ASSERT_TRUE(lifecycle_query_server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(lifecycle->state(), ReplicatedFollowerDistributedVectorAggregateQueryStateV2::kComplete)
      << lifecycle->failure().to_string();
  auto lifecycle_result = lifecycle->result();
  ASSERT_TRUE(lifecycle_result.has_value()) << lifecycle_result.error().to_string();
  // Guarded by the completed lifecycle and result assertion above.
  // NOLINTBEGIN(bugprone-unchecked-optional-access)
  EXPECT_EQ(lifecycle_result->get().row_count, 1U);
  ASSERT_EQ(lifecycle_result->get().result_schema.columns.size(), 2U);
  EXPECT_EQ(lifecycle_result->get().result_schema.columns[0].name, "count");
  EXPECT_EQ(lifecycle_result->get().result_schema.columns[1].name, "sum");
  auto lifecycle_batch = network::decode_query_result_batch(lifecycle_result->get().encoded_batch);
  ASSERT_TRUE(lifecycle_batch.has_value()) << lifecycle_batch.error().to_string();
  ASSERT_EQ(lifecycle_batch->row_count(), 1U);
  ASSERT_EQ(lifecycle_batch->columns().size(), 2U);
  const network::QueryResultCell* lifecycle_count_cell = lifecycle_batch->cell(0U, 0U);
  const network::QueryResultCell* lifecycle_sum_cell = lifecycle_batch->cell(0U, 1U);
  ASSERT_NE(lifecycle_count_cell, nullptr);
  ASSERT_NE(lifecycle_sum_cell, nullptr);
  ASSERT_FALSE(lifecycle_count_cell->is_null);
  ASSERT_FALSE(lifecycle_sum_cell->is_null);
  common::ByteReader lifecycle_count_reader{lifecycle_count_cell->value};
  common::ByteReader lifecycle_sum_reader{lifecycle_sum_cell->value};
  auto lifecycle_count = lifecycle_count_reader.read_i64_le();
  auto lifecycle_sum_bits = lifecycle_sum_reader.read_u64_le();
  ASSERT_TRUE(lifecycle_count.has_value());
  ASSERT_TRUE(lifecycle_sum_bits.has_value());
  EXPECT_EQ(*lifecycle_count, 2);
  EXPECT_EQ(std::bit_cast<double>(*lifecycle_sum_bits), 4.0);
  EXPECT_TRUE(lifecycle_count_reader.empty());
  EXPECT_TRUE(lifecycle_sum_reader.empty());
  // NOLINTEND(bugprone-unchecked-optional-access)
  EXPECT_EQ(provider.vector_calls, vector_calls_before_lifecycle + 2U);
  EXPECT_TRUE(lifecycle_inbound_authenticator.saw_fingerprint);
  EXPECT_TRUE(lifecycle_remote_authenticator.saw_fingerprint);
  const auto lifecycle_metrics = lifecycle->metrics();
  EXPECT_EQ(lifecycle_metrics.authority.completed_pairs, 1U);
  ASSERT_TRUE(lifecycle_metrics.execution.has_value());
  // Guarded by the execution-metrics assertion above.
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  EXPECT_EQ(lifecycle_metrics.execution->transport_completed_attempts, 1U);
  EXPECT_EQ(lifecycle_query_server->metrics().completed_connections, 1U);
  EXPECT_TRUE(lifecycle_query_server->shutdown().is_ok());
  EXPECT_TRUE(leader_observation_server->shutdown().is_ok());
  EXPECT_TRUE(metadata_barrier->shutdown().is_ok());
  EXPECT_TRUE(metadata_runtime->shutdown().is_ok());

  provider.set_raft_group_id(uuid(9U));
  EXPECT_EQ(worker->execute(dispatch).error().code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(provider.calls, 3U);

  provider.set_raft_group_id(group_id);
  provider.set_placement_epoch(13U);
  EXPECT_EQ(worker->execute(dispatch).error().code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(provider.calls, 4U);

  provider.clear_lineage();
  EXPECT_EQ(worker->execute(dispatch).error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(provider.calls, 5U);
}

TEST(ReplicatedDistributedQueryWorkerTest,
     CompletesTwoProductionFollowerCsegFragmentsAsOneAtomicResult) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  ASSERT_TRUE(std::filesystem::create_directory(directory.path() / manifest::kPartsDirectoryName));
  ASSERT_TRUE(
      std::filesystem::create_directory(directory.path() / manifest::kManifestDirectoryName));
  write_file(directory.path() / manifest::kManifestDirectoryName / manifest::kManifestLockFileName,
             {});

  const auto lineage = make_lineage();
  const auto schema_value = lineage->current();
  ASSERT_NE(schema_value, nullptr);
  const manifest::DatabaseId database_id = id<manifest::DatabaseId>(1U);
  const std::array tablet_ids{id<schema::TabletId>(3U), id<schema::TabletId>(7U)};
  const std::array<raft::GroupId, 2U> group_ids{uuid(8U), uuid(9U)};
  const std::array encoded_parts{
      cseg::test::make_valid_temporal_float64_part(
          cseg::PageCompression::kNone,
          {.commit_source = cseg::temporal_format::CommitSource::kRaft, .source_id = group_ids[0]}),
      cseg::test::make_valid_temporal_float64_part(
          cseg::PageCompression::kNone,
          {.commit_source = cseg::temporal_format::CommitSource::kRaft,
           .source_id = group_ids[1],
           .part_id_seed = 2U,
           .tablet_id_seed = 7U,
           .first_float64_value = 3.5,
           .second_float64_value = 4.5})};
  std::vector<manifest::TemporalPartDescriptor> parts;
  parts.reserve(encoded_parts.size());
  for (std::size_t index = 0U; index < encoded_parts.size(); ++index) {
    auto described = manifest::describe_manifest_v2_temporal_part_image(
        encoded_parts[index].bytes(), *schema_value, tablet_ids[index],
        manifest::ManifestCommitSource::kRaft, group_ids[index]);
    ASSERT_TRUE(described.has_value()) << described.error().to_string();
    parts.push_back(*described);
    write_file(directory.path() / manifest::kPartsDirectoryName /
                   manifest::part_file_name(parts[index].part_id),
               encoded_parts[index].bytes());
  }
  const std::array tablets{
      manifest::TemporalTabletDescriptor{.table_id = schema_value->table_id(),
                                         .tablet_id = tablet_ids[0],
                                         .recovery_schema_id = schema_value->schema_id(),
                                         .recovery_schema_version = schema_value->version(),
                                         .source_id = group_ids[0],
                                         .durable_position = 10U,
                                         .reclaim_position = 0U,
                                         .first_part_index = 0U,
                                         .part_count = 1U,
                                         .durable_version_count = 2U,
                                         .commit_source = manifest::ManifestCommitSource::kRaft},
      manifest::TemporalTabletDescriptor{.table_id = schema_value->table_id(),
                                         .tablet_id = tablet_ids[1],
                                         .recovery_schema_id = schema_value->schema_id(),
                                         .recovery_schema_version = schema_value->version(),
                                         .source_id = group_ids[1],
                                         .durable_position = 10U,
                                         .reclaim_position = 0U,
                                         .first_part_index = 1U,
                                         .part_count = 1U,
                                         .durable_version_count = 2U,
                                         .commit_source = manifest::ManifestCommitSource::kRaft}};
  auto encoded_manifest =
      manifest::encode_manifest_v2_temporal({.generation = 1U,
                                             .database_id = database_id,
                                             .wal_reclaim_checkpoint = std::nullopt,
                                             .tablets = tablets,
                                             .parts = parts,
                                             .retries = {}});
  ASSERT_TRUE(encoded_manifest.has_value()) << encoded_manifest.error().to_string();
  write_file(directory.path() / manifest::kManifestDirectoryName /
                 *manifest::manifest_file_name(1U),
             encoded_manifest->bytes());
  auto storage =
      manifest::ManifestStorage::open_existing({.database_root = directory.path().string()});
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  const std::array schema_bindings{
      manifest::TabletSchemaBinding{.tablet_id = tablet_ids[0], .lineage = std::cref(*lineage)},
      manifest::TabletSchemaBinding{.tablet_id = tablet_ids[1], .lineage = std::cref(*lineage)}};
  const std::array source_bindings{
      manifest::TemporalTabletSourceBinding{.tablet_id = tablet_ids[0],
                                            .commit_source = manifest::ManifestCommitSource::kRaft,
                                            .source_id = group_ids[0]},
      manifest::TemporalTabletSourceBinding{.tablet_id = tablet_ids[1],
                                            .commit_source = manifest::ManifestCommitSource::kRaft,
                                            .source_id = group_ids[1]}};
  auto loaded = storage->load_selected_temporal_manifest({.expected_database_id = database_id,
                                                          .schema_bindings = schema_bindings,
                                                          .source_bindings = source_bindings});
  ASSERT_TRUE(loaded.has_value()) << loaded.error().to_string();
  auto selected =
      std::make_shared<const manifest::LoadedTemporalManifestGeneration>(std::move(*loaded));
  auto publisher = manifest::TemporalDatabaseStoragePublisher::create(selected, schema_bindings);
  ASSERT_TRUE(publisher.has_value()) << publisher.error().to_string();
  auto first_worker_snapshot = publisher->snapshot();
  auto second_worker_snapshot = publisher->snapshot();
  auto coordinator_snapshot = publisher->snapshot();
  ASSERT_TRUE(first_worker_snapshot.has_value()) << first_worker_snapshot.error().to_string();
  ASSERT_TRUE(second_worker_snapshot.has_value()) << second_worker_snapshot.error().to_string();
  ASSERT_TRUE(coordinator_snapshot.has_value()) << coordinator_snapshot.error().to_string();
  ContextProvider first_provider{std::move(*first_worker_snapshot),
                                 lineage,
                                 {.table_id = schema_value->table_id(),
                                  .tablet_id = tablet_ids[0],
                                  .placement_epoch = 12U,
                                  .replicas = {11U, 13U},
                                  .leader_hint = 13U},
                                 group_ids[0],
                                 std::nullopt};
  ContextProvider second_provider{std::move(*second_worker_snapshot),
                                  lineage,
                                  {.table_id = schema_value->table_id(),
                                   .tablet_id = tablet_ids[1],
                                   .placement_epoch = 12U,
                                   .replicas = {12U, 14U},
                                   .leader_hint = 14U},
                                  group_ids[1],
                                  std::nullopt};

  const raft::GroupId metadata_group = uuid(30U);
  ASSERT_TRUE(std::filesystem::create_directory(directory.path() / "metadata-raft"));
  auto metadata_runtime = raft::AsyncDurableMultiRaftRuntime::create_new(
      13U, {.directory_path = (directory.path() / "metadata-raft").string()},
      {{metadata_group, {13U}}});
  ASSERT_TRUE(metadata_runtime.has_value()) << metadata_runtime.error().to_string();
  auto metadata_election =
      metadata_runtime->try_submit({{metadata_group, raft::StartElectionOperation{}}});
  ASSERT_TRUE(metadata_election.has_value()) << metadata_election.error().to_string();
  ASSERT_TRUE(metadata_election->wait().has_value());
  auto metadata_barrier =
      ReplicatedReadBarrier::create_local(std::addressof(*metadata_runtime), {metadata_group});
  ASSERT_TRUE(metadata_barrier.has_value()) << metadata_barrier.error().to_string();
  auto metadata_authority = metadata_barrier->await_authority();
  ASSERT_TRUE(metadata_authority.has_value()) << metadata_authority.error().to_string();
  ASSERT_EQ(metadata_authority->size(), 1U);
  const raft::LogIndex metadata_applied_position =
      metadata_authority->front().barrier.barrier.read_index;
  auto metadata_applied = metadata_runtime->try_submit(
      {{metadata_group, raft::MarkAppliedOperation{.index = metadata_applied_position}}});
  ASSERT_TRUE(metadata_applied.has_value()) << metadata_applied.error().to_string();
  auto metadata_applied_result = metadata_applied->wait();
  ASSERT_TRUE(metadata_applied_result.has_value()) << metadata_applied_result.error().to_string();
  ASSERT_EQ(metadata_applied_result->size(), 1U);
  ASSERT_TRUE(metadata_applied_result->front().status.is_ok())
      << metadata_applied_result->front().status.to_string();

  std::array observation_services{LifecycleObservationService{{.node_id = 11U,
                                                               .role = raft::Role::kFollower,
                                                               .leader_id = 13U,
                                                               .position = 10U,
                                                               .voters = {11U, 13U}}},
                                  LifecycleObservationService{{.node_id = 12U,
                                                               .role = raft::Role::kFollower,
                                                               .leader_id = 14U,
                                                               .position = 10U,
                                                               .voters = {12U, 14U}}},
                                  LifecycleObservationService{{.node_id = 13U,
                                                               .role = raft::Role::kLeader,
                                                               .leader_id = 13U,
                                                               .position = 10U,
                                                               .voters = {11U, 13U}}},
                                  LifecycleObservationService{{.node_id = 14U,
                                                               .role = raft::Role::kLeader,
                                                               .leader_id = 14U,
                                                               .position = 10U,
                                                               .voters = {12U, 14U}}}};
  NodeAuthorizer node_authorizer;
  std::vector<cluster::RaftObservationReceiver> observation_receivers;
  observation_receivers.reserve(observation_services.size());
  for (std::size_t index = 0U; index < observation_services.size(); ++index) {
    auto receiver =
        cluster::RaftObservationReceiver::create({.local_node_id = 11U + index,
                                                  .authorizer = &node_authorizer,
                                                  .service = &observation_services[index]});
    ASSERT_TRUE(receiver.has_value()) << receiver.error().to_string();
    observation_receivers.push_back(*receiver);
  }
  Authenticator inbound_authenticator{91U};
  std::vector<cluster::RaftObservationTcpServer> observation_servers;
  observation_servers.reserve(observation_receivers.size());
  for (cluster::RaftObservationReceiver& receiver : observation_receivers) {
    auto observation_server = cluster::RaftObservationTcpServer::start(
        {.listener = {},
         .tls = tls_server_config(),
         .authenticator = &inbound_authenticator,
         .receiver = &receiver,
         .session_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                            .exchange_timeout = std::chrono::milliseconds{1000}},
         .maximum_connections = 4U,
         .maximum_accepts_per_poll = 4U});
    ASSERT_TRUE(observation_server.has_value()) << observation_server.error().to_string();
    observation_servers.push_back(std::move(*observation_server));
  }
  auto tls_context = network::TlsClientContext::create(tls_client_config());
  ASSERT_TRUE(tls_context.has_value()) << tls_context.error().to_string();
  const raft::MetadataCatalogSnapshot catalog{
      .applied_index = metadata_applied_position,
      .cluster_nodes = {{11U, endpoint_text(observation_servers[0].bound_endpoint())},
                        {12U, endpoint_text(observation_servers[1].bound_endpoint())},
                        {13U, endpoint_text(observation_servers[2].bound_endpoint())},
                        {14U, endpoint_text(observation_servers[3].bound_endpoint())}},
      .schema_definitions = {{"metrics", false, schema_value}},
      .active_schemas = {{schema_value->table_id(), schema_value->schema_id()}},
      .tablet_placements = {{schema_value->table_id(),
                             tablet_ids[0],
                             12U,
                             {11U, 13U},
                             std::optional<raft::NodeId>{13U}},
                            {schema_value->table_id(),
                             tablet_ids[1],
                             12U,
                             {12U, 14U},
                             std::optional<raft::NodeId>{14U}}},
      .tablet_group_bindings = {{tablet_ids[0], group_ids[0]}, {tablet_ids[1], group_ids[1]}}};
  const query::DistributedVectorQueryPlan plan{
      .query_id = uuid(31U),
      .read_policy = {.consistency = query::DistributedReadConsistency::kFollowerBoundedStale,
                      .maximum_staleness_positions = 1U},
      .fragments = {{.tablet_id = tablet_ids[0],
                     .minimum_event_time = 0,
                     .maximum_event_time = 100,
                     .leader_node = 13U,
                     .local_applied_position = 10U,
                     .known_leader_commit_position = 10U},
                    {.tablet_id = tablet_ids[1],
                     .minimum_event_time = 0,
                     .maximum_event_time = 100,
                     .leader_node = 14U,
                     .local_applied_position = 10U,
                     .known_leader_commit_position = 10U}},
      .intent = {
          .mode = query::DistributedVectorPlanMode::kUngroupedAggregate,
          .aggregates = {{.operation = query::VectorAggregateOperation::kCountStar},
                         {.operation = query::VectorAggregateOperation::kSum, .input_index = 1U}}}};
  const std::array<std::uint32_t, 2U> projection{0U, 1U};
  const std::array observation_tls_contexts{
      cluster::RaftObservationNodeTlsContext{11U, std::addressof(*tls_context)},
      cluster::RaftObservationNodeTlsContext{12U, std::addressof(*tls_context)},
      cluster::RaftObservationNodeTlsContext{13U, std::addressof(*tls_context)},
      cluster::RaftObservationNodeTlsContext{14U, std::addressof(*tls_context)}};
  const std::array query_tls_contexts{
      cluster::DistributedQueryNodeTlsContext{11U, std::addressof(*tls_context)},
      cluster::DistributedQueryNodeTlsContext{12U, std::addressof(*tls_context)}};
  Authenticator remote_authenticator{92U};
  auto lifecycle = ReplicatedFollowerDistributedVectorAggregateQueryV2::create(
      plan, std::move(*coordinator_snapshot),
      query::DistributedVectorResultSchema{
          .columns = {{.name = "count",
                       .type = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(),
                       .nullable = false},
                      {.name = "sum",
                       .type = schema_value->columns()[1].type(),
                       .nullable = true}}},
      {.source_node_id = 1U,
       .first_correlation_id = 301U,
       .tls_contexts = observation_tls_contexts,
       .authenticator = &remote_authenticator,
       .node_authorizer = &node_authorizer,
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000}},
       .connect_timeout = std::chrono::milliseconds{1000},
       .retry = {.maximum_attempts = 1U,
                 .initial_backoff = std::chrono::milliseconds{1},
                 .maximum_backoff = std::chrono::milliseconds{1}},
       .maximum_pairs = 2U},
      {.source_node_id = 1U,
       .read_barrier = &*metadata_barrier,
       .metadata_group_id = metadata_group,
       .catalog = std::cref(catalog),
       .table_id = schema_value->table_id(),
       .destination_column_ordinals = projection,
       .tls_contexts = query_tls_contexts,
       .authenticator = &remote_authenticator,
       .node_authorizer = &node_authorizer,
       .binding_limits = {.maximum_fragments = 2U, .maximum_total_projection_ordinals = 4U},
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000},
                          .maximum_response_frames = 2U,
                          .maximum_response_bytes = std::size_t{1024U} * 1024U},
       .connect_timeout = std::chrono::milliseconds{1000}});
  ASSERT_TRUE(lifecycle.has_value()) << lifecycle.error().to_string();
  for (std::size_t iteration = 0U;
       iteration < 4096U &&
       lifecycle->state() ==
           ReplicatedFollowerDistributedVectorAggregateQueryStateV2::kAcquiringAuthority;
       ++iteration) {
    ASSERT_TRUE(lifecycle->poll_once(std::chrono::milliseconds{1}).is_ok())
        << lifecycle->failure().to_string();
    for (cluster::RaftObservationTcpServer& observation_server : observation_servers)
      ASSERT_TRUE(observation_server.poll_once(std::chrono::milliseconds{0}).is_ok());
  }
  ASSERT_EQ(lifecycle->state(),
            ReplicatedFollowerDistributedVectorAggregateQueryStateV2::kExecuting)
      << lifecycle->failure().to_string();
  for (const LifecycleObservationService& observation_service : observation_services)
    EXPECT_EQ(observation_service.calls, 1U);

  const std::array query_endpoints{observation_servers[0].bound_endpoint(),
                                   observation_servers[1].bound_endpoint()};
  ASSERT_TRUE(observation_servers[0].shutdown().is_ok());
  ASSERT_TRUE(observation_servers[1].shutdown().is_ok());
  auto first_query_server = ReplicatedDistributedVectorAggregateQueryTcpServerV2::start(
      {.worker = {.local_node_id = 11U, .storage = &*storage, .context_provider = &first_provider},
       .listener = {.bind_endpoint = query_endpoints[0]},
       .tls = tls_server_config(),
       .authenticator = &inbound_authenticator,
       .node_authorizer = &node_authorizer,
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000},
                          .maximum_response_frames = 2U,
                          .maximum_response_bytes = std::size_t{1024U} * 1024U},
       .maximum_connections = 4U,
       .maximum_accepts_per_poll = 4U});
  auto second_query_server = ReplicatedDistributedVectorAggregateQueryTcpServerV2::start(
      {.worker = {.local_node_id = 12U, .storage = &*storage, .context_provider = &second_provider},
       .listener = {.bind_endpoint = query_endpoints[1]},
       .tls = tls_server_config(),
       .authenticator = &inbound_authenticator,
       .node_authorizer = &node_authorizer,
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000},
                          .maximum_response_frames = 2U,
                          .maximum_response_bytes = std::size_t{1024U} * 1024U},
       .maximum_connections = 4U,
       .maximum_accepts_per_poll = 4U});
  ASSERT_TRUE(first_query_server.has_value()) << first_query_server.error().to_string();
  ASSERT_TRUE(second_query_server.has_value()) << second_query_server.error().to_string();
  for (std::size_t iteration = 0U;
       iteration < 256U && first_query_server->metrics().completed_connections == 0U; ++iteration) {
    ASSERT_TRUE(lifecycle->poll_once(std::chrono::milliseconds{0}).is_ok())
        << lifecycle->failure().to_string();
    ASSERT_TRUE(first_query_server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(first_query_server->metrics().completed_connections, 1U);
  EXPECT_EQ(lifecycle->state(),
            ReplicatedFollowerDistributedVectorAggregateQueryStateV2::kExecuting);
  EXPECT_EQ(lifecycle->result().error().code(), common::StatusCode::kInvalidArgument);

  for (std::size_t iteration = 0U;
       iteration < 4096U &&
       lifecycle->state() == ReplicatedFollowerDistributedVectorAggregateQueryStateV2::kExecuting;
       ++iteration) {
    ASSERT_TRUE(lifecycle->poll_once(std::chrono::milliseconds{1}).is_ok())
        << lifecycle->failure().to_string();
    ASSERT_TRUE(first_query_server->poll_once(std::chrono::milliseconds{0}).is_ok());
    ASSERT_TRUE(second_query_server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(lifecycle->state(), ReplicatedFollowerDistributedVectorAggregateQueryStateV2::kComplete)
      << lifecycle->failure().to_string();
  auto result = lifecycle->result();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  // Guarded by the completed lifecycle and result assertion above.
  // NOLINTBEGIN(bugprone-unchecked-optional-access)
  auto batch = network::decode_query_result_batch(result->get().encoded_batch);
  ASSERT_TRUE(batch.has_value()) << batch.error().to_string();
  ASSERT_EQ(batch->row_count(), 1U);
  ASSERT_EQ(batch->columns().size(), 2U);
  const network::QueryResultCell* count_cell = batch->cell(0U, 0U);
  const network::QueryResultCell* sum_cell = batch->cell(0U, 1U);
  ASSERT_NE(count_cell, nullptr);
  ASSERT_NE(sum_cell, nullptr);
  common::ByteReader count_reader{count_cell->value};
  common::ByteReader sum_reader{sum_cell->value};
  auto count = count_reader.read_i64_le();
  auto sum_bits = sum_reader.read_u64_le();
  ASSERT_TRUE(count.has_value());
  ASSERT_TRUE(sum_bits.has_value());
  EXPECT_EQ(*count, 4);
  EXPECT_EQ(std::bit_cast<double>(*sum_bits), 12.0);
  EXPECT_TRUE(count_reader.empty());
  EXPECT_TRUE(sum_reader.empty());
  // NOLINTEND(bugprone-unchecked-optional-access)
  EXPECT_EQ(first_provider.vector_calls, 2U);
  EXPECT_EQ(second_provider.vector_calls, 2U);
  const auto metrics = lifecycle->metrics();
  EXPECT_EQ(metrics.authority.completed_pairs, 2U);
  ASSERT_TRUE(metrics.execution.has_value());
  // Guarded by the execution-metrics assertion above.
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  EXPECT_EQ(metrics.execution->transport_completed_attempts, 2U);
  EXPECT_EQ(second_query_server->metrics().completed_connections, 1U);
  EXPECT_TRUE(first_query_server->shutdown().is_ok());
  EXPECT_TRUE(second_query_server->shutdown().is_ok());
  EXPECT_TRUE(observation_servers[2].shutdown().is_ok());
  EXPECT_TRUE(observation_servers[3].shutdown().is_ok());
  EXPECT_TRUE(metadata_barrier->shutdown().is_ok());
  EXPECT_TRUE(metadata_runtime->shutdown().is_ok());
}

} // namespace
} // namespace chronos::service
