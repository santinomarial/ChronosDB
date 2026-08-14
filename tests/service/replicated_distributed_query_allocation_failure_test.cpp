#include "chronos/cluster/raft_observation_tcp_server.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/network/tls_socket.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/service/replicated_distributed_query.hpp"
#include "support/failing_allocator.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::service {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-query-allocation-XXXXXX").string();
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

class ThreadJoinGuard {
public:
  ThreadJoinGuard(std::atomic<bool>& stop, std::thread& thread) noexcept
      : stop_(stop), thread_(thread) {}

  ~ThreadJoinGuard() {
    stop_and_join();
  }

  ThreadJoinGuard(const ThreadJoinGuard&) = delete;
  ThreadJoinGuard& operator=(const ThreadJoinGuard&) = delete;

  void stop_and_join() noexcept {
    stop_.store(true);
    if (thread_.joinable())
      thread_.join();
  }

private:
  std::atomic<bool>& stop_;
  std::thread& thread_;
};

template <typename Operation>
[[nodiscard]] auto run_failure(const std::size_t fail_after, Operation&& operation) {
  using Result = decltype(operation());
  std::optional<Result> result;
  {
    test::ScopedAllocationFailure failure{fail_after};
    try {
      result.emplace(operation());
    } catch (...) {
      failure.disable();
      throw;
    }
    failure.disable();
  }
  return std::move(*result);
}

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

[[nodiscard]] std::filesystem::path tls_fixture(const char* const name) {
  return std::filesystem::path{CHRONOS_NETWORK_FIXTURE_DIR} / "tls" / name;
}

class QueryAuthenticator final : public network::ConnectionAuthenticator {
public:
  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest&) override {
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = 91U};
  }
};

class QueryNodeAuthorizer final : public cluster::ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal_id,
                                      const raft::NodeId node_id) const override {
    return principal_id == 91U && (node_id == 1U || node_id == 11U || node_id == 12U);
  }
};

class ObservationService final : public cluster::RaftObservationService {
public:
  ObservationService(const raft::NodeId node, const raft::Role role, const raft::LogIndex position)
      : node_(node), role_(role), position_(position) {}

  common::Result<raft::RaftGroupObservation> observe(const raft::GroupId& group_id) override {
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

[[nodiscard]] query::DistributedVectorQueryPlan make_plan(const schema::TabletId& tablet_id,
                                                          const raft::LogIndex applied_position) {
  return {.query_id = uuid(17U),
          .read_policy = {.consistency = query::DistributedReadConsistency::kFollowerBoundedStale,
                          .maximum_staleness_positions = 1U},
          .fragments = {{.tablet_id = tablet_id,
                         .minimum_event_time = 0,
                         .maximum_event_time = 100,
                         .leader_node = 11U,
                         .local_applied_position = applied_position,
                         .known_leader_commit_position = applied_position}},
          .intent = {.mode = query::DistributedVectorPlanMode::kUngroupedAggregate,
                     .aggregates = {{.operation = query::VectorAggregateOperation::kAverage,
                                     .input_index = 1U}}}};
}

TEST(ReplicatedDistributedQueryAllocationFailureTest,
     ClassifiesEveryFollowerVectorOwnerConstructionAllocationAndReleasesSnapshotPin) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const schema::TableSchema schema_value = make_schema();
  const schema::SchemaLineage lineage = schema::SchemaLineage::create(schema_value).value();
  const schema::TabletId tablet_id = id<schema::TabletId>(3U);
  const raft::GroupId metadata_group = uuid(1U);
  const raft::GroupId tablet_group = uuid(8U);
  auto publisher =
      make_publisher(directory.path() / "database", lineage, tablet_id, tablet_group, 1U);
  ASSERT_TRUE(publisher.has_value()) << publisher.error().to_string();

  const raft::MetadataCatalogSnapshot catalog{
      .applied_index = 1U,
      .cluster_nodes = {{11U, "127.0.0.1:1"}, {12U, "127.0.0.1:2"}},
      .schema_definitions = {{"metrics", false,
                              std::make_shared<const schema::TableSchema>(schema_value)}},
      .active_schemas = {{schema_value.table_id(), schema_value.schema_id()}},
      .tablet_placements =
          {{schema_value.table_id(), tablet_id, 1U, {11U, 12U}, std::optional<raft::NodeId>{11U}}},
      .tablet_group_bindings = {{tablet_id, tablet_group}}};
  auto tls_context = network::TlsClientContext::create(
      {.certificate_chain_file = tls_fixture("client.pem").string(),
       .private_key_file = tls_fixture("client-key.pem").string(),
       .trust_store_file = tls_fixture("ca.pem").string(),
       .expected_server_identity = "127.0.0.1"});
  ASSERT_TRUE(tls_context.has_value()) << tls_context.error().to_string();
  const std::array observation_tls_contexts{
      cluster::RaftObservationNodeTlsContext{11U, std::addressof(*tls_context)},
      cluster::RaftObservationNodeTlsContext{12U, std::addressof(*tls_context)}};
  const std::array query_tls_contexts{
      cluster::DistributedQueryNodeTlsContext{11U, std::addressof(*tls_context)},
      cluster::DistributedQueryNodeTlsContext{12U, std::addressof(*tls_context)}};
  const std::array<std::uint32_t, 2U> projection{0U, 1U};
  QueryAuthenticator authenticator;
  QueryNodeAuthorizer authorizer;
  auto barrier = ReplicatedReadBarrier::create_transported({metadata_group});
  ASSERT_TRUE(barrier.has_value()) << barrier.error().to_string();

  const cluster::RaftObservationTcpBatchConstructionConfig authority_config{
      .source_node_id = 1U,
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
      .maximum_pairs = 1U};
  const ReplicatedDistributedVectorAggregateQueryConfigV2 query_config{
      .source_node_id = 1U,
      .read_barrier = std::addressof(*barrier),
      .metadata_group_id = metadata_group,
      .catalog = std::cref(catalog),
      .table_id = schema_value.table_id(),
      .destination_column_ordinals = projection,
      .tls_contexts = query_tls_contexts,
      .authenticator = &authenticator,
      .node_authorizer = &authorizer,
      .binding_limits = {.maximum_fragments = 1U,
                         .maximum_total_projection_ordinals = projection.size()}};

  std::shared_ptr<const manifest::LoadedTemporalManifestGeneration> selected_manifest;
  {
    auto snapshot = publisher->snapshot();
    ASSERT_TRUE(snapshot.has_value()) << snapshot.error().to_string();
    selected_manifest = snapshot->selected_manifest();
  }
  const long baseline_use_count = selected_manifest.use_count();

  bool saw_failure = false;
  bool saw_success = false;
  for (std::size_t fail_after = 0U; fail_after < 512U; ++fail_after) {
    SCOPED_TRACE(testing::Message{} << "fail_after=" << fail_after);
    {
      auto snapshot = publisher->snapshot();
      ASSERT_TRUE(snapshot.has_value()) << snapshot.error().to_string();
      auto plan = make_plan(tablet_id, 1U);
      query::DistributedVectorResultSchema result_schema{
          .columns = {{"average", schema_value.columns()[1].type(), true}}};
      auto result = run_failure(fail_after, [&] {
        return ReplicatedFollowerDistributedVectorAggregateQueryV2::create(
            std::move(plan), std::move(*snapshot), std::move(result_schema), authority_config,
            query_config);
      });
      if (!result.has_value()) {
        saw_failure = true;
        EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted)
            << "fail_after=" << fail_after << ": " << result.error().to_string();
      } else {
        saw_success = true;
        EXPECT_EQ(result->state(),
                  ReplicatedFollowerDistributedVectorAggregateQueryStateV2::kAcquiringAuthority);
        EXPECT_EQ(result->metrics().authority.active_pairs, 1U);
        EXPECT_EQ(result->cancel().code(), common::StatusCode::kCancelled);
        EXPECT_EQ(result->metrics().authority.active_pairs, 0U);
      }
    }
    EXPECT_EQ(selected_manifest.use_count(), baseline_use_count) << "fail_after=" << fail_after;
    if (saw_success)
      break;
  }

  EXPECT_TRUE(saw_failure);
  EXPECT_TRUE(saw_success);
  EXPECT_TRUE(barrier->shutdown().is_ok());
}

TEST(ReplicatedDistributedQueryAllocationFailureTest,
     ClassifiesEveryFollowerVectorAuthorityTransitionAllocationAndFailsAtomically) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  ASSERT_TRUE(std::filesystem::create_directory(directory.path() / "raft"));
  const raft::GroupId metadata_group = uuid(31U);
  const raft::GroupId tablet_group = uuid(32U);
  auto runtime = raft::AsyncDurableMultiRaftRuntime::create_new(
      11U, {.directory_path = (directory.path() / "raft").string()},
      {{metadata_group, {11U}}, {tablet_group, {11U}}});
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  for (const raft::GroupId group_id : {metadata_group, tablet_group}) {
    auto election = runtime->try_submit({{group_id, raft::StartElectionOperation{}}});
    ASSERT_TRUE(election.has_value()) << election.error().to_string();
    auto elected = election->wait();
    ASSERT_TRUE(elected.has_value()) << elected.error().to_string();
    ASSERT_EQ(elected->size(), 1U);
    ASSERT_TRUE(elected->front().status.is_ok()) << elected->front().status.to_string();
  }
  auto inspection_barrier =
      ReplicatedReadBarrier::create_local(std::addressof(*runtime), {tablet_group, metadata_group});
  ASSERT_TRUE(inspection_barrier.has_value()) << inspection_barrier.error().to_string();
  auto inspected_authority = inspection_barrier->await_authority();
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
  const auto tablet_authority = std::ranges::find(
      *inspected_authority, tablet_group,
      [](const ReplicatedReadAuthority& authority) { return authority.observation.group_id; });
  const auto metadata_authority = std::ranges::find(
      *inspected_authority, metadata_group,
      [](const ReplicatedReadAuthority& authority) { return authority.observation.group_id; });
  ASSERT_NE(tablet_authority, inspected_authority->end());
  ASSERT_NE(metadata_authority, inspected_authority->end());
  const raft::LogIndex applied_position = tablet_authority->barrier.barrier.read_index;
  const raft::LogIndex metadata_applied_position = metadata_authority->barrier.barrier.read_index;
  ASSERT_TRUE(inspection_barrier->shutdown().is_ok());
  auto query_barrier =
      ReplicatedReadBarrier::create_local(std::addressof(*runtime), {metadata_group});
  ASSERT_TRUE(query_barrier.has_value()) << query_barrier.error().to_string();

  const schema::TableSchema schema_value = make_schema();
  const schema::SchemaLineage lineage = schema::SchemaLineage::create(schema_value).value();
  const schema::TabletId tablet_id = id<schema::TabletId>(33U);
  auto publisher = make_publisher(directory.path() / "database", lineage, tablet_id, tablet_group,
                                  applied_position);
  ASSERT_TRUE(publisher.has_value()) << publisher.error().to_string();
  auto tls_context = network::TlsClientContext::create(
      {.certificate_chain_file = tls_fixture("client.pem").string(),
       .private_key_file = tls_fixture("client-key.pem").string(),
       .trust_store_file = tls_fixture("ca.pem").string(),
       .expected_server_identity = "127.0.0.1"});
  ASSERT_TRUE(tls_context.has_value()) << tls_context.error().to_string();
  QueryAuthenticator authenticator;
  QueryNodeAuthorizer authorizer;
  ObservationService leader_service{11U, raft::Role::kLeader, applied_position};
  ObservationService follower_service{12U, raft::Role::kFollower, applied_position};
  auto leader_receiver = cluster::RaftObservationReceiver::create(
      {.local_node_id = 11U, .authorizer = &authorizer, .service = &leader_service});
  auto follower_receiver = cluster::RaftObservationReceiver::create(
      {.local_node_id = 12U, .authorizer = &authorizer, .service = &follower_service});
  ASSERT_TRUE(leader_receiver.has_value()) << leader_receiver.error().to_string();
  ASSERT_TRUE(follower_receiver.has_value()) << follower_receiver.error().to_string();
  auto server_config = [&](cluster::RaftObservationReceiver& receiver) {
    return cluster::RaftObservationTcpServerConfig{
        .listener = {},
        .tls = observation_server_tls_config(),
        .authenticator = &authenticator,
        .receiver = &receiver,
        .session_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                           .exchange_timeout = std::chrono::milliseconds{1000}},
        .maximum_connections = 16U,
        .maximum_accepts_per_poll = 16U};
  };
  auto leader_server = cluster::RaftObservationTcpServer::start(server_config(*leader_receiver));
  auto follower_server =
      cluster::RaftObservationTcpServer::start(server_config(*follower_receiver));
  ASSERT_TRUE(leader_server.has_value()) << leader_server.error().to_string();
  ASSERT_TRUE(follower_server.has_value()) << follower_server.error().to_string();

  const raft::MetadataCatalogSnapshot catalog{
      .applied_index = metadata_applied_position,
      .cluster_nodes = {{11U, endpoint_text(leader_server->bound_endpoint())},
                        {12U, endpoint_text(follower_server->bound_endpoint())}},
      .schema_definitions = {{"metrics", false,
                              std::make_shared<const schema::TableSchema>(schema_value)}},
      .active_schemas = {{schema_value.table_id(), schema_value.schema_id()}},
      .tablet_placements =
          {{schema_value.table_id(), tablet_id, 1U, {11U, 12U}, std::optional<raft::NodeId>{11U}}},
      .tablet_group_bindings = {{tablet_id, tablet_group}}};
  const std::array observation_tls_contexts{
      cluster::RaftObservationNodeTlsContext{11U, std::addressof(*tls_context)},
      cluster::RaftObservationNodeTlsContext{12U, std::addressof(*tls_context)}};
  const std::array query_tls_contexts{
      cluster::DistributedQueryNodeTlsContext{12U, std::addressof(*tls_context)}};
  const std::array<std::uint32_t, 2U> projection{0U, 1U};
  const cluster::RaftObservationTcpBatchConstructionConfig authority_config{
      .source_node_id = 1U,
      .first_correlation_id = 181U,
      .tls_contexts = observation_tls_contexts,
      .authenticator = &authenticator,
      .node_authorizer = &authorizer,
      .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                         .exchange_timeout = std::chrono::milliseconds{1000}},
      .connect_timeout = std::chrono::milliseconds{1000},
      .retry = {.maximum_attempts = 1U,
                .initial_backoff = std::chrono::milliseconds{1},
                .maximum_backoff = std::chrono::milliseconds{1}},
      .maximum_pairs = 1U};
  const ReplicatedDistributedVectorAggregateQueryConfigV2 query_config{
      .source_node_id = 1U,
      .read_barrier = std::addressof(*query_barrier),
      .metadata_group_id = metadata_group,
      .catalog = std::cref(catalog),
      .table_id = schema_value.table_id(),
      .destination_column_ordinals = projection,
      .tls_contexts = query_tls_contexts,
      .authenticator = &authenticator,
      .node_authorizer = &authorizer,
      .binding_limits = {.maximum_fragments = 1U,
                         .maximum_total_projection_ordinals = projection.size()},
      .execution_limits = {.maximum_query_memory_bytes = 1U << 20U},
      .connect_timeout = std::chrono::milliseconds{1000}};

  std::atomic<bool> stop_server;
  std::atomic<bool> server_failed;
  std::thread server_thread([&] {
    while (!stop_server.load() && !server_failed.load()) {
      if (!leader_server->poll_once(std::chrono::milliseconds{1}).is_ok() ||
          !follower_server->poll_once(std::chrono::milliseconds{1}).is_ok()) {
        server_failed.store(true);
      }
    }
  });
  ThreadJoinGuard server_thread_guard{stop_server, server_thread};

  std::shared_ptr<const manifest::LoadedTemporalManifestGeneration> selected_manifest;
  {
    auto snapshot = publisher->snapshot();
    ASSERT_TRUE(snapshot.has_value()) << snapshot.error().to_string();
    selected_manifest = snapshot->selected_manifest();
  }
  const long baseline_use_count = selected_manifest.use_count();
  bool saw_failure = false;
  bool saw_success = false;
  for (std::size_t fail_after = 0U; fail_after < 512U; ++fail_after) {
    SCOPED_TRACE(testing::Message{} << "fail_after=" << fail_after);
    {
      auto snapshot = publisher->snapshot();
      ASSERT_TRUE(snapshot.has_value()) << snapshot.error().to_string();
      auto lifecycle = ReplicatedFollowerDistributedVectorAggregateQueryV2::create(
          make_plan(tablet_id, applied_position), std::move(*snapshot),
          query::DistributedVectorResultSchema{
              .columns = {{"average", schema_value.columns()[1].type(), true}}},
          authority_config, query_config);
      ASSERT_TRUE(lifecycle.has_value()) << lifecycle.error().to_string();

      common::Status progress;
      bool terminal = false;
      {
        test::ScopedAllocationFailure failure{fail_after};
        try {
          for (std::size_t poll = 0U; poll < 8192U; ++poll) {
            progress = lifecycle->poll_once(std::chrono::milliseconds{1});
            if (!progress.is_ok() ||
                lifecycle->state() !=
                    ReplicatedFollowerDistributedVectorAggregateQueryStateV2::kAcquiringAuthority) {
              terminal = true;
              break;
            }
          }
        } catch (...) {
          failure.disable();
          throw;
        }
        failure.disable();
      }
      ASSERT_TRUE(terminal);
      if (!progress.is_ok()) {
        saw_failure = true;
        EXPECT_EQ(progress.code(), common::StatusCode::kResourceExhausted) << progress.to_string();
        EXPECT_EQ(lifecycle->state(),
                  ReplicatedFollowerDistributedVectorAggregateQueryStateV2::kFailed);
        EXPECT_EQ(lifecycle->failure(), progress);
        EXPECT_EQ(lifecycle->metrics().authority.active_pairs, 0U);
        EXPECT_FALSE(lifecycle->metrics().execution.has_value());
        EXPECT_EQ(lifecycle->result().error(), progress);
        EXPECT_EQ(lifecycle->poll_once(std::chrono::milliseconds{0}), progress);
      } else {
        saw_success = true;
        EXPECT_EQ(lifecycle->state(),
                  ReplicatedFollowerDistributedVectorAggregateQueryStateV2::kExecuting);
        EXPECT_EQ(lifecycle->metrics().authority.completed_pairs, 1U);
        ASSERT_TRUE(lifecycle->metrics().execution.has_value());
        // Guarded by the execution-metrics assertion above.
        // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
        EXPECT_EQ(lifecycle->metrics().execution->active_attempts, 0U);
        EXPECT_EQ(lifecycle->result().error().code(), common::StatusCode::kInvalidArgument);
        EXPECT_EQ(lifecycle->cancel().code(), common::StatusCode::kCancelled);
        EXPECT_EQ(lifecycle->metrics().authority.active_pairs, 0U);
        ASSERT_TRUE(lifecycle->metrics().execution.has_value());
        // Guarded by the execution-metrics assertion above.
        // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
        EXPECT_EQ(lifecycle->metrics().execution->active_attempts, 0U);
      }
    }
    EXPECT_EQ(selected_manifest.use_count(), baseline_use_count) << "fail_after=" << fail_after;
    ASSERT_FALSE(server_failed.load());
    if (saw_success)
      break;
  }
  EXPECT_TRUE(saw_failure);
  EXPECT_TRUE(saw_success);

  server_thread_guard.stop_and_join();
  EXPECT_FALSE(server_failed.load());
  EXPECT_TRUE(follower_server->shutdown().is_ok());
  EXPECT_TRUE(leader_server->shutdown().is_ok());
  EXPECT_TRUE(query_barrier->shutdown().is_ok());
  EXPECT_TRUE(runtime->shutdown().is_ok());
}

} // namespace
} // namespace chronos::service
