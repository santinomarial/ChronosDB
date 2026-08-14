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
               const schema::TabletId& tablet_id, const raft::GroupId& group_id) {
  const schema::TableSchema& current = *lineage.current();
  const manifest::TemporalTabletDescriptor tablet{.table_id = current.table_id(),
                                                  .tablet_id = tablet_id,
                                                  .recovery_schema_id = current.schema_id(),
                                                  .recovery_schema_version = current.version(),
                                                  .source_id = group_id,
                                                  .durable_position = 1U,
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

[[nodiscard]] query::DistributedVectorQueryPlan make_plan(const schema::TabletId& tablet_id) {
  return {.query_id = uuid(17U),
          .read_policy = {.consistency = query::DistributedReadConsistency::kFollowerBoundedStale,
                          .maximum_staleness_positions = 1U},
          .fragments = {{.tablet_id = tablet_id,
                         .minimum_event_time = 0,
                         .maximum_event_time = 100,
                         .leader_node = 11U,
                         .local_applied_position = 1U,
                         .known_leader_commit_position = 1U}},
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
  auto publisher = make_publisher(directory.path() / "database", lineage, tablet_id, tablet_group);
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
      auto plan = make_plan(tablet_id);
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

} // namespace
} // namespace chronos::service
