#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/common/byte_reader.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/query/binder.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/physical_lowering.hpp"
#include "chronos/raft/metadata_codec.hpp"
#include "chronos/raft/schema_definition_codec.hpp"
#include "chronos/raft/tablet_group_binding_codec.hpp"
#include "chronos/service/replicated_ingest_database.hpp"
#include "columnar/columnar_test_support.hpp"
#include "ingest/ingest_test_support.hpp"

#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
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
        (std::filesystem::temp_directory_path() / "chronos-replicated-database-XXXXXX").string();
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

[[nodiscard]] common::Uuid id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(static_cast<std::byte>(seed));
  return common::Uuid{bytes};
}

[[nodiscard]] raft::GroupId metadata_group() {
  return id(0x70U);
}

[[nodiscard]] raft::GroupId tablet_group() {
  return id(0x71U);
}

[[nodiscard]] raft::GroupId remote_tablet_group() {
  return id(0x73U);
}

[[nodiscard]] schema::TabletId tablet_id() {
  return columnar::test::id<schema::TabletId>(83U);
}

[[nodiscard]] runtime::DatabaseBootstrapDescriptor descriptor() {
  return {.database_id = id(0x72U),
          .metadata_group_id = metadata_group(),
          .local_node_id = 1U,
          .mutable_head_rows = 8U,
          .maximum_sealed_generations = 2U,
          .variable_column_bytes = 8U,
          .maximum_retry_entries = 8U,
          .wal_segment_target_bytes = 64U * 1024U,
          .raft_segment_target_bytes = 64U * 1024U};
}

[[nodiscard]] std::vector<raft::RaftGroupConfiguration> groups() {
  return {{metadata_group(), {1U}}, {tablet_group(), {1U}}};
}

[[nodiscard]] std::vector<std::byte> command() {
  auto batch = columnar::OwnedColumnarBatch::create(columnar::test::batch_schema(),
                                                    columnar::test::batch_columns())
                   .value();
  const auto batch_bytes = columnar::encode_columnar_batch_v1(batch).value();
  const auto append = ingest::encode_columnar_append_v1(
                          {.client_id = ingest::test::request_id<ingest::ClientId>(3U),
                           .client_batch_id = ingest::test::request_id<ingest::ClientBatchId>(35U),
                           .tablet_id = tablet_id()},
                          batch_bytes)
                          .value();
  return {append.bytes().begin(), append.bytes().end()};
}

[[nodiscard]] network::NetworkTask request() {
  const network::IngestProtocolContext context{.protocol_major = network::kProtocolV2Major,
                                               .protocol_minor = network::kProtocolV2LatestMinor,
                                               .feature_bits =
                                                   network::kProtocolV2QuorumSyncFeature};
  auto payload =
      network::encode_ingest_request(network::DurabilityMode::kQuorumSync, command(), context)
          .value();
  return {.connection_id = 20U,
          .principal_id = 19U,
          .protocol = {.protocol_major = context.protocol_major,
                       .protocol_minor = context.protocol_minor,
                       .feature_bits = context.feature_bits,
                       .maximum_payload_size = network::kDefaultMaximumPayloadSize},
          .frame = {.header = {.protocol_major = context.protocol_major,
                               .protocol_minor = context.protocol_minor,
                               .message_type = network::MessageType::kIngestRequest,
                               .request_id = 1U,
                               .payload_size = static_cast<std::uint32_t>(payload.size())},
                    .payload = std::move(payload)}};
}

[[nodiscard]] ReplicatedIngestRuntimeConfig
initial_runtime_config(const runtime::DatabaseBootstrap& bootstrap) {
  auto tablet = ingest::TabletState::create(
                    columnar::test::batch_schema(), tablet_id(),
                    {.head_capacity = {.row_capacity = 8U, .variable_value_bytes = {0U, 8U, 0U}},
                     .maximum_schema_versions = 1U,
                     .maximum_sealed_generations = 2U,
                     .maximum_retry_entries = 8U})
                    .value();
  auto retries = ingest::RetryDirectory::create({.maximum_entries = 8U}).value();
  std::vector<ingest::AsyncRaftTabletApplicationConfig> tablets;
  tablets.push_back({.group_id = tablet_group(),
                     .snapshot_storage = std::nullopt,
                     .retry_directory = std::move(retries),
                     .tablet = std::move(tablet),
                     .retained_schemas = {columnar::test::batch_schema()},
                     .decode_limits = {}});
  return {.local_node_id = 1U,
          .log = {.directory_path = bootstrap.raft_directory_path(),
                  .target_segment_size = descriptor().raft_segment_target_bytes},
          .groups = groups(),
          .tablets = std::move(tablets),
          .metadata = {.group_id = metadata_group()}};
}

void elect_and_provision(ReplicatedIngestRuntime& owner, const bool include_remote = true) {
  auto election = owner.runtime()->try_submit({{metadata_group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value());
  ASSERT_TRUE(election->wait().has_value());
  const raft::ProposeOperation schema{
      raft::kRaftSchemaDefinitionEntryType,
      raft::encode_schema_definition_v1(
          {.name = "events", .quoted = false, .schema = columnar::test::batch_schema()})
          .value()};
  const raft::ProposeOperation policy{
      raft::kRaftMetadataCommandEntryType,
      raft::encode_metadata_command_v1(
          raft::TablePolicyMetadata{columnar::test::batch_schema()->table_id(), 1'000'000'000LL,
                                    86'400'000'000'000LL, 86'400'000'000'000LL, 0LL, 8U})
          .value()};
  const raft::ProposeOperation placement{
      raft::kRaftMetadataCommandEntryType,
      raft::encode_metadata_command_v1(
          raft::TabletPlacementMetadata{
              columnar::test::batch_schema()->table_id(), tablet_id(), 1U, {1U}, 1U})
          .value()};
  const raft::ProposeOperation binding{
      raft::kRaftTabletGroupBindingEntryType,
      raft::encode_tablet_group_binding_v1({tablet_id(), tablet_group()}).value()};
  const schema::TabletId remote_tablet = columnar::test::id<schema::TabletId>(84U);
  const raft::ProposeOperation remote_placement{
      raft::kRaftMetadataCommandEntryType,
      raft::encode_metadata_command_v1(
          raft::TabletPlacementMetadata{
              columnar::test::batch_schema()->table_id(), remote_tablet, 1U, {2U}, 2U})
          .value()};
  const raft::ProposeOperation remote_binding{
      raft::kRaftTabletGroupBindingEntryType,
      raft::encode_tablet_group_binding_v1({remote_tablet, remote_tablet_group()}).value()};
  auto metadata = include_remote
                      ? owner.runtime()->try_submit({{metadata_group(), schema},
                                                     {metadata_group(), policy},
                                                     {metadata_group(), placement},
                                                     {metadata_group(), binding},
                                                     {metadata_group(), remote_placement},
                                                     {metadata_group(), remote_binding}})
                      : owner.runtime()->try_submit({{metadata_group(), schema},
                                                     {metadata_group(), policy},
                                                     {metadata_group(), placement},
                                                     {metadata_group(), binding}});
  ASSERT_TRUE(metadata.has_value());
  ASSERT_TRUE(metadata->wait().has_value());
  election = owner.runtime()->try_submit({{tablet_group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value());
  ASSERT_TRUE(election->wait().has_value());
}

[[nodiscard]] network::NetworkTask await_response(ReplicatedIngestRuntime& owner) {
  for (std::size_t attempt = 0U; attempt < 10'000U; ++attempt) {
    auto response = owner.coordinator()->poll();
    EXPECT_TRUE(response.has_value());
    if (response.has_value() && response->has_value())
      return std::move(**response);
    std::this_thread::yield();
  }
  ADD_FAILURE() << "replicated database response timed out";
  return {};
}

TEST(ReplicatedIngestDatabaseTest, RebuildsTabletOwnersFromCommittedMetadataUnderRootLock) {
  TemporaryDirectory directory;
  runtime::DatabaseBootstrapConfig bootstrap_config{.database_root = directory.path().string(),
                                                    .new_database = descriptor()};
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(bootstrap_config);
  ASSERT_TRUE(bootstrap.has_value()) << bootstrap.error().to_string();
  auto initial = ReplicatedIngestRuntime::create_new(initial_runtime_config(*bootstrap));
  ASSERT_TRUE(initial.has_value()) << initial.error().to_string();
  elect_and_provision(*initial);
  ASSERT_TRUE(initial->coordinator()->admit(request()).is_ok());
  auto applied = await_response(*initial);
  ASSERT_EQ(applied.frame.header.message_type,
            network::MessageType::kQuorumSyncIngestAcknowledgement);
  ASSERT_TRUE(initial->shutdown().is_ok());
  ASSERT_TRUE(bootstrap->close().is_ok());

  auto database =
      ReplicatedIngestDatabase::open_existing({.bootstrap = bootstrap_config, .groups = groups()});
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  EXPECT_TRUE(database->is_running());
  ASSERT_NE(database->ingest_runtime(), nullptr);
  auto catalog = database->ingest_runtime()->metadata_application()->catalog_snapshot();
  ASSERT_TRUE(catalog.has_value());
  ASSERT_EQ((*catalog)->tablet_group_bindings.size(), 2U);
  auto snapshot = database->ingest_runtime()->tablet_application()->snapshot(tablet_group());
  ASSERT_TRUE(snapshot.has_value());
  EXPECT_EQ(snapshot->visible_row_count(), 2U);

  auto election = database->ingest_runtime()->runtime()->try_submit(
      {{tablet_group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value());
  ASSERT_TRUE(election->wait().has_value());
  ASSERT_TRUE(database->ingest_runtime()->coordinator()->admit(request()).is_ok());
  auto retry = await_response(*database->ingest_runtime());
  auto acknowledgement = network::decode_quorum_sync_ingest_acknowledgement(retry.frame.payload);
  ASSERT_TRUE(acknowledgement.has_value());
  EXPECT_EQ(acknowledgement->outcome, network::IngestOutcome::kMatchingRetry);
  EXPECT_TRUE(database->shutdown().is_ok());
  EXPECT_TRUE(database->shutdown().is_ok());
  EXPECT_FALSE(database->is_running());
  EXPECT_EQ(database->ingest_runtime(), nullptr);
}

TEST(ReplicatedIngestDatabaseTest, PinsCommittedWholeTableQueryStateBeyondOwnerShutdown) {
  TemporaryDirectory directory;
  runtime::DatabaseBootstrapConfig bootstrap_config{.database_root = directory.path().string(),
                                                    .new_database = descriptor()};
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(bootstrap_config);
  ASSERT_TRUE(bootstrap.has_value()) << bootstrap.error().to_string();
  auto initial = ReplicatedIngestRuntime::create_new(initial_runtime_config(*bootstrap));
  ASSERT_TRUE(initial.has_value()) << initial.error().to_string();
  elect_and_provision(*initial, false);
  ASSERT_TRUE(initial->coordinator()->admit(request()).is_ok());
  ASSERT_EQ(await_response(*initial).frame.header.message_type,
            network::MessageType::kQuorumSyncIngestAcknowledgement);
  ASSERT_TRUE(initial->shutdown().is_ok());
  ASSERT_TRUE(bootstrap->close().is_ok());

  auto database =
      ReplicatedIngestDatabase::open_existing({.bootstrap = bootstrap_config, .groups = groups()});
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  auto snapshot = database->acquire_query_snapshot();
  ASSERT_TRUE(snapshot.has_value()) << snapshot.error().to_string();
  ASSERT_EQ(snapshot->catalog()->tables().size(), 1U);
  auto parsed = query::parse_sql_v1_select("SELECT count(*) AS rows FROM events");
  ASSERT_TRUE(parsed.has_value()) << parsed.error().status().to_string();
  auto bound = query::bind_sql_v1_select(std::move(*parsed), snapshot->catalog());
  ASSERT_TRUE(bound.has_value()) << bound.error().status().to_string();
  auto lowered = query::lower_bound_sql_select(*bound);
  ASSERT_TRUE(lowered.has_value()) << lowered.error().status().to_string();
  ASSERT_TRUE(database->shutdown().is_ok());

  query::QueryResourceContext resources =
      query::QueryResourceContext::create(8U * 1024U * 1024U).value();
  const auto schema = columnar::test::batch_schema();
  auto pipeline = snapshot->instantiate_table_pipeline(resources, schema->table_id(),
                                                       schema->schema_id(), *lowered);
  ASSERT_TRUE(pipeline.has_value()) << pipeline.error().to_string();
  auto step = (*pipeline)->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().to_string();
  ASSERT_EQ(step->kind(), query::PhysicalOperatorStepKind::kChunk);
  const auto cell = step->chunk()->chunk().cell({.column_ordinal = 0U, .selected_row = 0U});
  ASSERT_TRUE(cell.has_value());
  common::ByteReader count{cell->bytes().value()};
  EXPECT_EQ(count.read_i64_le().value(), 2);
  step = (*pipeline)->next(resources);
  ASSERT_TRUE(step.has_value());
  EXPECT_EQ(step->kind(), query::PhysicalOperatorStepKind::kEnd);
  pipeline->reset();
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(ReplicatedIngestDatabaseTest, RejectsAQueryOverAPartiallyResidentTable) {
  TemporaryDirectory directory;
  runtime::DatabaseBootstrapConfig bootstrap_config{.database_root = directory.path().string(),
                                                    .new_database = descriptor()};
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(bootstrap_config);
  ASSERT_TRUE(bootstrap.has_value());
  auto initial = ReplicatedIngestRuntime::create_new(initial_runtime_config(*bootstrap));
  ASSERT_TRUE(initial.has_value());
  elect_and_provision(*initial);
  ASSERT_TRUE(initial->shutdown().is_ok());
  ASSERT_TRUE(bootstrap->close().is_ok());

  auto database =
      ReplicatedIngestDatabase::open_existing({.bootstrap = bootstrap_config, .groups = groups()});
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  auto snapshot = database->acquire_query_snapshot();
  ASSERT_TRUE(snapshot.has_value()) << snapshot.error().to_string();
  auto parsed = query::parse_sql_v1_select("SELECT count(*) FROM events");
  ASSERT_TRUE(parsed.has_value());
  auto bound = query::bind_sql_v1_select(std::move(*parsed), snapshot->catalog());
  ASSERT_TRUE(bound.has_value());
  auto lowered = query::lower_bound_sql_select(*bound);
  ASSERT_TRUE(lowered.has_value());
  query::QueryResourceContext resources =
      query::QueryResourceContext::create(1024U * 1024U).value();
  const auto schema = columnar::test::batch_schema();
  auto pipeline = snapshot->instantiate_table_pipeline(resources, schema->table_id(),
                                                       schema->schema_id(), *lowered);
  ASSERT_FALSE(pipeline.has_value());
  EXPECT_EQ(pipeline.error().code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  EXPECT_TRUE(database->shutdown().is_ok());
}

TEST(ReplicatedIngestDatabaseTest, RejectsAnOmittedLocallyPlacedTabletGroup) {
  TemporaryDirectory directory;
  runtime::DatabaseBootstrapConfig bootstrap_config{.database_root = directory.path().string(),
                                                    .new_database = descriptor()};
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(bootstrap_config);
  ASSERT_TRUE(bootstrap.has_value());
  auto initial = ReplicatedIngestRuntime::create_new(initial_runtime_config(*bootstrap));
  ASSERT_TRUE(initial.has_value());
  elect_and_provision(*initial);
  ASSERT_TRUE(initial->shutdown().is_ok());
  ASSERT_TRUE(bootstrap->close().is_ok());
  auto database = ReplicatedIngestDatabase::open_existing(
      {.bootstrap = bootstrap_config, .groups = {{metadata_group(), {1U}}}});
  ASSERT_FALSE(database.has_value());
  EXPECT_EQ(database.error().code(), common::StatusCode::kCorruption);
}

} // namespace
} // namespace chronos::service
