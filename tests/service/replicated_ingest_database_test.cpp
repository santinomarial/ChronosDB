#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/common/byte_reader.hpp"
#include "chronos/ingest/raft_tablet_state_machine.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/query/binder.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/physical_lowering.hpp"
#include "chronos/raft/metadata_codec.hpp"
#include "chronos/raft/metadata_runtime.hpp"
#include "chronos/raft/schema_definition_codec.hpp"
#include "chronos/raft/tablet_group_binding_codec.hpp"
#include "chronos/service/native_protocol_service.hpp"
#include "chronos/service/replicated_ingest_database.hpp"
#include "chronos/service/replicated_ingest_service.hpp"
#include "chronos/service/replicated_read_barrier.hpp"
#include "columnar/columnar_test_support.hpp"
#include "ingest/ingest_test_support.hpp"

#include <algorithm>
#include <array>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::service {
namespace {

class RecordingStartupObserver final : public ReplicatedIngestDatabaseStartupObserver {
public:
  void on_startup_stage(const ReplicatedIngestDatabaseStartupStage stage) noexcept override {
    if (count < stages.size())
      stages[count] = stage;
    else
      overflow = true;
    ++count;
  }

  std::array<ReplicatedIngestDatabaseStartupStage, 4U> stages{};
  std::size_t count{};
  bool overflow{};
};

class RecordingShutdownObserver final : public ReplicatedIngestDatabaseShutdownObserver {
public:
  void on_shutdown_stage(const ReplicatedIngestDatabaseShutdownStage stage) noexcept override {
    if (count < stages.size())
      stages[count] = stage;
    else
      overflow = true;
    ++count;
  }

  std::array<ReplicatedIngestDatabaseShutdownStage, 6U> stages{};
  std::size_t count{};
  bool overflow{};
};

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

[[nodiscard]] raft::GroupId second_tablet_group() {
  return id(0x74U);
}

[[nodiscard]] schema::TabletId tablet_id() {
  return columnar::test::id<schema::TabletId>(83U);
}

[[nodiscard]] schema::TabletId second_tablet_id() {
  return columnar::test::id<schema::TabletId>(85U);
}

[[nodiscard]] runtime::DatabaseBootstrapDescriptor descriptor() {
  return {.database_id = id(0x72U),
          .metadata_group_id = metadata_group(),
          .local_node_id = 1U,
          .mutable_head_rows = 8U,
          .maximum_sealed_generations = 2U,
          .variable_column_bytes = 8U,
          .maximum_retry_entries = 8U,
          .wal_segment_target_bytes = std::uint64_t{64U} * 1024U,
          .raft_segment_target_bytes = std::uint64_t{64U} * 1024U};
}

[[nodiscard]] std::vector<raft::RaftGroupConfiguration> groups() {
  return {{metadata_group(), {1U}}, {tablet_group(), {1U}}};
}

[[nodiscard]] std::vector<raft::RaftGroupConfiguration> joint_groups() {
  return {{metadata_group(), {1U}}, {tablet_group(), {1U, 2U}}};
}

[[nodiscard]] std::vector<raft::RaftGroupConfiguration> two_node_groups() {
  return {{metadata_group(), {1U, 2U}}, {tablet_group(), {1U, 2U}}};
}

[[nodiscard]] std::vector<raft::RaftGroupConfiguration> multi_tablet_groups() {
  return {{metadata_group(), {1U}}, {tablet_group(), {1U}}, {second_tablet_group(), {1U}}};
}

[[nodiscard]] std::vector<std::byte> command(std::shared_ptr<const schema::TableSchema> schema,
                                             std::vector<columnar::OwnedColumnVector> columns,
                                             const std::uint8_t request_seed,
                                             const schema::TabletId target_tablet = tablet_id()) {
  auto batch = columnar::OwnedColumnarBatch::create(std::move(schema), std::move(columns)).value();
  const auto batch_bytes = columnar::encode_columnar_batch_v1(batch).value();
  const auto append =
      ingest::encode_columnar_append_v1(
          {.client_id = ingest::test::request_id<ingest::ClientId>(request_seed),
           .client_batch_id = ingest::test::request_id<ingest::ClientBatchId>(request_seed + 32U),
           .tablet_id = target_tablet},
          batch_bytes)
          .value();
  return {append.bytes().begin(), append.bytes().end()};
}

[[nodiscard]] std::vector<std::byte> command() {
  return command(columnar::test::batch_schema(), columnar::test::batch_columns(), 3U);
}

[[nodiscard]] std::vector<std::byte> successor_command() {
  return command(columnar::test::successor_batch_schema(),
                 columnar::test::successor_batch_columns(), 4U);
}

[[nodiscard]] network::NetworkTask request(std::vector<std::byte> command_bytes = command(),
                                           const std::uint64_t request_id = 1U) {
  const network::IngestProtocolContext context{.protocol_major = network::kProtocolV2Major,
                                               .protocol_minor = network::kProtocolV2LatestMinor,
                                               .feature_bits =
                                                   network::kProtocolV2QuorumSyncFeature};
  auto payload =
      network::encode_ingest_request(network::DurabilityMode::kQuorumSync, command_bytes, context)
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
                               .request_id = request_id,
                               .payload_size = static_cast<std::uint32_t>(payload.size())},
                    .payload = std::move(payload)}};
}

[[nodiscard]] network::NetworkTask query_request(const std::string_view sql,
                                                 const bool redirects = false) {
  auto payload = network::encode_query_request(sql).value();
  const std::uint64_t features = network::kProtocolV2QuorumSyncFeature |
                                 (redirects ? network::kProtocolV2LeaderRedirectFeature : 0U);
  return {.connection_id = 21U,
          .principal_id = 19U,
          .protocol = {.protocol_major = network::kProtocolV2Major,
                       .protocol_minor = network::kProtocolV2LatestMinor,
                       .feature_bits = features,
                       .maximum_payload_size = network::kDefaultMaximumPayloadSize},
          .frame = {.header = {.protocol_major = network::kProtocolV2Major,
                               .protocol_minor = network::kProtocolV2LatestMinor,
                               .message_type = network::MessageType::kQueryRequest,
                               .request_id = 4U,
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

[[nodiscard]] ReplicatedIngestRuntimeConfig
multi_tablet_runtime_config(const runtime::DatabaseBootstrap& bootstrap) {
  auto config = initial_runtime_config(bootstrap);
  config.groups = multi_tablet_groups();
  auto tablet = ingest::TabletState::create(
                    columnar::test::batch_schema(), second_tablet_id(),
                    {.head_capacity = {.row_capacity = 8U, .variable_value_bytes = {0U, 8U, 0U}},
                     .maximum_schema_versions = 1U,
                     .maximum_sealed_generations = 2U,
                     .maximum_retry_entries = 8U})
                    .value();
  auto retries = ingest::RetryDirectory::create({.maximum_entries = 8U}).value();
  config.tablets.push_back({.group_id = second_tablet_group(),
                            .snapshot_storage = std::nullopt,
                            .retry_directory = std::move(retries),
                            .tablet = std::move(tablet),
                            .retained_schemas = {columnar::test::batch_schema()},
                            .decode_limits = {}});
  return config;
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

[[nodiscard]] raft::RaftGroupObservation observe(ReplicatedIngestRuntime& owner,
                                                 const raft::GroupId& group_id) {
  auto completion = owner.runtime()->try_observe_group(group_id);
  EXPECT_TRUE(completion.has_value());
  if (!completion.has_value())
    return {};
  auto results = completion->wait();
  EXPECT_TRUE(results.has_value());
  if (!results.has_value() || results->size() != 1U || !results->front().observation.has_value())
    return {};
  EXPECT_TRUE(results->front().status.is_ok()) << results->front().status.to_string();
  EXPECT_FALSE(results->front().transition.has_value());
  return *results->front().observation;
}

void replicate_current(ReplicatedIngestRuntime& owner, const raft::GroupId& group_id) {
  const raft::RaftGroupObservation observation = observe(owner, group_id);
  auto replicated = owner.runtime()->try_submit(
      {{group_id, raft::ReceiveOperation{2U, raft::AppendEntriesResponse{
                                                 .term = observation.current_term,
                                                 .success = true,
                                                 .match_index = observation.last_log_index}}}});
  ASSERT_TRUE(replicated.has_value()) << replicated.error().to_string();
  ASSERT_TRUE(replicated->wait().has_value());
}

void elect_two_node_group(ReplicatedIngestRuntime& owner, const raft::GroupId& group_id) {
  auto election = owner.runtime()->try_submit({{group_id, raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value()) << election.error().to_string();
  ASSERT_TRUE(election->wait().has_value());
  const raft::RaftGroupObservation candidate = observe(owner, group_id);
  auto vote = owner.runtime()->try_submit(
      {{group_id,
        raft::ReceiveOperation{2U, raft::RequestVoteResponse{candidate.current_term, true}}}});
  ASSERT_TRUE(vote.has_value()) << vote.error().to_string();
  ASSERT_TRUE(vote->wait().has_value());
  replicate_current(owner, group_id);
}

void propose_and_replicate(ReplicatedIngestRuntime& owner, const raft::GroupId& group_id,
                           raft::ProposeOperation operation) {
  auto proposed = owner.runtime()->try_submit({{group_id, std::move(operation)}});
  ASSERT_TRUE(proposed.has_value()) << proposed.error().to_string();
  ASSERT_TRUE(proposed->wait().has_value());
  replicate_current(owner, group_id);
}

void provision_two_node_query(ReplicatedIngestRuntime& owner) {
  elect_two_node_group(owner, metadata_group());
  propose_and_replicate(
      owner, metadata_group(),
      {raft::kRaftSchemaDefinitionEntryType,
       raft::encode_schema_definition_v1(
           {.name = "events", .quoted = false, .schema = columnar::test::batch_schema()})
           .value()});
  propose_and_replicate(
      owner, metadata_group(),
      {raft::kRaftMetadataCommandEntryType,
       raft::encode_metadata_command_v1(
           raft::TablePolicyMetadata{columnar::test::batch_schema()->table_id(), 1'000'000'000LL,
                                     86'400'000'000'000LL, 86'400'000'000'000LL, 0LL, 8U})
           .value()});
  propose_and_replicate(
      owner, metadata_group(),
      {raft::kRaftMetadataCommandEntryType,
       raft::encode_metadata_command_v1(
           raft::TabletPlacementMetadata{
               columnar::test::batch_schema()->table_id(), tablet_id(), 1U, {1U, 2U}, 1U})
           .value()});
  propose_and_replicate(
      owner, metadata_group(),
      {raft::kRaftTabletGroupBindingEntryType,
       raft::encode_tablet_group_binding_v1({tablet_id(), tablet_group()}).value()});
  elect_two_node_group(owner, tablet_group());
}

void elect_and_provision_multiple_tablets(ReplicatedIngestRuntime& owner) {
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
  const raft::ProposeOperation first_placement{
      raft::kRaftMetadataCommandEntryType,
      raft::encode_metadata_command_v1(
          raft::TabletPlacementMetadata{
              columnar::test::batch_schema()->table_id(), tablet_id(), 1U, {1U}, 1U})
          .value()};
  const raft::ProposeOperation first_binding{
      raft::kRaftTabletGroupBindingEntryType,
      raft::encode_tablet_group_binding_v1({tablet_id(), tablet_group()}).value()};
  const raft::ProposeOperation second_placement{
      raft::kRaftMetadataCommandEntryType,
      raft::encode_metadata_command_v1(
          raft::TabletPlacementMetadata{
              columnar::test::batch_schema()->table_id(), second_tablet_id(), 1U, {1U}, 1U})
          .value()};
  const raft::ProposeOperation second_binding{
      raft::kRaftTabletGroupBindingEntryType,
      raft::encode_tablet_group_binding_v1({second_tablet_id(), second_tablet_group()}).value()};
  auto metadata = owner.runtime()->try_submit({{metadata_group(), schema},
                                               {metadata_group(), policy},
                                               {metadata_group(), first_placement},
                                               {metadata_group(), first_binding},
                                               {metadata_group(), second_placement},
                                               {metadata_group(), second_binding}});
  ASSERT_TRUE(metadata.has_value());
  ASSERT_TRUE(metadata->wait().has_value());
  for (const raft::GroupId group_id : {tablet_group(), second_tablet_group()}) {
    election = owner.runtime()->try_submit({{group_id, raft::StartElectionOperation{}}});
    ASSERT_TRUE(election.has_value());
    ASSERT_TRUE(election->wait().has_value());
  }
}

[[nodiscard]] network::NetworkTask await_response(ReplicatedIngestRuntime& owner) {
  for (std::size_t attempt = 0U; attempt < 10'000U; ++attempt) {
    auto response = owner.coordinator()->poll();
    if (!response.has_value()) {
      ADD_FAILURE() << response.error().to_string();
      return {};
    }
    auto& available_response = *response;
    if (available_response.has_value())
      return std::move(*available_response);
    std::this_thread::yield();
  }
  ADD_FAILURE() << "replicated database response timed out";
  return {};
}

TEST(ReplicatedIngestDatabaseTest, ReportsPackagedLifecycleStagesInOwnershipOrder) {
  TemporaryDirectory directory;
  runtime::DatabaseBootstrapConfig bootstrap_config{.database_root = directory.path().string(),
                                                    .new_database = descriptor()};
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(bootstrap_config);
  ASSERT_TRUE(bootstrap.has_value()) << bootstrap.error().to_string();
  auto initial = ReplicatedIngestRuntime::create_new(initial_runtime_config(*bootstrap));
  ASSERT_TRUE(initial.has_value()) << initial.error().to_string();
  elect_and_provision(*initial);
  ASSERT_TRUE(initial->shutdown().is_ok());
  ASSERT_TRUE(bootstrap->close().is_ok());

  RecordingStartupObserver observer;
  ReplicatedIngestDatabaseConfig config{.bootstrap = bootstrap_config, .groups = groups()};
  config.startup_observer = &observer;
  auto database = ReplicatedIngestDatabase::open_existing(std::move(config));
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  EXPECT_EQ(observer.count, observer.stages.size());
  EXPECT_FALSE(observer.overflow);
  EXPECT_EQ(observer.stages,
            (std::array{ReplicatedIngestDatabaseStartupStage::kRootOwnerReady,
                        ReplicatedIngestDatabaseStartupStage::kCatalogRecovered,
                        ReplicatedIngestDatabaseStartupStage::kTabletOwnersPrepared,
                        ReplicatedIngestDatabaseStartupStage::kRuntimeReady}));
  RecordingShutdownObserver shutdown_observer;
  ASSERT_TRUE(database->shutdown(shutdown_observer).is_ok());
  EXPECT_EQ(shutdown_observer.count, shutdown_observer.stages.size());
  EXPECT_FALSE(shutdown_observer.overflow);
  EXPECT_EQ(shutdown_observer.stages,
            (std::array{ReplicatedIngestDatabaseShutdownStage::kCoordinatorReleased,
                        ReplicatedIngestDatabaseShutdownStage::kAcceptedWorkDrained,
                        ReplicatedIngestDatabaseShutdownStage::kApplicationsStopped,
                        ReplicatedIngestDatabaseShutdownStage::kLogClosed,
                        ReplicatedIngestDatabaseShutdownStage::kRuntimeStopped,
                        ReplicatedIngestDatabaseShutdownStage::kRootReleased}));
  ASSERT_TRUE(database->shutdown(shutdown_observer).is_ok());
  EXPECT_EQ(shutdown_observer.count, shutdown_observer.stages.size());
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

TEST(ReplicatedIngestDatabaseTest, RebuildsCompactedApplicationPrefixesAndRetainedSuffixes) {
  TemporaryDirectory directory;
  runtime::DatabaseBootstrapConfig bootstrap_config{.database_root = directory.path().string(),
                                                    .new_database = descriptor()};
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(bootstrap_config);
  ASSERT_TRUE(bootstrap.has_value()) << bootstrap.error().to_string();
  const raft::MetadataSnapshotStorageConfig metadata_snapshot_config{
      .directory_path = (directory.path() / "metadata-snapshots").string(),
      .group_id = metadata_group()};
  const ingest::RaftTabletSnapshotStorageConfig tablet_snapshot_config{
      .directory_path = (directory.path() / "tablet-snapshots").string(),
      .group_id = tablet_group()};
  ASSERT_TRUE(std::filesystem::create_directories(metadata_snapshot_config.directory_path));
  ASSERT_TRUE(std::filesystem::create_directories(tablet_snapshot_config.directory_path));

  auto configured = initial_runtime_config(*bootstrap);
  auto durable = raft::DurableMultiRaftRuntime::create_new(configured.local_node_id, configured.log,
                                                           configured.groups);
  ASSERT_TRUE(durable.has_value()) << durable.error().to_string();
  {
    auto metadata_storage = raft::MetadataSnapshotStorage::create(metadata_snapshot_config);
    ASSERT_TRUE(metadata_storage.has_value()) << metadata_storage.error().to_string();
    auto metadata = raft::DurableMetadataStateMachine::recover(metadata_group(), *durable,
                                                               std::move(*metadata_storage));
    ASSERT_TRUE(metadata.has_value()) << metadata.error().to_string();
    auto tablet_storage = ingest::RaftTabletSnapshotStorage::create(tablet_snapshot_config);
    ASSERT_TRUE(tablet_storage.has_value()) << tablet_storage.error().to_string();
    ASSERT_EQ(configured.tablets.size(), 1U);
    auto& tablet_config = configured.tablets.front();
    auto tablet = ingest::RaftTabletStateMachine::recover(
        tablet_group(), *durable, std::move(*tablet_storage),
        std::move(tablet_config.retry_directory), std::move(tablet_config.tablet),
        std::move(tablet_config.retained_schemas), tablet_config.decode_limits);
    ASSERT_TRUE(tablet.has_value()) << tablet.error().to_string();

    auto election = durable->execute_batch({{metadata_group(), raft::StartElectionOperation{}}});
    ASSERT_TRUE(election.has_value()) << election.error().to_string();
    election = durable->execute_batch({{tablet_group(), raft::StartElectionOperation{}}});
    ASSERT_TRUE(election.has_value()) << election.error().to_string();
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
    auto metadata_entries = durable->execute_batch({{metadata_group(), schema},
                                                    {metadata_group(), policy},
                                                    {metadata_group(), placement},
                                                    {metadata_group(), binding}});
    ASSERT_TRUE(metadata_entries.has_value()) << metadata_entries.error().to_string();
    auto tablet_entry = durable->execute_batch(
        {{tablet_group(),
          raft::ProposeOperation{ingest::kRaftColumnarAppendEntryType, command()}}});
    ASSERT_TRUE(tablet_entry.has_value()) << tablet_entry.error().to_string();
    auto metadata_applied = metadata->apply_committed();
    ASSERT_TRUE(metadata_applied.has_value()) << metadata_applied.error().to_string();
    ASSERT_EQ(metadata_applied->last_applied_index, 4U);
    auto tablet_applied = tablet->apply_committed();
    ASSERT_TRUE(tablet_applied.has_value()) << tablet_applied.error().to_string();
    ASSERT_EQ(tablet_applied->last_applied_index, 1U);
    auto metadata_compacted = metadata->compact_applied_prefix(4U);
    ASSERT_TRUE(metadata_compacted.has_value()) << metadata_compacted.error().to_string();
    auto tablet_compacted = tablet->compact_applied_prefix(1U, 1U, {});
    ASSERT_TRUE(tablet_compacted.has_value()) << tablet_compacted.error().to_string();

    const raft::ProposeOperation node{
        raft::kRaftMetadataCommandEntryType,
        raft::encode_metadata_command_v1(raft::ClusterNodeMetadata{1U, "node-1.example:7000"})
            .value()};
    metadata_entries = durable->execute_batch({{metadata_group(), node}});
    ASSERT_TRUE(metadata_entries.has_value()) << metadata_entries.error().to_string();
    auto suffix_command =
        command(columnar::test::batch_schema(), columnar::test::batch_columns(), 5U);
    tablet_entry = durable->execute_batch(
        {{tablet_group(),
          raft::ProposeOperation{ingest::kRaftColumnarAppendEntryType, suffix_command}}});
    ASSERT_TRUE(tablet_entry.has_value()) << tablet_entry.error().to_string();
    EXPECT_EQ(durable->find_group(metadata_group())->commit_index(), 5U);
    EXPECT_EQ(durable->find_group(metadata_group())->applied_index(), 4U);
    EXPECT_EQ(durable->find_group(tablet_group())->commit_index(), 2U);
    EXPECT_EQ(durable->find_group(tablet_group())->applied_index(), 1U);
  }
  ASSERT_TRUE(durable->close().is_ok());
  ASSERT_TRUE(bootstrap->close().is_ok());

  auto missing_snapshots =
      ReplicatedIngestDatabase::open_existing({.bootstrap = bootstrap_config, .groups = groups()});
  ASSERT_FALSE(missing_snapshots.has_value());
  EXPECT_EQ(missing_snapshots.error().code(), common::StatusCode::kNotSupported);
  auto database =
      ReplicatedIngestDatabase::open_existing({.bootstrap = bootstrap_config,
                                               .groups = groups(),
                                               .metadata_snapshots = metadata_snapshot_config,
                                               .tablet_snapshots = {tablet_snapshot_config}});
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  auto catalog = database->ingest_runtime()->metadata_application()->catalog_snapshot();
  ASSERT_TRUE(catalog.has_value()) << catalog.error().to_string();
  EXPECT_EQ((*catalog)->applied_index, 5U);
  ASSERT_EQ((*catalog)->cluster_nodes.size(), 1U);
  EXPECT_EQ((*catalog)->cluster_nodes.front(),
            (raft::ClusterNodeMetadata{1U, "node-1.example:7000"}));
  auto recovered = database->ingest_runtime()->tablet_application()->snapshot(tablet_group());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_EQ(recovered->visible_row_count(), 4U);
  EXPECT_EQ(recovered->retry_entry_count(), 2U);
  EXPECT_EQ(recovered->applied_position(), head::HeadCommitPosition::raft(tablet_group(), 2U));

  auto election = database->ingest_runtime()->runtime()->try_submit(
      {{tablet_group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value()) << election.error().to_string();
  ASSERT_TRUE(election->wait().has_value());
  auto suffix_command =
      command(columnar::test::batch_schema(), columnar::test::batch_columns(), 5U);
  ASSERT_TRUE(
      database->ingest_runtime()->coordinator()->admit(request(suffix_command, 2U)).is_ok());
  auto retry_response = await_response(*database->ingest_runtime());
  auto retry_acknowledgement =
      network::decode_quorum_sync_ingest_acknowledgement(retry_response.frame.payload);
  ASSERT_TRUE(retry_acknowledgement.has_value());
  EXPECT_EQ(retry_acknowledgement->outcome, network::IngestOutcome::kMatchingRetry);
  EXPECT_EQ(retry_acknowledgement->log_index, 3U);
  ASSERT_TRUE(database->shutdown().is_ok());

  auto repeated =
      ReplicatedIngestDatabase::open_existing({.bootstrap = bootstrap_config,
                                               .groups = groups(),
                                               .metadata_snapshots = metadata_snapshot_config,
                                               .tablet_snapshots = {tablet_snapshot_config}});
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  recovered = repeated->ingest_runtime()->tablet_application()->snapshot(tablet_group());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_EQ(recovered->visible_row_count(), 4U);
  EXPECT_EQ(recovered->retry_entry_count(), 2U);
  EXPECT_EQ(recovered->applied_position(), head::HeadCommitPosition::raft(tablet_group(), 3U));
  ASSERT_TRUE(repeated->shutdown().is_ok());
}

TEST(ReplicatedIngestDatabaseTest, RebuildsMultipleTabletGroupsAndPinsTheirWholeTableView) {
  TemporaryDirectory directory;
  runtime::DatabaseBootstrapConfig bootstrap_config{.database_root = directory.path().string(),
                                                    .new_database = descriptor()};
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(bootstrap_config);
  ASSERT_TRUE(bootstrap.has_value()) << bootstrap.error().to_string();
  auto initial = ReplicatedIngestRuntime::create_new(multi_tablet_runtime_config(*bootstrap));
  ASSERT_TRUE(initial.has_value()) << initial.error().to_string();
  elect_and_provision_multiple_tablets(*initial);

  ASSERT_TRUE(initial->coordinator()->admit(request(command(), 1U)).is_ok());
  auto first_response = await_response(*initial);
  auto first_acknowledgement =
      network::decode_quorum_sync_ingest_acknowledgement(first_response.frame.payload);
  ASSERT_TRUE(first_acknowledgement.has_value());
  EXPECT_EQ(first_acknowledgement->outcome, network::IngestOutcome::kApplied);
  EXPECT_EQ(first_acknowledgement->log_index, 1U);
  auto second_command = command(columnar::test::batch_schema(), columnar::test::batch_columns(), 5U,
                                second_tablet_id());
  ASSERT_TRUE(initial->coordinator()->admit(request(second_command, 2U)).is_ok());
  auto second_response = await_response(*initial);
  auto second_acknowledgement =
      network::decode_quorum_sync_ingest_acknowledgement(second_response.frame.payload);
  ASSERT_TRUE(second_acknowledgement.has_value());
  EXPECT_EQ(second_acknowledgement->outcome, network::IngestOutcome::kApplied);
  EXPECT_EQ(second_acknowledgement->log_index, 1U);
  ASSERT_TRUE(initial->shutdown().is_ok());
  ASSERT_TRUE(bootstrap->close().is_ok());

  auto database = ReplicatedIngestDatabase::open_existing(
      {.bootstrap = bootstrap_config, .groups = multi_tablet_groups()});
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  EXPECT_EQ(database->query_barrier_groups().size(), 3U);
  for (const raft::GroupId group_id : {tablet_group(), second_tablet_group()}) {
    auto recovered = database->ingest_runtime()->tablet_application()->snapshot(group_id);
    ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
    EXPECT_EQ(recovered->visible_row_count(), 2U);
    EXPECT_EQ(recovered->retry_entry_count(), 1U);
    auto election = database->ingest_runtime()->runtime()->try_submit(
        {{group_id, raft::StartElectionOperation{}}});
    ASSERT_TRUE(election.has_value());
    ASSERT_TRUE(election->wait().has_value());
  }

  ASSERT_TRUE(database->ingest_runtime()->coordinator()->admit(request(command(), 3U)).is_ok());
  first_response = await_response(*database->ingest_runtime());
  first_acknowledgement =
      network::decode_quorum_sync_ingest_acknowledgement(first_response.frame.payload);
  ASSERT_TRUE(first_acknowledgement.has_value());
  EXPECT_EQ(first_acknowledgement->outcome, network::IngestOutcome::kMatchingRetry);
  EXPECT_EQ(first_acknowledgement->log_index, 2U);
  ASSERT_TRUE(
      database->ingest_runtime()->coordinator()->admit(request(second_command, 4U)).is_ok());
  second_response = await_response(*database->ingest_runtime());
  second_acknowledgement =
      network::decode_quorum_sync_ingest_acknowledgement(second_response.frame.payload);
  ASSERT_TRUE(second_acknowledgement.has_value());
  EXPECT_EQ(second_acknowledgement->outcome, network::IngestOutcome::kMatchingRetry);
  EXPECT_EQ(second_acknowledgement->log_index, 2U);

  auto metadata_election = database->ingest_runtime()->runtime()->try_submit(
      {{metadata_group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(metadata_election.has_value()) << metadata_election.error().to_string();
  ASSERT_TRUE(metadata_election->wait().has_value());
  auto read_barrier = ReplicatedReadBarrier::create_local(
      database->ingest_runtime()->runtime(),
      {database->query_barrier_groups().begin(), database->query_barrier_groups().end()});
  ASSERT_TRUE(read_barrier.has_value()) << read_barrier.error().to_string();
  auto authorities = read_barrier->await_authority();
  ASSERT_TRUE(authorities.has_value()) << authorities.error().to_string();
  std::vector<raft::GroupReadBarrier> barriers;
  for (const ReplicatedReadAuthority& authority : *authorities)
    barriers.push_back(authority.barrier);

  auto mutable_snapshot = database->acquire_query_snapshot(barriers);
  ASSERT_TRUE(mutable_snapshot.has_value()) << mutable_snapshot.error().to_string();
  const auto first_publication =
      database->ingest_runtime()->tablet_application()->snapshot(tablet_group());
  const auto second_publication =
      database->ingest_runtime()->tablet_application()->snapshot(second_tablet_group());
  ASSERT_TRUE(first_publication.has_value());
  ASSERT_TRUE(second_publication.has_value());
  ASSERT_TRUE(first_publication->applied_position().has_value());
  ASSERT_TRUE(second_publication->applied_position().has_value());
  const auto first_authority_iterator =
      std::ranges::find(*authorities, tablet_group(), [](const ReplicatedReadAuthority& authority) {
        return authority.observation.group_id;
      });
  const auto second_authority_iterator = std::ranges::find(
      *authorities, second_tablet_group(),
      [](const ReplicatedReadAuthority& authority) { return authority.observation.group_id; });
  ASSERT_NE(first_authority_iterator, authorities->end());
  ASSERT_NE(second_authority_iterator, authorities->end());
  const ReplicatedReadAuthority& first_authority = *first_authority_iterator;
  const ReplicatedReadAuthority& second_authority = *second_authority_iterator;
  query::DistributedVectorQueryPlan mutable_plan{
      .query_id = id(0x79U),
      .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
      .fragments = {{.tablet_id = second_tablet_id(),
                     .minimum_event_time = 0,
                     .maximum_event_time = 0,
                     .leader_node = second_authority.observation.node_id,
                     .local_applied_position =
                         second_publication->applied_position()->record_sequence,
                     .known_leader_commit_position = second_authority.observation.commit_index},
                    {.tablet_id = tablet_id(),
                     .minimum_event_time = 0,
                     .maximum_event_time = 0,
                     .leader_node = first_authority.observation.node_id,
                     .local_applied_position =
                         first_publication->applied_position()->record_sequence,
                     .known_leader_commit_position = first_authority.observation.commit_index}},
      .intent = {.mode = query::DistributedVectorPlanMode::kRows, .row_output_indices = {0U, 1U}}};
  const std::array<std::uint32_t, 2U> projection{0U, 1U};
  const query::DistributedVectorResultSchema result_schema{
      .columns = {{.name = "ts",
                   .type = columnar::test::type(schema::LogicalTypeKind::kTimestampNs),
                   .nullable = false},
                  {.name = "tag",
                   .type = columnar::test::type(schema::LogicalTypeKind::kString),
                   .nullable = true}}};
  auto mutable_fragments = mutable_snapshot->bind_linearizable_mutable_vector_fragments(
      {.plan = std::cref(mutable_plan),
       .table_id = columnar::test::batch_schema()->table_id(),
       .group_authorities = *authorities,
       .destination_column_ordinals = projection,
       .result_schema = std::cref(result_schema)});
  ASSERT_TRUE(mutable_fragments.has_value()) << mutable_fragments.error().to_string();
  ASSERT_EQ(mutable_fragments->size(), 2U);
  EXPECT_EQ((*mutable_fragments)[0].tablet_id, second_tablet_id());
  EXPECT_EQ((*mutable_fragments)[0].raft_group_id, second_tablet_group());
  EXPECT_EQ((*mutable_fragments)[0].database_id.uuid(), descriptor().database_id);
  EXPECT_EQ((*mutable_fragments)[1].tablet_id, tablet_id());
  EXPECT_EQ((*mutable_fragments)[1].raft_group_id, tablet_group());
  EXPECT_EQ((*mutable_fragments)[1].linearizable_barrier, first_authority.barrier.barrier);

  ++mutable_plan.fragments.front().local_applied_position;
  auto mixed_publication = mutable_snapshot->bind_linearizable_mutable_vector_fragments(
      {.plan = std::cref(mutable_plan),
       .table_id = columnar::test::batch_schema()->table_id(),
       .group_authorities = *authorities,
       .destination_column_ordinals = projection,
       .result_schema = std::cref(result_schema)});
  ASSERT_FALSE(mixed_publication.has_value());
  EXPECT_EQ(mixed_publication.error().code(), common::StatusCode::kUnavailable);
  --mutable_plan.fragments.front().local_applied_position;
  std::vector<ReplicatedReadAuthority> incomplete_authorities = *authorities;
  incomplete_authorities.pop_back();
  auto incomplete_fragments = mutable_snapshot->bind_linearizable_mutable_vector_fragments(
      {.plan = std::cref(mutable_plan),
       .table_id = columnar::test::batch_schema()->table_id(),
       .group_authorities = incomplete_authorities,
       .destination_column_ordinals = projection,
       .result_schema = std::cref(result_schema)});
  ASSERT_FALSE(incomplete_fragments.has_value());
  EXPECT_EQ(incomplete_fragments.error().code(), common::StatusCode::kUnavailable);
  ASSERT_TRUE(read_barrier->shutdown().is_ok());

  auto snapshot = database->acquire_query_snapshot();
  ASSERT_TRUE(snapshot.has_value()) << snapshot.error().to_string();
  auto parsed = query::parse_sql_v1_select("SELECT count(*) AS rows FROM events");
  ASSERT_TRUE(parsed.has_value()) << parsed.error().status().to_string();
  auto bound = query::bind_sql_v1_select(std::move(*parsed), snapshot->catalog());
  ASSERT_TRUE(bound.has_value()) << bound.error().status().to_string();
  auto lowered = query::lower_bound_sql_select(*bound);
  ASSERT_TRUE(lowered.has_value()) << lowered.error().status().to_string();
  ASSERT_TRUE(database->shutdown().is_ok());

  query::QueryResourceContext resources =
      query::QueryResourceContext::create(std::size_t{8U} * 1024U * 1024U).value();
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
  EXPECT_EQ(count.read_i64_le().value(), 4);
  step = (*pipeline)->next(resources);
  ASSERT_TRUE(step.has_value());
  EXPECT_EQ(step->kind(), query::PhysicalOperatorStepKind::kEnd);
  pipeline->reset();
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(ReplicatedIngestDatabaseTest, RebuildsRetainedSchemaLineageAfterCommittedCatalogEvolution) {
  TemporaryDirectory directory;
  runtime::DatabaseBootstrapConfig bootstrap_config{.database_root = directory.path().string(),
                                                    .new_database = descriptor()};
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(bootstrap_config);
  ASSERT_TRUE(bootstrap.has_value()) << bootstrap.error().to_string();
  auto initial = ReplicatedIngestRuntime::create_new(initial_runtime_config(*bootstrap));
  ASSERT_TRUE(initial.has_value()) << initial.error().to_string();
  elect_and_provision(*initial, false);
  ASSERT_TRUE(initial->coordinator()->admit(request()).is_ok());
  auto base_response = await_response(*initial);
  auto base_acknowledgement =
      network::decode_quorum_sync_ingest_acknowledgement(base_response.frame.payload);
  ASSERT_TRUE(base_acknowledgement.has_value());
  EXPECT_EQ(base_acknowledgement->outcome, network::IngestOutcome::kApplied);
  EXPECT_EQ(base_acknowledgement->log_index, 1U);

  const raft::ProposeOperation successor{
      raft::kRaftSchemaDefinitionEntryType,
      raft::encode_schema_definition_v1(
          {.name = "events", .quoted = false, .schema = columnar::test::successor_batch_schema()})
          .value()};
  auto evolved = initial->runtime()->try_submit({{metadata_group(), successor}});
  ASSERT_TRUE(evolved.has_value()) << evolved.error().to_string();
  ASSERT_TRUE(evolved->wait().has_value());
  auto evolved_catalog = initial->metadata_application()->catalog_snapshot();
  ASSERT_TRUE(evolved_catalog.has_value());
  ASSERT_EQ((*evolved_catalog)->schema_definitions.size(), 2U);
  ASSERT_EQ((*evolved_catalog)->active_schemas.size(), 1U);
  EXPECT_EQ((*evolved_catalog)->active_schemas.front().schema_id,
            columnar::test::successor_batch_schema()->schema_id());
  ASSERT_TRUE(initial->shutdown().is_ok());
  ASSERT_TRUE(bootstrap->close().is_ok());

  auto database =
      ReplicatedIngestDatabase::open_existing({.bootstrap = bootstrap_config, .groups = groups()});
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  auto recovered_catalog = database->ingest_runtime()->metadata_application()->catalog_snapshot();
  ASSERT_TRUE(recovered_catalog.has_value());
  ASSERT_EQ((*recovered_catalog)->schema_definitions.size(), 2U);
  auto recovered = database->ingest_runtime()->tablet_application()->snapshot(tablet_group());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_EQ(recovered->visible_row_count(), 2U);
  EXPECT_EQ(recovered->retry_entry_count(), 1U);
  EXPECT_EQ(recovered->schema_ptr()->schema_id(), columnar::test::batch_schema()->schema_id());

  auto election = database->ingest_runtime()->runtime()->try_submit(
      {{tablet_group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value()) << election.error().to_string();
  ASSERT_TRUE(election->wait().has_value());
  ASSERT_TRUE(
      database->ingest_runtime()->coordinator()->admit(request(successor_command(), 2U)).is_ok());
  auto successor_response = await_response(*database->ingest_runtime());
  auto successor_acknowledgement =
      network::decode_quorum_sync_ingest_acknowledgement(successor_response.frame.payload);
  ASSERT_TRUE(successor_acknowledgement.has_value());
  EXPECT_EQ(successor_acknowledgement->outcome, network::IngestOutcome::kApplied);
  EXPECT_EQ(successor_acknowledgement->log_index, 2U);
  recovered = database->ingest_runtime()->tablet_application()->snapshot(tablet_group());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_EQ(recovered->visible_row_count(), 4U);
  EXPECT_EQ(recovered->retry_entry_count(), 2U);
  EXPECT_EQ(recovered->schema_ptr()->schema_id(),
            columnar::test::successor_batch_schema()->schema_id());
  ASSERT_EQ(recovered->sealed_generations().size(), 1U);
  EXPECT_EQ(recovered->sealed_generations().front().schema_ptr()->schema_id(),
            columnar::test::batch_schema()->schema_id());
  ASSERT_TRUE(database->shutdown().is_ok());

  auto repeated =
      ReplicatedIngestDatabase::open_existing({.bootstrap = bootstrap_config, .groups = groups()});
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  auto repeated_snapshot =
      repeated->ingest_runtime()->tablet_application()->snapshot(tablet_group());
  ASSERT_TRUE(repeated_snapshot.has_value()) << repeated_snapshot.error().to_string();
  EXPECT_EQ(repeated_snapshot->visible_row_count(), 4U);
  EXPECT_EQ(repeated_snapshot->retry_entry_count(), 2U);
  EXPECT_EQ(repeated_snapshot->schema_ptr()->schema_id(),
            columnar::test::successor_batch_schema()->schema_id());
  election = repeated->ingest_runtime()->runtime()->try_submit(
      {{tablet_group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value()) << election.error().to_string();
  ASSERT_TRUE(election->wait().has_value());
  ASSERT_TRUE(
      repeated->ingest_runtime()->coordinator()->admit(request(successor_command(), 3U)).is_ok());
  auto retry_response = await_response(*repeated->ingest_runtime());
  auto retry_acknowledgement =
      network::decode_quorum_sync_ingest_acknowledgement(retry_response.frame.payload);
  ASSERT_TRUE(retry_acknowledgement.has_value());
  EXPECT_EQ(retry_acknowledgement->outcome, network::IngestOutcome::kMatchingRetry);
  EXPECT_EQ(retry_acknowledgement->log_index, 3U);
  ASSERT_TRUE(repeated->shutdown().is_ok());
}

TEST(ReplicatedIngestDatabaseTest, ReopensAndCompletesACommittedJointReconfiguration) {
  TemporaryDirectory directory;
  runtime::DatabaseBootstrapConfig bootstrap_config{.database_root = directory.path().string(),
                                                    .new_database = descriptor()};
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(bootstrap_config);
  ASSERT_TRUE(bootstrap.has_value()) << bootstrap.error().to_string();
  auto configured = initial_runtime_config(*bootstrap);
  configured.groups = joint_groups();
  auto initial = ReplicatedIngestRuntime::create_new(std::move(configured));
  ASSERT_TRUE(initial.has_value()) << initial.error().to_string();

  auto metadata_election =
      initial->runtime()->try_submit({{metadata_group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(metadata_election.has_value()) << metadata_election.error().to_string();
  ASSERT_TRUE(metadata_election->wait().has_value());
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
              columnar::test::batch_schema()->table_id(), tablet_id(), 1U, {1U, 2U}, 1U})
          .value()};
  const raft::ProposeOperation binding{
      raft::kRaftTabletGroupBindingEntryType,
      raft::encode_tablet_group_binding_v1({tablet_id(), tablet_group()}).value()};
  auto metadata = initial->runtime()->try_submit({{metadata_group(), schema},
                                                  {metadata_group(), policy},
                                                  {metadata_group(), placement},
                                                  {metadata_group(), binding}});
  ASSERT_TRUE(metadata.has_value()) << metadata.error().to_string();
  ASSERT_TRUE(metadata->wait().has_value());

  auto tablet_election =
      initial->runtime()->try_submit({{tablet_group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(tablet_election.has_value()) << tablet_election.error().to_string();
  ASSERT_TRUE(tablet_election->wait().has_value());
  auto vote = initial->runtime()->try_submit(
      {{tablet_group(), raft::ReceiveOperation{2U, raft::RequestVoteResponse{1U, true}}}});
  ASSERT_TRUE(vote.has_value()) << vote.error().to_string();
  ASSERT_TRUE(vote->wait().has_value());
  auto proposed = initial->runtime()->try_submit(
      {{tablet_group(), raft::ProposeOperation{ingest::kRaftColumnarAppendEntryType, command()}}});
  ASSERT_TRUE(proposed.has_value()) << proposed.error().to_string();
  ASSERT_TRUE(proposed->wait().has_value());
  auto replicated = initial->runtime()->try_submit(
      {{tablet_group(),
        raft::ReceiveOperation{
            2U, raft::AppendEntriesResponse{.term = 1U, .success = true, .match_index = 1U}}}});
  ASSERT_TRUE(replicated.has_value()) << replicated.error().to_string();
  ASSERT_TRUE(replicated->wait().has_value());
  auto before_joint = initial->tablet_application()->snapshot(tablet_group());
  ASSERT_TRUE(before_joint.has_value()) << before_joint.error().to_string();
  EXPECT_EQ(before_joint->visible_row_count(), 2U);

  auto joint = initial->runtime()->try_submit(
      {{tablet_group(), raft::BeginMembershipChangeOperation{{1U}}}});
  ASSERT_TRUE(joint.has_value()) << joint.error().to_string();
  ASSERT_TRUE(joint->wait().has_value());
  replicated = initial->runtime()->try_submit(
      {{tablet_group(),
        raft::ReceiveOperation{
            2U, raft::AppendEntriesResponse{.term = 1U, .success = true, .match_index = 2U}}}});
  ASSERT_TRUE(replicated.has_value()) << replicated.error().to_string();
  ASSERT_TRUE(replicated->wait().has_value());
  ASSERT_TRUE(initial->shutdown().is_ok());
  ASSERT_TRUE(bootstrap->close().is_ok());

  auto database = ReplicatedIngestDatabase::open_existing(
      {.bootstrap = bootstrap_config, .groups = joint_groups()});
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  auto recovered = database->ingest_runtime()->tablet_application()->snapshot(tablet_group());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_EQ(recovered->visible_row_count(), 2U);
  EXPECT_EQ(recovered->retry_entry_count(), 1U);
  auto observed = database->ingest_runtime()->runtime()->try_submit(
      {{tablet_group(), raft::ObserveGroupOperation{}}});
  ASSERT_TRUE(observed.has_value()) << observed.error().to_string();
  auto observation = observed->wait();
  ASSERT_TRUE(observation.has_value()) << observation.error().to_string();
  ASSERT_EQ(observation->size(), 1U);
  const auto& joint_observation = observation->front().observation;
  if (!joint_observation.has_value()) {
    ADD_FAILURE() << "expected recovered joint-group observation";
    return;
  }
  EXPECT_TRUE(joint_observation->joint_membership_active);
  EXPECT_TRUE(joint_observation->joint_membership_can_finalize);
  EXPECT_EQ(joint_observation->joint_old_voters, (std::vector<raft::NodeId>{1U, 2U}));
  EXPECT_EQ(joint_observation->joint_new_voters, (std::vector<raft::NodeId>{1U}));

  tablet_election = database->ingest_runtime()->runtime()->try_submit(
      {{tablet_group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(tablet_election.has_value()) << tablet_election.error().to_string();
  ASSERT_TRUE(tablet_election->wait().has_value());
  vote = database->ingest_runtime()->runtime()->try_submit(
      {{tablet_group(), raft::ReceiveOperation{2U, raft::RequestVoteResponse{2U, true}}}});
  ASSERT_TRUE(vote.has_value()) << vote.error().to_string();
  ASSERT_TRUE(vote->wait().has_value());
  auto finalized = database->ingest_runtime()->runtime()->try_submit(
      {{tablet_group(), raft::FinalizeMembershipChangeOperation{}}});
  ASSERT_TRUE(finalized.has_value()) << finalized.error().to_string();
  ASSERT_TRUE(finalized->wait().has_value());
  replicated = database->ingest_runtime()->runtime()->try_submit(
      {{tablet_group(),
        raft::ReceiveOperation{
            2U, raft::AppendEntriesResponse{.term = 2U, .success = true, .match_index = 3U}}}});
  ASSERT_TRUE(replicated.has_value()) << replicated.error().to_string();
  ASSERT_TRUE(replicated->wait().has_value());

  metadata_election = database->ingest_runtime()->runtime()->try_submit(
      {{metadata_group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(metadata_election.has_value()) << metadata_election.error().to_string();
  ASSERT_TRUE(metadata_election->wait().has_value());
  const raft::ProposeOperation final_placement{
      raft::kRaftMetadataCommandEntryType,
      raft::encode_metadata_command_v1(
          raft::TabletPlacementMetadata{
              columnar::test::batch_schema()->table_id(), tablet_id(), 2U, {1U}, 1U})
          .value()};
  metadata =
      database->ingest_runtime()->runtime()->try_submit({{metadata_group(), final_placement}});
  ASSERT_TRUE(metadata.has_value()) << metadata.error().to_string();
  ASSERT_TRUE(metadata->wait().has_value());
  ASSERT_TRUE(database->ingest_runtime()->coordinator()->admit(request()).is_ok());
  auto retry_response = await_response(*database->ingest_runtime());
  auto acknowledgement =
      network::decode_quorum_sync_ingest_acknowledgement(retry_response.frame.payload);
  ASSERT_TRUE(acknowledgement.has_value());
  EXPECT_EQ(acknowledgement->outcome, network::IngestOutcome::kMatchingRetry);
  EXPECT_EQ(acknowledgement->log_index, 4U);
  ASSERT_TRUE(database->shutdown().is_ok());

  auto repeated = ReplicatedIngestDatabase::open_existing(
      {.bootstrap = bootstrap_config, .groups = joint_groups()});
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  observed = repeated->ingest_runtime()->runtime()->try_submit(
      {{tablet_group(), raft::ObserveGroupOperation{}}});
  ASSERT_TRUE(observed.has_value()) << observed.error().to_string();
  observation = observed->wait();
  ASSERT_TRUE(observation.has_value()) << observation.error().to_string();
  ASSERT_EQ(observation->size(), 1U);
  const auto& stable_observation = observation->front().observation;
  if (!stable_observation.has_value()) {
    ADD_FAILURE() << "expected recovered stable-group observation";
    return;
  }
  EXPECT_FALSE(stable_observation->joint_membership_active);
  EXPECT_EQ(stable_observation->committed_voters, (std::vector<raft::NodeId>{1U}));
  recovered = repeated->ingest_runtime()->tablet_application()->snapshot(tablet_group());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_EQ(recovered->visible_row_count(), 2U);
  EXPECT_EQ(recovered->retry_entry_count(), 1U);
  ASSERT_TRUE(repeated->shutdown().is_ok());
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
  for (const raft::GroupId group_id : {metadata_group(), tablet_group()}) {
    auto election = database->ingest_runtime()->runtime()->try_submit(
        {{group_id, raft::StartElectionOperation{}}});
    ASSERT_TRUE(election.has_value());
    ASSERT_TRUE(election->wait().has_value());
  }
  auto read_barrier = ReplicatedReadBarrier::create_local(
      database->ingest_runtime()->runtime(),
      {database->query_barrier_groups().begin(), database->query_barrier_groups().end()});
  ASSERT_TRUE(read_barrier.has_value()) << read_barrier.error().to_string();
  NativeProtocolService native_queries{*database, *read_barrier};
  auto requests = network::SpscNetworkTaskQueue::create(4U).value();
  auto responses = network::SpscNetworkTaskQueue::create(1U).value();
  ASSERT_TRUE(responses.try_push(
      {.connection_id = 99U, .frame = {.header = {.message_type = network::MessageType::kPong}}}));
  auto native_service =
      ReplicatedIngestService::create({.coordinator = database->ingest_runtime()->coordinator(),
                                       .queries = &native_queries,
                                       .requests = &requests,
                                       .responses = &responses});
  ASSERT_TRUE(native_service.has_value()) << native_service.error().to_string();
  ASSERT_TRUE(requests.try_push(query_request("SELECT count(*) AS rows FROM events", true)));
  auto polled = native_service->poll_once();
  ASSERT_TRUE(polled.has_value()) << polled.error().to_string();
  ASSERT_FALSE(polled->response_enqueued);
  ASSERT_TRUE(native_service->metrics().response_retained);
  ASSERT_TRUE(responses.try_pop().has_value());
  polled = native_service->poll_once();
  ASSERT_TRUE(polled.has_value());
  ASSERT_TRUE(polled->response_enqueued);
  auto native_result = responses.try_pop();
  if (!native_result.has_value()) {
    ADD_FAILURE() << "expected native query result batch";
    return;
  }
  const network::NetworkTask& native_batch = *native_result;
  ASSERT_EQ(native_batch.frame.header.message_type, network::MessageType::kQueryResult);
  auto batch = network::decode_query_result_batch(native_batch.frame.payload);
  ASSERT_TRUE(batch.has_value()) << batch.error().to_string();
  common::ByteReader native_count{batch->cell(0U, 0U)->value};
  EXPECT_EQ(native_count.read_i64_le().value(), 2);
  polled = native_service->poll_once();
  ASSERT_TRUE(polled.has_value());
  ASSERT_TRUE(polled->response_enqueued);
  native_result = responses.try_pop();
  if (!native_result.has_value()) {
    ADD_FAILURE() << "expected native query end";
    return;
  }
  EXPECT_EQ(native_result->frame.header.message_type, network::MessageType::kQueryEnd);
  EXPECT_EQ(native_service->metrics().query_requests, 1U);
  EXPECT_EQ(native_service->metrics().response_backpressure, 1U);
  native_service->begin_shutdown();
  EXPECT_TRUE(native_service->drained());
  auto unsupported_ddl = native_queries.execute_query(query_request("CREATE TABLE denied"));
  ASSERT_TRUE(unsupported_ddl.has_value()) << unsupported_ddl.error().to_string();
  ASSERT_EQ(unsupported_ddl->responses.size(), 1U);
  ASSERT_EQ(unsupported_ddl->responses.front().frame.header.message_type,
            network::MessageType::kError);
  auto ddl_error = network::decode_error_message(unsupported_ddl->responses.front().frame.payload);
  ASSERT_TRUE(ddl_error.has_value());
  EXPECT_EQ(ddl_error->code, network::ProtocolErrorCode::kExecutionFailure);

  auto snapshot = database->acquire_query_snapshot();
  ASSERT_TRUE(snapshot.has_value()) << snapshot.error().to_string();
  ASSERT_EQ(snapshot->catalog()->tables().size(), 1U);
  const ReplicatedSingleGroupQueryRoute* const query_route =
      snapshot->single_group_route(columnar::test::batch_schema()->table_id());
  ASSERT_NE(query_route, nullptr);
  EXPECT_EQ(query_route->group_id, tablet_group());
  EXPECT_EQ(query_route->placement_epoch, 1U);
  EXPECT_EQ(query_route->replicas, std::vector<raft::NodeId>{1U});
  auto local_leader = database->resolve_query_leader(*query_route);
  ASSERT_TRUE(local_leader.has_value()) << local_leader.error().to_string();
  EXPECT_FALSE(local_leader->has_value());
  const auto catalog_publication =
      database->ingest_runtime()->metadata_application()->catalog_snapshot();
  const auto tablet_publication =
      database->ingest_runtime()->tablet_application()->snapshot(tablet_group());
  ASSERT_TRUE(catalog_publication.has_value());
  ASSERT_TRUE(tablet_publication.has_value());
  const auto applied_position = tablet_publication->applied_position();
  if (!applied_position.has_value()) {
    ADD_FAILURE() << "expected a published tablet applied position";
    return;
  }
  std::vector<raft::GroupReadBarrier> barriers{
      {metadata_group(),
       {.term = 1U, .context = 1U, .read_index = (*catalog_publication)->applied_index}},
      {tablet_group(),
       {.term = 1U, .context = 2U, .read_index = applied_position->record_sequence}}};
  auto confirmed = database->acquire_query_snapshot(barriers);
  ASSERT_TRUE(confirmed.has_value()) << confirmed.error().to_string();
  ++barriers.back().barrier.read_index;
  auto trailing = database->acquire_query_snapshot(barriers);
  ASSERT_FALSE(trailing.has_value());
  EXPECT_EQ(trailing.error().code(), common::StatusCode::kUnavailable);
  barriers.pop_back();
  auto incomplete = database->acquire_query_snapshot(barriers);
  ASSERT_FALSE(incomplete.has_value());
  EXPECT_EQ(incomplete.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(database->query_barrier_groups().size(), 2U);
  auto parsed = query::parse_sql_v1_select("SELECT count(*) AS rows FROM events");
  ASSERT_TRUE(parsed.has_value()) << parsed.error().status().to_string();
  auto bound = query::bind_sql_v1_select(std::move(*parsed), snapshot->catalog());
  ASSERT_TRUE(bound.has_value()) << bound.error().status().to_string();
  auto lowered = query::lower_bound_sql_select(*bound);
  ASSERT_TRUE(lowered.has_value()) << lowered.error().status().to_string();
  ASSERT_TRUE(database->shutdown().is_ok());

  query::QueryResourceContext resources =
      query::QueryResourceContext::create(std::size_t{8U} * 1024U * 1024U).value();
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

TEST(ReplicatedIngestDatabaseTest, EmitsAnAuthoritativeRedirectForOneCommonRemoteQueryLeader) {
  TemporaryDirectory directory;
  runtime::DatabaseBootstrapConfig bootstrap_config{.database_root = directory.path().string(),
                                                    .new_database = descriptor()};
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(bootstrap_config);
  ASSERT_TRUE(bootstrap.has_value()) << bootstrap.error().to_string();
  auto config = initial_runtime_config(*bootstrap);
  config.groups = two_node_groups();
  auto initial = ReplicatedIngestRuntime::create_new(std::move(config));
  ASSERT_TRUE(initial.has_value()) << initial.error().to_string();
  provision_two_node_query(*initial);
  ASSERT_TRUE(initial->shutdown().is_ok());
  ASSERT_TRUE(bootstrap->close().is_ok());

  auto database = ReplicatedIngestDatabase::open_existing(
      {.bootstrap = bootstrap_config, .groups = two_node_groups()});
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  ReplicatedIngestRuntime* const runtime = database->ingest_runtime();
  ASSERT_NE(runtime, nullptr);
  for (const raft::GroupId group_id : {metadata_group(), tablet_group()}) {
    const raft::RaftGroupObservation before = observe(*runtime, group_id);
    ASSERT_NE(before.current_term, 0U);
    auto followed = runtime->runtime()->try_submit(
        {{group_id,
          raft::ReceiveOperation{
              2U, raft::AppendEntriesRequest{
                      .term = before.current_term + 1U,
                      .leader_id = 2U,
                      .previous_log_index = before.last_log_index,
                      .previous_log_term = before.last_log_index == 0U ? 0U : before.current_term,
                      .entries = {},
                      .leader_commit = before.commit_index}}}});
    ASSERT_TRUE(followed.has_value()) << followed.error().to_string();
    auto result = followed->wait();
    ASSERT_TRUE(result.has_value()) << result.error().to_string();
    ASSERT_EQ(result->size(), 1U);
    ASSERT_TRUE(result->front().status.is_ok()) << result->front().status.to_string();
  }

  auto snapshot = database->acquire_query_snapshot();
  ASSERT_TRUE(snapshot.has_value()) << snapshot.error().to_string();
  const ReplicatedSingleGroupQueryRoute* const query_route =
      snapshot->single_group_route(columnar::test::batch_schema()->table_id());
  ASSERT_NE(query_route, nullptr);
  auto resolved = database->resolve_query_leader(*query_route);
  ASSERT_TRUE(resolved.has_value()) << resolved.error().to_string();
  ASSERT_TRUE(resolved->has_value());
  EXPECT_EQ((**resolved).group_id, tablet_group());
  EXPECT_EQ((**resolved).leader_node_id, 2U);
  EXPECT_EQ((**resolved).leader_term, 2U);
  EXPECT_EQ((**resolved).placement_epoch, 1U);

  auto read_barrier = ReplicatedReadBarrier::create_local(
      runtime->runtime(),
      {database->query_barrier_groups().begin(), database->query_barrier_groups().end()});
  ASSERT_TRUE(read_barrier.has_value()) << read_barrier.error().to_string();
  NativeProtocolService service{*database, *read_barrier};
  auto response = service.execute_query(query_request("SELECT count(*) AS rows FROM events", true));
  ASSERT_TRUE(response.has_value()) << response.error().to_string();
  ASSERT_EQ(response->responses.size(), 1U);
  const network::NetworkTask& redirected = response->responses.front();
  EXPECT_EQ(redirected.protocol.protocol_major, network::kProtocolV2Major);
  EXPECT_EQ(redirected.frame.header.protocol_major, network::kProtocolV2Major);
  ASSERT_EQ(redirected.frame.header.message_type, network::MessageType::kLeaderRedirect);
  auto redirect = network::decode_leader_redirect(redirected.frame.payload);
  ASSERT_TRUE(redirect.has_value()) << redirect.error().to_string();
  EXPECT_EQ(redirect->group_id, tablet_group());
  EXPECT_EQ(redirect->leader_node_id, 2U);
  EXPECT_EQ(redirect->leader_term, 2U);
  EXPECT_EQ(redirect->placement_epoch, 1U);

  auto election =
      runtime->runtime()->try_submit({{metadata_group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value()) << election.error().to_string();
  ASSERT_TRUE(election->wait().has_value());
  const raft::RaftGroupObservation candidate = observe(*runtime, metadata_group());
  auto vote = runtime->runtime()->try_submit(
      {{metadata_group(),
        raft::ReceiveOperation{2U, raft::RequestVoteResponse{candidate.current_term, true}}}});
  ASSERT_TRUE(vote.has_value()) << vote.error().to_string();
  ASSERT_TRUE(vote->wait().has_value());
  auto split = database->resolve_query_leader(*query_route);
  ASSERT_FALSE(split.has_value());
  EXPECT_EQ(split.error().code(), common::StatusCode::kUnavailable);
  ASSERT_TRUE(read_barrier->shutdown().is_ok());
  ASSERT_TRUE(database->shutdown().is_ok());
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
  EXPECT_EQ(snapshot->single_group_route(columnar::test::batch_schema()->table_id()), nullptr);
  auto parsed = query::parse_sql_v1_select("SELECT count(*) FROM events");
  ASSERT_TRUE(parsed.has_value());
  auto bound = query::bind_sql_v1_select(std::move(*parsed), snapshot->catalog());
  ASSERT_TRUE(bound.has_value());
  auto lowered = query::lower_bound_sql_select(*bound);
  ASSERT_TRUE(lowered.has_value());
  query::QueryResourceContext resources =
      query::QueryResourceContext::create(std::size_t{1024U} * 1024U).value();
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
