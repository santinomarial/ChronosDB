#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/ingest/columnar_append.hpp"
#include "chronos/live/raft_subscription_reclaimer.hpp"
#include "columnar/columnar_test_support.hpp"
#include "ingest/ingest_test_support.hpp"

#include <array>
#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::live {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-raft-subscription-reclaim-XXXXXX")
            .string();
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

[[nodiscard]] common::Uuid uuid(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return common::Uuid{bytes};
}

[[nodiscard]] schema::TabletId tablet_id() {
  return columnar::test::id<schema::TabletId>(0x61U);
}

[[nodiscard]] raft::GroupId group_id() {
  return uuid(std::byte{0x51U});
}

[[nodiscard]] ingest::TabletState tablet() {
  return ingest::TabletState::create(
             columnar::test::batch_schema(), tablet_id(),
             {.head_capacity = {.row_capacity = 8U, .variable_value_bytes = {0U, 8U, 0U}},
              .maximum_schema_versions = 1U,
              .maximum_sealed_generations = 2U,
              .maximum_retry_entries = 8U})
      .value();
}

[[nodiscard]] ingest::RetryDirectory retry_directory() {
  return ingest::RetryDirectory::create({.maximum_entries = 8U}).value();
}

[[nodiscard]] std::vector<std::shared_ptr<const schema::TableSchema>> schemas() {
  return {columnar::test::batch_schema()};
}

[[nodiscard]] std::vector<std::byte> command() {
  auto batch = columnar::OwnedColumnarBatch::create(columnar::test::batch_schema(),
                                                    columnar::test::batch_columns())
                   .value();
  const auto encoded_batch = columnar::encode_columnar_batch_v1(batch).value();
  const auto encoded = ingest::encode_columnar_append_v1(
                           {.client_id = ingest::test::request_id<ingest::ClientId>(1U),
                            .client_batch_id = ingest::test::request_id<ingest::ClientBatchId>(2U),
                            .tablet_id = tablet_id()},
                           encoded_batch)
                           .value();
  return {encoded.bytes().begin(), encoded.bytes().end()};
}

TEST(RaftSubscriptionSourceReclaimerTest,
     RejectsAnAppliedFrontierWithoutADurableApplicationSnapshot) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  std::vector<ingest::AsyncRaftTabletApplicationConfig> applications;
  applications.push_back({.group_id = group_id(),
                          .snapshot_storage = std::nullopt,
                          .retry_directory = retry_directory(),
                          .tablet = tablet(),
                          .retained_schemas = schemas(),
                          .decode_limits = {}});
  auto application = ingest::AsyncRaftTabletApplication::create(std::move(applications));
  ASSERT_TRUE(application.has_value()) << application.error().to_string();
  auto runtime = raft::AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{group_id(), {1U}}}, {}, *application);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  auto election = runtime->try_submit({{group_id(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value());
  ASSERT_TRUE(election->wait().has_value());
  auto proposal = runtime->try_submit(
      {{group_id(), raft::ProposeOperation{ingest::kRaftColumnarAppendEntryType, command()}}});
  ASSERT_TRUE(proposal.has_value());
  ASSERT_TRUE(proposal->wait().has_value());

  auto metadata = raft::MetadataStateMachine::create();
  ASSERT_TRUE(metadata.has_value());
  const schema::TableId table = columnar::test::batch_schema()->table_id();
  ASSERT_TRUE(
      metadata->apply_committed(1U, raft::TabletPlacementMetadata{table, tablet_id(), 7U, {1U}, 1U})
          .is_ok());
  ASSERT_TRUE(metadata
                  ->apply_committed_tablet_group_binding(
                      2U, raft::TabletGroupBindingMetadata{tablet_id(), group_id()})
                  .is_ok());
  auto retention = SubscriptionRetentionCoordinator::create(
      {.database_id = uuid(std::byte{0x31U}),
       .table_id = table,
       .local_node_id = 1U,
       .members = {SubscriptionRetentionMember::raft(tablet_id(), group_id(), 7U)}});
  ASSERT_TRUE(retention.has_value()) << retention.error().to_string();
  auto reclaimer =
      RaftSubscriptionSourceReclaimer::create({.sources = {{tablet_id(), group_id(), 7U}},
                                               .runtime = &*runtime,
                                               .application = application->get()});
  ASSERT_TRUE(reclaimer.has_value()) << reclaimer.error().to_string();
  const std::vector<SourcePosition> storage_safe{SourcePosition::raft(tablet_id(), group_id(), 1U)};

  const auto report = retention->advance(*metadata, storage_safe, *reclaimer);

  ASSERT_FALSE(report.has_value());
  EXPECT_EQ(report.error().code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(runtime->metrics().admitted_reclamations, 0U);
  EXPECT_TRUE(runtime->shutdown().is_ok());
}

TEST(RaftSubscriptionSourceReclaimerTest,
     RequiresApplicationSnapshotCoverageBeforeNodeWidePhysicalReclamation) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const std::filesystem::path log_directory = directory.path() / "raft";
  const std::filesystem::path snapshot_directory = directory.path() / "snapshot";
  ASSERT_TRUE(std::filesystem::create_directory(log_directory));
  ASSERT_TRUE(std::filesystem::create_directory(snapshot_directory));
  const raft::RaftPersistentLogConfig log_config{.directory_path = log_directory.string(),
                                                 .target_segment_size = 2048U};
  const std::vector<raft::RaftGroupConfiguration> groups{{group_id(), {1U}}};
  const ingest::RaftTabletSnapshotStorageConfig snapshot_config{
      .directory_path = snapshot_directory.string(), .group_id = group_id()};

  {
    auto runtime = raft::DurableMultiRaftRuntime::create_new(1U, log_config, groups);
    ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
    auto elected = runtime->execute_batch({{group_id(), raft::StartElectionOperation{}}});
    ASSERT_TRUE(elected.has_value()) << elected.error().to_string();
    auto storage = ingest::RaftTabletSnapshotStorage::create(snapshot_config);
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    auto machine = ingest::RaftTabletStateMachine::recover(
        group_id(), *runtime, std::move(*storage), retry_directory(), tablet(), schemas());
    ASSERT_TRUE(machine.has_value()) << machine.error().to_string();
    auto proposed = runtime->execute_batch(
        {{group_id(), raft::ProposeOperation{ingest::kRaftColumnarAppendEntryType, command()}}});
    ASSERT_TRUE(proposed.has_value()) << proposed.error().to_string();
    ASSERT_TRUE(machine->apply_committed().has_value());
    std::array<std::byte, 32U> checksum{};
    checksum.fill(std::byte{0x5aU});
    auto compacted = machine->compact_applied_prefix(1U, 1U, checksum);
    ASSERT_TRUE(compacted.has_value()) << compacted.error().to_string();
    EXPECT_EQ(compacted->snapshot.last_included_index, 1U);
    EXPECT_TRUE(runtime->close().is_ok());
  }

  auto snapshot_storage = ingest::RaftTabletSnapshotStorage::open_existing(snapshot_config);
  ASSERT_TRUE(snapshot_storage.has_value()) << snapshot_storage.error().to_string();
  std::vector<ingest::AsyncRaftTabletApplicationConfig> applications;
  applications.push_back({.group_id = group_id(),
                          .snapshot_storage = std::move(*snapshot_storage),
                          .retry_directory = retry_directory(),
                          .tablet = tablet(),
                          .retained_schemas = schemas(),
                          .decode_limits = {}});
  auto application = ingest::AsyncRaftTabletApplication::create(std::move(applications));
  ASSERT_TRUE(application.has_value()) << application.error().to_string();
  auto runtime = raft::AsyncDurableMultiRaftRuntime::open_existing(1U, log_config, {}, groups, {},
                                                                   *application);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();

  auto metadata = raft::MetadataStateMachine::create();
  ASSERT_TRUE(metadata.has_value());
  const schema::TableId table = columnar::test::batch_schema()->table_id();
  ASSERT_TRUE(
      metadata->apply_committed(1U, raft::TabletPlacementMetadata{table, tablet_id(), 7U, {1U}, 1U})
          .is_ok());
  ASSERT_TRUE(metadata
                  ->apply_committed_tablet_group_binding(
                      2U, raft::TabletGroupBindingMetadata{tablet_id(), group_id()})
                  .is_ok());
  auto retention = SubscriptionRetentionCoordinator::create(
      {.database_id = uuid(std::byte{0x31U}),
       .table_id = table,
       .local_node_id = 1U,
       .members = {SubscriptionRetentionMember::raft(tablet_id(), group_id(), 7U)}});
  ASSERT_TRUE(retention.has_value()) << retention.error().to_string();
  auto reclaimer =
      RaftSubscriptionSourceReclaimer::create({.sources = {{tablet_id(), group_id(), 7U}},
                                               .runtime = &*runtime,
                                               .application = application->get()});
  ASSERT_TRUE(reclaimer.has_value()) << reclaimer.error().to_string();
  const std::vector<SourcePosition> storage_safe{SourcePosition::raft(tablet_id(), group_id(), 1U)};

  const auto report = retention->advance(*metadata, storage_safe, *reclaimer);

  ASSERT_TRUE(report.has_value()) << report.error().to_string();
  EXPECT_TRUE(report->advanced);
  EXPECT_EQ(report->authorized_frontiers, storage_safe);
  EXPECT_TRUE(runtime->shutdown().is_ok());

  auto physical = raft::RaftPersistentLog::open_existing(log_config);
  ASSERT_TRUE(physical.has_value()) << physical.error().to_string();
  EXPECT_GT(physical->recovery().base_segment_number, 1U);
  EXPECT_EQ(physical->recovery().latest_group_states.front().state.snapshot.last_included_index,
            1U);
  EXPECT_TRUE(physical->close().is_ok());
}

} // namespace
} // namespace chronos::live
