#include "chronos/common/status.hpp"
#include "chronos/ingest/raft_tablet_snapshot_storage.hpp"
#include "chronos/ingest/raft_tablet_state_machine.hpp"
#include "chronos/raft/durable_runtime.hpp"
#include "chronos/raft/node.hpp"
#include "ingest/raft_tablet_snapshot_storage_fault.hpp"
#include "ingest/raft_tablet_snapshot_storage_internal.hpp"
#include "ingest/tablet_snapshot_install_crash_fixture.hpp"

#include <array>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <utility>

namespace chronos::ingest {
namespace {

class CompactionFailureDirectory {
public:
  CompactionFailureDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-tablet-compaction-failure-XXXXXX")
            .string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr) {
      path_ = created;
      std::error_code error;
      static_cast<void>(std::filesystem::create_directory(path_ / "raft", error));
      if (!error)
        static_cast<void>(std::filesystem::create_directory(path_ / "snapshots", error));
      if (error) {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
        path_.clear();
      }
    }
  }

  ~CompactionFailureDirectory() {
    std::error_code ignored;
    if (!path_.empty())
      std::filesystem::remove_all(path_, ignored);
  }

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

struct TabletCompactionApplicationFailureCase {
  test::SnapshotStorageFault fault;
  std::string_view name;
  bool application_snapshot_visible_after_failure;
};

constexpr std::array<TabletCompactionApplicationFailureCase, 10U> kApplicationFailures{
    TabletCompactionApplicationFailureCase{test::SnapshotStorageFault::kTemporaryCreate,
                                           "temporary_create", false},
    TabletCompactionApplicationFailureCase{test::SnapshotStorageFault::kTemporaryValidationStat,
                                           "temporary_validation_stat", false},
    TabletCompactionApplicationFailureCase{test::SnapshotStorageFault::kTemporaryWrite,
                                           "temporary_write", false},
    TabletCompactionApplicationFailureCase{test::SnapshotStorageFault::kTemporaryPartialWrite,
                                           "temporary_partial_write", false},
    TabletCompactionApplicationFailureCase{test::SnapshotStorageFault::kTemporarySizeStat,
                                           "temporary_size_stat", false},
    TabletCompactionApplicationFailureCase{test::SnapshotStorageFault::kTemporaryReadback,
                                           "temporary_readback", false},
    TabletCompactionApplicationFailureCase{test::SnapshotStorageFault::kTemporaryFileSync,
                                           "temporary_file_sync", false},
    TabletCompactionApplicationFailureCase{test::SnapshotStorageFault::kTemporaryClose,
                                           "temporary_close", false},
    TabletCompactionApplicationFailureCase{test::SnapshotStorageFault::kFinalRename, "final_rename",
                                           false},
    TabletCompactionApplicationFailureCase{test::SnapshotStorageFault::kFinalDirectorySync,
                                           "final_directory_sync", true},
};

void expect_tablet_recovered(const RaftTabletStateMachine& machine) {
  auto snapshot = machine.tablet().snapshot();
  ASSERT_TRUE(snapshot.has_value()) << snapshot.error().to_string();
  EXPECT_EQ(snapshot->visible_row_count(), 2U);
  EXPECT_EQ(snapshot->applied_position(),
            head::HeadCommitPosition::raft(test::crash_group_id(), 1U));
  EXPECT_EQ(snapshot->retry_entry_count(), 1U);
}

class TabletSnapshotCompactionApplicationFailureTest
    : public ::testing::TestWithParam<TabletCompactionApplicationFailureCase> {};

TEST_P(TabletSnapshotCompactionApplicationFailureTest,
       WithholdsRaftMutationAndConvergesAfterReopen) {
  CompactionFailureDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const TabletCompactionApplicationFailureCase failure = GetParam();
  test::OneShotSnapshotStorageFault syscalls{failure.fault};
  {
    auto runtime = raft::DurableMultiRaftRuntime::create_new(
        4U, test::crash_log_config(directory.path()), test::crash_compaction_groups());
    ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
    auto election =
        runtime->execute_batch({{test::crash_group_id(), raft::StartElectionOperation{}}});
    ASSERT_TRUE(election.has_value()) << election.error().to_string();
    ASSERT_EQ(election->size(), 1U);
    ASSERT_TRUE(election->front().status.is_ok()) << election->front().status.to_string();
    auto storage = detail::RaftTabletSnapshotStorageTestAccess::create(
        test::crash_snapshot_config(directory.path()), syscalls);
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    auto recovered = RaftTabletStateMachine::recover(
        test::crash_group_id(), *runtime, std::move(*storage),
        test::crash_compaction_retry_directory(), test::crash_compaction_tablet(),
        test::crash_compaction_schemas());
    ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
    std::optional<RaftTabletStateMachine> machine{std::move(*recovered)};
    auto proposed = runtime->execute_batch(
        {{test::crash_group_id(),
          raft::ProposeOperation{kRaftColumnarAppendEntryType, test::crash_compaction_command()}}});
    ASSERT_TRUE(proposed.has_value()) << proposed.error().to_string();
    ASSERT_EQ(proposed->size(), 1U);
    ASSERT_TRUE(proposed->front().status.is_ok()) << proposed->front().status.to_string();
    auto applied = machine->apply_committed();
    ASSERT_TRUE(applied.has_value()) << applied.error().to_string();
    ASSERT_EQ(applied->last_applied_index, 1U);
    expect_tablet_recovered(*machine);
    syscalls.arm();

    auto compacted = machine->compact_applied_prefix(1U, 1U, {});

    ASSERT_FALSE(compacted.has_value());
    EXPECT_EQ(compacted.error().code(), common::StatusCode::kIoError);
    EXPECT_TRUE(syscalls.fired());
    EXPECT_FALSE(machine->failed());
    EXPECT_FALSE(runtime->failed());
    const raft::RaftNode* node = runtime->find_group(test::crash_group_id());
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->persistent_state().snapshot.last_included_index, 0U);
    ASSERT_EQ(node->persistent_state().log.size(), 1U);
    EXPECT_EQ(node->persistent_state().log.front().index, 1U);
    machine.reset();
    ASSERT_TRUE(runtime->close().is_ok());
  }

  const std::filesystem::path temporary =
      directory.path() / "snapshots" / "snapshot-00000000000000000001.rtas.tmp";
  const std::filesystem::path final =
      directory.path() / "snapshots" / "snapshot-00000000000000000001.rtas";
  auto runtime = raft::DurableMultiRaftRuntime::open_existing(
      4U, test::crash_log_config(directory.path()), {}, test::crash_compaction_groups());
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  const raft::RaftNode* node = runtime->find_group(test::crash_group_id());
  ASSERT_NE(node, nullptr);
  EXPECT_EQ(node->persistent_state().snapshot.last_included_index, 0U);
  EXPECT_EQ(node->persistent_state().log.size(), 1U);
  auto storage =
      RaftTabletSnapshotStorage::open_existing(test::crash_snapshot_config(directory.path()));
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  EXPECT_FALSE(std::filesystem::exists(temporary));
  EXPECT_EQ(std::filesystem::exists(final), failure.application_snapshot_visible_after_failure);
  auto before_retry = storage->load(1U);
  EXPECT_EQ(before_retry.has_value(), failure.application_snapshot_visible_after_failure);
  if (!failure.application_snapshot_visible_after_failure)
    EXPECT_EQ(before_retry.error().code(), common::StatusCode::kNotFound);
  auto recovered = RaftTabletStateMachine::recover(
      test::crash_group_id(), *runtime, std::move(*storage),
      test::crash_compaction_retry_directory(), test::crash_compaction_tablet(),
      test::crash_compaction_schemas());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  std::optional<RaftTabletStateMachine> machine{std::move(*recovered)};
  expect_tablet_recovered(*machine);

  auto compacted = machine->compact_applied_prefix(1U, 1U, {});

  ASSERT_TRUE(compacted.has_value()) << compacted.error().to_string();
  EXPECT_EQ(compacted->application_snapshot_already_present,
            failure.application_snapshot_visible_after_failure);
  EXPECT_EQ(compacted->application_entries, 1U);
  node = runtime->find_group(test::crash_group_id());
  ASSERT_NE(node, nullptr);
  EXPECT_EQ(node->persistent_state().snapshot, compacted->snapshot);
  EXPECT_TRUE(node->persistent_state().log.empty());
  auto reclaimed = machine->reclaim_obsolete_snapshots();
  ASSERT_TRUE(reclaimed.has_value()) << reclaimed.error().to_string();
  EXPECT_EQ(reclaimed->authoritative_index, 1U);
  EXPECT_EQ(reclaimed->reclaimed_files, 0U);
  machine.reset();
  ASSERT_TRUE(runtime->close().is_ok());

  auto repeated_runtime = raft::DurableMultiRaftRuntime::open_existing(
      4U, test::crash_log_config(directory.path()), {}, test::crash_compaction_groups());
  ASSERT_TRUE(repeated_runtime.has_value()) << repeated_runtime.error().to_string();
  auto repeated_storage =
      RaftTabletSnapshotStorage::open_existing(test::crash_snapshot_config(directory.path()));
  ASSERT_TRUE(repeated_storage.has_value()) << repeated_storage.error().to_string();
  auto installed = repeated_storage->load(1U);
  ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
  EXPECT_EQ(repeated_runtime->find_group(test::crash_group_id())->persistent_state().snapshot,
            installed->snapshot.raft_snapshot);
  {
    auto repeated = RaftTabletStateMachine::recover(
        test::crash_group_id(), *repeated_runtime, std::move(*repeated_storage),
        test::crash_compaction_retry_directory(), test::crash_compaction_tablet(),
        test::crash_compaction_schemas());
    ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
    expect_tablet_recovered(*repeated);
  }
  ASSERT_TRUE(repeated_runtime->close().is_ok());
}

INSTANTIATE_TEST_SUITE_P(
    EveryApplicationInstallFailure, TabletSnapshotCompactionApplicationFailureTest,
    ::testing::ValuesIn(kApplicationFailures),
    [](const ::testing::TestParamInfo<TabletCompactionApplicationFailureCase>& parameter) {
      return std::string{parameter.param.name};
    });

} // namespace
} // namespace chronos::ingest
