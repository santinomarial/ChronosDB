#include "chronos/common/status.hpp"
#include "chronos/ingest/raft_tablet_snapshot_storage.hpp"
#include "chronos/ingest/raft_tablet_state_machine.hpp"
#include "chronos/raft/durable_runtime.hpp"
#include "chronos/raft/node.hpp"
#include "ingest/raft_tablet_snapshot_storage_fault.hpp"
#include "ingest/raft_tablet_snapshot_storage_internal.hpp"
#include "ingest/tablet_snapshot_install_crash_fixture.hpp"
#include "io/posix_syscalls.hpp"
#include "raft/durable_runtime_internal.hpp"
#include "raft/raft_test_posix.hpp"

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <system_error>
#include <tuple>
#include <unistd.h>
#include <utility>
#include <vector>

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

constexpr std::size_t kPartialRaftRecordBytes = 16U;

enum class TabletCompactionRaftFault : std::uint8_t {
  kWriteBefore,
  kWritePrefixThenError,
  kWriteAfter,
  kDataSyncBefore,
  kDataSyncAfter,
};

class OneShotTabletCompactionRaftFault final : public io::detail::PosixSyscalls {
public:
  explicit OneShotTabletCompactionRaftFault(const TabletCompactionRaftFault fault)
      : delegate_(io::detail::system_posix_syscalls()), fault_(fault) {}

  void arm() noexcept {
    armed_ = true;
  }

  [[nodiscard]] bool fired() const noexcept {
    return fired_;
  }

  int open_directory(const char* path, const int flags) override {
    return delegate_.open_directory(path, flags);
  }

  int open_at(const io::detail::OpenAtRequest& request) override {
    return delegate_.open_at(request);
  }

  int mkdir_at(const io::detail::MkdirAtRequest& request) override {
    return delegate_.mkdir_at(request);
  }

  ssize_t pread(const io::detail::ReadAtRequest& request) override {
    return delegate_.pread(request);
  }

  ssize_t pwrite(const io::detail::WriteAtRequest& request) override {
    if (armed_ && fault_ == TabletCompactionRaftFault::kWriteBefore)
      return fail_ssize();
    if (armed_ && fault_ == TabletCompactionRaftFault::kWritePrefixThenError) {
      if (!partial_write_started_) {
        partial_write_started_ = true;
        const io::detail::WriteAtRequest prefix{request.descriptor, request.source,
                                                kPartialRaftRecordBytes, request.offset};
        return delegate_.pwrite(prefix);
      }
      return fail_ssize();
    }
    const ssize_t result = delegate_.pwrite(request);
    if (armed_ && fault_ == TabletCompactionRaftFault::kWriteAfter && result >= 0 &&
        static_cast<std::size_t>(result) == request.size) {
      return fail_ssize();
    }
    return result;
  }

  int fstat(const int descriptor, struct stat* metadata) override {
    return delegate_.fstat(descriptor, metadata);
  }

  int ftruncate(const io::detail::TruncateRequest& request) override {
    return delegate_.ftruncate(request);
  }

  int fdatasync(const int descriptor) override {
    if (armed_ && fault_ == TabletCompactionRaftFault::kDataSyncBefore)
      return fail();
    const int result = delegate_.fdatasync(descriptor);
    if (armed_ && fault_ == TabletCompactionRaftFault::kDataSyncAfter && result == 0)
      return fail();
    return result;
  }

  int fsync(const int descriptor) override {
    return delegate_.fsync(descriptor);
  }

  int rename_no_replace(const io::detail::RenameAtRequest& request) override {
    return delegate_.rename_no_replace(request);
  }

  int try_lock_exclusive(const int descriptor) override {
    return delegate_.try_lock_exclusive(descriptor);
  }

  int list_directory_entries(const int descriptor,
                             std::vector<io::DirectoryEntry>& entries) override {
    return delegate_.list_directory_entries(descriptor, entries);
  }

  int unlink_at(const int directory_descriptor, const char* name) override {
    return delegate_.unlink_at(directory_descriptor, name);
  }

  int close(const int descriptor) override {
    return delegate_.close(descriptor);
  }

private:
  int fail() noexcept {
    armed_ = false;
    fired_ = true;
    errno = EIO;
    return -1;
  }

  ssize_t fail_ssize() noexcept {
    static_cast<void>(fail());
    return -1;
  }

  io::detail::PosixSyscalls& delegate_;
  TabletCompactionRaftFault fault_;
  bool partial_write_started_{false};
  bool armed_{false};
  bool fired_{false};
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

struct TabletCompactionRaftFailureCase {
  TabletCompactionRaftFault fault;
  std::string_view name;
  bool tail_repair_required;
  bool raft_authority_recovered;
};

constexpr std::array<TabletCompactionRaftFailureCase, 5U> kRaftFailures{
    TabletCompactionRaftFailureCase{TabletCompactionRaftFault::kWriteBefore, "raft_write_before",
                                    false, false},
    TabletCompactionRaftFailureCase{TabletCompactionRaftFault::kWritePrefixThenError,
                                    "raft_partial_write", true, false},
    TabletCompactionRaftFailureCase{TabletCompactionRaftFault::kWriteAfter, "raft_write_after",
                                    false, true},
    TabletCompactionRaftFailureCase{TabletCompactionRaftFault::kDataSyncBefore,
                                    "raft_data_sync_before", false, true},
    TabletCompactionRaftFailureCase{TabletCompactionRaftFault::kDataSyncAfter,
                                    "raft_data_sync_after", false, true},
};

using TabletCompactionMixedFailureCase =
    std::tuple<TabletCompactionApplicationFailureCase, TabletCompactionRaftFailureCase>;

struct TabletCompactionCleanupFailureCase {
  test::SnapshotStorageFault fault;
  std::string_view name;
  bool temporary_removed;
};

constexpr std::array<TabletCompactionCleanupFailureCase, 2U> kCleanupFailures{
    TabletCompactionCleanupFailureCase{test::SnapshotStorageFault::kPriorTemporaryUnlink,
                                       "cleanup_unlink", false},
    TabletCompactionCleanupFailureCase{test::SnapshotStorageFault::kFinalDirectorySync,
                                       "cleanup_directory_sync", true},
};

using TabletCompactionCleanupPersistenceFailureCase =
    std::tuple<TabletCompactionCleanupFailureCase, TabletCompactionRaftFailureCase>;

struct TabletCompactionReopenFailureCase {
  test::SnapshotStorageFault application_reopen_fault;
  raft::test::DurableIoFault raft_reopen_fault;
  std::size_t raft_matching_calls_to_skip;
  std::string_view name;
  std::string_view expected_raft_operation;
  bool application_temporary_removed;
  bool raft_tail_removed;
};

constexpr std::array<TabletCompactionReopenFailureCase, 8U> kReopenFailures{
    TabletCompactionReopenFailureCase{
        test::SnapshotStorageFault::kPriorTemporaryUnlink, raft::test::DurableIoFault::kStat, 4U,
        "cleanup_unlink_then_repair_size_stat", "fstat file size", false, false},
    TabletCompactionReopenFailureCase{
        test::SnapshotStorageFault::kPriorTemporaryUnlink, raft::test::DurableIoFault::kTruncate,
        0U, "cleanup_unlink_then_repair_truncate", "ftruncate", false, true},
    TabletCompactionReopenFailureCase{
        test::SnapshotStorageFault::kPriorTemporaryUnlink, raft::test::DurableIoFault::kFullSync,
        0U, "cleanup_unlink_then_repair_file_sync", "fsync regular file", false, true},
    TabletCompactionReopenFailureCase{test::SnapshotStorageFault::kPriorTemporaryUnlink,
                                      raft::test::DurableIoFault::kDirectorySync, 0U,
                                      "cleanup_unlink_then_repair_directory_sync",
                                      "fsync directory", false, true},
    TabletCompactionReopenFailureCase{
        test::SnapshotStorageFault::kFinalDirectorySync, raft::test::DurableIoFault::kStat, 4U,
        "cleanup_sync_then_repair_size_stat", "fstat file size", true, false},
    TabletCompactionReopenFailureCase{test::SnapshotStorageFault::kFinalDirectorySync,
                                      raft::test::DurableIoFault::kTruncate, 0U,
                                      "cleanup_sync_then_repair_truncate", "ftruncate", true, true},
    TabletCompactionReopenFailureCase{
        test::SnapshotStorageFault::kFinalDirectorySync, raft::test::DurableIoFault::kFullSync, 0U,
        "cleanup_sync_then_repair_file_sync", "fsync regular file", true, true},
    TabletCompactionReopenFailureCase{
        test::SnapshotStorageFault::kFinalDirectorySync, raft::test::DurableIoFault::kDirectorySync,
        0U, "cleanup_sync_then_repair_directory_sync", "fsync directory", true, true},
};

enum class TabletCompactionMismatch : std::uint8_t {
  kTable,
  kTablet,
  kTerm,
  kManifestGeneration,
  kPartSetChecksum,
  kVoters,
  kEntryPayload,
};

struct TabletCompactionMismatchCase {
  TabletCompactionMismatch mismatch;
  std::string_view name;
};

constexpr std::array<TabletCompactionMismatchCase, 7U> kMismatchCases{
    TabletCompactionMismatchCase{TabletCompactionMismatch::kTable, "table"},
    TabletCompactionMismatchCase{TabletCompactionMismatch::kTablet, "tablet"},
    TabletCompactionMismatchCase{TabletCompactionMismatch::kTerm, "term"},
    TabletCompactionMismatchCase{TabletCompactionMismatch::kManifestGeneration,
                                 "manifest_generation"},
    TabletCompactionMismatchCase{TabletCompactionMismatch::kPartSetChecksum, "part_set_checksum"},
    TabletCompactionMismatchCase{TabletCompactionMismatch::kVoters, "voters"},
    TabletCompactionMismatchCase{TabletCompactionMismatch::kEntryPayload, "entry_payload"},
};

[[nodiscard]] RaftTabletApplicationSnapshot
conflicting_application_snapshot(const TabletCompactionMismatch mismatch) {
  RaftTabletApplicationSnapshot snapshot{
      .group_id = test::crash_group_id(),
      .table_id = test::crash_compaction_schemas().front()->table_id(),
      .tablet_id = test::crash_compaction_tablet_id(),
      .raft_snapshot = {.last_included_index = 1U,
                        .last_included_term = 1U,
                        .manifest_generation = 1U,
                        .part_set_checksum = {},
                        .configuration_index = 0U,
                        .voters = {4U}},
      .entries = {{.index = 1U, .term = 1U, .payload = test::crash_compaction_command()}}};
  switch (mismatch) {
  case TabletCompactionMismatch::kTable:
    snapshot.table_id = test::crash_id<schema::TableId>(std::byte{91U});
    break;
  case TabletCompactionMismatch::kTablet:
    snapshot.tablet_id = test::crash_id<schema::TabletId>(std::byte{92U});
    break;
  case TabletCompactionMismatch::kTerm:
    snapshot.raft_snapshot.last_included_term = 2U;
    snapshot.entries.front().term = 2U;
    break;
  case TabletCompactionMismatch::kManifestGeneration:
    snapshot.raft_snapshot.manifest_generation = 2U;
    break;
  case TabletCompactionMismatch::kPartSetChecksum:
    snapshot.raft_snapshot.part_set_checksum.front() = std::byte{1U};
    break;
  case TabletCompactionMismatch::kVoters:
    snapshot.raft_snapshot.voters.push_back(5U);
    break;
  case TabletCompactionMismatch::kEntryPayload:
    snapshot.entries.front().payload.front() ^= std::byte{1U};
    break;
  }
  return snapshot;
}

void expect_application_snapshot(const RaftTabletApplicationSnapshot& snapshot) {
  EXPECT_EQ(snapshot.group_id, test::crash_group_id());
  EXPECT_EQ(snapshot.table_id, test::crash_compaction_schemas().front()->table_id());
  EXPECT_EQ(snapshot.tablet_id, test::crash_compaction_tablet_id());
  EXPECT_EQ(snapshot.raft_snapshot.last_included_index, 1U);
  EXPECT_EQ(snapshot.raft_snapshot.last_included_term, 1U);
  EXPECT_EQ(snapshot.raft_snapshot.manifest_generation, 1U);
  EXPECT_EQ(snapshot.raft_snapshot.part_set_checksum, (std::array<std::byte, 32U>{}));
  EXPECT_EQ(snapshot.raft_snapshot.configuration_index, 0U);
  EXPECT_EQ(snapshot.raft_snapshot.voters, std::vector<raft::NodeId>{4U});
  ASSERT_EQ(snapshot.entries.size(), 1U);
  EXPECT_EQ(snapshot.entries.front().index, 1U);
  EXPECT_EQ(snapshot.entries.front().term, 1U);
  EXPECT_EQ(snapshot.entries.front().payload, test::crash_compaction_command());
}

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

class TabletSnapshotCompactionMixedFailureTest
    : public ::testing::TestWithParam<TabletCompactionMixedFailureCase> {};

class TabletSnapshotCompactionCleanupPersistenceFailureTest
    : public ::testing::TestWithParam<TabletCompactionCleanupPersistenceFailureCase> {};

class TabletSnapshotCompactionReopenFailureTest
    : public ::testing::TestWithParam<TabletCompactionReopenFailureCase> {};

class TabletSnapshotCompactionMismatchTest
    : public ::testing::TestWithParam<TabletCompactionMismatchCase> {};

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

TEST_P(TabletSnapshotCompactionMixedFailureTest,
       WithholdsBothAttemptsAndConvergesFromRecoveredOwners) {
  CompactionFailureDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const auto& [application_failure, raft_failure] = GetParam();
  test::OneShotSnapshotStorageFault application_syscalls{application_failure.fault};
  OneShotTabletCompactionRaftFault raft_syscalls{raft_failure.fault};

  {
    auto runtime = raft::detail::DurableMultiRaftRuntimeTestAccess::create_new(
        4U, test::crash_log_config(directory.path()), test::crash_compaction_groups(), {},
        raft_syscalls);
    ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
    auto election =
        runtime->execute_batch({{test::crash_group_id(), raft::StartElectionOperation{}}});
    ASSERT_TRUE(election.has_value()) << election.error().to_string();
    ASSERT_EQ(election->size(), 1U);
    ASSERT_TRUE(election->front().status.is_ok()) << election->front().status.to_string();
    auto storage = detail::RaftTabletSnapshotStorageTestAccess::create(
        test::crash_snapshot_config(directory.path()), application_syscalls);
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
    application_syscalls.arm();
    raft_syscalls.arm();

    auto compacted = machine->compact_applied_prefix(1U, 1U, {});

    ASSERT_FALSE(compacted.has_value());
    EXPECT_EQ(compacted.error().code(), common::StatusCode::kIoError);
    EXPECT_TRUE(application_syscalls.fired());
    EXPECT_FALSE(raft_syscalls.fired());
    EXPECT_FALSE(machine->failed());
    EXPECT_FALSE(runtime->failed());
    const raft::RaftNode* node = runtime->find_group(test::crash_group_id());
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->persistent_state().snapshot.last_included_index, 0U);
    ASSERT_EQ(node->persistent_state().log.size(), 1U);
    machine.reset();
    ASSERT_TRUE(runtime->close().is_ok());
  }

  {
    auto storage =
        RaftTabletSnapshotStorage::open_existing(test::crash_snapshot_config(directory.path()));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    auto after_first_failure = storage->load(1U);
    if (application_failure.application_snapshot_visible_after_failure) {
      ASSERT_TRUE(after_first_failure.has_value()) << after_first_failure.error().to_string();
      expect_application_snapshot(after_first_failure->snapshot);
    } else {
      ASSERT_FALSE(after_first_failure.has_value());
      EXPECT_EQ(after_first_failure.error().code(), common::StatusCode::kNotFound);
    }
  }

  {
    auto runtime = raft::detail::DurableMultiRaftRuntimeTestAccess::open_existing(
        4U, test::crash_log_config(directory.path()), {}, test::crash_compaction_groups(), {},
        raft_syscalls);
    ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
    auto storage =
        RaftTabletSnapshotStorage::open_existing(test::crash_snapshot_config(directory.path()));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    auto recovered = RaftTabletStateMachine::recover(
        test::crash_group_id(), *runtime, std::move(*storage),
        test::crash_compaction_retry_directory(), test::crash_compaction_tablet(),
        test::crash_compaction_schemas());
    ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
    std::optional<RaftTabletStateMachine> machine{std::move(*recovered)};
    expect_tablet_recovered(*machine);
    EXPECT_FALSE(raft_syscalls.fired());

    auto compacted = machine->compact_applied_prefix(1U, 1U, {});

    ASSERT_FALSE(compacted.has_value());
    EXPECT_EQ(compacted.error().code(), common::StatusCode::kIoError);
    EXPECT_TRUE(raft_syscalls.fired());
    EXPECT_TRUE(machine->failed());
    EXPECT_TRUE(runtime->failed());
    machine.reset();
    ASSERT_TRUE(runtime->close().is_ok());
  }

  auto installed_storage =
      RaftTabletSnapshotStorage::open_existing(test::crash_snapshot_config(directory.path()));
  ASSERT_TRUE(installed_storage.has_value()) << installed_storage.error().to_string();
  auto installed = installed_storage->load(1U);
  ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
  expect_application_snapshot(installed->snapshot);

  if (raft_failure.tail_repair_required) {
    auto strict = raft::DurableMultiRaftRuntime::open_existing(
        4U, test::crash_log_config(directory.path()), {}, test::crash_compaction_groups());
    ASSERT_FALSE(strict.has_value());
    EXPECT_EQ(strict.error().code(), common::StatusCode::kCorruption);
  }
  const raft::RaftPersistentLogOpenOptions open_options{.repair_incomplete_final_tail =
                                                            raft_failure.tail_repair_required};
  auto runtime = raft::DurableMultiRaftRuntime::open_existing(
      4U, test::crash_log_config(directory.path()), open_options, test::crash_compaction_groups());
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  const raft::RaftNode* recovered_node = runtime->find_group(test::crash_group_id());
  ASSERT_NE(recovered_node, nullptr);
  EXPECT_EQ(recovered_node->persistent_state().snapshot.last_included_index,
            raft_failure.raft_authority_recovered ? 1U : 0U);
  EXPECT_EQ(recovered_node->persistent_state().log.size(),
            raft_failure.raft_authority_recovered ? 0U : 1U);
  auto recovered = RaftTabletStateMachine::recover(
      test::crash_group_id(), *runtime, std::move(*installed_storage),
      test::crash_compaction_retry_directory(), test::crash_compaction_tablet(),
      test::crash_compaction_schemas());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  std::optional<RaftTabletStateMachine> machine{std::move(*recovered)};
  expect_tablet_recovered(*machine);

  if (!raft_failure.raft_authority_recovered) {
    auto compacted = machine->compact_applied_prefix(1U, 1U, {});

    ASSERT_TRUE(compacted.has_value()) << compacted.error().to_string();
    EXPECT_TRUE(compacted->application_snapshot_already_present);
    EXPECT_EQ(compacted->application_entries, 1U);
    recovered_node = runtime->find_group(test::crash_group_id());
    ASSERT_NE(recovered_node, nullptr);
  }
  EXPECT_EQ(recovered_node->persistent_state().snapshot, installed->snapshot.raft_snapshot);
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
  auto final_snapshot = repeated_storage->load(1U);
  ASSERT_TRUE(final_snapshot.has_value()) << final_snapshot.error().to_string();
  expect_application_snapshot(final_snapshot->snapshot);
  const raft::RaftNode* repeated_node = repeated_runtime->find_group(test::crash_group_id());
  ASSERT_NE(repeated_node, nullptr);
  EXPECT_EQ(repeated_node->persistent_state().snapshot, final_snapshot->snapshot.raft_snapshot);
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
    EveryCrossOwnerFailure, TabletSnapshotCompactionMixedFailureTest,
    ::testing::Combine(::testing::ValuesIn(kApplicationFailures),
                       ::testing::ValuesIn(kRaftFailures)),
    [](const ::testing::TestParamInfo<TabletCompactionMixedFailureCase>& parameter) {
      std::string name{std::get<0>(parameter.param).name};
      name += "_then_";
      name += std::get<1>(parameter.param).name;
      return name;
    });

TEST_P(TabletSnapshotCompactionCleanupPersistenceFailureTest,
       SurvivesCleanupFailureBeforeEveryRaftPersistenceOutcome) {
  CompactionFailureDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const auto& [cleanup_failure, raft_failure] = GetParam();
  const std::filesystem::path snapshot_temporary =
      directory.path() / "snapshots" / "snapshot-00000000000000000001.rtas.tmp";

  {
    test::OneShotSnapshotStorageFault application_syscalls{
        test::SnapshotStorageFault::kTemporaryPartialWrite};
    auto runtime = raft::DurableMultiRaftRuntime::create_new(
        4U, test::crash_log_config(directory.path()), test::crash_compaction_groups());
    ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
    auto election =
        runtime->execute_batch({{test::crash_group_id(), raft::StartElectionOperation{}}});
    ASSERT_TRUE(election.has_value()) << election.error().to_string();
    ASSERT_EQ(election->size(), 1U);
    ASSERT_TRUE(election->front().status.is_ok()) << election->front().status.to_string();
    auto storage = detail::RaftTabletSnapshotStorageTestAccess::create(
        test::crash_snapshot_config(directory.path()), application_syscalls);
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
    application_syscalls.arm();

    auto compacted = machine->compact_applied_prefix(1U, 1U, {});

    ASSERT_FALSE(compacted.has_value());
    EXPECT_EQ(compacted.error().code(), common::StatusCode::kIoError);
    EXPECT_TRUE(application_syscalls.fired());
    EXPECT_TRUE(std::filesystem::exists(snapshot_temporary));
    EXPECT_FALSE(machine->failed());
    EXPECT_FALSE(runtime->failed());
    machine.reset();
    ASSERT_TRUE(runtime->close().is_ok());
  }

  test::OneShotSnapshotStorageFault cleanup_syscalls{cleanup_failure.fault};
  cleanup_syscalls.arm();
  auto failed_storage = detail::RaftTabletSnapshotStorageTestAccess::open_existing(
      test::crash_snapshot_config(directory.path()), cleanup_syscalls);
  ASSERT_FALSE(failed_storage.has_value());
  EXPECT_EQ(failed_storage.error().code(), common::StatusCode::kIoError);
  EXPECT_TRUE(cleanup_syscalls.fired());
  EXPECT_EQ(std::filesystem::exists(snapshot_temporary), !cleanup_failure.temporary_removed);

  auto storage =
      RaftTabletSnapshotStorage::open_existing(test::crash_snapshot_config(directory.path()));
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  EXPECT_FALSE(std::filesystem::exists(snapshot_temporary));
  auto absent = storage->load(1U);
  ASSERT_FALSE(absent.has_value());
  EXPECT_EQ(absent.error().code(), common::StatusCode::kNotFound);

  OneShotTabletCompactionRaftFault raft_syscalls{raft_failure.fault};
  {
    auto runtime = raft::detail::DurableMultiRaftRuntimeTestAccess::open_existing(
        4U, test::crash_log_config(directory.path()), {}, test::crash_compaction_groups(), {},
        raft_syscalls);
    ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
    auto recovered = RaftTabletStateMachine::recover(
        test::crash_group_id(), *runtime, std::move(*storage),
        test::crash_compaction_retry_directory(), test::crash_compaction_tablet(),
        test::crash_compaction_schemas());
    ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
    std::optional<RaftTabletStateMachine> machine{std::move(*recovered)};
    expect_tablet_recovered(*machine);
    raft_syscalls.arm();

    auto compacted = machine->compact_applied_prefix(1U, 1U, {});

    ASSERT_FALSE(compacted.has_value());
    EXPECT_EQ(compacted.error().code(), common::StatusCode::kIoError);
    EXPECT_TRUE(raft_syscalls.fired());
    EXPECT_TRUE(machine->failed());
    EXPECT_TRUE(runtime->failed());
    machine.reset();
    ASSERT_TRUE(runtime->close().is_ok());
  }

  auto installed_storage =
      RaftTabletSnapshotStorage::open_existing(test::crash_snapshot_config(directory.path()));
  ASSERT_TRUE(installed_storage.has_value()) << installed_storage.error().to_string();
  auto installed = installed_storage->load(1U);
  ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
  expect_application_snapshot(installed->snapshot);

  if (raft_failure.tail_repair_required) {
    auto strict = raft::DurableMultiRaftRuntime::open_existing(
        4U, test::crash_log_config(directory.path()), {}, test::crash_compaction_groups());
    ASSERT_FALSE(strict.has_value());
    EXPECT_EQ(strict.error().code(), common::StatusCode::kCorruption);
  }
  const raft::RaftPersistentLogOpenOptions open_options{.repair_incomplete_final_tail =
                                                            raft_failure.tail_repair_required};
  auto runtime = raft::DurableMultiRaftRuntime::open_existing(
      4U, test::crash_log_config(directory.path()), open_options, test::crash_compaction_groups());
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  const raft::RaftNode* recovered_node = runtime->find_group(test::crash_group_id());
  ASSERT_NE(recovered_node, nullptr);
  EXPECT_EQ(recovered_node->persistent_state().snapshot.last_included_index,
            raft_failure.raft_authority_recovered ? 1U : 0U);
  EXPECT_EQ(recovered_node->persistent_state().log.size(),
            raft_failure.raft_authority_recovered ? 0U : 1U);
  auto recovered = RaftTabletStateMachine::recover(
      test::crash_group_id(), *runtime, std::move(*installed_storage),
      test::crash_compaction_retry_directory(), test::crash_compaction_tablet(),
      test::crash_compaction_schemas());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  std::optional<RaftTabletStateMachine> machine{std::move(*recovered)};
  expect_tablet_recovered(*machine);

  if (!raft_failure.raft_authority_recovered) {
    auto compacted = machine->compact_applied_prefix(1U, 1U, {});

    ASSERT_TRUE(compacted.has_value()) << compacted.error().to_string();
    EXPECT_TRUE(compacted->application_snapshot_already_present);
    EXPECT_EQ(compacted->application_entries, 1U);
    recovered_node = runtime->find_group(test::crash_group_id());
    ASSERT_NE(recovered_node, nullptr);
  }
  EXPECT_EQ(recovered_node->persistent_state().snapshot, installed->snapshot.raft_snapshot);
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
  auto final_snapshot = repeated_storage->load(1U);
  ASSERT_TRUE(final_snapshot.has_value()) << final_snapshot.error().to_string();
  expect_application_snapshot(final_snapshot->snapshot);
  auto repeated = RaftTabletStateMachine::recover(
      test::crash_group_id(), *repeated_runtime, std::move(*repeated_storage),
      test::crash_compaction_retry_directory(), test::crash_compaction_tablet(),
      test::crash_compaction_schemas());
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  expect_tablet_recovered(*repeated);
  const raft::RaftNode* repeated_node = repeated_runtime->find_group(test::crash_group_id());
  ASSERT_NE(repeated_node, nullptr);
  EXPECT_EQ(repeated_node->persistent_state().snapshot, final_snapshot->snapshot.raft_snapshot);
  ASSERT_TRUE(repeated_runtime->close().is_ok());
}

INSTANTIATE_TEST_SUITE_P(
    EveryCleanupToPersistenceFailure, TabletSnapshotCompactionCleanupPersistenceFailureTest,
    ::testing::Combine(::testing::ValuesIn(kCleanupFailures), ::testing::ValuesIn(kRaftFailures)),
    [](const ::testing::TestParamInfo<TabletCompactionCleanupPersistenceFailureCase>& parameter) {
      std::string name{std::get<0>(parameter.param).name};
      name += "_then_";
      name += std::get<1>(parameter.param).name;
      return name;
    });

TEST_P(TabletSnapshotCompactionMismatchTest,
       RejectsImmutableSameIndexConflictAndAdvancesAtLaterBoundary) {
  CompactionFailureDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const TabletCompactionMismatchCase mismatch = GetParam();
  const std::filesystem::path conflicting_path =
      directory.path() / "snapshots" / "snapshot-00000000000000000001.rtas";
  const std::filesystem::path later_path =
      directory.path() / "snapshots" / "snapshot-00000000000000000002.rtas";

  auto runtime = raft::DurableMultiRaftRuntime::create_new(
      4U, test::crash_log_config(directory.path()), test::crash_compaction_groups());
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  auto election =
      runtime->execute_batch({{test::crash_group_id(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value()) << election.error().to_string();
  ASSERT_EQ(election->size(), 1U);
  ASSERT_TRUE(election->front().status.is_ok()) << election->front().status.to_string();
  auto storage = RaftTabletSnapshotStorage::create(test::crash_snapshot_config(directory.path()));
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  const RaftTabletApplicationSnapshot conflicting =
      conflicting_application_snapshot(mismatch.mismatch);
  auto planted = storage->install(conflicting);
  ASSERT_TRUE(planted.has_value()) << planted.error().to_string();
  EXPECT_FALSE(planted->already_present);
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

  auto rejected = machine->compact_applied_prefix(1U, 1U, {});

  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kCorruption);
  EXPECT_FALSE(machine->failed());
  EXPECT_FALSE(runtime->failed());
  const raft::RaftNode* node = runtime->find_group(test::crash_group_id());
  ASSERT_NE(node, nullptr);
  EXPECT_EQ(node->persistent_state().snapshot.last_included_index, 0U);
  ASSERT_EQ(node->persistent_state().log.size(), 1U);
  EXPECT_TRUE(std::filesystem::exists(conflicting_path));

  proposed = runtime->execute_batch(
      {{test::crash_group_id(),
        raft::ProposeOperation{kRaftColumnarAppendEntryType, test::crash_compaction_command()}}});
  ASSERT_TRUE(proposed.has_value()) << proposed.error().to_string();
  ASSERT_EQ(proposed->size(), 1U);
  ASSERT_TRUE(proposed->front().status.is_ok()) << proposed->front().status.to_string();
  applied = machine->apply_committed();
  ASSERT_TRUE(applied.has_value()) << applied.error().to_string();
  EXPECT_EQ(applied->last_applied_index, 2U);
  EXPECT_EQ(applied->matching_retries, 1U);
  auto publication = machine->tablet().snapshot();
  ASSERT_TRUE(publication.has_value()) << publication.error().to_string();
  EXPECT_EQ(publication->visible_row_count(), 2U);
  EXPECT_EQ(publication->retry_entry_count(), 1U);
  EXPECT_EQ(publication->applied_position(),
            head::HeadCommitPosition::raft(test::crash_group_id(), 2U));

  auto compacted = machine->compact_applied_prefix(2U, 2U, {});

  ASSERT_TRUE(compacted.has_value()) << compacted.error().to_string();
  EXPECT_FALSE(compacted->application_snapshot_already_present);
  EXPECT_EQ(compacted->application_entries, 2U);
  EXPECT_EQ(compacted->snapshot.last_included_index, 2U);
  node = runtime->find_group(test::crash_group_id());
  ASSERT_NE(node, nullptr);
  EXPECT_EQ(node->persistent_state().snapshot, compacted->snapshot);
  EXPECT_TRUE(node->persistent_state().log.empty());
  auto reclaimed = machine->reclaim_obsolete_snapshots();
  ASSERT_TRUE(reclaimed.has_value()) << reclaimed.error().to_string();
  EXPECT_EQ(reclaimed->authoritative_index, 2U);
  EXPECT_EQ(reclaimed->reclaimed_files, 1U);
  EXPECT_FALSE(std::filesystem::exists(conflicting_path));
  EXPECT_TRUE(std::filesystem::exists(later_path));
  machine.reset();
  ASSERT_TRUE(runtime->close().is_ok());

  auto repeated_runtime = raft::DurableMultiRaftRuntime::open_existing(
      4U, test::crash_log_config(directory.path()), {}, test::crash_compaction_groups());
  ASSERT_TRUE(repeated_runtime.has_value()) << repeated_runtime.error().to_string();
  auto repeated_storage =
      RaftTabletSnapshotStorage::open_existing(test::crash_snapshot_config(directory.path()));
  ASSERT_TRUE(repeated_storage.has_value()) << repeated_storage.error().to_string();
  auto installed = repeated_storage->load(2U);
  ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
  EXPECT_EQ(installed->snapshot.raft_snapshot,
            repeated_runtime->find_group(test::crash_group_id())->persistent_state().snapshot);
  ASSERT_EQ(installed->snapshot.entries.size(), 2U);
  auto repeated = RaftTabletStateMachine::recover(
      test::crash_group_id(), *repeated_runtime, std::move(*repeated_storage),
      test::crash_compaction_retry_directory(), test::crash_compaction_tablet(),
      test::crash_compaction_schemas());
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  publication = repeated->tablet().snapshot();
  ASSERT_TRUE(publication.has_value()) << publication.error().to_string();
  EXPECT_EQ(publication->visible_row_count(), 2U);
  EXPECT_EQ(publication->retry_entry_count(), 1U);
  EXPECT_EQ(publication->applied_position(),
            head::HeadCommitPosition::raft(test::crash_group_id(), 2U));
  ASSERT_TRUE(repeated_runtime->close().is_ok());
}

INSTANTIATE_TEST_SUITE_P(
    EveryImmutableFieldMismatch, TabletSnapshotCompactionMismatchTest,
    ::testing::ValuesIn(kMismatchCases),
    [](const ::testing::TestParamInfo<TabletCompactionMismatchCase>& parameter) {
      return std::string{parameter.param.name};
    });

TEST(TabletSnapshotCompactionRepeatedFailureTest,
     SurvivesTwoPartialWritesInEachOwnerBeforeSuccess) {
  CompactionFailureDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const std::filesystem::path snapshot_temporary =
      directory.path() / "snapshots" / "snapshot-00000000000000000001.rtas.tmp";
  const std::filesystem::path snapshot_final =
      directory.path() / "snapshots" / "snapshot-00000000000000000001.rtas";

  {
    auto runtime = raft::DurableMultiRaftRuntime::create_new(
        4U, test::crash_log_config(directory.path()), test::crash_compaction_groups());
    ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
    auto election =
        runtime->execute_batch({{test::crash_group_id(), raft::StartElectionOperation{}}});
    ASSERT_TRUE(election.has_value()) << election.error().to_string();
    ASSERT_EQ(election->size(), 1U);
    ASSERT_TRUE(election->front().status.is_ok()) << election->front().status.to_string();
    auto storage = RaftTabletSnapshotStorage::create(test::crash_snapshot_config(directory.path()));
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
    machine.reset();
    ASSERT_TRUE(runtime->close().is_ok());
  }

  for (std::size_t attempt = 0U; attempt < 2U; ++attempt) {
    SCOPED_TRACE(attempt);
    test::OneShotSnapshotStorageFault application_syscalls{
        test::SnapshotStorageFault::kTemporaryPartialWrite};
    auto storage = detail::RaftTabletSnapshotStorageTestAccess::open_existing(
        test::crash_snapshot_config(directory.path()), application_syscalls);
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    EXPECT_FALSE(std::filesystem::exists(snapshot_temporary));
    auto runtime = raft::DurableMultiRaftRuntime::open_existing(
        4U, test::crash_log_config(directory.path()), {}, test::crash_compaction_groups());
    ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
    auto recovered = RaftTabletStateMachine::recover(
        test::crash_group_id(), *runtime, std::move(*storage),
        test::crash_compaction_retry_directory(), test::crash_compaction_tablet(),
        test::crash_compaction_schemas());
    ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
    std::optional<RaftTabletStateMachine> machine{std::move(*recovered)};
    expect_tablet_recovered(*machine);
    application_syscalls.arm();

    auto compacted = machine->compact_applied_prefix(1U, 1U, {});

    ASSERT_FALSE(compacted.has_value());
    EXPECT_EQ(compacted.error().code(), common::StatusCode::kIoError);
    EXPECT_TRUE(application_syscalls.fired());
    EXPECT_TRUE(std::filesystem::exists(snapshot_temporary));
    EXPECT_FALSE(std::filesystem::exists(snapshot_final));
    EXPECT_FALSE(machine->failed());
    EXPECT_FALSE(runtime->failed());
    const raft::RaftNode* node = runtime->find_group(test::crash_group_id());
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->persistent_state().snapshot.last_included_index, 0U);
    ASSERT_EQ(node->persistent_state().log.size(), 1U);
    machine.reset();
    ASSERT_TRUE(runtime->close().is_ok());
  }

  {
    auto storage =
        RaftTabletSnapshotStorage::open_existing(test::crash_snapshot_config(directory.path()));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    EXPECT_FALSE(std::filesystem::exists(snapshot_temporary));
    auto before_raft_failures = storage->load(1U);
    ASSERT_FALSE(before_raft_failures.has_value());
    EXPECT_EQ(before_raft_failures.error().code(), common::StatusCode::kNotFound);
  }

  for (std::size_t attempt = 0U; attempt < 2U; ++attempt) {
    SCOPED_TRACE(attempt);
    OneShotTabletCompactionRaftFault raft_syscalls{
        TabletCompactionRaftFault::kWritePrefixThenError};
    auto runtime = raft::detail::DurableMultiRaftRuntimeTestAccess::open_existing(
        4U, test::crash_log_config(directory.path()),
        raft::RaftPersistentLogOpenOptions{.repair_incomplete_final_tail = true},
        test::crash_compaction_groups(), {}, raft_syscalls);
    ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
    auto storage =
        RaftTabletSnapshotStorage::open_existing(test::crash_snapshot_config(directory.path()));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    auto recovered = RaftTabletStateMachine::recover(
        test::crash_group_id(), *runtime, std::move(*storage),
        test::crash_compaction_retry_directory(), test::crash_compaction_tablet(),
        test::crash_compaction_schemas());
    ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
    std::optional<RaftTabletStateMachine> machine{std::move(*recovered)};
    expect_tablet_recovered(*machine);
    raft_syscalls.arm();

    auto compacted = machine->compact_applied_prefix(1U, 1U, {});

    ASSERT_FALSE(compacted.has_value());
    EXPECT_EQ(compacted.error().code(), common::StatusCode::kIoError);
    EXPECT_TRUE(raft_syscalls.fired());
    EXPECT_TRUE(machine->failed());
    EXPECT_TRUE(runtime->failed());
    EXPECT_TRUE(std::filesystem::exists(snapshot_final));
    machine.reset();
    ASSERT_TRUE(runtime->close().is_ok());

    {
      auto installed_storage =
          RaftTabletSnapshotStorage::open_existing(test::crash_snapshot_config(directory.path()));
      ASSERT_TRUE(installed_storage.has_value()) << installed_storage.error().to_string();
      auto installed = installed_storage->load(1U);
      ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
      expect_application_snapshot(installed->snapshot);
    }

    std::filesystem::path active_segment;
    for (const std::filesystem::directory_entry& entry :
         std::filesystem::directory_iterator(directory.path() / "raft")) {
      if (entry.path().extension() == ".rlog")
        active_segment = entry.path();
    }
    ASSERT_FALSE(active_segment.empty());
    const std::uintmax_t incomplete_size = std::filesystem::file_size(active_segment);
    auto strict = raft::DurableMultiRaftRuntime::open_existing(
        4U, test::crash_log_config(directory.path()), {}, test::crash_compaction_groups());
    ASSERT_FALSE(strict.has_value());
    EXPECT_EQ(strict.error().code(), common::StatusCode::kCorruption);
    EXPECT_EQ(std::filesystem::file_size(active_segment), incomplete_size);
    auto repaired = raft::DurableMultiRaftRuntime::open_existing(
        4U, test::crash_log_config(directory.path()),
        raft::RaftPersistentLogOpenOptions{.repair_incomplete_final_tail = true},
        test::crash_compaction_groups());
    ASSERT_TRUE(repaired.has_value()) << repaired.error().to_string();
    EXPECT_EQ(std::filesystem::file_size(active_segment) + kPartialRaftRecordBytes,
              incomplete_size);
    const raft::RaftNode* repaired_node = repaired->find_group(test::crash_group_id());
    ASSERT_NE(repaired_node, nullptr);
    EXPECT_EQ(repaired_node->persistent_state().snapshot.last_included_index, 0U);
    ASSERT_EQ(repaired_node->persistent_state().log.size(), 1U);
    ASSERT_TRUE(repaired->close().is_ok());
  }

  auto runtime = raft::DurableMultiRaftRuntime::open_existing(
      4U, test::crash_log_config(directory.path()), {}, test::crash_compaction_groups());
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  auto storage =
      RaftTabletSnapshotStorage::open_existing(test::crash_snapshot_config(directory.path()));
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  auto orphan = storage->load(1U);
  ASSERT_TRUE(orphan.has_value()) << orphan.error().to_string();
  expect_application_snapshot(orphan->snapshot);
  auto recovered = RaftTabletStateMachine::recover(
      test::crash_group_id(), *runtime, std::move(*storage),
      test::crash_compaction_retry_directory(), test::crash_compaction_tablet(),
      test::crash_compaction_schemas());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  std::optional<RaftTabletStateMachine> machine{std::move(*recovered)};
  expect_tablet_recovered(*machine);

  auto compacted = machine->compact_applied_prefix(1U, 1U, {});

  ASSERT_TRUE(compacted.has_value()) << compacted.error().to_string();
  EXPECT_TRUE(compacted->application_snapshot_already_present);
  EXPECT_EQ(compacted->application_entries, 1U);
  const raft::RaftNode* node = runtime->find_group(test::crash_group_id());
  ASSERT_NE(node, nullptr);
  EXPECT_EQ(node->persistent_state().snapshot, orphan->snapshot.raft_snapshot);
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
  auto final_snapshot = repeated_storage->load(1U);
  ASSERT_TRUE(final_snapshot.has_value()) << final_snapshot.error().to_string();
  expect_application_snapshot(final_snapshot->snapshot);
  auto repeated = RaftTabletStateMachine::recover(
      test::crash_group_id(), *repeated_runtime, std::move(*repeated_storage),
      test::crash_compaction_retry_directory(), test::crash_compaction_tablet(),
      test::crash_compaction_schemas());
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  expect_tablet_recovered(*repeated);
  const raft::RaftNode* repeated_node = repeated_runtime->find_group(test::crash_group_id());
  ASSERT_NE(repeated_node, nullptr);
  EXPECT_EQ(repeated_node->persistent_state().snapshot, final_snapshot->snapshot.raft_snapshot);
  ASSERT_TRUE(repeated_runtime->close().is_ok());
}

TEST_P(TabletSnapshotCompactionReopenFailureTest,
       ReleasesBothFailedReopensAndConvergesFromObservedBytes) {
  CompactionFailureDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const TabletCompactionReopenFailureCase failure = GetParam();
  const std::filesystem::path snapshot_temporary =
      directory.path() / "snapshots" / "snapshot-00000000000000000001.rtas.tmp";

  {
    test::OneShotSnapshotStorageFault application_syscalls{
        test::SnapshotStorageFault::kTemporaryPartialWrite};
    auto runtime = raft::DurableMultiRaftRuntime::create_new(
        4U, test::crash_log_config(directory.path()), test::crash_compaction_groups());
    ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
    auto election =
        runtime->execute_batch({{test::crash_group_id(), raft::StartElectionOperation{}}});
    ASSERT_TRUE(election.has_value()) << election.error().to_string();
    ASSERT_EQ(election->size(), 1U);
    ASSERT_TRUE(election->front().status.is_ok()) << election->front().status.to_string();
    auto storage = detail::RaftTabletSnapshotStorageTestAccess::create(
        test::crash_snapshot_config(directory.path()), application_syscalls);
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
    application_syscalls.arm();

    auto compacted = machine->compact_applied_prefix(1U, 1U, {});

    ASSERT_FALSE(compacted.has_value());
    EXPECT_EQ(compacted.error().code(), common::StatusCode::kIoError);
    EXPECT_TRUE(application_syscalls.fired());
    EXPECT_TRUE(std::filesystem::exists(snapshot_temporary));
    EXPECT_FALSE(runtime->failed());
    machine.reset();
    ASSERT_TRUE(runtime->close().is_ok());
  }

  test::OneShotSnapshotStorageFault application_reopen_syscalls{failure.application_reopen_fault};
  application_reopen_syscalls.arm();
  auto failed_storage = detail::RaftTabletSnapshotStorageTestAccess::open_existing(
      test::crash_snapshot_config(directory.path()), application_reopen_syscalls);
  ASSERT_FALSE(failed_storage.has_value());
  EXPECT_EQ(failed_storage.error().code(), common::StatusCode::kIoError);
  EXPECT_TRUE(application_reopen_syscalls.fired());
  EXPECT_EQ(std::filesystem::exists(snapshot_temporary), !failure.application_temporary_removed);

  auto storage =
      RaftTabletSnapshotStorage::open_existing(test::crash_snapshot_config(directory.path()));
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  EXPECT_FALSE(std::filesystem::exists(snapshot_temporary));
  auto before_raft_failure = storage->load(1U);
  ASSERT_FALSE(before_raft_failure.has_value());
  EXPECT_EQ(before_raft_failure.error().code(), common::StatusCode::kNotFound);

  OneShotTabletCompactionRaftFault raft_compaction_syscalls{
      TabletCompactionRaftFault::kWritePrefixThenError};
  {
    auto runtime = raft::detail::DurableMultiRaftRuntimeTestAccess::open_existing(
        4U, test::crash_log_config(directory.path()), {}, test::crash_compaction_groups(), {},
        raft_compaction_syscalls);
    ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
    auto recovered = RaftTabletStateMachine::recover(
        test::crash_group_id(), *runtime, std::move(*storage),
        test::crash_compaction_retry_directory(), test::crash_compaction_tablet(),
        test::crash_compaction_schemas());
    ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
    std::optional<RaftTabletStateMachine> machine{std::move(*recovered)};
    expect_tablet_recovered(*machine);
    raft_compaction_syscalls.arm();

    auto compacted = machine->compact_applied_prefix(1U, 1U, {});

    ASSERT_FALSE(compacted.has_value());
    EXPECT_EQ(compacted.error().code(), common::StatusCode::kIoError);
    EXPECT_TRUE(raft_compaction_syscalls.fired());
    EXPECT_TRUE(machine->failed());
    EXPECT_TRUE(runtime->failed());
    machine.reset();
    ASSERT_TRUE(runtime->close().is_ok());
  }

  auto installed_storage =
      RaftTabletSnapshotStorage::open_existing(test::crash_snapshot_config(directory.path()));
  ASSERT_TRUE(installed_storage.has_value()) << installed_storage.error().to_string();
  auto installed = installed_storage->load(1U);
  ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
  expect_application_snapshot(installed->snapshot);

  std::filesystem::path active_segment;
  for (const std::filesystem::directory_entry& entry :
       std::filesystem::directory_iterator(directory.path() / "raft")) {
    if (entry.path().extension() == ".rlog")
      active_segment = entry.path();
  }
  ASSERT_FALSE(active_segment.empty());
  const std::uintmax_t incomplete_size = std::filesystem::file_size(active_segment);
  auto strict = raft::DurableMultiRaftRuntime::open_existing(
      4U, test::crash_log_config(directory.path()), {}, test::crash_compaction_groups());
  ASSERT_FALSE(strict.has_value());
  EXPECT_EQ(strict.error().code(), common::StatusCode::kCorruption);
  EXPECT_EQ(std::filesystem::file_size(active_segment), incomplete_size);

  raft::test::DurableIoFaultPosixSyscalls raft_reopen_syscalls;
  raft_reopen_syscalls.arm(failure.raft_reopen_fault, failure.raft_matching_calls_to_skip);
  auto failed_runtime = raft::detail::DurableMultiRaftRuntimeTestAccess::open_existing(
      4U, test::crash_log_config(directory.path()),
      raft::RaftPersistentLogOpenOptions{.repair_incomplete_final_tail = true},
      test::crash_compaction_groups(), {}, raft_reopen_syscalls);
  ASSERT_FALSE(failed_runtime.has_value());
  EXPECT_EQ(failed_runtime.error().code(), common::StatusCode::kIoError);
  EXPECT_NE(failed_runtime.error().to_string().find(failure.expected_raft_operation),
            std::string::npos);
  EXPECT_EQ(raft_reopen_syscalls.injected_faults(), 1U);
  EXPECT_EQ(std::filesystem::file_size(active_segment),
            failure.raft_tail_removed ? incomplete_size - kPartialRaftRecordBytes
                                      : incomplete_size);

  auto runtime = raft::DurableMultiRaftRuntime::open_existing(
      4U, test::crash_log_config(directory.path()),
      raft::RaftPersistentLogOpenOptions{.repair_incomplete_final_tail = true},
      test::crash_compaction_groups());
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  EXPECT_EQ(std::filesystem::file_size(active_segment) + kPartialRaftRecordBytes, incomplete_size);
  const raft::RaftNode* recovered_node = runtime->find_group(test::crash_group_id());
  ASSERT_NE(recovered_node, nullptr);
  EXPECT_EQ(recovered_node->persistent_state().snapshot.last_included_index, 0U);
  ASSERT_EQ(recovered_node->persistent_state().log.size(), 1U);
  auto orphan = installed_storage->load(1U);
  ASSERT_TRUE(orphan.has_value()) << orphan.error().to_string();
  expect_application_snapshot(orphan->snapshot);
  auto recovered = RaftTabletStateMachine::recover(
      test::crash_group_id(), *runtime, std::move(*installed_storage),
      test::crash_compaction_retry_directory(), test::crash_compaction_tablet(),
      test::crash_compaction_schemas());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  std::optional<RaftTabletStateMachine> machine{std::move(*recovered)};
  expect_tablet_recovered(*machine);

  auto compacted = machine->compact_applied_prefix(1U, 1U, {});

  ASSERT_TRUE(compacted.has_value()) << compacted.error().to_string();
  EXPECT_TRUE(compacted->application_snapshot_already_present);
  EXPECT_EQ(compacted->application_entries, 1U);
  recovered_node = runtime->find_group(test::crash_group_id());
  ASSERT_NE(recovered_node, nullptr);
  EXPECT_EQ(recovered_node->persistent_state().snapshot, orphan->snapshot.raft_snapshot);
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
  auto final_snapshot = repeated_storage->load(1U);
  ASSERT_TRUE(final_snapshot.has_value()) << final_snapshot.error().to_string();
  expect_application_snapshot(final_snapshot->snapshot);
  auto repeated = RaftTabletStateMachine::recover(
      test::crash_group_id(), *repeated_runtime, std::move(*repeated_storage),
      test::crash_compaction_retry_directory(), test::crash_compaction_tablet(),
      test::crash_compaction_schemas());
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  expect_tablet_recovered(*repeated);
  const raft::RaftNode* repeated_node = repeated_runtime->find_group(test::crash_group_id());
  ASSERT_NE(repeated_node, nullptr);
  EXPECT_EQ(repeated_node->persistent_state().snapshot, final_snapshot->snapshot.raft_snapshot);
  ASSERT_TRUE(repeated_runtime->close().is_ok());
}

INSTANTIATE_TEST_SUITE_P(
    EveryOwnerReopenFailure, TabletSnapshotCompactionReopenFailureTest,
    ::testing::ValuesIn(kReopenFailures),
    [](const ::testing::TestParamInfo<TabletCompactionReopenFailureCase>& parameter) {
      return std::string{parameter.param.name};
    });

} // namespace
} // namespace chronos::ingest
