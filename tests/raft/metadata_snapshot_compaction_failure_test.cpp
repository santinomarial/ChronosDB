#include "chronos/common/status.hpp"
#include "chronos/raft/durable_runtime.hpp"
#include "chronos/raft/metadata_runtime.hpp"
#include "chronos/raft/metadata_snapshot_storage.hpp"
#include "chronos/raft/node.hpp"
#include "raft/durable_runtime_internal.hpp"
#include "raft/metadata_snapshot_compaction_fault.hpp"
#include "raft/metadata_snapshot_install_crash_fixture.hpp"
#include "raft/metadata_snapshot_storage_internal.hpp"
#include "raft/raft_test_posix.hpp"

#include <array>
#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <tuple>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

class CompactionFailureDirectory {
public:
  CompactionFailureDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-metadata-compaction-failure-XXXXXX")
            .string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr) {
      path_ = created;
      std::error_code error;
      static_cast<void>(std::filesystem::create_directory(path_ / "raft", error));
      if (!error)
        static_cast<void>(std::filesystem::create_directory(path_ / "metadata-snapshots", error));
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

struct MetadataCompactionApplicationFailureCase {
  test::MetadataCompactionApplicationFault application_fault;
  std::string_view name;
  bool application_snapshot_visible_after_first_failure;
};

constexpr std::array<MetadataCompactionApplicationFailureCase, 10U> kApplicationFailures{
    MetadataCompactionApplicationFailureCase{
        test::MetadataCompactionApplicationFault::kTemporaryCreate, "temporary_create", false},
    MetadataCompactionApplicationFailureCase{
        test::MetadataCompactionApplicationFault::kTemporaryValidationStat,
        "temporary_validation_stat", false},
    MetadataCompactionApplicationFailureCase{
        test::MetadataCompactionApplicationFault::kTemporaryWrite, "temporary_write", false},
    MetadataCompactionApplicationFailureCase{
        test::MetadataCompactionApplicationFault::kTemporaryPartialWrite, "temporary_partial_write",
        false},
    MetadataCompactionApplicationFailureCase{
        test::MetadataCompactionApplicationFault::kTemporarySizeStat, "temporary_size_stat", false},
    MetadataCompactionApplicationFailureCase{
        test::MetadataCompactionApplicationFault::kTemporaryReadback, "temporary_readback", false},
    MetadataCompactionApplicationFailureCase{
        test::MetadataCompactionApplicationFault::kTemporaryFileSync, "temporary_file_sync", false},
    MetadataCompactionApplicationFailureCase{
        test::MetadataCompactionApplicationFault::kTemporaryClose, "temporary_close", false},
    MetadataCompactionApplicationFailureCase{test::MetadataCompactionApplicationFault::kFinalRename,
                                             "final_rename", false},
    MetadataCompactionApplicationFailureCase{
        test::MetadataCompactionApplicationFault::kFinalDirectorySync, "final_directory_sync",
        true},
};

struct MetadataCompactionRaftFailureCase {
  test::MetadataCompactionRaftFault raft_fault;
  std::string_view name;
  bool raft_tail_repair_required;
  bool raft_authority_recovered;
};

constexpr std::array<MetadataCompactionRaftFailureCase, 5U> kRaftFailures{
    MetadataCompactionRaftFailureCase{test::MetadataCompactionRaftFault::kWriteBefore,
                                      "raft_write_before", false, false},
    MetadataCompactionRaftFailureCase{test::MetadataCompactionRaftFault::kWritePrefixThenError,
                                      "raft_partial_write", true, false},
    MetadataCompactionRaftFailureCase{test::MetadataCompactionRaftFault::kWriteAfter,
                                      "raft_write_after", false, true},
    MetadataCompactionRaftFailureCase{test::MetadataCompactionRaftFault::kDataSyncBefore,
                                      "raft_data_sync_before", false, true},
    MetadataCompactionRaftFailureCase{test::MetadataCompactionRaftFault::kDataSyncAfter,
                                      "raft_data_sync_after", false, true},
};

using MetadataCompactionMixedFailureCase =
    std::tuple<MetadataCompactionApplicationFailureCase, MetadataCompactionRaftFailureCase>;

struct MetadataCompactionCleanupFailureCase {
  test::MetadataCompactionApplicationFault application_reopen_fault;
  std::string_view name;
  bool application_temporary_removed;
};

constexpr std::array<MetadataCompactionCleanupFailureCase, 2U> kCleanupFailures{
    MetadataCompactionCleanupFailureCase{
        test::MetadataCompactionApplicationFault::kPriorTemporaryUnlink, "cleanup_unlink", false},
    MetadataCompactionCleanupFailureCase{
        test::MetadataCompactionApplicationFault::kFinalDirectorySync, "cleanup_directory_sync",
        true},
};

using MetadataCompactionCleanupPersistenceFailureCase =
    std::tuple<MetadataCompactionCleanupFailureCase, MetadataCompactionRaftFailureCase>;

struct MetadataCompactionReopenFailureCase {
  test::MetadataCompactionApplicationFault application_reopen_fault;
  test::DurableIoFault raft_reopen_fault;
  std::size_t raft_matching_calls_to_skip;
  std::string_view name;
  std::string_view expected_raft_operation;
  bool application_temporary_removed;
  bool raft_tail_removed;
};

constexpr std::array<MetadataCompactionReopenFailureCase, 8U> kReopenFailures{
    MetadataCompactionReopenFailureCase{
        test::MetadataCompactionApplicationFault::kPriorTemporaryUnlink,
        test::DurableIoFault::kStat, 4U, "cleanup_unlink_then_repair_size_stat", "fstat file size",
        false, false},
    MetadataCompactionReopenFailureCase{
        test::MetadataCompactionApplicationFault::kPriorTemporaryUnlink,
        test::DurableIoFault::kTruncate, 0U, "cleanup_unlink_then_repair_truncate", "ftruncate",
        false, true},
    MetadataCompactionReopenFailureCase{
        test::MetadataCompactionApplicationFault::kPriorTemporaryUnlink,
        test::DurableIoFault::kFullSync, 0U, "cleanup_unlink_then_repair_file_sync",
        "fsync regular file", false, true},
    MetadataCompactionReopenFailureCase{
        test::MetadataCompactionApplicationFault::kPriorTemporaryUnlink,
        test::DurableIoFault::kDirectorySync, 0U, "cleanup_unlink_then_repair_directory_sync",
        "fsync directory", false, true},
    MetadataCompactionReopenFailureCase{
        test::MetadataCompactionApplicationFault::kFinalDirectorySync, test::DurableIoFault::kStat,
        4U, "cleanup_sync_then_repair_size_stat", "fstat file size", true, false},
    MetadataCompactionReopenFailureCase{
        test::MetadataCompactionApplicationFault::kFinalDirectorySync,
        test::DurableIoFault::kTruncate, 0U, "cleanup_sync_then_repair_truncate", "ftruncate", true,
        true},
    MetadataCompactionReopenFailureCase{
        test::MetadataCompactionApplicationFault::kFinalDirectorySync,
        test::DurableIoFault::kFullSync, 0U, "cleanup_sync_then_repair_file_sync",
        "fsync regular file", true, true},
    MetadataCompactionReopenFailureCase{
        test::MetadataCompactionApplicationFault::kFinalDirectorySync,
        test::DurableIoFault::kDirectorySync, 0U, "cleanup_sync_then_repair_directory_sync",
        "fsync directory", true, true},
};

void expect_catalog(const DurableMetadataStateMachine& metadata) {
  EXPECT_EQ(metadata.state().applied_index(), 1U);
  const ClusterNodeMetadata* const node = metadata.state().find_node(1U);
  ASSERT_NE(node, nullptr);
  EXPECT_EQ(node->endpoint, "node-1");
}

void expect_application_snapshot(const MetadataApplicationSnapshot& snapshot) {
  EXPECT_EQ(snapshot.group_id, test::metadata_crash_group_id());
  EXPECT_EQ(snapshot.raft_snapshot.last_included_index, 1U);
  EXPECT_EQ(snapshot.raft_snapshot.last_included_term, 1U);
  EXPECT_EQ(snapshot.raft_snapshot.manifest_generation, 1U);
  EXPECT_EQ(snapshot.raft_snapshot.configuration_index, 0U);
  EXPECT_EQ(snapshot.raft_snapshot.voters, std::vector<NodeId>{1U});
  ASSERT_EQ(snapshot.entries.size(), 1U);
  EXPECT_EQ(snapshot.entries.front().index, 1U);
  EXPECT_EQ(snapshot.entries.front().term, 1U);
  EXPECT_EQ(snapshot.entries.front().type, kRaftMetadataCommandEntryType);
  EXPECT_EQ(snapshot.entries.front().payload, test::metadata_compaction_proposal().payload);
}

class MetadataSnapshotCompactionMixedFailureTest
    : public ::testing::TestWithParam<MetadataCompactionMixedFailureCase> {};

class MetadataSnapshotCompactionCleanupPersistenceFailureTest
    : public ::testing::TestWithParam<MetadataCompactionCleanupPersistenceFailureCase> {};

class MetadataSnapshotCompactionReopenFailureTest
    : public ::testing::TestWithParam<MetadataCompactionReopenFailureCase> {};

TEST_P(MetadataSnapshotCompactionMixedFailureTest,
       WithholdsBothAttemptsAndConvergesFromRecoveredOwners) {
  CompactionFailureDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const auto& [application_failure, raft_failure] = GetParam();
  const RaftPersistentLogConfig log_config = test::metadata_compaction_log_config(directory.path());
  const std::vector<RaftGroupConfiguration> groups = test::metadata_crash_groups();
  test::MetadataCompactionApplicationFaultSyscalls application_syscalls{
      application_failure.application_fault};
  test::MetadataCompactionRaftFaultSyscalls raft_syscalls{raft_failure.raft_fault};

  {
    auto runtime = detail::DurableMultiRaftRuntimeTestAccess::create_new(1U, log_config, groups, {},
                                                                         raft_syscalls);
    ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
    auto election =
        runtime->execute_batch({{test::metadata_crash_group_id(), StartElectionOperation{}}});
    ASSERT_TRUE(election.has_value()) << election.error().to_string();
    ASSERT_EQ(election->size(), 1U);
    ASSERT_TRUE(election->front().status.is_ok()) << election->front().status.to_string();
    auto storage = detail::MetadataSnapshotStorageTestAccess::create(
        test::metadata_compaction_storage_config(directory.path()), application_syscalls);
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    auto recovered = DurableMetadataStateMachine::recover(test::metadata_crash_group_id(), *runtime,
                                                          std::move(*storage));
    ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
    std::optional<DurableMetadataStateMachine> metadata{std::move(*recovered)};
    auto proposed = runtime->execute_batch(
        {{test::metadata_crash_group_id(), test::metadata_compaction_proposal()}});
    ASSERT_TRUE(proposed.has_value()) << proposed.error().to_string();
    ASSERT_EQ(proposed->size(), 1U);
    ASSERT_TRUE(proposed->front().status.is_ok()) << proposed->front().status.to_string();
    auto applied = metadata->apply_committed();
    ASSERT_TRUE(applied.has_value()) << applied.error().to_string();
    ASSERT_EQ(applied->last_applied_index, 1U);
    application_syscalls.arm();
    raft_syscalls.arm();

    auto compacted = metadata->compact_applied_prefix(1U);

    ASSERT_FALSE(compacted.has_value());
    EXPECT_EQ(compacted.error().code(), common::StatusCode::kIoError);
    EXPECT_TRUE(application_syscalls.fired());
    EXPECT_FALSE(raft_syscalls.fired());
    EXPECT_FALSE(runtime->failed());
    const RaftNode* node = runtime->find_group(test::metadata_crash_group_id());
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->persistent_state().snapshot.last_included_index, 0U);
    ASSERT_EQ(node->persistent_state().log.size(), 1U);
    expect_catalog(*metadata);
    metadata.reset();
    ASSERT_TRUE(runtime->close().is_ok());
  }

  {
    auto storage = MetadataSnapshotStorage::open_existing(
        test::metadata_compaction_storage_config(directory.path()));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    auto after_first_failure = storage->load(1U);
    if (application_failure.application_snapshot_visible_after_first_failure) {
      ASSERT_TRUE(after_first_failure.has_value()) << after_first_failure.error().to_string();
      expect_application_snapshot(after_first_failure->snapshot);
    } else {
      ASSERT_FALSE(after_first_failure.has_value());
      EXPECT_EQ(after_first_failure.error().code(), common::StatusCode::kNotFound);
    }
  }

  {
    auto runtime = detail::DurableMultiRaftRuntimeTestAccess::open_existing(
        1U, log_config, {}, groups, {}, raft_syscalls);
    ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
    auto storage = MetadataSnapshotStorage::open_existing(
        test::metadata_compaction_storage_config(directory.path()));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    auto recovered = DurableMetadataStateMachine::recover(test::metadata_crash_group_id(), *runtime,
                                                          std::move(*storage));
    ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
    std::optional<DurableMetadataStateMachine> metadata{std::move(*recovered)};
    expect_catalog(*metadata);
    EXPECT_FALSE(raft_syscalls.fired());

    auto compacted = metadata->compact_applied_prefix(1U);

    ASSERT_FALSE(compacted.has_value());
    EXPECT_EQ(compacted.error().code(), common::StatusCode::kIoError);
    EXPECT_TRUE(raft_syscalls.fired());
    EXPECT_TRUE(runtime->failed());
    metadata.reset();
    ASSERT_TRUE(runtime->close().is_ok());
  }

  if (raft_failure.raft_tail_repair_required) {
    auto strict = DurableMultiRaftRuntime::open_existing(1U, log_config, {}, groups);
    ASSERT_FALSE(strict.has_value());
    EXPECT_EQ(strict.error().code(), common::StatusCode::kCorruption);
  }
  const RaftPersistentLogOpenOptions open_options{.repair_incomplete_final_tail =
                                                      raft_failure.raft_tail_repair_required};
  auto runtime = DurableMultiRaftRuntime::open_existing(1U, log_config, open_options, groups);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  const RaftNode* recovered_node = runtime->find_group(test::metadata_crash_group_id());
  ASSERT_NE(recovered_node, nullptr);
  EXPECT_EQ(recovered_node->persistent_state().snapshot.last_included_index,
            raft_failure.raft_authority_recovered ? 1U : 0U);
  EXPECT_EQ(recovered_node->persistent_state().log.size(),
            raft_failure.raft_authority_recovered ? 0U : 1U);
  auto storage = MetadataSnapshotStorage::open_existing(
      test::metadata_compaction_storage_config(directory.path()));
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  auto orphan = storage->load(1U);
  ASSERT_TRUE(orphan.has_value()) << orphan.error().to_string();
  expect_application_snapshot(orphan->snapshot);
  auto recovered = DurableMetadataStateMachine::recover(test::metadata_crash_group_id(), *runtime,
                                                        std::move(*storage));
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  std::optional<DurableMetadataStateMachine> metadata{std::move(*recovered)};
  expect_catalog(*metadata);

  if (!raft_failure.raft_authority_recovered) {
    auto compacted = metadata->compact_applied_prefix(1U);

    ASSERT_TRUE(compacted.has_value()) << compacted.error().to_string();
    EXPECT_TRUE(compacted->application_snapshot_already_present);
    EXPECT_EQ(compacted->application_entries, 1U);
    recovered_node = runtime->find_group(test::metadata_crash_group_id());
    ASSERT_NE(recovered_node, nullptr);
  }
  EXPECT_EQ(recovered_node->persistent_state().snapshot, orphan->snapshot.raft_snapshot);
  auto reclaimed = metadata->reclaim_obsolete_snapshots();
  ASSERT_TRUE(reclaimed.has_value()) << reclaimed.error().to_string();
  EXPECT_EQ(reclaimed->authoritative_index, 1U);
  EXPECT_EQ(reclaimed->reclaimed_files, 0U);
  metadata.reset();
  ASSERT_TRUE(runtime->close().is_ok());

  auto repeated_runtime = DurableMultiRaftRuntime::open_existing(1U, log_config, {}, groups);
  ASSERT_TRUE(repeated_runtime.has_value()) << repeated_runtime.error().to_string();
  auto repeated_storage = MetadataSnapshotStorage::open_existing(
      test::metadata_compaction_storage_config(directory.path()));
  ASSERT_TRUE(repeated_storage.has_value()) << repeated_storage.error().to_string();
  auto final_snapshot = repeated_storage->load(1U);
  ASSERT_TRUE(final_snapshot.has_value()) << final_snapshot.error().to_string();
  expect_application_snapshot(final_snapshot->snapshot);
  auto repeated = DurableMetadataStateMachine::recover(
      test::metadata_crash_group_id(), *repeated_runtime, std::move(*repeated_storage));
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  expect_catalog(*repeated);
  const RaftNode* repeated_node = repeated_runtime->find_group(test::metadata_crash_group_id());
  ASSERT_NE(repeated_node, nullptr);
  EXPECT_EQ(repeated_node->persistent_state().snapshot, final_snapshot->snapshot.raft_snapshot);
  ASSERT_TRUE(repeated_runtime->close().is_ok());
}

INSTANTIATE_TEST_SUITE_P(
    EveryCrossOwnerFailure, MetadataSnapshotCompactionMixedFailureTest,
    ::testing::Combine(::testing::ValuesIn(kApplicationFailures),
                       ::testing::ValuesIn(kRaftFailures)),
    [](const ::testing::TestParamInfo<MetadataCompactionMixedFailureCase>& parameter) {
      std::string name{std::get<0>(parameter.param).name};
      name += "_then_";
      name += std::get<1>(parameter.param).name;
      return name;
    });

TEST_P(MetadataSnapshotCompactionCleanupPersistenceFailureTest,
       SurvivesCleanupFailureBeforeEveryRaftPersistenceOutcome) {
  CompactionFailureDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const auto& [cleanup_failure, raft_failure] = GetParam();
  const RaftPersistentLogConfig log_config = test::metadata_compaction_log_config(directory.path());
  const std::vector<RaftGroupConfiguration> groups = test::metadata_crash_groups();
  const std::filesystem::path snapshot_temporary =
      directory.path() / "metadata-snapshots" / "metadata-snapshot-00000000000000000001.rmas.tmp";

  {
    test::MetadataCompactionApplicationFaultSyscalls application_syscalls{
        test::MetadataCompactionApplicationFault::kTemporaryPartialWrite};
    auto runtime = DurableMultiRaftRuntime::create_new(1U, log_config, groups);
    ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
    auto election =
        runtime->execute_batch({{test::metadata_crash_group_id(), StartElectionOperation{}}});
    ASSERT_TRUE(election.has_value()) << election.error().to_string();
    ASSERT_EQ(election->size(), 1U);
    ASSERT_TRUE(election->front().status.is_ok()) << election->front().status.to_string();
    auto storage = detail::MetadataSnapshotStorageTestAccess::create(
        test::metadata_compaction_storage_config(directory.path()), application_syscalls);
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    auto recovered = DurableMetadataStateMachine::recover(test::metadata_crash_group_id(), *runtime,
                                                          std::move(*storage));
    ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
    std::optional<DurableMetadataStateMachine> metadata{std::move(*recovered)};
    auto proposed = runtime->execute_batch(
        {{test::metadata_crash_group_id(), test::metadata_compaction_proposal()}});
    ASSERT_TRUE(proposed.has_value()) << proposed.error().to_string();
    ASSERT_EQ(proposed->size(), 1U);
    ASSERT_TRUE(proposed->front().status.is_ok()) << proposed->front().status.to_string();
    auto applied = metadata->apply_committed();
    ASSERT_TRUE(applied.has_value()) << applied.error().to_string();
    ASSERT_EQ(applied->last_applied_index, 1U);
    expect_catalog(*metadata);
    application_syscalls.arm();

    auto compacted = metadata->compact_applied_prefix(1U);

    ASSERT_FALSE(compacted.has_value());
    EXPECT_EQ(compacted.error().code(), common::StatusCode::kIoError);
    EXPECT_TRUE(application_syscalls.fired());
    EXPECT_TRUE(std::filesystem::exists(snapshot_temporary));
    EXPECT_FALSE(runtime->failed());
    metadata.reset();
    ASSERT_TRUE(runtime->close().is_ok());
  }

  test::MetadataCompactionApplicationFaultSyscalls cleanup_syscalls{
      cleanup_failure.application_reopen_fault};
  cleanup_syscalls.arm();
  auto failed_storage = detail::MetadataSnapshotStorageTestAccess::open_existing(
      test::metadata_compaction_storage_config(directory.path()), cleanup_syscalls);
  ASSERT_FALSE(failed_storage.has_value());
  EXPECT_EQ(failed_storage.error().code(), common::StatusCode::kIoError);
  EXPECT_TRUE(cleanup_syscalls.fired());
  EXPECT_EQ(std::filesystem::exists(snapshot_temporary),
            !cleanup_failure.application_temporary_removed);

  auto storage = MetadataSnapshotStorage::open_existing(
      test::metadata_compaction_storage_config(directory.path()));
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  EXPECT_FALSE(std::filesystem::exists(snapshot_temporary));
  auto absent = storage->load(1U);
  ASSERT_FALSE(absent.has_value());
  EXPECT_EQ(absent.error().code(), common::StatusCode::kNotFound);

  test::MetadataCompactionRaftFaultSyscalls raft_syscalls{raft_failure.raft_fault};
  {
    auto runtime = detail::DurableMultiRaftRuntimeTestAccess::open_existing(
        1U, log_config, {}, groups, {}, raft_syscalls);
    ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
    auto recovered = DurableMetadataStateMachine::recover(test::metadata_crash_group_id(), *runtime,
                                                          std::move(*storage));
    ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
    std::optional<DurableMetadataStateMachine> metadata{std::move(*recovered)};
    expect_catalog(*metadata);
    raft_syscalls.arm();

    auto compacted = metadata->compact_applied_prefix(1U);

    ASSERT_FALSE(compacted.has_value());
    EXPECT_EQ(compacted.error().code(), common::StatusCode::kIoError);
    EXPECT_TRUE(raft_syscalls.fired());
    EXPECT_TRUE(runtime->failed());
    metadata.reset();
    ASSERT_TRUE(runtime->close().is_ok());
  }

  auto installed_storage = MetadataSnapshotStorage::open_existing(
      test::metadata_compaction_storage_config(directory.path()));
  ASSERT_TRUE(installed_storage.has_value()) << installed_storage.error().to_string();
  auto installed = installed_storage->load(1U);
  ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
  expect_application_snapshot(installed->snapshot);

  if (raft_failure.raft_tail_repair_required) {
    auto strict = DurableMultiRaftRuntime::open_existing(1U, log_config, {}, groups);
    ASSERT_FALSE(strict.has_value());
    EXPECT_EQ(strict.error().code(), common::StatusCode::kCorruption);
  }
  const RaftPersistentLogOpenOptions open_options{.repair_incomplete_final_tail =
                                                      raft_failure.raft_tail_repair_required};
  auto runtime = DurableMultiRaftRuntime::open_existing(1U, log_config, open_options, groups);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  const RaftNode* recovered_node = runtime->find_group(test::metadata_crash_group_id());
  ASSERT_NE(recovered_node, nullptr);
  EXPECT_EQ(recovered_node->persistent_state().snapshot.last_included_index,
            raft_failure.raft_authority_recovered ? 1U : 0U);
  EXPECT_EQ(recovered_node->persistent_state().log.size(),
            raft_failure.raft_authority_recovered ? 0U : 1U);
  auto recovered = DurableMetadataStateMachine::recover(test::metadata_crash_group_id(), *runtime,
                                                        std::move(*installed_storage));
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  std::optional<DurableMetadataStateMachine> metadata{std::move(*recovered)};
  expect_catalog(*metadata);

  if (!raft_failure.raft_authority_recovered) {
    auto compacted = metadata->compact_applied_prefix(1U);

    ASSERT_TRUE(compacted.has_value()) << compacted.error().to_string();
    EXPECT_TRUE(compacted->application_snapshot_already_present);
    EXPECT_EQ(compacted->application_entries, 1U);
    recovered_node = runtime->find_group(test::metadata_crash_group_id());
    ASSERT_NE(recovered_node, nullptr);
  }
  EXPECT_EQ(recovered_node->persistent_state().snapshot, installed->snapshot.raft_snapshot);
  metadata.reset();
  ASSERT_TRUE(runtime->close().is_ok());

  auto repeated_runtime = DurableMultiRaftRuntime::open_existing(1U, log_config, {}, groups);
  ASSERT_TRUE(repeated_runtime.has_value()) << repeated_runtime.error().to_string();
  auto repeated_storage = MetadataSnapshotStorage::open_existing(
      test::metadata_compaction_storage_config(directory.path()));
  ASSERT_TRUE(repeated_storage.has_value()) << repeated_storage.error().to_string();
  auto final_snapshot = repeated_storage->load(1U);
  ASSERT_TRUE(final_snapshot.has_value()) << final_snapshot.error().to_string();
  expect_application_snapshot(final_snapshot->snapshot);
  auto repeated = DurableMetadataStateMachine::recover(
      test::metadata_crash_group_id(), *repeated_runtime, std::move(*repeated_storage));
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  expect_catalog(*repeated);
  const RaftNode* repeated_node = repeated_runtime->find_group(test::metadata_crash_group_id());
  ASSERT_NE(repeated_node, nullptr);
  EXPECT_EQ(repeated_node->persistent_state().snapshot, final_snapshot->snapshot.raft_snapshot);
  ASSERT_TRUE(repeated_runtime->close().is_ok());
}

INSTANTIATE_TEST_SUITE_P(
    EveryCleanupToPersistenceFailure, MetadataSnapshotCompactionCleanupPersistenceFailureTest,
    ::testing::Combine(::testing::ValuesIn(kCleanupFailures), ::testing::ValuesIn(kRaftFailures)),
    [](const ::testing::TestParamInfo<MetadataCompactionCleanupPersistenceFailureCase>& parameter) {
      std::string name{std::get<0>(parameter.param).name};
      name += "_then_";
      name += std::get<1>(parameter.param).name;
      return name;
    });

TEST(MetadataSnapshotCompactionRepeatedFailureTest,
     SurvivesTwoPartialWritesInEachOwnerBeforeSuccess) {
  CompactionFailureDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const RaftPersistentLogConfig log_config = test::metadata_compaction_log_config(directory.path());
  const std::vector<RaftGroupConfiguration> groups = test::metadata_crash_groups();
  const std::filesystem::path snapshot_temporary =
      directory.path() / "metadata-snapshots" / "metadata-snapshot-00000000000000000001.rmas.tmp";
  const std::filesystem::path snapshot_final =
      directory.path() / "metadata-snapshots" / "metadata-snapshot-00000000000000000001.rmas";

  {
    auto runtime = DurableMultiRaftRuntime::create_new(1U, log_config, groups);
    ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
    auto election =
        runtime->execute_batch({{test::metadata_crash_group_id(), StartElectionOperation{}}});
    ASSERT_TRUE(election.has_value()) << election.error().to_string();
    ASSERT_EQ(election->size(), 1U);
    ASSERT_TRUE(election->front().status.is_ok()) << election->front().status.to_string();
    auto storage =
        MetadataSnapshotStorage::create(test::metadata_compaction_storage_config(directory.path()));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    auto recovered = DurableMetadataStateMachine::recover(test::metadata_crash_group_id(), *runtime,
                                                          std::move(*storage));
    ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
    std::optional<DurableMetadataStateMachine> metadata{std::move(*recovered)};
    auto proposed = runtime->execute_batch(
        {{test::metadata_crash_group_id(), test::metadata_compaction_proposal()}});
    ASSERT_TRUE(proposed.has_value()) << proposed.error().to_string();
    ASSERT_EQ(proposed->size(), 1U);
    ASSERT_TRUE(proposed->front().status.is_ok()) << proposed->front().status.to_string();
    auto applied = metadata->apply_committed();
    ASSERT_TRUE(applied.has_value()) << applied.error().to_string();
    ASSERT_EQ(applied->last_applied_index, 1U);
    expect_catalog(*metadata);
    metadata.reset();
    ASSERT_TRUE(runtime->close().is_ok());
  }

  for (std::size_t attempt = 0U; attempt < 2U; ++attempt) {
    SCOPED_TRACE(attempt);
    test::MetadataCompactionApplicationFaultSyscalls application_syscalls{
        test::MetadataCompactionApplicationFault::kTemporaryPartialWrite};
    auto storage = detail::MetadataSnapshotStorageTestAccess::open_existing(
        test::metadata_compaction_storage_config(directory.path()), application_syscalls);
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    EXPECT_FALSE(std::filesystem::exists(snapshot_temporary));
    auto runtime = DurableMultiRaftRuntime::open_existing(1U, log_config, {}, groups);
    ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
    auto recovered = DurableMetadataStateMachine::recover(test::metadata_crash_group_id(), *runtime,
                                                          std::move(*storage));
    ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
    std::optional<DurableMetadataStateMachine> metadata{std::move(*recovered)};
    expect_catalog(*metadata);
    application_syscalls.arm();

    auto compacted = metadata->compact_applied_prefix(1U);

    ASSERT_FALSE(compacted.has_value());
    EXPECT_EQ(compacted.error().code(), common::StatusCode::kIoError);
    EXPECT_TRUE(application_syscalls.fired());
    EXPECT_TRUE(std::filesystem::exists(snapshot_temporary));
    EXPECT_FALSE(std::filesystem::exists(snapshot_final));
    EXPECT_FALSE(runtime->failed());
    const RaftNode* node = runtime->find_group(test::metadata_crash_group_id());
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->persistent_state().snapshot.last_included_index, 0U);
    ASSERT_EQ(node->persistent_state().log.size(), 1U);
    metadata.reset();
    ASSERT_TRUE(runtime->close().is_ok());
  }

  {
    auto storage = MetadataSnapshotStorage::open_existing(
        test::metadata_compaction_storage_config(directory.path()));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    EXPECT_FALSE(std::filesystem::exists(snapshot_temporary));
    auto before_raft_failures = storage->load(1U);
    ASSERT_FALSE(before_raft_failures.has_value());
    EXPECT_EQ(before_raft_failures.error().code(), common::StatusCode::kNotFound);
  }

  for (std::size_t attempt = 0U; attempt < 2U; ++attempt) {
    SCOPED_TRACE(attempt);
    test::MetadataCompactionRaftFaultSyscalls raft_syscalls{
        test::MetadataCompactionRaftFault::kWritePrefixThenError};
    auto runtime = detail::DurableMultiRaftRuntimeTestAccess::open_existing(
        1U, log_config, RaftPersistentLogOpenOptions{.repair_incomplete_final_tail = true}, groups,
        {}, raft_syscalls);
    ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
    auto storage = MetadataSnapshotStorage::open_existing(
        test::metadata_compaction_storage_config(directory.path()));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    auto recovered = DurableMetadataStateMachine::recover(test::metadata_crash_group_id(), *runtime,
                                                          std::move(*storage));
    ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
    std::optional<DurableMetadataStateMachine> metadata{std::move(*recovered)};
    expect_catalog(*metadata);
    raft_syscalls.arm();

    auto compacted = metadata->compact_applied_prefix(1U);

    ASSERT_FALSE(compacted.has_value());
    EXPECT_EQ(compacted.error().code(), common::StatusCode::kIoError);
    EXPECT_TRUE(raft_syscalls.fired());
    EXPECT_TRUE(runtime->failed());
    EXPECT_TRUE(std::filesystem::exists(snapshot_final));
    metadata.reset();
    ASSERT_TRUE(runtime->close().is_ok());

    {
      auto installed_storage = MetadataSnapshotStorage::open_existing(
          test::metadata_compaction_storage_config(directory.path()));
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
    auto strict = DurableMultiRaftRuntime::open_existing(1U, log_config, {}, groups);
    ASSERT_FALSE(strict.has_value());
    EXPECT_EQ(strict.error().code(), common::StatusCode::kCorruption);
    EXPECT_EQ(std::filesystem::file_size(active_segment), incomplete_size);
    auto repaired = DurableMultiRaftRuntime::open_existing(
        1U, log_config, RaftPersistentLogOpenOptions{.repair_incomplete_final_tail = true}, groups);
    ASSERT_TRUE(repaired.has_value()) << repaired.error().to_string();
    EXPECT_EQ(std::filesystem::file_size(active_segment) +
                  test::kMetadataCompactionPartialRecordBytes,
              incomplete_size);
    const RaftNode* repaired_node = repaired->find_group(test::metadata_crash_group_id());
    ASSERT_NE(repaired_node, nullptr);
    EXPECT_EQ(repaired_node->persistent_state().snapshot.last_included_index, 0U);
    ASSERT_EQ(repaired_node->persistent_state().log.size(), 1U);
    ASSERT_TRUE(repaired->close().is_ok());
  }

  auto runtime = DurableMultiRaftRuntime::open_existing(1U, log_config, {}, groups);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  auto storage = MetadataSnapshotStorage::open_existing(
      test::metadata_compaction_storage_config(directory.path()));
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  auto orphan = storage->load(1U);
  ASSERT_TRUE(orphan.has_value()) << orphan.error().to_string();
  expect_application_snapshot(orphan->snapshot);
  auto recovered = DurableMetadataStateMachine::recover(test::metadata_crash_group_id(), *runtime,
                                                        std::move(*storage));
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  std::optional<DurableMetadataStateMachine> metadata{std::move(*recovered)};
  expect_catalog(*metadata);

  auto compacted = metadata->compact_applied_prefix(1U);

  ASSERT_TRUE(compacted.has_value()) << compacted.error().to_string();
  EXPECT_TRUE(compacted->application_snapshot_already_present);
  EXPECT_EQ(compacted->application_entries, 1U);
  const RaftNode* node = runtime->find_group(test::metadata_crash_group_id());
  ASSERT_NE(node, nullptr);
  EXPECT_EQ(node->persistent_state().snapshot, orphan->snapshot.raft_snapshot);
  metadata.reset();
  ASSERT_TRUE(runtime->close().is_ok());

  auto repeated_runtime = DurableMultiRaftRuntime::open_existing(1U, log_config, {}, groups);
  ASSERT_TRUE(repeated_runtime.has_value()) << repeated_runtime.error().to_string();
  auto repeated_storage = MetadataSnapshotStorage::open_existing(
      test::metadata_compaction_storage_config(directory.path()));
  ASSERT_TRUE(repeated_storage.has_value()) << repeated_storage.error().to_string();
  auto final_snapshot = repeated_storage->load(1U);
  ASSERT_TRUE(final_snapshot.has_value()) << final_snapshot.error().to_string();
  expect_application_snapshot(final_snapshot->snapshot);
  auto repeated = DurableMetadataStateMachine::recover(
      test::metadata_crash_group_id(), *repeated_runtime, std::move(*repeated_storage));
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  expect_catalog(*repeated);
  const RaftNode* repeated_node = repeated_runtime->find_group(test::metadata_crash_group_id());
  ASSERT_NE(repeated_node, nullptr);
  EXPECT_EQ(repeated_node->persistent_state().snapshot, final_snapshot->snapshot.raft_snapshot);
  ASSERT_TRUE(repeated_runtime->close().is_ok());
}

TEST_P(MetadataSnapshotCompactionReopenFailureTest,
       ReleasesBothFailedReopensAndConvergesFromObservedBytes) {
  CompactionFailureDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const MetadataCompactionReopenFailureCase failure = GetParam();
  const RaftPersistentLogConfig log_config = test::metadata_compaction_log_config(directory.path());
  const std::vector<RaftGroupConfiguration> groups = test::metadata_crash_groups();
  const std::filesystem::path snapshot_temporary =
      directory.path() / "metadata-snapshots" / "metadata-snapshot-00000000000000000001.rmas.tmp";

  {
    test::MetadataCompactionApplicationFaultSyscalls application_syscalls{
        test::MetadataCompactionApplicationFault::kTemporaryPartialWrite};
    auto runtime = DurableMultiRaftRuntime::create_new(1U, log_config, groups);
    ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
    auto election =
        runtime->execute_batch({{test::metadata_crash_group_id(), StartElectionOperation{}}});
    ASSERT_TRUE(election.has_value()) << election.error().to_string();
    ASSERT_EQ(election->size(), 1U);
    ASSERT_TRUE(election->front().status.is_ok()) << election->front().status.to_string();
    auto storage = detail::MetadataSnapshotStorageTestAccess::create(
        test::metadata_compaction_storage_config(directory.path()), application_syscalls);
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    auto recovered = DurableMetadataStateMachine::recover(test::metadata_crash_group_id(), *runtime,
                                                          std::move(*storage));
    ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
    std::optional<DurableMetadataStateMachine> metadata{std::move(*recovered)};
    auto proposed = runtime->execute_batch(
        {{test::metadata_crash_group_id(), test::metadata_compaction_proposal()}});
    ASSERT_TRUE(proposed.has_value()) << proposed.error().to_string();
    ASSERT_EQ(proposed->size(), 1U);
    ASSERT_TRUE(proposed->front().status.is_ok()) << proposed->front().status.to_string();
    auto applied = metadata->apply_committed();
    ASSERT_TRUE(applied.has_value()) << applied.error().to_string();
    ASSERT_EQ(applied->last_applied_index, 1U);
    expect_catalog(*metadata);
    application_syscalls.arm();

    auto compacted = metadata->compact_applied_prefix(1U);

    ASSERT_FALSE(compacted.has_value());
    EXPECT_EQ(compacted.error().code(), common::StatusCode::kIoError);
    EXPECT_TRUE(application_syscalls.fired());
    EXPECT_TRUE(std::filesystem::exists(snapshot_temporary));
    EXPECT_FALSE(runtime->failed());
    metadata.reset();
    ASSERT_TRUE(runtime->close().is_ok());
  }

  test::MetadataCompactionApplicationFaultSyscalls application_reopen_syscalls{
      failure.application_reopen_fault};
  application_reopen_syscalls.arm();
  auto failed_storage = detail::MetadataSnapshotStorageTestAccess::open_existing(
      test::metadata_compaction_storage_config(directory.path()), application_reopen_syscalls);
  ASSERT_FALSE(failed_storage.has_value());
  EXPECT_EQ(failed_storage.error().code(), common::StatusCode::kIoError);
  EXPECT_TRUE(application_reopen_syscalls.fired());
  EXPECT_EQ(std::filesystem::exists(snapshot_temporary), !failure.application_temporary_removed);

  auto storage = MetadataSnapshotStorage::open_existing(
      test::metadata_compaction_storage_config(directory.path()));
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  EXPECT_FALSE(std::filesystem::exists(snapshot_temporary));
  auto before_raft_failure = storage->load(1U);
  ASSERT_FALSE(before_raft_failure.has_value());
  EXPECT_EQ(before_raft_failure.error().code(), common::StatusCode::kNotFound);

  test::MetadataCompactionRaftFaultSyscalls raft_compaction_syscalls{
      test::MetadataCompactionRaftFault::kWritePrefixThenError};
  {
    auto runtime = detail::DurableMultiRaftRuntimeTestAccess::open_existing(
        1U, log_config, {}, groups, {}, raft_compaction_syscalls);
    ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
    auto recovered = DurableMetadataStateMachine::recover(test::metadata_crash_group_id(), *runtime,
                                                          std::move(*storage));
    ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
    std::optional<DurableMetadataStateMachine> metadata{std::move(*recovered)};
    expect_catalog(*metadata);
    raft_compaction_syscalls.arm();

    auto compacted = metadata->compact_applied_prefix(1U);

    ASSERT_FALSE(compacted.has_value());
    EXPECT_EQ(compacted.error().code(), common::StatusCode::kIoError);
    EXPECT_TRUE(raft_compaction_syscalls.fired());
    EXPECT_TRUE(runtime->failed());
    metadata.reset();
    ASSERT_TRUE(runtime->close().is_ok());
  }

  auto installed_storage = MetadataSnapshotStorage::open_existing(
      test::metadata_compaction_storage_config(directory.path()));
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
  auto strict = DurableMultiRaftRuntime::open_existing(1U, log_config, {}, groups);
  ASSERT_FALSE(strict.has_value());
  EXPECT_EQ(strict.error().code(), common::StatusCode::kCorruption);
  EXPECT_EQ(std::filesystem::file_size(active_segment), incomplete_size);

  test::DurableIoFaultPosixSyscalls raft_reopen_syscalls;
  raft_reopen_syscalls.arm(failure.raft_reopen_fault, failure.raft_matching_calls_to_skip);
  auto failed_runtime = detail::DurableMultiRaftRuntimeTestAccess::open_existing(
      1U, log_config, RaftPersistentLogOpenOptions{.repair_incomplete_final_tail = true}, groups,
      {}, raft_reopen_syscalls);
  ASSERT_FALSE(failed_runtime.has_value());
  EXPECT_EQ(failed_runtime.error().code(), common::StatusCode::kIoError);
  EXPECT_NE(failed_runtime.error().to_string().find(failure.expected_raft_operation),
            std::string::npos);
  EXPECT_EQ(raft_reopen_syscalls.injected_faults(), 1U);
  EXPECT_EQ(std::filesystem::file_size(active_segment),
            failure.raft_tail_removed
                ? incomplete_size - test::kMetadataCompactionPartialRecordBytes
                : incomplete_size);

  auto runtime = DurableMultiRaftRuntime::open_existing(
      1U, log_config, RaftPersistentLogOpenOptions{.repair_incomplete_final_tail = true}, groups);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  EXPECT_EQ(std::filesystem::file_size(active_segment) +
                test::kMetadataCompactionPartialRecordBytes,
            incomplete_size);
  const RaftNode* recovered_node = runtime->find_group(test::metadata_crash_group_id());
  ASSERT_NE(recovered_node, nullptr);
  EXPECT_EQ(recovered_node->persistent_state().snapshot.last_included_index, 0U);
  ASSERT_EQ(recovered_node->persistent_state().log.size(), 1U);
  auto orphan = installed_storage->load(1U);
  ASSERT_TRUE(orphan.has_value()) << orphan.error().to_string();
  expect_application_snapshot(orphan->snapshot);
  auto recovered = DurableMetadataStateMachine::recover(test::metadata_crash_group_id(), *runtime,
                                                        std::move(*installed_storage));
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  std::optional<DurableMetadataStateMachine> metadata{std::move(*recovered)};
  expect_catalog(*metadata);

  auto compacted = metadata->compact_applied_prefix(1U);

  ASSERT_TRUE(compacted.has_value()) << compacted.error().to_string();
  EXPECT_TRUE(compacted->application_snapshot_already_present);
  EXPECT_EQ(compacted->application_entries, 1U);
  recovered_node = runtime->find_group(test::metadata_crash_group_id());
  ASSERT_NE(recovered_node, nullptr);
  EXPECT_EQ(recovered_node->persistent_state().snapshot, orphan->snapshot.raft_snapshot);
  metadata.reset();
  ASSERT_TRUE(runtime->close().is_ok());

  auto repeated_runtime = DurableMultiRaftRuntime::open_existing(1U, log_config, {}, groups);
  ASSERT_TRUE(repeated_runtime.has_value()) << repeated_runtime.error().to_string();
  auto repeated_storage = MetadataSnapshotStorage::open_existing(
      test::metadata_compaction_storage_config(directory.path()));
  ASSERT_TRUE(repeated_storage.has_value()) << repeated_storage.error().to_string();
  auto final_snapshot = repeated_storage->load(1U);
  ASSERT_TRUE(final_snapshot.has_value()) << final_snapshot.error().to_string();
  expect_application_snapshot(final_snapshot->snapshot);
  auto repeated = DurableMetadataStateMachine::recover(
      test::metadata_crash_group_id(), *repeated_runtime, std::move(*repeated_storage));
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  expect_catalog(*repeated);
  const RaftNode* repeated_node = repeated_runtime->find_group(test::metadata_crash_group_id());
  ASSERT_NE(repeated_node, nullptr);
  EXPECT_EQ(repeated_node->persistent_state().snapshot, final_snapshot->snapshot.raft_snapshot);
  ASSERT_TRUE(repeated_runtime->close().is_ok());
}

INSTANTIATE_TEST_SUITE_P(
    EveryOwnerReopenFailure, MetadataSnapshotCompactionReopenFailureTest,
    ::testing::ValuesIn(kReopenFailures),
    [](const ::testing::TestParamInfo<MetadataCompactionReopenFailureCase>& parameter) {
      return std::string{parameter.param.name};
    });

} // namespace
} // namespace chronos::raft
