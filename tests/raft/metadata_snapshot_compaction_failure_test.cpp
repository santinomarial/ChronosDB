#include "chronos/common/status.hpp"
#include "chronos/raft/durable_runtime.hpp"
#include "chronos/raft/metadata_runtime.hpp"
#include "chronos/raft/metadata_snapshot_storage.hpp"
#include "chronos/raft/node.hpp"
#include "raft/durable_runtime_internal.hpp"
#include "raft/metadata_snapshot_compaction_fault.hpp"
#include "raft/metadata_snapshot_install_crash_fixture.hpp"
#include "raft/metadata_snapshot_storage_internal.hpp"

#include <array>
#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
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

struct MetadataCompactionMixedFailureCase {
  test::MetadataCompactionApplicationFault application_fault;
  test::MetadataCompactionRaftFault raft_fault;
  std::string_view name;
  bool application_snapshot_visible_after_first_failure;
  bool raft_tail_repair_required;
};

constexpr std::array<MetadataCompactionMixedFailureCase, 4U> kMixedFailures{
    MetadataCompactionMixedFailureCase{
        test::MetadataCompactionApplicationFault::kTemporaryPartialWrite,
        test::MetadataCompactionRaftFault::kWriteBefore, "temporary_partial_then_raft_write_before",
        false, false},
    MetadataCompactionMixedFailureCase{
        test::MetadataCompactionApplicationFault::kTemporaryPartialWrite,
        test::MetadataCompactionRaftFault::kWritePrefixThenError,
        "temporary_partial_then_raft_partial", false, true},
    MetadataCompactionMixedFailureCase{
        test::MetadataCompactionApplicationFault::kFinalDirectorySync,
        test::MetadataCompactionRaftFault::kWriteBefore, "directory_sync_then_raft_write_before",
        true, false},
    MetadataCompactionMixedFailureCase{
        test::MetadataCompactionApplicationFault::kFinalDirectorySync,
        test::MetadataCompactionRaftFault::kWritePrefixThenError,
        "directory_sync_then_raft_partial", true, true},
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

TEST_P(MetadataSnapshotCompactionMixedFailureTest,
       WithholdsBothAttemptsAndConvergesFromRecoveredOwners) {
  CompactionFailureDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const MetadataCompactionMixedFailureCase failure = GetParam();
  const RaftPersistentLogConfig log_config = test::metadata_compaction_log_config(directory.path());
  const std::vector<RaftGroupConfiguration> groups = test::metadata_crash_groups();
  test::MetadataCompactionApplicationFaultSyscalls application_syscalls{failure.application_fault};
  test::MetadataCompactionRaftFaultSyscalls raft_syscalls{failure.raft_fault};

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
    if (failure.application_snapshot_visible_after_first_failure) {
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

  if (failure.raft_tail_repair_required) {
    auto strict = DurableMultiRaftRuntime::open_existing(1U, log_config, {}, groups);
    ASSERT_FALSE(strict.has_value());
    EXPECT_EQ(strict.error().code(), common::StatusCode::kCorruption);
  }
  const RaftPersistentLogOpenOptions open_options{.repair_incomplete_final_tail =
                                                      failure.raft_tail_repair_required};
  auto runtime = DurableMultiRaftRuntime::open_existing(1U, log_config, open_options, groups);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  const RaftNode* recovered_node = runtime->find_group(test::metadata_crash_group_id());
  ASSERT_NE(recovered_node, nullptr);
  EXPECT_EQ(recovered_node->persistent_state().snapshot.last_included_index, 0U);
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
  recovered_node = runtime->find_group(test::metadata_crash_group_id());
  ASSERT_NE(recovered_node, nullptr);
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
    ::testing::ValuesIn(kMixedFailures),
    [](const ::testing::TestParamInfo<MetadataCompactionMixedFailureCase>& parameter) {
      return std::string{parameter.param.name};
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

} // namespace
} // namespace chronos::raft
