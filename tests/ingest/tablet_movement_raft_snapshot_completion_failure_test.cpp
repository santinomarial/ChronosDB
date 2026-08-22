#include "chronos/ingest/tablet_movement_raft_snapshot_completion.hpp"
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
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::ingest {
namespace {

constexpr std::size_t kPartialRecordBytes = 16U;

class TemporaryCompletionRoot {
public:
  TemporaryCompletionRoot() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-snapshot-complete-fault-XXXXXX")
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

  ~TemporaryCompletionRoot() {
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

enum class CompletionIoFault : std::uint8_t {
  kWriteBefore,
  kWritePrefixThenError,
  kWriteAfter,
  kDataSyncBefore,
  kDataSyncAfter,
};

class CompletionFaultSyscalls final : public io::detail::PosixSyscalls {
public:
  explicit CompletionFaultSyscalls(const CompletionIoFault fault)
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
    if (armed_ && fault_ == CompletionIoFault::kWriteBefore)
      return fail_ssize();
    if (armed_ && fault_ == CompletionIoFault::kWritePrefixThenError) {
      if (!partial_write_started_) {
        partial_write_started_ = true;
        const io::detail::WriteAtRequest prefix{request.descriptor, request.source,
                                                kPartialRecordBytes, request.offset};
        return delegate_.pwrite(prefix);
      }
      return fail_ssize();
    }
    const ssize_t result = delegate_.pwrite(request);
    if (armed_ && fault_ == CompletionIoFault::kWriteAfter && result >= 0 &&
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
    if (armed_ && fault_ == CompletionIoFault::kDataSyncBefore)
      return fail();
    const int result = delegate_.fdatasync(descriptor);
    if (armed_ && fault_ == CompletionIoFault::kDataSyncAfter && result == 0)
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
  CompletionIoFault fault_;
  bool partial_write_started_{false};
  bool armed_{false};
  bool fired_{false};
};

struct CompletionFailureCase {
  CompletionIoFault fault;
  std::string_view name;
  bool raft_authority_recovered;
  bool tail_repair_required;
};

constexpr std::array<CompletionFailureCase, 5U> kCompletionFailures{
    CompletionFailureCase{CompletionIoFault::kWriteBefore, "write_before", false, false},
    CompletionFailureCase{CompletionIoFault::kWritePrefixThenError, "write_prefix_then_error",
                          false, true},
    CompletionFailureCase{CompletionIoFault::kWriteAfter, "write_after", true, false},
    CompletionFailureCase{CompletionIoFault::kDataSyncBefore, "data_sync_before", true, false},
    CompletionFailureCase{CompletionIoFault::kDataSyncAfter, "data_sync_after", true, false},
};

struct RepairFailureCase {
  raft::test::DurableIoFault fault;
  std::size_t matching_calls_to_skip;
  std::string_view name;
  std::string_view expected_operation;
  bool truncates_before_failure;
};

constexpr std::array<RepairFailureCase, 4U> kRepairFailures{
    RepairFailureCase{raft::test::DurableIoFault::kStat, 4U, "size_stat", "fstat file size", false},
    RepairFailureCase{raft::test::DurableIoFault::kTruncate, 0U, "truncate", "ftruncate", true},
    RepairFailureCase{raft::test::DurableIoFault::kFullSync, 0U, "file_sync", "fsync regular file",
                      true},
    RepairFailureCase{raft::test::DurableIoFault::kDirectorySync, 0U, "directory_sync",
                      "fsync directory", true},
};

void expect_recovered_retry_success(const raft::DurableRaftResult& result,
                                    const raft::SnapshotMetadata& expected) {
  ASSERT_TRUE(result.status.is_ok()) << result.status.to_string();
  if (!result.transition.has_value()) {
    ADD_FAILURE() << "recovered exact retry returned no transition";
    return;
  }
  const raft::MultiRaftTransition& transition = result.transition.value();
  EXPECT_FALSE(transition.snapshot_install.has_value());
  ASSERT_EQ(transition.outbound.size(), 1U);
  const auto* response =
      std::get_if<raft::InstallSnapshotResponse>(&transition.outbound.front().outbound.message);
  ASSERT_NE(response, nullptr);
  EXPECT_TRUE(response->success);
  EXPECT_EQ(response->last_included_index, expected.last_included_index);
}

class TabletMovementRaftSnapshotCompletionFailureTest
    : public ::testing::TestWithParam<CompletionFailureCase> {};

class TabletMovementRaftSnapshotRepairFailureTest
    : public ::testing::TestWithParam<RepairFailureCase> {};

TEST_P(TabletMovementRaftSnapshotCompletionFailureTest,
       ReleasesNoSuccessAndConvergesFromRecoveredAuthority) {
  TemporaryCompletionRoot root;
  ASSERT_FALSE(root.path().empty());
  const CompletionFailureCase failure = GetParam();
  const RaftTabletApplicationSnapshot expected = test::crash_application_snapshot();
  auto bytes = encode_raft_tablet_application_snapshot_v1(expected);
  ASSERT_TRUE(bytes.has_value()) << bytes.error().to_string();
  auto recovered_movement = test::crash_recovered_movement(*bytes);
  ASSERT_TRUE(recovered_movement.has_value()) << recovered_movement.error().to_string();
  auto storage = RaftTabletSnapshotStorage::create(test::crash_snapshot_config(root.path()));
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  CompletionFaultSyscalls syscalls{failure.fault};
  {
    auto runtime = raft::detail::DurableMultiRaftRuntimeTestAccess::create_new(
        4U, test::crash_log_config(root.path()), test::crash_groups(), {}, syscalls);
    ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
    auto pending = test::request_crash_snapshot(*runtime, expected.raft_snapshot);
    ASSERT_TRUE(pending.has_value()) << pending.error().to_string();
    syscalls.arm();

    auto completed = complete_recovered_tablet_movement_raft_snapshot(
        *recovered_movement, test::crash_table_id(), *storage, *pending, *runtime);

    ASSERT_FALSE(completed.has_value());
    EXPECT_EQ(completed.error().code(), common::StatusCode::kIoError);
    EXPECT_TRUE(syscalls.fired());
    EXPECT_TRUE(runtime->failed());
    EXPECT_EQ(runtime->failure_status().code(), common::StatusCode::kIoError);
    auto installed = storage->load(expected.raft_snapshot.last_included_index);
    ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
    EXPECT_EQ(installed->bytes, *bytes);
  }

  std::filesystem::path active_segment;
  for (const std::filesystem::directory_entry& entry :
       std::filesystem::directory_iterator(root.path() / "raft")) {
    if (entry.path().extension() == ".rlog")
      active_segment = entry.path();
  }
  ASSERT_FALSE(active_segment.empty());
  const std::uintmax_t failed_size = std::filesystem::file_size(active_segment);
  if (failure.tail_repair_required) {
    auto strict = raft::DurableMultiRaftRuntime::open_existing(
        4U, test::crash_log_config(root.path()), {}, test::crash_groups());
    ASSERT_FALSE(strict.has_value());
    EXPECT_EQ(strict.error().code(), common::StatusCode::kCorruption);
    EXPECT_EQ(std::filesystem::file_size(active_segment), failed_size);
  }
  const raft::RaftPersistentLogOpenOptions open_options{.repair_incomplete_final_tail =
                                                            failure.tail_repair_required};
  auto runtime = raft::DurableMultiRaftRuntime::open_existing(
      4U, test::crash_log_config(root.path()), open_options, test::crash_groups());
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  if (failure.tail_repair_required)
    EXPECT_EQ(std::filesystem::file_size(active_segment) + 16U, failed_size);
  const raft::RaftNode* group = runtime->find_group(test::crash_group_id());
  ASSERT_NE(group, nullptr);
  EXPECT_EQ(group->persistent_state().snapshot == expected.raft_snapshot,
            failure.raft_authority_recovered);
  if (failure.raft_authority_recovered) {
    auto repeated = runtime->execute_batch(
        {{test::crash_group_id(),
          raft::ReceiveOperation{1U,
                                 raft::InstallSnapshotRequest{4U, 1U, expected.raft_snapshot}}}});
    ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
    ASSERT_EQ(repeated->size(), 1U);
    expect_recovered_retry_success(repeated->front(), expected.raft_snapshot);
  } else {
    auto pending = test::request_crash_snapshot(*runtime, expected.raft_snapshot);
    ASSERT_TRUE(pending.has_value()) << pending.error().to_string();
    auto completed = complete_recovered_tablet_movement_raft_snapshot(
        *recovered_movement, test::crash_table_id(), *storage, *pending, *runtime);
    ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
    const auto* response =
        std::get_if<raft::InstallSnapshotResponse>(&completed->acknowledgement.outbound.message);
    ASSERT_NE(response, nullptr);
    EXPECT_TRUE(response->success);
  }
  EXPECT_EQ(runtime->find_group(test::crash_group_id())->persistent_state().snapshot,
            expected.raft_snapshot);
  EXPECT_TRUE(runtime->close().is_ok());

  auto repeated = raft::DurableMultiRaftRuntime::open_existing(
      4U, test::crash_log_config(root.path()), {}, test::crash_groups());
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  EXPECT_EQ(repeated->find_group(test::crash_group_id())->persistent_state().snapshot,
            expected.raft_snapshot);
  EXPECT_TRUE(repeated->close().is_ok());
}

INSTANTIATE_TEST_SUITE_P(EveryRaftPersistenceFailure,
                         TabletMovementRaftSnapshotCompletionFailureTest,
                         ::testing::ValuesIn(kCompletionFailures),
                         [](const ::testing::TestParamInfo<CompletionFailureCase>& parameter) {
                           return std::string{parameter.param.name};
                         });

TEST_P(TabletMovementRaftSnapshotRepairFailureTest,
       ReleasesRecoveryOwnershipAndConvergesAfterExactRetry) {
  TemporaryCompletionRoot root;
  ASSERT_FALSE(root.path().empty());
  const RepairFailureCase failure = GetParam();
  const RaftTabletApplicationSnapshot expected = test::crash_application_snapshot();
  auto bytes = encode_raft_tablet_application_snapshot_v1(expected);
  ASSERT_TRUE(bytes.has_value()) << bytes.error().to_string();
  auto recovered_movement = test::crash_recovered_movement(*bytes);
  ASSERT_TRUE(recovered_movement.has_value()) << recovered_movement.error().to_string();
  auto storage = RaftTabletSnapshotStorage::create(test::crash_snapshot_config(root.path()));
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  CompletionFaultSyscalls completion_syscalls{CompletionIoFault::kWritePrefixThenError};
  {
    auto runtime = raft::detail::DurableMultiRaftRuntimeTestAccess::create_new(
        4U, test::crash_log_config(root.path()), test::crash_groups(), {}, completion_syscalls);
    ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
    auto pending = test::request_crash_snapshot(*runtime, expected.raft_snapshot);
    ASSERT_TRUE(pending.has_value()) << pending.error().to_string();
    completion_syscalls.arm();

    auto completed = complete_recovered_tablet_movement_raft_snapshot(
        *recovered_movement, test::crash_table_id(), *storage, *pending, *runtime);

    ASSERT_FALSE(completed.has_value());
    EXPECT_EQ(completed.error().code(), common::StatusCode::kIoError);
    EXPECT_TRUE(completion_syscalls.fired());
    EXPECT_TRUE(runtime->failed());
  }

  std::filesystem::path active_segment;
  for (const std::filesystem::directory_entry& entry :
       std::filesystem::directory_iterator(root.path() / "raft")) {
    if (entry.path().extension() == ".rlog")
      active_segment = entry.path();
  }
  ASSERT_FALSE(active_segment.empty());
  const std::uintmax_t incomplete_size = std::filesystem::file_size(active_segment);

  raft::test::DurableIoFaultPosixSyscalls recovery_syscalls;
  recovery_syscalls.arm(failure.fault, failure.matching_calls_to_skip);
  auto failed = raft::detail::DurableMultiRaftRuntimeTestAccess::open_existing(
      4U, test::crash_log_config(root.path()),
      raft::RaftPersistentLogOpenOptions{.repair_incomplete_final_tail = true},
      test::crash_groups(), {}, recovery_syscalls);
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kIoError);
  EXPECT_NE(failed.error().to_string().find(failure.expected_operation), std::string::npos);
  EXPECT_EQ(recovery_syscalls.injected_faults(), 1U);
  EXPECT_EQ(std::filesystem::file_size(active_segment), failure.truncates_before_failure
                                                            ? incomplete_size - kPartialRecordBytes
                                                            : incomplete_size);
  auto installed = storage->load(expected.raft_snapshot.last_included_index);
  ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
  EXPECT_EQ(installed->bytes, *bytes);

  auto runtime = raft::DurableMultiRaftRuntime::open_existing(
      4U, test::crash_log_config(root.path()),
      raft::RaftPersistentLogOpenOptions{.repair_incomplete_final_tail = true},
      test::crash_groups());
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  EXPECT_EQ(std::filesystem::file_size(active_segment) + kPartialRecordBytes, incomplete_size);
  const raft::RaftNode* recovered_group = runtime->find_group(test::crash_group_id());
  ASSERT_NE(recovered_group, nullptr);
  EXPECT_NE(recovered_group->persistent_state().snapshot, expected.raft_snapshot);

  auto pending = test::request_crash_snapshot(*runtime, expected.raft_snapshot);
  ASSERT_TRUE(pending.has_value()) << pending.error().to_string();
  auto completed = complete_recovered_tablet_movement_raft_snapshot(
      *recovered_movement, test::crash_table_id(), *storage, *pending, *runtime);
  ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  const auto* response =
      std::get_if<raft::InstallSnapshotResponse>(&completed->acknowledgement.outbound.message);
  ASSERT_NE(response, nullptr);
  EXPECT_TRUE(response->success);
  EXPECT_EQ(runtime->find_group(test::crash_group_id())->persistent_state().snapshot,
            expected.raft_snapshot);
  EXPECT_TRUE(runtime->close().is_ok());

  auto repeated = raft::DurableMultiRaftRuntime::open_existing(
      4U, test::crash_log_config(root.path()), {}, test::crash_groups());
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  EXPECT_EQ(repeated->find_group(test::crash_group_id())->persistent_state().snapshot,
            expected.raft_snapshot);
  EXPECT_TRUE(repeated->close().is_ok());
}

INSTANTIATE_TEST_SUITE_P(EveryRaftRepairFailure, TabletMovementRaftSnapshotRepairFailureTest,
                         ::testing::ValuesIn(kRepairFailures),
                         [](const ::testing::TestParamInfo<RepairFailureCase>& parameter) {
                           return std::string{parameter.param.name};
                         });

} // namespace
} // namespace chronos::ingest
