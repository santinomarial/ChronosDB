#include "chronos/common/status.hpp"
#include "chronos/ingest/raft_tablet_state_machine.hpp"
#include "chronos/raft/durable_runtime.hpp"
#include "chronos/raft/node.hpp"
#include "ingest/tablet_snapshot_install_crash_fixture.hpp"
#include "io/posix_syscalls.hpp"
#include "raft/durable_runtime_internal.hpp"

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

class TabletApplicationFailureDirectory {
public:
  TabletApplicationFailureDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-tablet-apply-failure-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr) {
      path_ = created;
      std::error_code error;
      static_cast<void>(std::filesystem::create_directory(path_ / "raft", error));
      if (error) {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
        path_.clear();
      }
    }
  }

  ~TabletApplicationFailureDirectory() {
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

enum class TabletApplicationIoFault : std::uint8_t {
  kWriteBefore,
  kWritePrefixThenError,
  kWriteAfter,
  kDataSyncBefore,
  kDataSyncAfter,
};

class OneShotTabletApplicationIoFault final : public io::detail::PosixSyscalls {
public:
  explicit OneShotTabletApplicationIoFault(const TabletApplicationIoFault fault)
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
    if (armed_ && fault_ == TabletApplicationIoFault::kWriteBefore)
      return fail_ssize();
    if (armed_ && fault_ == TabletApplicationIoFault::kWritePrefixThenError) {
      if (!partial_write_started_) {
        partial_write_started_ = true;
        const io::detail::WriteAtRequest prefix{request.descriptor, request.source,
                                                kPartialRaftRecordBytes, request.offset};
        return delegate_.pwrite(prefix);
      }
      return fail_ssize();
    }
    const ssize_t result = delegate_.pwrite(request);
    if (armed_ && fault_ == TabletApplicationIoFault::kWriteAfter && result >= 0 &&
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
    if (armed_ && fault_ == TabletApplicationIoFault::kDataSyncBefore)
      return fail();
    const int result = delegate_.fdatasync(descriptor);
    if (armed_ && fault_ == TabletApplicationIoFault::kDataSyncAfter && result == 0)
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
  TabletApplicationIoFault fault_;
  bool partial_write_started_{false};
  bool armed_{false};
  bool fired_{false};
};

struct TabletApplicationFailureCase {
  TabletApplicationIoFault fault;
  std::string_view name;
  bool tail_repair_required;
  bool applied_index_record_recovered;
};

constexpr std::array<TabletApplicationFailureCase, 5U> kFailureCases{
    TabletApplicationFailureCase{TabletApplicationIoFault::kWriteBefore, "write_before", false,
                                 false},
    TabletApplicationFailureCase{TabletApplicationIoFault::kWritePrefixThenError,
                                 "write_prefix_then_error", true, false},
    TabletApplicationFailureCase{TabletApplicationIoFault::kWriteAfter, "write_after", false, true},
    TabletApplicationFailureCase{TabletApplicationIoFault::kDataSyncBefore, "data_sync_before",
                                 false, true},
    TabletApplicationFailureCase{TabletApplicationIoFault::kDataSyncAfter, "data_sync_after", false,
                                 true},
};

struct TabletApplicationSchedule {
  std::string_view name;
  std::array<std::uint8_t, 3U> request_seeds;
  std::size_t request_count;
  bool prepend_internal_entry;
  std::size_t expected_rows;
  std::size_t expected_retry_entries;
};

constexpr std::array<TabletApplicationSchedule, 4U> kApplicationSchedules{
    TabletApplicationSchedule{"single", {1U, 0U, 0U}, 1U, false, 2U, 1U},
    TabletApplicationSchedule{"three_distinct", {1U, 2U, 3U}, 3U, false, 6U, 3U},
    TabletApplicationSchedule{"matching_retry", {1U, 1U, 2U}, 3U, false, 4U, 2U},
    TabletApplicationSchedule{"internal_and_retry", {1U, 2U, 1U}, 3U, true, 4U, 2U},
};

[[nodiscard]] raft::LogIndex last_index(const TabletApplicationSchedule& schedule) {
  return schedule.request_count + (schedule.prepend_internal_entry ? 1U : 0U);
}

void expect_recovered_tablet(const RaftTabletStateMachine& machine,
                             const TabletApplicationSchedule& schedule) {
  auto publication = machine.tablet().snapshot();
  ASSERT_TRUE(publication.has_value()) << publication.error().to_string();
  EXPECT_EQ(publication->visible_row_count(), schedule.expected_rows);
  EXPECT_EQ(publication->retry_entry_count(), schedule.expected_retry_entries);
  std::array<bool, 256U> observed_seeds{};
  for (std::size_t index = 0U; index < schedule.request_count; ++index) {
    const std::uint8_t seed = schedule.request_seeds[index];
    if (observed_seeds[seed])
      continue;
    observed_seeds[seed] = true;
    const RetryIdentity identity{test::request_id<ClientId>(seed),
                                 test::request_id<ClientBatchId>(seed + 32U)};
    EXPECT_NE(publication->retry_outcome(identity), nullptr);
  }
  EXPECT_EQ(publication->applied_position(),
            head::HeadCommitPosition::raft(test::crash_group_id(), last_index(schedule)));
}

using TabletApplicationFailureSchedule =
    std::tuple<TabletApplicationFailureCase, TabletApplicationSchedule>;

class TabletApplicationFailureTest
    : public ::testing::TestWithParam<TabletApplicationFailureSchedule> {};

TEST_P(TabletApplicationFailureTest, FailsClosedThenRebuildsFromTheRetainedCommittedEntry) {
  TabletApplicationFailureDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const auto& [failure, schedule] = GetParam();
  OneShotTabletApplicationIoFault syscalls{failure.fault};

  auto runtime = raft::detail::DurableMultiRaftRuntimeTestAccess::create_new(
      4U, test::crash_log_config(directory.path()), test::crash_compaction_groups(), {}, syscalls);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  auto election =
      runtime->execute_batch({{test::crash_group_id(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value()) << election.error().to_string();
  ASSERT_EQ(election->size(), 1U);
  ASSERT_TRUE(election->front().status.is_ok()) << election->front().status.to_string();
  auto recovered = RaftTabletStateMachine::recover(
      test::crash_group_id(), *runtime, test::crash_compaction_retry_directory(),
      test::crash_compaction_tablet(), test::crash_compaction_schemas());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  std::optional<RaftTabletStateMachine> machine{std::move(*recovered)};
  if (schedule.prepend_internal_entry) {
    auto internal =
        runtime->execute_batch({{test::crash_group_id(), raft::CommitCurrentTermOperation{}}});
    ASSERT_TRUE(internal.has_value()) << internal.error().to_string();
    ASSERT_EQ(internal->size(), 1U);
    ASSERT_TRUE(internal->front().status.is_ok()) << internal->front().status.to_string();
  }
  for (std::size_t index = 0U; index < schedule.request_count; ++index) {
    auto proposed = runtime->execute_batch(
        {{test::crash_group_id(),
          raft::ProposeOperation{kRaftColumnarAppendEntryType,
                                 test::crash_compaction_command(schedule.request_seeds[index])}}});
    ASSERT_TRUE(proposed.has_value()) << proposed.error().to_string();
    ASSERT_EQ(proposed->size(), 1U);
    ASSERT_TRUE(proposed->front().status.is_ok()) << proposed->front().status.to_string();
  }
  syscalls.arm();

  auto applied = machine->apply_committed();

  ASSERT_FALSE(applied.has_value());
  EXPECT_EQ(applied.error().code(), common::StatusCode::kIoError);
  EXPECT_TRUE(syscalls.fired());
  EXPECT_TRUE(machine->failed());
  EXPECT_TRUE(runtime->failed());
  expect_recovered_tablet(*machine, schedule);
  machine.reset();
  ASSERT_TRUE(runtime->close().is_ok());

  if (failure.tail_repair_required) {
    auto strict = raft::DurableMultiRaftRuntime::open_existing(
        4U, test::crash_log_config(directory.path()), {}, test::crash_compaction_groups());
    ASSERT_FALSE(strict.has_value());
    EXPECT_EQ(strict.error().code(), common::StatusCode::kCorruption);
  }
  const raft::RaftPersistentLogOpenOptions open_options{.repair_incomplete_final_tail =
                                                            failure.tail_repair_required};
  auto reopened = raft::DurableMultiRaftRuntime::open_existing(
      4U, test::crash_log_config(directory.path()), open_options, test::crash_compaction_groups());
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  const raft::RaftNode* node = reopened->find_group(test::crash_group_id());
  ASSERT_NE(node, nullptr);
  EXPECT_EQ(node->persistent_state().commit_index, last_index(schedule));
  EXPECT_EQ(node->persistent_state().applied_index,
            failure.applied_index_record_recovered ? last_index(schedule) : 0U);
  ASSERT_EQ(node->persistent_state().log.size(), last_index(schedule));
  EXPECT_EQ(node->persistent_state().log.front().index, 1U);
  EXPECT_EQ(node->persistent_state().log.back().index, last_index(schedule));

  recovered = RaftTabletStateMachine::recover(
      test::crash_group_id(), *reopened, test::crash_compaction_retry_directory(),
      test::crash_compaction_tablet(), test::crash_compaction_schemas());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  std::optional<RaftTabletStateMachine> rebuilt{std::move(*recovered)};
  expect_recovered_tablet(*rebuilt, schedule);
  node = reopened->find_group(test::crash_group_id());
  ASSERT_NE(node, nullptr);
  EXPECT_EQ(node->persistent_state().applied_index, last_index(schedule));
  rebuilt.reset();
  ASSERT_TRUE(reopened->close().is_ok());

  auto repeated_runtime = raft::DurableMultiRaftRuntime::open_existing(
      4U, test::crash_log_config(directory.path()), {}, test::crash_compaction_groups());
  ASSERT_TRUE(repeated_runtime.has_value()) << repeated_runtime.error().to_string();
  recovered = RaftTabletStateMachine::recover(
      test::crash_group_id(), *repeated_runtime, test::crash_compaction_retry_directory(),
      test::crash_compaction_tablet(), test::crash_compaction_schemas());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  std::optional<RaftTabletStateMachine> repeated{std::move(*recovered)};
  expect_recovered_tablet(*repeated, schedule);
  node = repeated_runtime->find_group(test::crash_group_id());
  ASSERT_NE(node, nullptr);
  EXPECT_EQ(node->persistent_state().applied_index, last_index(schedule));
  repeated.reset();
  ASSERT_TRUE(repeated_runtime->close().is_ok());
}

INSTANTIATE_TEST_SUITE_P(
    EveryAppliedIndexPersistenceFailure, TabletApplicationFailureTest,
    ::testing::Combine(::testing::ValuesIn(kFailureCases),
                       ::testing::ValuesIn(kApplicationSchedules)),
    [](const ::testing::TestParamInfo<TabletApplicationFailureSchedule>& parameter) {
      std::string name{std::get<1>(parameter.param).name};
      name += "_then_";
      name += std::get<0>(parameter.param).name;
      return name;
    });

} // namespace
} // namespace chronos::ingest
