#include "chronos/common/status.hpp"
#include "chronos/raft/persistent_log.hpp"
#include "raft/persistent_log_internal.hpp"
#include "raft/raft_test_posix.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-raft-log-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  TemporaryDirectory(const TemporaryDirectory&) = delete;
  TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

[[nodiscard]] GroupId group_id(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return GroupId{bytes};
}

[[nodiscard]] GroupPersistentState state(const GroupId group, const std::uint64_t sequence,
                                         const std::byte value) {
  PersistentState persistent{};
  persistent.current_term = 1U;
  persistent.log.push_back(LogEntry{1U, 1U, 1U, {value}});
  persistent.commit_index = 1U;
  return GroupPersistentState{group, sequence, std::move(persistent)};
}

[[nodiscard]] std::filesystem::path highest_segment(const std::filesystem::path& directory) {
  std::filesystem::path selected;
  for (const auto& entry : std::filesystem::directory_iterator(directory)) {
    if (entry.path().extension() == ".rlog" && entry.path().filename() > selected.filename())
      selected = entry.path();
  }
  return selected;
}

[[nodiscard]] std::filesystem::path recovery_anchor(const std::filesystem::path& directory) {
  for (const auto& entry : std::filesystem::directory_iterator(directory)) {
    if (entry.path().extension() == ".rbase")
      return entry.path();
  }
  return {};
}

TEST(RaftPersistentLogTest, RotatesRecoversLatestGroupStatesAndContinuesSequence) {
  TemporaryDirectory directory;
  const RaftPersistentLogConfig config{.directory_path = directory.path().string(),
                                       .target_segment_size = 300U};
  auto log = RaftPersistentLog::create_new(config);
  ASSERT_TRUE(log.has_value()) << log.error().to_string();
  const GroupId first = group_id(std::byte{1U});
  const GroupId second = group_id(std::byte{2U});
  ASSERT_TRUE(log->append(state(first, 1U, std::byte{0x11U})).has_value());
  ASSERT_TRUE(log->append(state(second, 2U, std::byte{0x22U})).has_value());
  ASSERT_TRUE(log->append(state(first, 3U, std::byte{0x33U})).has_value());
  auto durable = log->synchronize();
  ASSERT_TRUE(durable.has_value()) << durable.error().to_string();
  EXPECT_EQ(durable->physical_sequence, 3U);
  EXPECT_EQ(log->recovery().segment_count, 3U);
  ASSERT_TRUE(log->close().is_ok());

  auto reopened = RaftPersistentLog::open_existing(config);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  ASSERT_EQ(reopened->recovery().latest_group_states.size(), 2U);
  EXPECT_EQ(reopened->recovery().latest_group_states[0], state(first, 3U, std::byte{0x33U}));
  EXPECT_EQ(reopened->recovery().latest_group_states[1], state(second, 2U, std::byte{0x22U}));
  EXPECT_EQ(reopened->durable_physical_sequence(), 3U);
  auto appended = reopened->append(state(second, 4U, std::byte{0x44U}));
  ASSERT_TRUE(appended.has_value()) << appended.error().to_string();
  EXPECT_EQ(appended->physical_sequence, 4U);
}

TEST(RaftPersistentLogTest,
     AmbiguousPredecessorSyncAndCloseFailuresDuringRotationRecoverExactPrefix) {
  constexpr std::array faults{
      test::DurableIoFault::kDataSync,
      test::DurableIoFault::kFileClose,
  };
  constexpr std::array expected_operations{"fdatasync", "close regular file"};

  for (std::size_t fault_index = 0U; fault_index < faults.size(); ++fault_index) {
    SCOPED_TRACE(fault_index);
    TemporaryDirectory directory;
    const RaftPersistentLogConfig config{.directory_path = directory.path().string(),
                                         .target_segment_size = 300U};
    test::DurableIoFaultPosixSyscalls syscalls;
    auto log = detail::RaftPersistentLogTestAccess::create_new(config, syscalls);
    ASSERT_TRUE(log.has_value()) << log.error().to_string();
    const GroupId group = group_id(static_cast<std::byte>(0x40U + fault_index));
    const GroupPersistentState first = state(group, 1U, std::byte{0x51U});
    const GroupPersistentState second = state(group, 2U, std::byte{0x52U});
    ASSERT_TRUE(log->append(first).has_value());

    syscalls.arm(faults[fault_index]);
    auto failed = log->append(second);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error().code(), common::StatusCode::kIoError);
    EXPECT_NE(failed.error().to_string().find(expected_operations[fault_index]), std::string::npos);
    EXPECT_TRUE(log->is_failed());
    EXPECT_EQ(log->failure_status(), failed.error());
    EXPECT_EQ(log->written_position().segment_number, 1U);
    EXPECT_EQ(log->written_position().physical_sequence, 1U);
    EXPECT_EQ(log->recovery().segment_count, 1U);
    EXPECT_EQ(syscalls.injected_faults(), 1U);
    auto rejected = log->append(second);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), failed.error());
    EXPECT_TRUE(log->close().is_ok());

    auto reopened = RaftPersistentLog::open_existing(config);
    ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
    ASSERT_EQ(reopened->recovery().latest_group_states.size(), 1U);
    EXPECT_EQ(reopened->recovery().latest_group_states.front(), first);
    EXPECT_EQ(reopened->durable_physical_sequence(), 1U);
    EXPECT_EQ(reopened->recovery().segment_count, 1U);
    auto appended = reopened->append(second);
    ASSERT_TRUE(appended.has_value()) << appended.error().to_string();
    EXPECT_EQ(appended->segment_number, 2U);
    EXPECT_EQ(appended->physical_sequence, 2U);
    EXPECT_TRUE(reopened->close().is_ok());
  }
}

TEST(RaftPersistentLogTest, SuccessorInstallationFailuresRecoverAtTheExactVisibleBoundary) {
  struct InstallationFailure {
    test::DurableIoFault fault;
    std::string_view expected_operation;
    bool temporary_visible;
    bool successor_visible;
  };
  constexpr std::array failures{
      InstallationFailure{test::DurableIoFault::kFileCreate, "openat exclusive regular file", false,
                          false},
      InstallationFailure{test::DurableIoFault::kWrite, "pwrite", true, false},
      InstallationFailure{test::DurableIoFault::kFullSync, "fsync regular file", true, false},
      InstallationFailure{test::DurableIoFault::kRename, "same-directory no-replace rename", false,
                          true},
      InstallationFailure{test::DurableIoFault::kDirectorySync, "fsync directory", false, true},
  };

  for (std::size_t failure_index = 0U; failure_index < failures.size(); ++failure_index) {
    SCOPED_TRACE(failure_index);
    TemporaryDirectory directory;
    const RaftPersistentLogConfig config{.directory_path = directory.path().string(),
                                         .target_segment_size = 300U};
    test::DurableIoFaultPosixSyscalls syscalls;
    auto log = detail::RaftPersistentLogTestAccess::create_new(config, syscalls);
    ASSERT_TRUE(log.has_value()) << log.error().to_string();
    const GroupId group = group_id(static_cast<std::byte>(0x50U + failure_index));
    const GroupPersistentState first = state(group, 1U, std::byte{0x61U});
    const GroupPersistentState second = state(group, 2U, std::byte{0x62U});
    ASSERT_TRUE(log->append(first).has_value());

    syscalls.arm(failures[failure_index].fault);
    auto failed = log->append(second);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error().code(), common::StatusCode::kIoError);
    EXPECT_NE(failed.error().to_string().find(failures[failure_index].expected_operation),
              std::string::npos);
    EXPECT_TRUE(log->is_failed());
    EXPECT_EQ(log->failure_status(), failed.error());
    EXPECT_EQ(log->written_position().segment_number, 1U);
    EXPECT_EQ(log->written_position().physical_sequence, 1U);
    EXPECT_EQ(log->recovery().segment_count, 1U);
    EXPECT_EQ(syscalls.injected_faults(), 1U);
    EXPECT_TRUE(log->close().is_ok());

    const std::filesystem::path temporary = directory.path() / "raft-00000000000000000002.tmp";
    const std::filesystem::path successor = directory.path() / "raft-00000000000000000002.rlog";
    EXPECT_EQ(std::filesystem::exists(temporary), failures[failure_index].temporary_visible);
    EXPECT_EQ(std::filesystem::exists(successor), failures[failure_index].successor_visible);

    auto reopened = RaftPersistentLog::open_existing(config);
    ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
    ASSERT_EQ(reopened->recovery().latest_group_states.size(), 1U);
    EXPECT_EQ(reopened->recovery().latest_group_states.front(), first);
    EXPECT_EQ(reopened->durable_physical_sequence(), 1U);
    EXPECT_EQ(reopened->recovery().segment_count,
              failures[failure_index].successor_visible ? 2U : 1U);
    EXPECT_FALSE(std::filesystem::exists(temporary));
    auto appended = reopened->append(second);
    ASSERT_TRUE(appended.has_value()) << appended.error().to_string();
    EXPECT_EQ(appended->segment_number, 2U);
    EXPECT_EQ(appended->physical_sequence, 2U);
    ASSERT_TRUE(reopened->synchronize().has_value());
    EXPECT_TRUE(reopened->close().is_ok());
  }
}

TEST(RaftPersistentLogTest, CloseInvalidatesEveryHandleAndReturnsTheFirstPhysicalError) {
  constexpr std::array<std::string_view, 3U> close_operations{{
      "close regular file",
      "close advisory lock",
      "close directory",
  }};

  for (std::uint8_t failure_mask = 1U; failure_mask < 8U; ++failure_mask) {
    SCOPED_TRACE(static_cast<std::uint32_t>(failure_mask));
    TemporaryDirectory directory;
    const RaftPersistentLogConfig config{.directory_path = directory.path().string()};
    test::CloseFaultPosixSyscalls syscalls{failure_mask};
    auto log = detail::RaftPersistentLogTestAccess::create_new(config, syscalls);
    ASSERT_TRUE(log.has_value()) << log.error().to_string();
    const GroupPersistentState persisted =
        state(group_id(static_cast<std::byte>(failure_mask)), 1U, std::byte{0x31U});
    ASSERT_TRUE(log->append(persisted).has_value());
    ASSERT_TRUE(log->synchronize().has_value());

    const common::Status closed = log->close();

    std::size_t first_failure{};
    while ((failure_mask & (std::uint8_t{1U} << first_failure)) == 0U)
      ++first_failure;
    EXPECT_EQ(closed.code(), common::StatusCode::kIoError);
    EXPECT_NE(closed.to_string().find(close_operations[first_failure]), std::string::npos);
    EXPECT_EQ(syscalls.close_calls(), 3U);
    EXPECT_FALSE(log->is_open());
    EXPECT_TRUE(log->close().is_ok());
    EXPECT_EQ(syscalls.close_calls(), 3U);

    auto reopened = RaftPersistentLog::open_existing(config);
    ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
    ASSERT_EQ(reopened->recovery().latest_group_states.size(), 1U);
    EXPECT_EQ(reopened->recovery().latest_group_states.front(), persisted);
    EXPECT_TRUE(reopened->close().is_ok());
  }
}

TEST(RaftPersistentLogTest, CheckpointsEveryGroupBeforeReclaimingSharedSegmentPrefix) {
  TemporaryDirectory directory;
  const RaftPersistentLogConfig config{.directory_path = directory.path().string(),
                                       .target_segment_size = 300U};
  auto log = RaftPersistentLog::create_new(config);
  ASSERT_TRUE(log.has_value()) << log.error().to_string();
  const GroupId first = group_id(std::byte{11U});
  const GroupId second = group_id(std::byte{12U});
  ASSERT_TRUE(log->append(state(first, 1U, std::byte{0x11U})).has_value());
  ASSERT_TRUE(log->append(state(second, 2U, std::byte{0x22U})).has_value());
  ASSERT_TRUE(log->append(state(first, 3U, std::byte{0x33U})).has_value());
  ASSERT_TRUE(log->synchronize().has_value());

  const std::vector<GroupPersistentState> checkpoint{state(first, 4U, std::byte{0x33U}),
                                                     state(second, 5U, std::byte{0x22U})};
  auto reclaimed = log->checkpoint_and_reclaim(checkpoint);

  ASSERT_TRUE(reclaimed.has_value()) << reclaimed.error().to_string();
  EXPECT_EQ(reclaimed->base_segment_number, 4U);
  EXPECT_EQ(reclaimed->checkpoint_first_physical_sequence, 4U);
  EXPECT_EQ(reclaimed->checkpoint_last_physical_sequence, 5U);
  EXPECT_EQ(reclaimed->reclaimed_segments, 3U);
  EXPECT_EQ(reclaimed->reclaimed_records, 3U);
  EXPECT_EQ(log->recovery().base_segment_number, 4U);
  EXPECT_EQ(log->recovery().segment_count, 2U);
  EXPECT_EQ(log->recovery().record_count, 2U);
  ASSERT_TRUE(log->close().is_ok());

  auto reopened = RaftPersistentLog::open_existing(config);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_EQ(reopened->recovery().base_segment_number, 4U);
  ASSERT_EQ(reopened->recovery().latest_group_states.size(), 2U);
  EXPECT_EQ(reopened->recovery().latest_group_states[0], checkpoint[0]);
  EXPECT_EQ(reopened->recovery().latest_group_states[1], checkpoint[1]);
  auto appended = reopened->append(state(first, 6U, std::byte{0x44U}));
  ASSERT_TRUE(appended.has_value()) << appended.error().to_string();
  EXPECT_EQ(appended->physical_sequence, 6U);
}

TEST(RaftPersistentLogTest, RecoveryAnchorInstallationFailuresPreserveOneExactAuthority) {
  struct AnchorInstallationFailure {
    test::DurableIoFault fault;
    std::size_t matching_calls_to_skip;
    std::string_view expected_operation;
    bool temporary_visible;
    bool anchor_visible;
  };
  constexpr std::array failures{
      AnchorInstallationFailure{test::DurableIoFault::kFileCreate, 1U,
                                "openat exclusive regular file", false, false},
      AnchorInstallationFailure{test::DurableIoFault::kWrite, 3U, "pwrite", true, false},
      AnchorInstallationFailure{test::DurableIoFault::kFullSync, 1U, "fsync regular file", true,
                                false},
      AnchorInstallationFailure{test::DurableIoFault::kRename, 1U,
                                "same-directory no-replace rename", false, true},
      AnchorInstallationFailure{test::DurableIoFault::kDirectorySync, 1U, "fsync directory", false,
                                true},
      AnchorInstallationFailure{test::DurableIoFault::kFileClose, 1U, "close regular file", false,
                                true},
  };

  for (std::size_t failure_index = 0U; failure_index < failures.size(); ++failure_index) {
    SCOPED_TRACE(failure_index);
    TemporaryDirectory directory;
    const RaftPersistentLogConfig config{.directory_path = directory.path().string()};
    test::DurableIoFaultPosixSyscalls syscalls;
    auto log = detail::RaftPersistentLogTestAccess::create_new(config, syscalls);
    ASSERT_TRUE(log.has_value()) << log.error().to_string();
    const GroupId first_group = group_id(static_cast<std::byte>(0x60U + failure_index));
    const GroupId second_group = group_id(static_cast<std::byte>(0x70U + failure_index));
    ASSERT_TRUE(log->append(state(first_group, 1U, std::byte{0x71U})).has_value());
    ASSERT_TRUE(log->append(state(second_group, 2U, std::byte{0x72U})).has_value());
    ASSERT_TRUE(log->synchronize().has_value());
    const std::vector<GroupPersistentState> checkpoint{
        state(first_group, 3U, std::byte{0x73U}),
        state(second_group, 4U, std::byte{0x74U}),
    };

    syscalls.arm(failures[failure_index].fault, failures[failure_index].matching_calls_to_skip);
    auto failed = log->checkpoint_and_reclaim(checkpoint);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error().code(), common::StatusCode::kIoError);
    EXPECT_NE(failed.error().to_string().find(failures[failure_index].expected_operation),
              std::string::npos);
    EXPECT_TRUE(log->is_failed());
    EXPECT_EQ(log->failure_status(), failed.error());
    EXPECT_EQ(log->written_position().segment_number, 2U);
    EXPECT_EQ(log->written_position().physical_sequence, 4U);
    EXPECT_EQ(log->durable_physical_sequence(), 4U);
    EXPECT_EQ(log->recovery().base_segment_number, 1U);
    EXPECT_EQ(log->recovery().segment_count, 2U);
    EXPECT_EQ(log->recovery().record_count, 4U);
    EXPECT_EQ(syscalls.injected_faults(), 1U);
    EXPECT_TRUE(log->close().is_ok());

    const std::filesystem::path old_segment = directory.path() / "raft-00000000000000000001.rlog";
    const std::filesystem::path temporary =
        directory.path() / "raft-base-00000000000000000002.rbase.tmp";
    const std::filesystem::path anchor = directory.path() / "raft-base-00000000000000000002.rbase";
    EXPECT_TRUE(std::filesystem::exists(old_segment));
    EXPECT_EQ(std::filesystem::exists(temporary), failures[failure_index].temporary_visible);
    EXPECT_EQ(std::filesystem::exists(anchor), failures[failure_index].anchor_visible);

    auto reopened = RaftPersistentLog::open_existing(config);
    ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
    ASSERT_EQ(reopened->recovery().latest_group_states.size(), 2U);
    EXPECT_EQ(reopened->recovery().latest_group_states[0], checkpoint[0]);
    EXPECT_EQ(reopened->recovery().latest_group_states[1], checkpoint[1]);
    EXPECT_EQ(reopened->durable_physical_sequence(), 4U);
    EXPECT_EQ(reopened->recovery().base_segment_number,
              failures[failure_index].anchor_visible ? 2U : 1U);
    EXPECT_EQ(reopened->recovery().segment_count, failures[failure_index].anchor_visible ? 1U : 2U);
    EXPECT_EQ(reopened->recovery().record_count, failures[failure_index].anchor_visible ? 2U : 4U);
    EXPECT_EQ(std::filesystem::exists(old_segment), !failures[failure_index].anchor_visible);
    EXPECT_FALSE(std::filesystem::exists(temporary));
    auto appended = reopened->append(state(first_group, 5U, std::byte{0x75U}));
    ASSERT_TRUE(appended.has_value()) << appended.error().to_string();
    EXPECT_EQ(appended->physical_sequence, 5U);
    ASSERT_TRUE(reopened->synchronize().has_value());
    EXPECT_TRUE(reopened->close().is_ok());
  }
}

TEST(RaftPersistentLogTest, ReclamationDeletionAndSyncFailuresRecoverNewestAuthority) {
  struct ReclamationFailure {
    bool during_second_checkpoint;
    test::DurableIoFault fault;
    std::size_t matching_calls_to_skip;
    std::string_view expected_operation;
  };
  constexpr std::array failures{
      ReclamationFailure{false, test::DurableIoFault::kUnlink, 0U, "unlinkat WAL entry"},
      ReclamationFailure{false, test::DurableIoFault::kDirectorySync, 2U, "fsync directory"},
      ReclamationFailure{true, test::DurableIoFault::kUnlink, 1U, "unlinkat WAL entry"},
      ReclamationFailure{true, test::DurableIoFault::kDirectorySync, 3U, "fsync directory"},
  };

  for (std::size_t failure_index = 0U; failure_index < failures.size(); ++failure_index) {
    SCOPED_TRACE(failure_index);
    TemporaryDirectory directory;
    const RaftPersistentLogConfig config{.directory_path = directory.path().string()};
    test::DurableIoFaultPosixSyscalls syscalls;
    auto log = detail::RaftPersistentLogTestAccess::create_new(config, syscalls);
    ASSERT_TRUE(log.has_value()) << log.error().to_string();
    const GroupId first_group = group_id(static_cast<std::byte>(0x80U + failure_index));
    const GroupId second_group = group_id(static_cast<std::byte>(0x90U + failure_index));
    ASSERT_TRUE(log->append(state(first_group, 1U, std::byte{0x81U})).has_value());
    ASSERT_TRUE(log->append(state(second_group, 2U, std::byte{0x82U})).has_value());
    ASSERT_TRUE(log->synchronize().has_value());
    const std::vector<GroupPersistentState> first_checkpoint{
        state(first_group, 3U, std::byte{0x83U}),
        state(second_group, 4U, std::byte{0x84U}),
    };
    const std::vector<GroupPersistentState> second_checkpoint{
        state(first_group, 5U, std::byte{0x85U}),
        state(second_group, 6U, std::byte{0x86U}),
    };
    if (failures[failure_index].during_second_checkpoint) {
      auto reclaimed = log->checkpoint_and_reclaim(first_checkpoint);
      ASSERT_TRUE(reclaimed.has_value()) << reclaimed.error().to_string();
      EXPECT_EQ(reclaimed->base_segment_number, 2U);
    }
    const auto& checkpoint =
        failures[failure_index].during_second_checkpoint ? second_checkpoint : first_checkpoint;
    const std::uint64_t old_base = failures[failure_index].during_second_checkpoint ? 2U : 1U;
    const std::uint64_t new_base = old_base + 1U;

    syscalls.arm(failures[failure_index].fault, failures[failure_index].matching_calls_to_skip);
    auto failed = log->checkpoint_and_reclaim(checkpoint);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error().code(), common::StatusCode::kIoError);
    EXPECT_NE(failed.error().to_string().find(failures[failure_index].expected_operation),
              std::string::npos);
    EXPECT_TRUE(log->is_failed());
    EXPECT_EQ(log->failure_status(), failed.error());
    EXPECT_EQ(log->written_position().segment_number, new_base);
    EXPECT_EQ(log->written_position().physical_sequence, checkpoint.back().physical_sequence);
    EXPECT_EQ(log->durable_physical_sequence(), checkpoint.back().physical_sequence);
    EXPECT_EQ(log->recovery().base_segment_number, old_base);
    EXPECT_EQ(log->recovery().segment_count, 2U);
    EXPECT_EQ(log->recovery().record_count, 4U);
    EXPECT_EQ(syscalls.injected_faults(), 1U);
    EXPECT_TRUE(log->close().is_ok());

    const std::filesystem::path obsolete_segment =
        directory.path() /
        (old_base == 1U ? "raft-00000000000000000001.rlog" : "raft-00000000000000000002.rlog");
    const std::filesystem::path obsolete_anchor =
        directory.path() / "raft-base-00000000000000000002.rbase";
    const std::filesystem::path newest_anchor =
        directory.path() / (new_base == 2U ? "raft-base-00000000000000000002.rbase"
                                           : "raft-base-00000000000000000003.rbase");
    EXPECT_FALSE(std::filesystem::exists(obsolete_segment));
    if (old_base == 2U)
      EXPECT_FALSE(std::filesystem::exists(obsolete_anchor));
    EXPECT_TRUE(std::filesystem::exists(newest_anchor));

    auto reopened = RaftPersistentLog::open_existing(config);
    ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
    EXPECT_EQ(reopened->recovery().base_segment_number, new_base);
    EXPECT_EQ(reopened->recovery().segment_count, 1U);
    EXPECT_EQ(reopened->recovery().record_count, 2U);
    EXPECT_EQ(reopened->durable_physical_sequence(), checkpoint.back().physical_sequence);
    ASSERT_EQ(reopened->recovery().latest_group_states.size(), 2U);
    EXPECT_EQ(reopened->recovery().latest_group_states[0], checkpoint[0]);
    EXPECT_EQ(reopened->recovery().latest_group_states[1], checkpoint[1]);
    const std::uint64_t next_sequence = checkpoint.back().physical_sequence + 1U;
    auto appended =
        reopened->append(state(first_group, next_sequence, static_cast<std::byte>(0x87U)));
    ASSERT_TRUE(appended.has_value()) << appended.error().to_string();
    EXPECT_EQ(appended->physical_sequence, next_sequence);
    ASSERT_TRUE(reopened->synchronize().has_value());
    EXPECT_TRUE(reopened->close().is_ok());
  }
}

TEST(RaftPersistentLogTest, RecoveryNamespaceCleanupAndFinalSyncFailuresReleaseLockForExactRetry) {
  struct RecoveryFailure {
    test::DurableIoFault fault;
    bool create_stale_temporary;
    std::string_view expected_operation;
  };
  constexpr std::array failures{
      RecoveryFailure{test::DurableIoFault::kListDirectory, false, "list directory entries"},
      RecoveryFailure{test::DurableIoFault::kUnlink, true, "unlinkat WAL entry"},
      RecoveryFailure{test::DurableIoFault::kDirectorySync, true, "fsync directory"},
      RecoveryFailure{test::DurableIoFault::kFullSync, false, "fsync regular file"},
      RecoveryFailure{test::DurableIoFault::kDirectorySync, false, "fsync directory"},
  };

  for (std::size_t failure_index = 0U; failure_index < failures.size(); ++failure_index) {
    SCOPED_TRACE(failure_index);
    TemporaryDirectory directory;
    const RaftPersistentLogConfig config{.directory_path = directory.path().string()};
    const GroupPersistentState persisted =
        state(group_id(static_cast<std::byte>(0xA0U + failure_index)), 1U, std::byte{0x91U});
    auto log = RaftPersistentLog::create_new(config);
    ASSERT_TRUE(log.has_value()) << log.error().to_string();
    ASSERT_TRUE(log->append(persisted).has_value());
    ASSERT_TRUE(log->synchronize().has_value());
    ASSERT_TRUE(log->close().is_ok());

    const std::filesystem::path temporary = directory.path() / "raft-00000000000000000002.tmp";
    if (failures[failure_index].create_stale_temporary) {
      std::ofstream output(temporary, std::ios::binary);
      output.write("stale", 5);
      output.close();
      ASSERT_TRUE(std::filesystem::exists(temporary));
    }

    test::DurableIoFaultPosixSyscalls syscalls;
    syscalls.arm(failures[failure_index].fault);
    auto failed = detail::RaftPersistentLogTestAccess::open_existing(config, {}, syscalls);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error().code(), common::StatusCode::kIoError);
    EXPECT_NE(failed.error().to_string().find(failures[failure_index].expected_operation),
              std::string::npos);
    EXPECT_EQ(syscalls.injected_faults(), 1U);
    EXPECT_FALSE(std::filesystem::exists(temporary));

    auto reopened = RaftPersistentLog::open_existing(config);
    ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
    EXPECT_EQ(reopened->recovery().base_segment_number, 1U);
    EXPECT_EQ(reopened->recovery().segment_count, 1U);
    EXPECT_EQ(reopened->recovery().record_count, 1U);
    EXPECT_EQ(reopened->durable_physical_sequence(), 1U);
    ASSERT_EQ(reopened->recovery().latest_group_states.size(), 1U);
    EXPECT_EQ(reopened->recovery().latest_group_states.front(), persisted);
    auto appended = reopened->append(state(persisted.group_id, 2U, static_cast<std::byte>(0x92U)));
    ASSERT_TRUE(appended.has_value()) << appended.error().to_string();
    EXPECT_EQ(appended->physical_sequence, 2U);
    ASSERT_TRUE(reopened->synchronize().has_value());
    EXPECT_TRUE(reopened->close().is_ok());
  }
}

TEST(RaftPersistentLogTest, RecoveryOpenStatAndReadFailuresReleaseLockForExactRetry) {
  struct RecoveryFailure {
    test::DurableIoFault fault;
    std::size_t matching_calls_to_skip;
    std::string_view stage;
    std::string_view expected_operation;
  };
  constexpr std::array failures{
      RecoveryFailure{test::DurableIoFault::kDirectoryOpen, 0U, "directory open", "open directory"},
      RecoveryFailure{test::DurableIoFault::kFileOpen, 0U, "lock open",
                      "openat existing advisory lock"},
      RecoveryFailure{test::DurableIoFault::kFileOpen, 1U, "anchor open", "openat regular file"},
      RecoveryFailure{test::DurableIoFault::kFileOpen, 2U, "retained-segment scan open",
                      "openat regular file"},
      RecoveryFailure{test::DurableIoFault::kFileOpen, 3U, "final active open",
                      "openat regular file"},
      RecoveryFailure{test::DurableIoFault::kStat, 0U, "directory validation", "fstat directory"},
      RecoveryFailure{test::DurableIoFault::kStat, 1U, "lock reservation identity",
                      "fstat advisory-lock directory"},
      RecoveryFailure{test::DurableIoFault::kStat, 2U, "lock validation", "fstat regular file"},
      RecoveryFailure{test::DurableIoFault::kStat, 3U, "anchor validation", "fstat regular file"},
      RecoveryFailure{test::DurableIoFault::kStat, 4U, "anchor size", "fstat file size"},
      RecoveryFailure{test::DurableIoFault::kStat, 5U, "retained-segment validation",
                      "fstat regular file"},
      RecoveryFailure{test::DurableIoFault::kStat, 6U, "retained-segment size", "fstat file size"},
      RecoveryFailure{test::DurableIoFault::kStat, 7U, "final active validation",
                      "fstat regular file"},
      RecoveryFailure{test::DurableIoFault::kRead, 0U, "anchor read", "pread"},
      RecoveryFailure{test::DurableIoFault::kRead, 1U, "segment-header read", "pread"},
      RecoveryFailure{test::DurableIoFault::kRead, 2U, "record-header read", "pread"},
      RecoveryFailure{test::DurableIoFault::kRead, 3U, "record-body read", "pread"},
  };

  for (std::size_t failure_index = 0U; failure_index < failures.size(); ++failure_index) {
    SCOPED_TRACE(failures[failure_index].stage);
    TemporaryDirectory directory;
    const RaftPersistentLogConfig config{.directory_path = directory.path().string()};
    const GroupId group = group_id(static_cast<std::byte>(0xB0U + failure_index));
    auto log = RaftPersistentLog::create_new(config);
    ASSERT_TRUE(log.has_value()) << log.error().to_string();
    ASSERT_TRUE(log->append(state(group, 1U, std::byte{0xA1U})).has_value());
    ASSERT_TRUE(log->synchronize().has_value());
    const GroupPersistentState checkpoint = state(group, 2U, std::byte{0xA2U});
    ASSERT_TRUE(log->checkpoint_and_reclaim({checkpoint}).has_value());
    ASSERT_TRUE(log->close().is_ok());

    test::DurableIoFaultPosixSyscalls syscalls;
    syscalls.arm(failures[failure_index].fault, failures[failure_index].matching_calls_to_skip);
    auto failed = detail::RaftPersistentLogTestAccess::open_existing(config, {}, syscalls);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error().code(), common::StatusCode::kIoError);
    EXPECT_NE(failed.error().to_string().find(failures[failure_index].expected_operation),
              std::string::npos);
    EXPECT_EQ(syscalls.injected_faults(), 1U);

    auto reopened = RaftPersistentLog::open_existing(config);
    ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
    EXPECT_EQ(reopened->recovery().base_segment_number, 2U);
    EXPECT_EQ(reopened->recovery().segment_count, 1U);
    EXPECT_EQ(reopened->recovery().record_count, 1U);
    EXPECT_EQ(reopened->durable_physical_sequence(), 2U);
    ASSERT_EQ(reopened->recovery().latest_group_states.size(), 1U);
    EXPECT_EQ(reopened->recovery().latest_group_states.front(), checkpoint);
    auto appended = reopened->append(state(group, 3U, std::byte{0xA3U}));
    ASSERT_TRUE(appended.has_value()) << appended.error().to_string();
    EXPECT_EQ(appended->physical_sequence, 3U);
    ASSERT_TRUE(reopened->synchronize().has_value());
    EXPECT_TRUE(reopened->close().is_ok());
  }
}

TEST(RaftPersistentLogTest, IncompleteTailRepairFailuresReleaseLockForIdempotentRetry) {
  struct RepairFailure {
    test::DurableIoFault fault;
    std::size_t matching_calls_to_skip;
    std::string_view stage;
    std::string_view expected_operation;
    bool truncates_before_failure;
  };
  constexpr std::array failures{
      RepairFailure{test::DurableIoFault::kStat, 4U, "pre-truncate size", "fstat file size", false},
      RepairFailure{test::DurableIoFault::kTruncate, 0U, "truncate", "ftruncate", true},
      RepairFailure{test::DurableIoFault::kFullSync, 0U, "repaired-file sync", "fsync regular file",
                    true},
      RepairFailure{test::DurableIoFault::kDirectorySync, 0U, "repair directory sync",
                    "fsync directory", true},
  };

  for (std::size_t failure_index = 0U; failure_index < failures.size(); ++failure_index) {
    SCOPED_TRACE(failures[failure_index].stage);
    TemporaryDirectory directory;
    const RaftPersistentLogConfig config{.directory_path = directory.path().string()};
    const GroupId group = group_id(static_cast<std::byte>(0xC0U + failure_index));
    const GroupPersistentState persisted = state(group, 1U, std::byte{0xB1U});
    auto log = RaftPersistentLog::create_new(config);
    ASSERT_TRUE(log.has_value()) << log.error().to_string();
    ASSERT_TRUE(log->append(persisted).has_value());
    ASSERT_TRUE(log->synchronize().has_value());
    const std::uint64_t first_record_end = log->written_position().end_offset;
    ASSERT_TRUE(log->append(state(group, 2U, std::byte{0xB2U})).has_value());
    ASSERT_TRUE(log->close().is_ok());
    const std::filesystem::path segment = highest_segment(directory.path());
    const std::uintmax_t complete_size = std::filesystem::file_size(segment);
    std::filesystem::resize_file(segment, complete_size - 8U);
    const std::uintmax_t incomplete_size = std::filesystem::file_size(segment);

    test::DurableIoFaultPosixSyscalls syscalls;
    syscalls.arm(failures[failure_index].fault, failures[failure_index].matching_calls_to_skip);
    auto failed = detail::RaftPersistentLogTestAccess::open_existing(
        config, RaftPersistentLogOpenOptions{.repair_incomplete_final_tail = true}, syscalls);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error().code(), common::StatusCode::kIoError);
    EXPECT_NE(failed.error().to_string().find(failures[failure_index].expected_operation),
              std::string::npos);
    EXPECT_EQ(syscalls.injected_faults(), 1U);
    EXPECT_EQ(std::filesystem::file_size(segment), failures[failure_index].truncates_before_failure
                                                       ? first_record_end
                                                       : incomplete_size);

    auto reopened = RaftPersistentLog::open_existing(
        config, RaftPersistentLogOpenOptions{.repair_incomplete_final_tail = true});
    ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
    EXPECT_EQ(reopened->recovery().base_segment_number, 1U);
    EXPECT_EQ(reopened->recovery().segment_count, 1U);
    EXPECT_EQ(reopened->recovery().record_count, 1U);
    EXPECT_EQ(reopened->durable_physical_sequence(), 1U);
    EXPECT_EQ(reopened->recovery().repaired_bytes, failures[failure_index].truncates_before_failure
                                                       ? 0U
                                                       : incomplete_size - first_record_end);
    ASSERT_EQ(reopened->recovery().latest_group_states.size(), 1U);
    EXPECT_EQ(reopened->recovery().latest_group_states.front(), persisted);
    auto appended = reopened->append(state(group, 2U, std::byte{0xB3U}));
    ASSERT_TRUE(appended.has_value()) << appended.error().to_string();
    EXPECT_EQ(appended->physical_sequence, 2U);
    ASSERT_TRUE(reopened->synchronize().has_value());
    EXPECT_TRUE(reopened->close().is_ok());
  }
}

TEST(RaftPersistentLogTest, RejectsNonconsecutiveCheckpointWithoutAdvancingTheWriter) {
  TemporaryDirectory directory;
  const RaftPersistentLogConfig config{.directory_path = directory.path().string()};
  auto log = RaftPersistentLog::create_new(config);
  ASSERT_TRUE(log.has_value()) << log.error().to_string();
  const GroupId group = group_id(std::byte{16U});
  ASSERT_TRUE(log->append(state(group, 1U, std::byte{0x11U})).has_value());
  ASSERT_TRUE(log->synchronize().has_value());
  const RaftPhysicalPosition before = log->written_position();

  auto rejected = log->checkpoint_and_reclaim({state(group, 3U, std::byte{0x22U})});

  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(log->written_position(), before);
  EXPECT_EQ(log->durable_physical_sequence(), 1U);
  auto reclaimed = log->checkpoint_and_reclaim({state(group, 2U, std::byte{0x22U})});
  ASSERT_TRUE(reclaimed.has_value()) << reclaimed.error().to_string();
  EXPECT_EQ(reclaimed->checkpoint_first_physical_sequence, 2U);
  EXPECT_EQ(reclaimed->checkpoint_last_physical_sequence, 2U);
}

TEST(RaftPersistentLogTest, RecoveryAnchorIgnoresAndCleansInterruptedOldSegmentDeletion) {
  TemporaryDirectory directory;
  const RaftPersistentLogConfig config{.directory_path = directory.path().string(),
                                       .target_segment_size = 300U};
  auto log = RaftPersistentLog::create_new(config);
  ASSERT_TRUE(log.has_value());
  const GroupId group = group_id(std::byte{13U});
  ASSERT_TRUE(log->append(state(group, 1U, std::byte{0x11U})).has_value());
  ASSERT_TRUE(log->synchronize().has_value());
  const std::filesystem::path old_segment = directory.path() / "raft-00000000000000000001.rlog";
  std::ifstream old_input(old_segment, std::ios::binary);
  const std::vector<char> old_bytes{std::istreambuf_iterator<char>{old_input},
                                    std::istreambuf_iterator<char>{}};
  ASSERT_FALSE(old_bytes.empty());
  auto reclaimed = log->checkpoint_and_reclaim({state(group, 2U, std::byte{0x11U})});
  ASSERT_TRUE(reclaimed.has_value()) << reclaimed.error().to_string();
  ASSERT_TRUE(log->close().is_ok());

  std::ofstream restored(old_segment, std::ios::binary);
  restored.write(old_bytes.data(), static_cast<std::streamsize>(old_bytes.size()));
  restored.close();
  ASSERT_TRUE(std::filesystem::exists(old_segment));

  auto reopened = RaftPersistentLog::open_existing(config);

  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_EQ(reopened->recovery().base_segment_number, reclaimed->base_segment_number);
  EXPECT_FALSE(std::filesystem::exists(old_segment));
  ASSERT_EQ(reopened->recovery().latest_group_states.size(), 1U);
  EXPECT_EQ(reopened->recovery().latest_group_states.front(), state(group, 2U, std::byte{0x11U}));
}

TEST(RaftPersistentLogTest, CorruptRecoveryAnchorNeverFallsBackToReclaimedHistory) {
  TemporaryDirectory directory;
  const RaftPersistentLogConfig config{.directory_path = directory.path().string()};
  auto log = RaftPersistentLog::create_new(config);
  ASSERT_TRUE(log.has_value());
  const GroupId group = group_id(std::byte{14U});
  ASSERT_TRUE(log->append(state(group, 1U, std::byte{0x14U})).has_value());
  ASSERT_TRUE(log->synchronize().has_value());
  ASSERT_TRUE(log->checkpoint_and_reclaim({state(group, 2U, std::byte{0x14U})}).has_value());
  ASSERT_TRUE(log->close().is_ok());
  const std::filesystem::path anchor = recovery_anchor(directory.path());
  ASSERT_FALSE(anchor.empty());
  std::fstream bytes(anchor, std::ios::in | std::ios::out | std::ios::binary);
  ASSERT_TRUE(bytes.is_open());
  bytes.seekg(24);
  char value{};
  bytes.read(&value, 1);
  value ^= 1;
  bytes.seekp(24);
  bytes.write(&value, 1);
  bytes.close();

  auto corrupted = RaftPersistentLog::open_existing(config);

  ASSERT_FALSE(corrupted.has_value());
  EXPECT_EQ(corrupted.error().code(), common::StatusCode::kCorruption);
}

TEST(RaftPersistentLogTest, AnchoredCheckpointIsNeverTreatedAsRepairableTail) {
  TemporaryDirectory directory;
  const RaftPersistentLogConfig config{.directory_path = directory.path().string()};
  auto log = RaftPersistentLog::create_new(config);
  ASSERT_TRUE(log.has_value());
  const GroupId group = group_id(std::byte{15U});
  ASSERT_TRUE(log->append(state(group, 1U, std::byte{0x15U})).has_value());
  ASSERT_TRUE(log->synchronize().has_value());
  ASSERT_TRUE(log->checkpoint_and_reclaim({state(group, 2U, std::byte{0x15U})}).has_value());
  ASSERT_TRUE(log->close().is_ok());
  const std::filesystem::path segment = highest_segment(directory.path());
  const std::uintmax_t complete_size = std::filesystem::file_size(segment);
  std::filesystem::resize_file(segment, complete_size - 8U);

  auto rejected = RaftPersistentLog::open_existing(
      config, RaftPersistentLogOpenOptions{.repair_incomplete_final_tail = true});

  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kCorruption);
  EXPECT_EQ(std::filesystem::file_size(segment), complete_size - 8U);
}

TEST(RaftPersistentLogTest, ExclusiveOwnerRejectsConcurrentOpen) {
  TemporaryDirectory directory;
  const RaftPersistentLogConfig config{.directory_path = directory.path().string()};
  auto owner = RaftPersistentLog::create_new(config);
  ASSERT_TRUE(owner.has_value());

  auto concurrent = RaftPersistentLog::open_existing(config);

  ASSERT_FALSE(concurrent.has_value());
  EXPECT_EQ(concurrent.error().code(), common::StatusCode::kUnavailable);
}

TEST(RaftPersistentLogTest, ExplicitRepairRemovesOnlyIncompleteFinalRecord) {
  TemporaryDirectory directory;
  const RaftPersistentLogConfig config{.directory_path = directory.path().string()};
  auto log = RaftPersistentLog::create_new(config);
  ASSERT_TRUE(log.has_value());
  const GroupId group = group_id(std::byte{3U});
  ASSERT_TRUE(log->append(state(group, 1U, std::byte{0x11U})).has_value());
  ASSERT_TRUE(log->synchronize().has_value());
  ASSERT_TRUE(log->append(state(group, 2U, std::byte{0x22U})).has_value());
  ASSERT_TRUE(log->close().is_ok());
  const std::filesystem::path segment = highest_segment(directory.path());
  const std::uintmax_t complete_size = std::filesystem::file_size(segment);
  std::filesystem::resize_file(segment, complete_size - 8U);

  auto strict = RaftPersistentLog::open_existing(config);
  ASSERT_FALSE(strict.has_value());
  EXPECT_EQ(strict.error().code(), common::StatusCode::kCorruption);

  auto repaired = RaftPersistentLog::open_existing(
      config, RaftPersistentLogOpenOptions{.repair_incomplete_final_tail = true});
  ASSERT_TRUE(repaired.has_value()) << repaired.error().to_string();
  EXPECT_EQ(repaired->recovery().record_count, 1U);
  EXPECT_EQ(repaired->recovery().written_position.physical_sequence, 1U);
  EXPECT_EQ(repaired->recovery().repaired_bytes,
            complete_size - 8U - repaired->recovery().written_position.end_offset);
  ASSERT_EQ(repaired->recovery().latest_group_states.size(), 1U);
  EXPECT_EQ(repaired->recovery().latest_group_states.front(), state(group, 1U, std::byte{0x11U}));
}

TEST(RaftPersistentLogTest, CompleteRecordCorruptionIsNeverTailRepaired) {
  TemporaryDirectory directory;
  const RaftPersistentLogConfig config{.directory_path = directory.path().string()};
  auto log = RaftPersistentLog::create_new(config);
  ASSERT_TRUE(log.has_value());
  ASSERT_TRUE(log->append(state(group_id(std::byte{4U}), 1U, std::byte{0x44U})).has_value());
  ASSERT_TRUE(log->synchronize().has_value());
  ASSERT_TRUE(log->close().is_ok());
  const std::filesystem::path segment = highest_segment(directory.path());
  std::fstream bytes(segment, std::ios::in | std::ios::out | std::ios::binary);
  ASSERT_TRUE(bytes.is_open());
  bytes.seekg(-5, std::ios::end);
  char value{};
  bytes.read(&value, 1);
  value ^= 1;
  bytes.seekp(-5, std::ios::end);
  bytes.write(&value, 1);
  bytes.close();

  auto corrupted = RaftPersistentLog::open_existing(
      config, RaftPersistentLogOpenOptions{.repair_incomplete_final_tail = true});

  ASSERT_FALSE(corrupted.has_value());
  EXPECT_EQ(corrupted.error().code(), common::StatusCode::kCorruption);
}

} // namespace
} // namespace chronos::raft
