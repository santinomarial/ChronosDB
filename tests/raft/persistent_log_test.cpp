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
