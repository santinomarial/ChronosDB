#include "chronos/raft/persistent_log.hpp"
#include "raft/persistent_log_crash_protocol.hpp"
#include "wal/wal_crash_protocol.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <utility>

namespace chronos::raft {
namespace {

class CrashDirectory {
public:
  CrashDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-raft-log-crash-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }

  ~CrashDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  CrashDirectory(const CrashDirectory&) = delete;
  CrashDirectory& operator=(const CrashDirectory&) = delete;

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

[[nodiscard]] GroupId crash_group_id() {
  common::Uuid::Bytes bytes{};
  bytes.fill(std::byte{0xE1U});
  return GroupId{bytes};
}

[[nodiscard]] GroupPersistentState crash_state(const std::uint64_t sequence,
                                               const std::byte value) {
  PersistentState persistent{};
  persistent.current_term = 1U;
  persistent.log.push_back(LogEntry{1U, 1U, 1U, {value}});
  persistent.commit_index = 1U;
  return GroupPersistentState{crash_group_id(), sequence, std::move(persistent)};
}

struct RaftCrashPoint {
  enum class Scenario : std::uint8_t {
    kInitialization,
    kRotation,
    kCheckpoint,
  };

  std::string_view failpoint;
  Scenario scenario;
  bool initial_segment_visible;
  std::uint64_t base_segment;
  std::uint64_t segment_count;
  std::uint64_t record_count;
  std::uint64_t physical_sequence;
  std::byte state_value;
};

constexpr std::array<RaftCrashPoint, 31U> kCrashPoints{
    RaftCrashPoint{test::kAfterInitialLockCreate, RaftCrashPoint::Scenario::kInitialization, false,
                   1U, 1U, 0U, 0U, std::byte{0}},
    RaftCrashPoint{test::kAfterInitialLockDirectorySync, RaftCrashPoint::Scenario::kInitialization,
                   false, 1U, 1U, 0U, 0U, std::byte{0}},
    RaftCrashPoint{test::kAfterInitialHeaderWrite, RaftCrashPoint::Scenario::kInitialization, false,
                   1U, 1U, 0U, 0U, std::byte{0}},
    RaftCrashPoint{test::kAfterInitialFileSync, RaftCrashPoint::Scenario::kInitialization, false,
                   1U, 1U, 0U, 0U, std::byte{0}},
    RaftCrashPoint{test::kAfterInitialRename, RaftCrashPoint::Scenario::kInitialization, true, 1U,
                   1U, 0U, 0U, std::byte{0}},
    RaftCrashPoint{test::kAfterInitialDirectorySync, RaftCrashPoint::Scenario::kInitialization,
                   true, 1U, 1U, 0U, 0U, std::byte{0}},
    RaftCrashPoint{test::kAfterPredecessorDataSync, RaftCrashPoint::Scenario::kRotation, false, 1U,
                   1U, 1U, 1U, std::byte{0xC1U}},
    RaftCrashPoint{test::kAfterPredecessorClose, RaftCrashPoint::Scenario::kRotation, false, 1U, 1U,
                   1U, 1U, std::byte{0xC1U}},
    RaftCrashPoint{test::kAfterSuccessorHeaderWrite, RaftCrashPoint::Scenario::kRotation, false, 1U,
                   1U, 1U, 1U, std::byte{0xC1U}},
    RaftCrashPoint{test::kAfterSuccessorFileSync, RaftCrashPoint::Scenario::kRotation, false, 1U,
                   1U, 1U, 1U, std::byte{0xC1U}},
    RaftCrashPoint{test::kAfterSuccessorRename, RaftCrashPoint::Scenario::kRotation, false, 1U, 2U,
                   1U, 1U, std::byte{0xC1U}},
    RaftCrashPoint{test::kAfterSuccessorDirectorySync, RaftCrashPoint::Scenario::kRotation, false,
                   1U, 2U, 1U, 1U, std::byte{0xC1U}},
    RaftCrashPoint{test::kAfterRotatedRecordWrite, RaftCrashPoint::Scenario::kRotation, false, 1U,
                   2U, 2U, 2U, std::byte{0xC2U}},
    RaftCrashPoint{test::kAfterRotatedRecordDataSync, RaftCrashPoint::Scenario::kRotation, false,
                   1U, 2U, 2U, 2U, std::byte{0xC2U}},
    RaftCrashPoint{test::kAfterPredecessorDataSync, RaftCrashPoint::Scenario::kCheckpoint, false,
                   2U, 1U, 1U, 2U, std::byte{0xC2U}},
    RaftCrashPoint{test::kAfterPredecessorClose, RaftCrashPoint::Scenario::kCheckpoint, false, 2U,
                   1U, 1U, 2U, std::byte{0xC2U}},
    RaftCrashPoint{test::kAfterSuccessorHeaderWrite, RaftCrashPoint::Scenario::kCheckpoint, false,
                   2U, 1U, 1U, 2U, std::byte{0xC2U}},
    RaftCrashPoint{test::kAfterSuccessorFileSync, RaftCrashPoint::Scenario::kCheckpoint, false, 2U,
                   1U, 1U, 2U, std::byte{0xC2U}},
    RaftCrashPoint{test::kAfterSuccessorRename, RaftCrashPoint::Scenario::kCheckpoint, false, 2U,
                   2U, 1U, 2U, std::byte{0xC2U}},
    RaftCrashPoint{test::kAfterSuccessorDirectorySync, RaftCrashPoint::Scenario::kCheckpoint, false,
                   2U, 2U, 1U, 2U, std::byte{0xC2U}},
    RaftCrashPoint{test::kAfterCheckpointRecordWrite, RaftCrashPoint::Scenario::kCheckpoint, false,
                   2U, 2U, 2U, 3U, std::byte{0xC3U}},
    RaftCrashPoint{test::kAfterCheckpointDataSync, RaftCrashPoint::Scenario::kCheckpoint, false, 2U,
                   2U, 2U, 3U, std::byte{0xC3U}},
    RaftCrashPoint{test::kAfterAnchorWrite, RaftCrashPoint::Scenario::kCheckpoint, false, 2U, 2U,
                   2U, 3U, std::byte{0xC3U}},
    RaftCrashPoint{test::kAfterAnchorFileSync, RaftCrashPoint::Scenario::kCheckpoint, false, 2U, 2U,
                   2U, 3U, std::byte{0xC3U}},
    RaftCrashPoint{test::kAfterAnchorRename, RaftCrashPoint::Scenario::kCheckpoint, false, 3U, 1U,
                   1U, 3U, std::byte{0xC3U}},
    RaftCrashPoint{test::kAfterAnchorDirectorySync, RaftCrashPoint::Scenario::kCheckpoint, false,
                   3U, 1U, 1U, 3U, std::byte{0xC3U}},
    RaftCrashPoint{test::kAfterAnchorClose, RaftCrashPoint::Scenario::kCheckpoint, false, 3U, 1U,
                   1U, 3U, std::byte{0xC3U}},
    RaftCrashPoint{test::kAfterObsoleteSegmentUnlink, RaftCrashPoint::Scenario::kCheckpoint, false,
                   3U, 1U, 1U, 3U, std::byte{0xC3U}},
    RaftCrashPoint{test::kAfterObsoleteSegmentDirectorySync, RaftCrashPoint::Scenario::kCheckpoint,
                   false, 3U, 1U, 1U, 3U, std::byte{0xC3U}},
    RaftCrashPoint{test::kAfterObsoleteAnchorUnlink, RaftCrashPoint::Scenario::kCheckpoint, false,
                   3U, 1U, 1U, 3U, std::byte{0xC3U}},
    RaftCrashPoint{test::kAfterObsoleteAnchorDirectorySync, RaftCrashPoint::Scenario::kCheckpoint,
                   false, 3U, 1U, 1U, 3U, std::byte{0xC3U}},
};

void expect_recovery(const RaftPersistentLogRecovery& recovery, const RaftCrashPoint& point) {
  EXPECT_EQ(recovery.base_segment_number, point.base_segment);
  EXPECT_EQ(recovery.segment_count, point.segment_count);
  EXPECT_EQ(recovery.record_count, point.record_count);
  EXPECT_EQ(recovery.written_position.physical_sequence, point.physical_sequence);
  EXPECT_EQ(recovery.durable_physical_sequence, point.physical_sequence);
  if (point.physical_sequence == 0U) {
    EXPECT_TRUE(recovery.latest_group_states.empty());
  } else {
    ASSERT_EQ(recovery.latest_group_states.size(), 1U);
    EXPECT_EQ(recovery.latest_group_states.front(),
              crash_state(point.physical_sequence, point.state_value));
  }
}

class PersistentLogCrashMatrixTest : public ::testing::TestWithParam<RaftCrashPoint> {};

TEST_P(PersistentLogCrashMatrixTest, ReopensOneExactAuthorityAndContinuesAfterSigkill) {
  CrashDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const RaftCrashPoint point = GetParam();
  common::Result<wal::test::CrashChildProcess> spawned = wal::test::CrashChildProcess::spawn(
      {.directory = directory.path(),
       .reopen = point.scenario == RaftCrashPoint::Scenario::kInitialization,
       .reclaim = point.scenario == RaftCrashPoint::Scenario::kCheckpoint,
       .target_segment_size = 300U,
       .pause_after = std::string{point.failpoint}});
  ASSERT_TRUE(spawned.has_value()) << spawned.error().to_string();
  wal::test::CrashChildProcess child = std::move(*spawned);
  auto reached = child.wait_for("FAILPOINT");
  ASSERT_TRUE(reached.has_value()) << reached.error().to_string();
  ASSERT_EQ(reached->fields.size(), 1U);
  EXPECT_EQ(reached->fields.front(), point.failpoint);
  ASSERT_TRUE(child.kill_abruptly().is_ok());

  const RaftPersistentLogConfig config{.directory_path = directory.path().string(),
                                       .target_segment_size = 300U};
  auto opened = RaftPersistentLog::open_existing(config);
  if (point.scenario == RaftCrashPoint::Scenario::kInitialization &&
      !point.initial_segment_visible) {
    ASSERT_FALSE(opened.has_value());
    EXPECT_EQ(opened.error().code(), common::StatusCode::kCorruption);
    opened = RaftPersistentLog::create_new(config);
  }
  ASSERT_TRUE(opened.has_value()) << opened.error().to_string();
  expect_recovery(opened->recovery(), point);
  ASSERT_TRUE(opened->close().is_ok());

  std::size_t segment_files = 0U;
  std::size_t anchor_files = 0U;
  for (const std::filesystem::directory_entry& entry :
       std::filesystem::directory_iterator(directory.path())) {
    const std::string name = entry.path().filename().string();
    EXPECT_FALSE(name.ends_with(".tmp"));
    if (name.ends_with(".rlog"))
      ++segment_files;
    if (name.ends_with(".rbase"))
      ++anchor_files;
  }
  EXPECT_EQ(segment_files, point.segment_count);
  EXPECT_EQ(anchor_files, point.scenario == RaftCrashPoint::Scenario::kCheckpoint ? 1U : 0U);

  auto repeated = RaftPersistentLog::open_existing(config);
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  expect_recovery(repeated->recovery(), point);
  auto appended =
      repeated->append(crash_state(point.physical_sequence + 1U, static_cast<std::byte>(0xD1U)));
  ASSERT_TRUE(appended.has_value()) << appended.error().to_string();
  EXPECT_EQ(appended->physical_sequence, point.physical_sequence + 1U);
  ASSERT_TRUE(repeated->synchronize().has_value());
  EXPECT_TRUE(repeated->close().is_ok());
}

INSTANTIATE_TEST_SUITE_P(EveryPersistentTransition, PersistentLogCrashMatrixTest,
                         ::testing::ValuesIn(kCrashPoints),
                         [](const ::testing::TestParamInfo<RaftCrashPoint>& parameter) {
                           std::string_view prefix = "initialization_";
                           if (parameter.param.scenario == RaftCrashPoint::Scenario::kRotation)
                             prefix = "rotation_";
                           if (parameter.param.scenario == RaftCrashPoint::Scenario::kCheckpoint)
                             prefix = "checkpoint_";
                           return std::string{prefix} + std::string{parameter.param.failpoint};
                         });

} // namespace
} // namespace chronos::raft
