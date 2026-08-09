#include "chronos/common/status.hpp"
#include "chronos/raft/persistent_log.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
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
                                         const std::uint8_t value) {
  PersistentState persistent{};
  persistent.current_term = 1U;
  persistent.log.push_back(LogEntry{1U, 1U, 1U, {std::byte{value}}});
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

TEST(RaftPersistentLogTest, RotatesRecoversLatestGroupStatesAndContinuesSequence) {
  TemporaryDirectory directory;
  const RaftPersistentLogConfig config{.directory_path = directory.path().string(),
                                       .target_segment_size = 300U};
  auto log = RaftPersistentLog::create_new(config);
  ASSERT_TRUE(log.has_value()) << log.error().to_string();
  const GroupId first = group_id(std::byte{1U});
  const GroupId second = group_id(std::byte{2U});
  ASSERT_TRUE(log->append(state(first, 1U, 0x11U)).has_value());
  ASSERT_TRUE(log->append(state(second, 2U, 0x22U)).has_value());
  ASSERT_TRUE(log->append(state(first, 3U, 0x33U)).has_value());
  auto durable = log->synchronize();
  ASSERT_TRUE(durable.has_value()) << durable.error().to_string();
  EXPECT_EQ(durable->physical_sequence, 3U);
  EXPECT_EQ(log->recovery().segment_count, 3U);
  ASSERT_TRUE(log->close().is_ok());

  auto reopened = RaftPersistentLog::open_existing(config);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  ASSERT_EQ(reopened->recovery().latest_group_states.size(), 2U);
  EXPECT_EQ(reopened->recovery().latest_group_states[0], state(first, 3U, 0x33U));
  EXPECT_EQ(reopened->recovery().latest_group_states[1], state(second, 2U, 0x22U));
  EXPECT_EQ(reopened->durable_physical_sequence(), 3U);
  auto appended = reopened->append(state(second, 4U, 0x44U));
  ASSERT_TRUE(appended.has_value()) << appended.error().to_string();
  EXPECT_EQ(appended->physical_sequence, 4U);
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
  ASSERT_TRUE(log->append(state(group, 1U, 0x11U)).has_value());
  ASSERT_TRUE(log->synchronize().has_value());
  ASSERT_TRUE(log->append(state(group, 2U, 0x22U)).has_value());
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
  EXPECT_EQ(repaired->recovery().latest_group_states.front(), state(group, 1U, 0x11U));
}

} // namespace
} // namespace chronos::raft
