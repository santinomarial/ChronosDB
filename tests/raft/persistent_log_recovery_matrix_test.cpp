#include "chronos/raft/multiplexed_log.hpp"
#include "chronos/raft/persistent_log.hpp"

#include <array>
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

constexpr std::uint64_t kMatrixRecordSize = 213U;

class RecoveryMatrixDirectory {
public:
  RecoveryMatrixDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-raft-recovery-matrix-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }

  ~RecoveryMatrixDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  RecoveryMatrixDirectory(const RecoveryMatrixDirectory&) = delete;
  RecoveryMatrixDirectory& operator=(const RecoveryMatrixDirectory&) = delete;

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

struct RecoveryMatrixCase {
  std::uint64_t target_segment_size;
  std::uint64_t group_count;
};

struct CheckpointLayoutInput {
  std::uint64_t base_segment;
  std::uint64_t first_sequence;
  std::uint64_t group_count;
  std::uint64_t target_segment_size;
};

struct ExpectedLayout {
  std::uint64_t base_segment{1U};
  std::uint64_t active_segment{1U};
  std::uint64_t segment_count{1U};
  std::uint64_t active_end{kRaftSegmentHeaderSize};
  std::uint64_t record_count{};
  std::uint64_t physical_sequence{};

  void append(const std::uint64_t target_segment_size) {
    if (active_end > target_segment_size - kMatrixRecordSize) {
      ++active_segment;
      ++segment_count;
      active_end = kRaftSegmentHeaderSize;
    }
    active_end += kMatrixRecordSize;
    ++record_count;
    ++physical_sequence;
  }

  [[nodiscard]] static ExpectedLayout checkpoint(const CheckpointLayoutInput& input) {
    ExpectedLayout layout{.base_segment = input.base_segment,
                          .active_segment = input.base_segment,
                          .physical_sequence = input.first_sequence - 1U};
    for (std::uint64_t group = 0U; group < input.group_count; ++group)
      layout.append(input.target_segment_size);
    return layout;
  }
};

[[nodiscard]] GroupId matrix_group_id(const std::uint64_t ordinal) {
  common::Uuid::Bytes bytes{};
  bytes.fill(static_cast<std::byte>(ordinal));
  return GroupId{bytes};
}

[[nodiscard]] GroupPersistentState matrix_state(const std::uint64_t group_ordinal,
                                                const std::uint64_t physical_sequence,
                                                const std::byte value) {
  PersistentState persistent{};
  persistent.current_term = 1U;
  persistent.log.push_back(LogEntry{1U, 1U, 1U, {value}});
  persistent.commit_index = 1U;
  return GroupPersistentState{matrix_group_id(group_ordinal), physical_sequence,
                              std::move(persistent)};
}

[[nodiscard]] std::byte sequence_value(const std::uint64_t sequence) {
  return static_cast<std::byte>(0x40U + sequence);
}

void expect_recovery(const RaftPersistentLogRecovery& recovery, const ExpectedLayout& layout,
                     const std::vector<GroupPersistentState>& latest) {
  EXPECT_EQ(recovery.base_segment_number, layout.base_segment);
  EXPECT_EQ(recovery.written_position.segment_number, layout.active_segment);
  EXPECT_EQ(recovery.written_position.end_offset, layout.active_end);
  EXPECT_EQ(recovery.written_position.physical_sequence, layout.physical_sequence);
  EXPECT_EQ(recovery.durable_physical_sequence, layout.physical_sequence);
  EXPECT_EQ(recovery.segment_count, layout.segment_count);
  EXPECT_EQ(recovery.record_count, layout.record_count);
  EXPECT_EQ(recovery.repaired_bytes, 0U);
  EXPECT_EQ(recovery.latest_group_states, latest);
}

class RaftPersistentLogRecoveryMatrixTest : public ::testing::TestWithParam<RecoveryMatrixCase> {};

TEST_P(RaftPersistentLogRecoveryMatrixTest,
       RecoversExactLayoutAndLatestStatesAcrossCheckpointAndContinuation) {
  const RecoveryMatrixCase parameter = GetParam();
  RecoveryMatrixDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const RaftPersistentLogConfig config{
      .directory_path = directory.path().string(),
      .target_segment_size = parameter.target_segment_size,
  };
  const auto exemplar = encode_multiplexed_log_record_v1(matrix_state(1U, 1U, sequence_value(1U)));
  ASSERT_TRUE(exemplar.has_value()) << exemplar.error().to_string();
  ASSERT_EQ(exemplar->size(), kMatrixRecordSize);

  auto log = RaftPersistentLog::create_new(config);
  ASSERT_TRUE(log.has_value()) << log.error().to_string();
  ExpectedLayout layout;
  std::vector<GroupPersistentState> latest(parameter.group_count);
  const std::uint64_t initial_record_count = parameter.group_count * 3U;
  for (std::uint64_t sequence = 1U; sequence <= initial_record_count; ++sequence) {
    const std::uint64_t group = (sequence - 1U) % parameter.group_count + 1U;
    GroupPersistentState persistent = matrix_state(group, sequence, sequence_value(sequence));
    auto appended = log->append(persistent);
    ASSERT_TRUE(appended.has_value()) << appended.error().to_string();
    layout.append(parameter.target_segment_size);
    EXPECT_EQ(*appended, (RaftPhysicalPosition{layout.active_segment, layout.active_end,
                                               layout.physical_sequence}));
    latest[group - 1U] = std::move(persistent);
  }
  ASSERT_TRUE(log->synchronize().has_value());
  ASSERT_TRUE(log->close().is_ok());

  auto reopened = RaftPersistentLog::open_existing(config);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  expect_recovery(reopened->recovery(), layout, latest);

  const std::uint64_t continuation_sequence = layout.physical_sequence + 1U;
  GroupPersistentState continuation =
      matrix_state(1U, continuation_sequence, sequence_value(continuation_sequence));
  ASSERT_TRUE(reopened->append(continuation).has_value());
  layout.append(parameter.target_segment_size);
  latest[0] = continuation;
  ASSERT_TRUE(reopened->synchronize().has_value());
  ASSERT_TRUE(reopened->close().is_ok());

  reopened = RaftPersistentLog::open_existing(config);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  expect_recovery(reopened->recovery(), layout, latest);

  const std::uint64_t old_segment_count = layout.segment_count;
  const std::uint64_t old_record_count = layout.record_count;
  const std::uint64_t checkpoint_base = layout.active_segment + 1U;
  const std::uint64_t checkpoint_first_sequence = layout.physical_sequence + 1U;
  std::vector<GroupPersistentState> checkpoint;
  checkpoint.reserve(parameter.group_count);
  for (std::uint64_t group = 1U; group <= parameter.group_count; ++group) {
    const std::uint64_t sequence = checkpoint_first_sequence + group - 1U;
    checkpoint.push_back(matrix_state(group, sequence, sequence_value(sequence)));
  }
  ExpectedLayout checkpoint_layout = ExpectedLayout::checkpoint({
      .base_segment = checkpoint_base,
      .first_sequence = checkpoint_first_sequence,
      .group_count = parameter.group_count,
      .target_segment_size = parameter.target_segment_size,
  });
  auto reclaimed = reopened->checkpoint_and_reclaim(checkpoint);
  ASSERT_TRUE(reclaimed.has_value()) << reclaimed.error().to_string();
  EXPECT_EQ(reclaimed->base_segment_number, checkpoint_base);
  EXPECT_EQ(reclaimed->checkpoint_first_physical_sequence, checkpoint_first_sequence);
  EXPECT_EQ(reclaimed->checkpoint_last_physical_sequence, checkpoint_layout.physical_sequence);
  EXPECT_EQ(reclaimed->reclaimed_segments, old_segment_count);
  EXPECT_EQ(reclaimed->reclaimed_records, old_record_count);
  ASSERT_TRUE(reopened->close().is_ok());

  reopened = RaftPersistentLog::open_existing(config);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  expect_recovery(reopened->recovery(), checkpoint_layout, checkpoint);

  const std::uint64_t tail_sequence = checkpoint_layout.physical_sequence + 1U;
  GroupPersistentState tail =
      matrix_state(parameter.group_count, tail_sequence, sequence_value(tail_sequence));
  ASSERT_TRUE(reopened->append(tail).has_value());
  checkpoint_layout.append(parameter.target_segment_size);
  checkpoint.back() = tail;
  ASSERT_TRUE(reopened->synchronize().has_value());
  ASSERT_TRUE(reopened->close().is_ok());

  auto final = RaftPersistentLog::open_existing(config);
  ASSERT_TRUE(final.has_value()) << final.error().to_string();
  expect_recovery(final->recovery(), checkpoint_layout, checkpoint);
  EXPECT_TRUE(final->close().is_ok());
}

constexpr std::array kTargetSegmentSizes{277U, 278U, 489U, 490U, 491U, 702U, 703U, 1024U};
constexpr std::array kGroupCounts{1U, 3U, 8U};

[[nodiscard]] consteval std::array<RecoveryMatrixCase, 24U> recovery_matrix_cases() {
  std::array<RecoveryMatrixCase, 24U> cases{};
  std::size_t index = 0U;
  for (const std::uint64_t target : kTargetSegmentSizes) {
    for (const std::uint64_t groups : kGroupCounts)
      cases[index++] = RecoveryMatrixCase{target, groups};
  }
  return cases;
}

constexpr auto kRecoveryMatrixCases = recovery_matrix_cases();

INSTANTIATE_TEST_SUITE_P(TargetAndGroupCrossProduct, RaftPersistentLogRecoveryMatrixTest,
                         ::testing::ValuesIn(kRecoveryMatrixCases),
                         [](const ::testing::TestParamInfo<RecoveryMatrixCase>& parameter) {
                           return "target_" + std::to_string(parameter.param.target_segment_size) +
                                  "_groups_" + std::to_string(parameter.param.group_count);
                         });

} // namespace
} // namespace chronos::raft
