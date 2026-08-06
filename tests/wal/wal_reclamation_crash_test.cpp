#include "chronos/wal/wal_paths.hpp"
#include "chronos/wal/wal_writer.hpp"
#include "wal/wal_crash_protocol.hpp"
#include "wal/wal_recovery_test_support.hpp"

#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::wal {
namespace {

struct ReclamationCrashPoint {
  std::string_view failpoint;
  std::uint64_t occurrence{};
  std::uint64_t removed_before_crash{};
};

class WalReclamationCrashTest : public ::testing::TestWithParam<ReclamationCrashPoint> {};

TEST_P(WalReclamationCrashTest, EveryCoveredPrefixSubsetReopensAndConverges) {
  test::TemporaryDirectory directory{"chronos-wal-reclamation-crash"};
  ASSERT_TRUE(directory.valid());
  test::create_wal(directory.path(),
                   {.record_count = 4U, .target_segment_size = kSegmentHeaderSize + 64U});

  const ReclamationCrashPoint point = GetParam();
  common::Result<test::CrashChildProcess> spawned =
      test::CrashChildProcess::spawn({.directory = directory.path(),
                                      .reclaim = true,
                                      .target_segment_size = kSegmentHeaderSize + 72U,
                                      .pause_after = std::string{point.failpoint},
                                      .pause_occurrence = point.occurrence});
  ASSERT_TRUE(spawned.has_value()) << spawned.error().to_string();
  test::CrashChildProcess child = std::move(*spawned);
  ASSERT_TRUE(child.wait_for("READY").has_value());
  ASSERT_TRUE(child.send("RECLAIM 3 3 128").is_ok());
  const common::Result<test::CrashEvent> reached = child.wait_for("FAILPOINT");
  ASSERT_TRUE(reached.has_value()) << reached.error().to_string();
  ASSERT_GE(reached->fields.size(), 2U);
  EXPECT_EQ(reached->fields.front(), point.failpoint);
  EXPECT_EQ(reached->fields[1], std::to_string(point.occurrence));
  ASSERT_TRUE(child.kill_abruptly().is_ok());

  for (std::uint64_t segment = 1U; segment <= 3U; ++segment) {
    const std::string name = wal_segment_file_name(segment).value();
    EXPECT_EQ(std::filesystem::exists(directory.path() / name),
              segment > point.removed_before_crash);
  }
  EXPECT_TRUE(std::filesystem::exists(directory.path() / wal_segment_file_name(4U).value()));

  const WalReplayCheckpoint checkpoint{.wal_id = test::make_wal_id(),
                                       .record_sequence = 3U,
                                       .segment_number = 3U,
                                       .byte_offset = 128U};
  const WalWriterConfig writer_config{.directory_path = directory.path().string(),
                                      .target_segment_size = kSegmentHeaderSize + 72U,
                                      .maximum_application_payload = test::kCrashPayloadSize};
  test::CollectingReplaySink suffix;
  common::Result<WalWriter> reopened =
      WalWriter::open_existing_from_checkpoint(writer_config, {}, checkpoint, suffix);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_EQ(suffix.replay_sequences, (std::vector<std::uint64_t>{4U}));
  const common::Result<WalSegmentReclamationReport> converged =
      reopened->reclaim_checkpointed_segments(checkpoint);
  ASSERT_TRUE(converged.has_value()) << converged.error().to_string();
  EXPECT_EQ(converged->removed_segment_count, 3U - point.removed_before_crash);
  EXPECT_EQ(converged->directory_sync_count, point.removed_before_crash == 3U ? 0U : 1U);
  EXPECT_TRUE(reopened->close().is_ok());

  test::CollectingReplaySink repeated_suffix;
  reopened =
      WalWriter::open_existing_from_checkpoint(writer_config, {}, checkpoint, repeated_suffix);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_EQ(repeated_suffix.replay_sequences, (std::vector<std::uint64_t>{4U}));
  const common::Result<WalSegmentReclamationReport> repeated =
      reopened->reclaim_checkpointed_segments(checkpoint);
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  EXPECT_EQ(repeated->removed_segment_count, 0U);
  EXPECT_EQ(repeated->directory_sync_count, 0U);
  EXPECT_TRUE(reopened->close().is_ok());
}

INSTANTIATE_TEST_SUITE_P(
    EveryReclamationDurabilityBoundary, WalReclamationCrashTest,
    ::testing::Values(ReclamationCrashPoint{.failpoint = test::kAfterReclamationRemove,
                                            .occurrence = 1U,
                                            .removed_before_crash = 1U},
                      ReclamationCrashPoint{.failpoint = test::kAfterReclamationRemove,
                                            .occurrence = 2U,
                                            .removed_before_crash = 2U},
                      ReclamationCrashPoint{.failpoint = test::kAfterReclamationRemove,
                                            .occurrence = 3U,
                                            .removed_before_crash = 3U},
                      ReclamationCrashPoint{.failpoint = test::kAfterReclamationDirectorySync,
                                            .occurrence = 1U,
                                            .removed_before_crash = 3U}),
    [](const ::testing::TestParamInfo<ReclamationCrashPoint>& parameter) {
      return std::string{parameter.param.failpoint} + "_" +
             std::to_string(parameter.param.occurrence);
    });

} // namespace
} // namespace chronos::wal
