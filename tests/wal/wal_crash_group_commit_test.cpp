#include "wal/wal_crash_test_support.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

namespace chronos::wal {
namespace {

[[nodiscard]] test::CrashChildProcess start_group_child(const test::CrashChildOptions& options) {
  common::Result<test::CrashChildProcess> spawned = test::CrashChildProcess::spawn(options);
  EXPECT_TRUE(spawned.has_value()) << spawned.error().to_string();
  if (!spawned.has_value()) {
    return {};
  }
  test::CrashChildProcess child = std::move(*spawned);
  const common::Result<test::CrashEvent> ready = child.wait_for("READY");
  EXPECT_TRUE(ready.has_value()) << ready.error().to_string();
  return child;
}

void admit(test::CrashChildProcess& child, const std::uint64_t id,
           const std::string_view durability) {
  ASSERT_TRUE(child.send("SUBMIT " + std::to_string(id) + " " + std::string{durability}).is_ok());
  const common::Result<test::CrashEvent> event = child.wait_for("ADMITTED", id);
  ASSERT_TRUE(event.has_value()) << event.error().to_string();
}

TEST(WalCrashGroupCommitTest, OneSynchronizationPublishesACompleteLocalSyncGroup) {
  test::CrashWalDirectory directory{"chronos-wal-group-commit"};
  ASSERT_TRUE(directory.valid());
  test::CrashChildProcess child = start_group_child({
      .directory = directory.path(),
      .maximum_sync_batch_requests = 4U,
      .maximum_sync_batch_delay = std::chrono::seconds{30},
  });
  ASSERT_GT(child.process_id(), 0);

  for (std::uint64_t id = 401U; id <= 404U; ++id) {
    admit(child, id, "LOCAL_SYNC");
  }
  for (std::uint64_t id = 401U; id <= 404U; ++id) {
    const common::Result<test::CrashEvent> completed = child.wait_for("COMPLETED", id);
    ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  }
  ASSERT_TRUE(child.send("SHUTDOWN").is_ok());
  const common::Result<test::CrashEvent> metrics = child.wait_for("METRICS");
  ASSERT_TRUE(metrics.has_value()) << metrics.error().to_string();
  ASSERT_EQ(metrics->fields.size(), 5U);
  EXPECT_EQ(metrics->fields[0], "1");
  EXPECT_EQ(metrics->fields[1], "1");
  EXPECT_EQ(metrics->fields[2], "4");
  EXPECT_EQ(metrics->fields[3], "0");
  EXPECT_EQ(metrics->fields[4], "4");
  ASSERT_TRUE(child.wait_for("SHUTDOWN").has_value());
  const common::Result<int> status = child.wait_for_exit();
  ASSERT_TRUE(status.has_value()) << status.error().to_string();
  ASSERT_TRUE(WIFEXITED(*status));
  EXPECT_EQ(WEXITSTATUS(*status), 0);

  const common::Result<test::CrashRecoveryResult> recovered =
      test::inspect_crash_wal(directory.path());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  ASSERT_EQ(recovered->records.size(), 4U);
  EXPECT_TRUE(test::validate_crash_prefix(recovered->records).is_ok());
}

TEST(WalCrashGroupCommitTest, EveryParentObservedGroupedLocalCompletionSurvivesAbruptExit) {
  test::CrashWalDirectory directory{"chronos-wal-group-kill"};
  ASSERT_TRUE(directory.valid());
  test::CrashChildProcess child = start_group_child({
      .directory = directory.path(),
      .maximum_sync_batch_requests = 3U,
      .maximum_sync_batch_delay = std::chrono::seconds{30},
  });
  ASSERT_GT(child.process_id(), 0);

  for (std::uint64_t id = 411U; id <= 413U; ++id) {
    admit(child, id, "LOCAL_SYNC");
  }
  for (std::uint64_t id = 411U; id <= 413U; ++id) {
    const common::Result<test::CrashEvent> completed = child.wait_for("COMPLETED", id);
    ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  }
  ASSERT_TRUE(child.kill_abruptly().is_ok());

  const common::Result<test::CrashRecoveryResult> recovered =
      test::inspect_crash_wal(directory.path());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_EQ(recovered->records,
            (std::vector<test::RecoveredCrashRecord>{{.sequence = 1U, .request_id = 411U},
                                                      {.sequence = 2U, .request_id = 412U},
                                                      {.sequence = 3U, .request_id = 413U}}));
  EXPECT_TRUE(test::validate_crash_prefix(recovered->records).is_ok());
}

TEST(WalCrashGroupCommitTest, RotationDurabilityReleasesOnlyTheCoveredPriorSegment) {
  test::CrashWalDirectory directory{"chronos-wal-group-rotation"};
  ASSERT_TRUE(directory.valid());
  constexpr std::uint64_t kOneRecordSegmentSize =
      kSegmentHeaderSize + static_cast<std::uint64_t>(72U);
  test::CrashChildProcess child = start_group_child({
      .directory = directory.path(),
      .target_segment_size = kOneRecordSegmentSize,
      .maximum_sync_batch_requests = 8U,
      .maximum_sync_batch_delay = std::chrono::seconds{30},
  });
  ASSERT_GT(child.process_id(), 0);

  admit(child, 421U, "LOCAL_SYNC");
  admit(child, 422U, "LOCAL_SYNC");
  const common::Result<test::CrashEvent> first = child.wait_for("COMPLETED", 421U);
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  EXPECT_FALSE(child.wait_for("COMPLETED", 422U, std::chrono::milliseconds{20}).has_value());
  ASSERT_TRUE(child.send("SHUTDOWN").is_ok());
  ASSERT_TRUE(child.wait_for("COMPLETED", 422U).has_value());
  ASSERT_TRUE(child.wait_for("SHUTDOWN").has_value());
  ASSERT_TRUE(child.wait_for_exit().has_value());

  const common::Result<test::CrashRecoveryResult> recovered =
      test::inspect_crash_wal(directory.path());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_EQ(recovered->report.segment_count, 2U);
  EXPECT_EQ(recovered->records.size(), 2U);
  EXPECT_TRUE(test::validate_crash_prefix(recovered->records).is_ok());
}

} // namespace
} // namespace chronos::wal
