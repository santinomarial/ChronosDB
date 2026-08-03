#include "wal/wal_crash_test_support.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <string>
#include <utility>

namespace chronos::wal {
namespace {

[[nodiscard]] test::CrashChildProcess launch_ready(const test::CrashChildOptions& options) {
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

void durable_submit(test::CrashChildProcess& child, const std::uint64_t request_id) {
  ASSERT_TRUE(child.send("SUBMIT " + std::to_string(request_id) + " LOCAL_SYNC").is_ok());
  ASSERT_TRUE(child.wait_for("ADMITTED", request_id).has_value());
  ASSERT_TRUE(child.wait_for("COMPLETED", request_id).has_value());
}

void shutdown_cleanly(test::CrashChildProcess& child) {
  ASSERT_TRUE(child.send("SHUTDOWN").is_ok());
  ASSERT_TRUE(child.wait_for("SHUTDOWN").has_value());
  const common::Result<int> status = child.wait_for_exit();
  ASSERT_TRUE(status.has_value()) << status.error().to_string();
  ASSERT_TRUE(WIFEXITED(*status));
  EXPECT_EQ(WEXITSTATUS(*status), 0);
}

TEST(WalRecoveryIdempotenceTest, RepeatedCleanRecoveryPreservesBytesReportAndReplay) {
  test::CrashWalDirectory directory{"chronos-wal-recovery-repeat"};
  ASSERT_TRUE(directory.valid());
  test::CrashChildProcess child = launch_ready(
      {.directory = directory.path(), .maximum_sync_batch_requests = 1U});
  ASSERT_GT(child.process_id(), 0);
  durable_submit(child, 501U);
  durable_submit(child, 502U);
  shutdown_cleanly(child);

  const std::vector<test::CrashFileImage> original = test::snapshot_crash_wal(directory.path());
  const common::Result<test::CrashRecoveryResult> first =
      test::recover_crash_wal(directory.path(), false);
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  const std::vector<test::CrashFileImage> after_first = test::snapshot_crash_wal(directory.path());
  const common::Result<test::CrashRecoveryResult> second =
      test::recover_crash_wal(directory.path(), false);
  ASSERT_TRUE(second.has_value()) << second.error().to_string();

  EXPECT_EQ(first->report, second->report);
  EXPECT_EQ(first->records, second->records);
  EXPECT_EQ(original, after_first);
  EXPECT_EQ(after_first, test::snapshot_crash_wal(directory.path()));
  EXPECT_TRUE(test::validate_crash_prefix(second->records).is_ok());
}

TEST(WalRecoveryIdempotenceTest, ExplicitFinalTailRepairIsSynchronizedAndConvergent) {
  test::CrashWalDirectory directory{"chronos-wal-tail-crash-repeat"};
  ASSERT_TRUE(directory.valid());
  test::CrashChildProcess creator = launch_ready(
      {.directory = directory.path(), .maximum_sync_batch_requests = 1U});
  ASSERT_GT(creator.process_id(), 0);
  durable_submit(creator, 511U);
  shutdown_cleanly(creator);

  test::CrashChildProcess appender = launch_ready({
      .directory = directory.path(),
      .reopen = true,
      .pause_after = std::string{test::kAfterShortRecordWrite},
      .short_record_prefix = 20U,
  });
  ASSERT_GT(appender.process_id(), 0);
  ASSERT_TRUE(appender.send("SUBMIT 512 ASYNC").is_ok());
  ASSERT_TRUE(appender.wait_for("ADMITTED", 512U).has_value());
  ASSERT_TRUE(appender.wait_for("FAILPOINT").has_value());
  ASSERT_TRUE(appender.kill_abruptly().is_ok());

  const common::Result<WalRecoveryReport> incomplete = scan_wal(directory.path().string());
  ASSERT_TRUE(incomplete.has_value()) << incomplete.error().to_string();
  ASSERT_EQ(incomplete->classification, WalScanClassification::kIncompleteFinalTail);
  ASSERT_EQ(incomplete->last_record_sequence, 1U);
  const std::vector<test::CrashFileImage> before_repair =
      test::snapshot_crash_wal(directory.path());

  const common::Result<test::CrashRecoveryResult> unauthorized =
      test::recover_crash_wal(directory.path(), false);
  ASSERT_FALSE(unauthorized.has_value());
  EXPECT_EQ(unauthorized.error().code(), common::StatusCode::kOutOfRange);
  EXPECT_EQ(test::snapshot_crash_wal(directory.path()), before_repair);

  const common::Result<test::CrashRecoveryResult> repaired =
      test::recover_crash_wal(directory.path(), true);
  ASSERT_TRUE(repaired.has_value()) << repaired.error().to_string();
  EXPECT_TRUE(repaired->report.repaired);
  EXPECT_EQ(repaired->records,
            (std::vector<test::RecoveredCrashRecord>{{.sequence = 1U, .request_id = 511U}}));
  const std::vector<test::CrashFileImage> repaired_image =
      test::snapshot_crash_wal(directory.path());

  const common::Result<test::CrashRecoveryResult> repeated =
      test::recover_crash_wal(directory.path(), true);
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  EXPECT_FALSE(repeated->report.repaired);
  EXPECT_EQ(repeated->records, repaired->records);
  EXPECT_EQ(test::snapshot_crash_wal(directory.path()), repaired_image);
}

} // namespace
} // namespace chronos::wal
