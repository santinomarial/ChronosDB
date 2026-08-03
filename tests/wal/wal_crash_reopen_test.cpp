#include "wal/wal_crash_test_support.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <string>
#include <utility>

namespace chronos::wal {
namespace {

[[nodiscard]] test::CrashChildProcess ready_process(const test::CrashChildOptions& options) {
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

[[nodiscard]] common::Result<test::CrashEvent>
submit_local(test::CrashChildProcess& child, const std::uint64_t id) {
  const common::Status sent = child.send("SUBMIT " + std::to_string(id) + " LOCAL_SYNC");
  if (!sent.is_ok()) {
    return common::make_unexpected(sent);
  }
  common::Result<test::CrashEvent> admitted = child.wait_for("ADMITTED", id);
  if (!admitted.has_value()) {
    return admitted;
  }
  return child.wait_for("COMPLETED", id);
}

void stop_process(test::CrashChildProcess& child) {
  ASSERT_TRUE(child.send("SHUTDOWN").is_ok());
  ASSERT_TRUE(child.wait_for("SHUTDOWN").has_value());
  const common::Result<int> status = child.wait_for_exit();
  ASSERT_TRUE(status.has_value()) << status.error().to_string();
  ASSERT_TRUE(WIFEXITED(*status));
  EXPECT_EQ(WEXITSTATUS(*status), 0);
}

TEST(WalCrashReopenTest, ReopenAfterAcknowledgedCrashContinuesAtExactNextSequenceAndRotates) {
  test::CrashWalDirectory directory{"chronos-wal-reopen-after-crash"};
  ASSERT_TRUE(directory.valid());
  constexpr std::uint64_t kOneRecordSegmentSize =
      kSegmentHeaderSize + static_cast<std::uint64_t>(72U);
  test::CrashChildProcess creator = ready_process({
      .directory = directory.path(),
      .target_segment_size = kOneRecordSegmentSize,
      .maximum_sync_batch_requests = 1U,
  });
  ASSERT_GT(creator.process_id(), 0);
  ASSERT_TRUE(submit_local(creator, 601U).has_value());
  ASSERT_TRUE(creator.kill_abruptly().is_ok());

  test::CrashChildProcess reopened = ready_process({
      .directory = directory.path(),
      .reopen = true,
      .target_segment_size = kOneRecordSegmentSize,
      .maximum_sync_batch_requests = 1U,
  });
  ASSERT_GT(reopened.process_id(), 0);
  const common::Result<test::CrashEvent> completed = submit_local(reopened, 602U);
  ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  ASSERT_GE(completed->fields.size(), 4U);
  EXPECT_EQ(completed->fields[2], "2");
  stop_process(reopened);

  const common::Result<test::CrashRecoveryResult> recovered =
      test::inspect_crash_wal(directory.path());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_EQ(recovered->report.segment_count, 2U);
  EXPECT_EQ(recovered->records,
            (std::vector<test::RecoveredCrashRecord>{{.sequence = 1U, .request_id = 601U},
                                                      {.sequence = 2U, .request_id = 602U}}));
  EXPECT_TRUE(test::validate_crash_prefix(recovered->records).is_ok());
}

TEST(WalCrashReopenTest, ReopenAfterTailRepairAppendsAfterTheVerifiedPrefix) {
  test::CrashWalDirectory directory{"chronos-wal-reopen-after-repair"};
  ASSERT_TRUE(directory.valid());
  test::CrashChildProcess creator = ready_process(
      {.directory = directory.path(), .maximum_sync_batch_requests = 1U});
  ASSERT_GT(creator.process_id(), 0);
  ASSERT_TRUE(submit_local(creator, 611U).has_value());
  stop_process(creator);

  test::CrashChildProcess torn = ready_process({
      .directory = directory.path(),
      .reopen = true,
      .pause_after = std::string{test::kAfterShortRecordWrite},
      .short_record_prefix = 12U,
  });
  ASSERT_GT(torn.process_id(), 0);
  ASSERT_TRUE(torn.send("SUBMIT 612 ASYNC").is_ok());
  ASSERT_TRUE(torn.wait_for("ADMITTED", 612U).has_value());
  ASSERT_TRUE(torn.wait_for("FAILPOINT").has_value());
  ASSERT_TRUE(torn.kill_abruptly().is_ok());

  const common::Result<test::CrashRecoveryResult> repaired =
      test::recover_crash_wal(directory.path(), true);
  ASSERT_TRUE(repaired.has_value()) << repaired.error().to_string();
  ASSERT_TRUE(repaired->report.repaired);

  test::CrashChildProcess reopened = ready_process({
      .directory = directory.path(), .reopen = true, .maximum_sync_batch_requests = 1U});
  ASSERT_GT(reopened.process_id(), 0);
  const common::Result<test::CrashEvent> completed = submit_local(reopened, 613U);
  ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  ASSERT_GE(completed->fields.size(), 4U);
  EXPECT_EQ(completed->fields[2], "2");
  stop_process(reopened);

  const common::Result<test::CrashRecoveryResult> recovered =
      test::inspect_crash_wal(directory.path());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_EQ(recovered->records,
            (std::vector<test::RecoveredCrashRecord>{{.sequence = 1U, .request_id = 611U},
                                                      {.sequence = 2U, .request_id = 613U}}));
}

} // namespace
} // namespace chronos::wal
