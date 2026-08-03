#include "wal/wal_crash_test_support.hpp"

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

namespace chronos::wal {
namespace {

class WalInstallationCrashMatrixTest : public ::testing::TestWithParam<std::string_view> {};

TEST_P(WalInstallationCrashMatrixTest, LeavesOnlyAContractPermittedDiscoverableState) {
  test::CrashWalDirectory directory{"chronos-wal-install-crash"};
  ASSERT_TRUE(directory.valid());
  const std::string_view failpoint = GetParam();
  common::Result<test::CrashChildProcess> spawned = test::CrashChildProcess::spawn(
      {.directory = directory.path(), .pause_after = std::string{failpoint}});
  ASSERT_TRUE(spawned.has_value()) << spawned.error().to_string();
  test::CrashChildProcess child = std::move(*spawned);

  const common::Result<test::CrashEvent> reached = child.wait_for("FAILPOINT");
  ASSERT_TRUE(reached.has_value()) << reached.error().to_string();
  ASSERT_GE(reached->fields.size(), 2U);
  EXPECT_EQ(reached->fields.front(), failpoint);
  ASSERT_TRUE(child.kill_abruptly().is_ok());

  const common::Result<WalRecoveryReport> scanned = scan_wal(directory.path().string());
  if (failpoint == test::kAfterSegmentHeaderWrite || failpoint == test::kAfterSegmentFileSync) {
    ASSERT_FALSE(scanned.has_value());
    EXPECT_EQ(scanned.error().code(), common::StatusCode::kNotFound);
    return;
  }
  ASSERT_TRUE(scanned.has_value()) << scanned.error().to_string();
  EXPECT_EQ(scanned->classification, WalScanClassification::kClean);
  EXPECT_EQ(scanned->segment_count, 1U);
  EXPECT_EQ(scanned->record_count, 0U);
  EXPECT_EQ(scanned->valid_end.byte_offset, kSegmentHeaderSize);
}

INSTANTIATE_TEST_SUITE_P(
    SegmentLifecycle, WalInstallationCrashMatrixTest,
    ::testing::Values(test::kAfterSegmentHeaderWrite, test::kAfterSegmentFileSync,
                      test::kAfterSegmentRename, test::kAfterSegmentDirectorySync));

[[nodiscard]] test::CrashChildProcess ready_child(const test::CrashChildOptions& options) {
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

void submit_request(test::CrashChildProcess& child, const std::uint64_t request_id,
                    const std::string_view mode) {
  ASSERT_TRUE(child.send("SUBMIT " + std::to_string(request_id) + " " + std::string{mode}).is_ok());
  const common::Result<test::CrashEvent> admitted = child.wait_for("ADMITTED", request_id);
  ASSERT_TRUE(admitted.has_value()) << admitted.error().to_string();
}

TEST(WalCrashMatrixTest, ParentObservedLocalSyncCompletionSurvivesSigkillExactlyOnce) {
  test::CrashWalDirectory directory{"chronos-wal-local-ack-crash"};
  ASSERT_TRUE(directory.valid());
  test::CrashChildProcess child = ready_child(
      {.directory = directory.path(), .maximum_sync_batch_requests = 1U});
  ASSERT_GT(child.process_id(), 0);

  submit_request(child, 301U, "LOCAL_SYNC");
  const common::Result<test::CrashEvent> completed = child.wait_for("COMPLETED", 301U);
  ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  ASSERT_TRUE(child.kill_abruptly().is_ok());

  const common::Result<test::CrashRecoveryResult> recovered =
      test::inspect_crash_wal(directory.path());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_TRUE(test::validate_crash_prefix(recovered->records).is_ok());
  EXPECT_EQ(recovered->records,
            (std::vector<test::RecoveredCrashRecord>{{.sequence = 1U, .request_id = 301U}}));
}

TEST(WalCrashMatrixTest, SynchronizedButUnpublishedRequestMayRecover) {
  test::CrashWalDirectory directory{"chronos-wal-durable-unacknowledged"};
  ASSERT_TRUE(directory.valid());
  test::CrashChildProcess child = ready_child({.directory = directory.path(),
                                                .maximum_sync_batch_requests = 1U,
                                                .pause_after =
                                                    std::string{test::kAfterDataSync}});
  ASSERT_GT(child.process_id(), 0);

  submit_request(child, 302U, "LOCAL_SYNC");
  const common::Result<test::CrashEvent> reached = child.wait_for("FAILPOINT");
  ASSERT_TRUE(reached.has_value()) << reached.error().to_string();
  EXPECT_FALSE(child.wait_for("COMPLETED", 302U, std::chrono::milliseconds{20}).has_value());
  ASSERT_TRUE(child.kill_abruptly().is_ok());

  const common::Result<test::CrashRecoveryResult> recovered =
      test::inspect_crash_wal(directory.path());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_EQ(recovered->records,
            (std::vector<test::RecoveredCrashRecord>{{.sequence = 1U, .request_id = 302U}}));
}

TEST(WalCrashMatrixTest, CompleteWriteBeforePublicationMayRecoverWithoutDuplication) {
  test::CrashWalDirectory directory{"chronos-wal-written-unacknowledged"};
  ASSERT_TRUE(directory.valid());
  test::CrashChildProcess child = ready_child(
      {.directory = directory.path(), .pause_after = std::string{test::kAfterRecordWrite}});
  ASSERT_GT(child.process_id(), 0);

  submit_request(child, 303U, "ASYNC");
  const common::Result<test::CrashEvent> reached = child.wait_for("FAILPOINT");
  ASSERT_TRUE(reached.has_value()) << reached.error().to_string();
  ASSERT_TRUE(child.kill_abruptly().is_ok());

  const common::Result<test::CrashRecoveryResult> recovered =
      test::inspect_crash_wal(directory.path());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_TRUE(test::validate_crash_prefix(recovered->records).is_ok());
  EXPECT_EQ(recovered->records,
            (std::vector<test::RecoveredCrashRecord>{{.sequence = 1U, .request_id = 303U}}));
}

} // namespace
} // namespace chronos::wal
