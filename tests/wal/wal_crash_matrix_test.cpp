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

struct RotationCrashPoint {
  std::string_view failpoint;
  std::uint64_t occurrence;
  std::uint64_t expected_segments;
  std::size_t expected_records;
};

class WalRotationCrashMatrixTest : public ::testing::TestWithParam<RotationCrashPoint> {};

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

void create_one_durable_record(const std::filesystem::path& directory,
                               const std::uint64_t target_segment_size) {
  test::CrashChildProcess child = ready_child({.directory = directory,
                                                .target_segment_size = target_segment_size,
                                                .maximum_sync_batch_requests = 1U});
  ASSERT_GT(child.process_id(), 0);
  submit_request(child, 320U, "LOCAL_SYNC");
  ASSERT_TRUE(child.wait_for("COMPLETED", 320U).has_value());
  ASSERT_TRUE(child.send("SHUTDOWN").is_ok());
  ASSERT_TRUE(child.wait_for("SHUTDOWN").has_value());
  ASSERT_TRUE(child.wait_for_exit().has_value());
}

TEST_P(WalRotationCrashMatrixTest, RecoversTheExactNamedPrefixAtEverySuccessorBoundary) {
  test::CrashWalDirectory directory{"chronos-wal-rotation-crash"};
  ASSERT_TRUE(directory.valid());
  constexpr std::uint64_t kOneRecordSegmentSize =
      kSegmentHeaderSize + static_cast<std::uint64_t>(72U);
  create_one_durable_record(directory.path(), kOneRecordSegmentSize);

  const RotationCrashPoint point = GetParam();
  test::CrashChildProcess child = ready_child({
      .directory = directory.path(),
      .reopen = true,
      .target_segment_size = kOneRecordSegmentSize,
      .pause_after = std::string{point.failpoint},
      .pause_occurrence = point.occurrence,
  });
  ASSERT_GT(child.process_id(), 0);
  submit_request(child, 321U, "ASYNC");
  const common::Result<test::CrashEvent> reached = child.wait_for("FAILPOINT");
  ASSERT_TRUE(reached.has_value()) << reached.error().to_string();
  ASSERT_TRUE(child.kill_abruptly().is_ok());

  const common::Result<test::CrashRecoveryResult> inspected =
      test::inspect_crash_wal(directory.path());
  ASSERT_TRUE(inspected.has_value()) << inspected.error().to_string();
  EXPECT_EQ(inspected->report.segment_count, point.expected_segments);
  EXPECT_EQ(inspected->records.size(), point.expected_records);
  EXPECT_TRUE(test::validate_crash_prefix(inspected->records).is_ok());
  ASSERT_FALSE(inspected->records.empty());
  EXPECT_EQ(inspected->records.front().request_id, 320U);
  if (point.expected_records == 2U) {
    EXPECT_EQ(inspected->records.back().request_id, 321U);
  }

  const common::Result<test::CrashRecoveryResult> recovered =
      test::recover_crash_wal(directory.path(), false);
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_EQ(recovered->records, inspected->records);
  EXPECT_EQ(recovered->report.temporary_files_removed,
            inspected->report.temporary_file_count);
}

INSTANTIATE_TEST_SUITE_P(
    SuccessorLifecycle, WalRotationCrashMatrixTest,
    ::testing::Values(
        RotationCrashPoint{.failpoint = test::kAfterDataSync,
                           .occurrence = 1U,
                           .expected_segments = 1U,
                           .expected_records = 1U},
        RotationCrashPoint{.failpoint = test::kAfterSegmentHeaderWrite,
                           .occurrence = 1U,
                           .expected_segments = 1U,
                           .expected_records = 1U},
        RotationCrashPoint{.failpoint = test::kAfterSegmentFileSync,
                           .occurrence = 2U,
                           .expected_segments = 1U,
                           .expected_records = 1U},
        RotationCrashPoint{.failpoint = test::kAfterSegmentRename,
                           .occurrence = 1U,
                           .expected_segments = 2U,
                           .expected_records = 1U},
        RotationCrashPoint{.failpoint = test::kAfterSegmentDirectorySync,
                           .occurrence = 2U,
                           .expected_segments = 2U,
                           .expected_records = 1U},
        RotationCrashPoint{.failpoint = test::kAfterRecordWrite,
                           .occurrence = 1U,
                           .expected_segments = 2U,
                           .expected_records = 2U}));

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
