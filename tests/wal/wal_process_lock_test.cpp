#include "wal/wal_crash_test_support.hpp"

#include <gtest/gtest.h>
#include <string>
#include <sys/wait.h>
#include <utility>

namespace chronos::wal {
namespace {

TEST(WalProcessLockTest, LiveChildExcludesRecoveryAndAnotherWriterUntilAbruptExit) {
  test::CrashWalDirectory directory{"chronos-wal-process-lock"};
  ASSERT_TRUE(directory.valid());
  common::Result<test::CrashChildProcess> owner_result =
      test::CrashChildProcess::spawn({.directory = directory.path()});
  ASSERT_TRUE(owner_result.has_value()) << owner_result.error().to_string();
  test::CrashChildProcess owner = std::move(*owner_result);
  ASSERT_TRUE(owner.wait_for("READY").has_value());

  const common::Result<WalRecoveryReport> locked_scan = scan_wal(directory.path().string());
  ASSERT_FALSE(locked_scan.has_value());
  EXPECT_EQ(locked_scan.error().code(), common::StatusCode::kUnavailable);

  common::Result<test::CrashChildProcess> contender_result =
      test::CrashChildProcess::spawn({.directory = directory.path(), .reopen = true});
  ASSERT_TRUE(contender_result.has_value()) << contender_result.error().to_string();
  test::CrashChildProcess contender = std::move(*contender_result);
  const common::Result<test::CrashEvent> error = contender.wait_for("ERROR");
  ASSERT_TRUE(error.has_value()) << error.error().to_string();
  const common::Result<int> contender_status = contender.wait_for_exit();
  ASSERT_TRUE(contender_status.has_value()) << contender_status.error().to_string();
  ASSERT_TRUE(WIFEXITED(*contender_status));
  EXPECT_EQ(WEXITSTATUS(*contender_status), 2);

  ASSERT_TRUE(owner.kill_abruptly().is_ok());
  const common::Result<WalRecoveryReport> after_exit = scan_wal(directory.path().string());
  ASSERT_TRUE(after_exit.has_value()) << after_exit.error().to_string();
  EXPECT_EQ(after_exit->classification, WalScanClassification::kClean);
}

TEST(WalProcessLockTest, GracefulShutdownAlsoReleasesTheProcessLock) {
  test::CrashWalDirectory directory{"chronos-wal-process-lock-shutdown"};
  ASSERT_TRUE(directory.valid());
  common::Result<test::CrashChildProcess> spawned =
      test::CrashChildProcess::spawn({.directory = directory.path()});
  ASSERT_TRUE(spawned.has_value()) << spawned.error().to_string();
  test::CrashChildProcess child = std::move(*spawned);
  ASSERT_TRUE(child.wait_for("READY").has_value());
  ASSERT_TRUE(child.send("SHUTDOWN").is_ok());
  ASSERT_TRUE(child.wait_for("SHUTDOWN").has_value());
  ASSERT_TRUE(child.wait_for_exit().has_value());

  common::Result<test::CrashChildProcess> reopened_result =
      test::CrashChildProcess::spawn({.directory = directory.path(), .reopen = true});
  ASSERT_TRUE(reopened_result.has_value()) << reopened_result.error().to_string();
  test::CrashChildProcess reopened = std::move(*reopened_result);
  ASSERT_TRUE(reopened.wait_for("READY").has_value());
  ASSERT_TRUE(reopened.send("SHUTDOWN").is_ok());
  ASSERT_TRUE(reopened.wait_for("SHUTDOWN").has_value());
  ASSERT_TRUE(reopened.wait_for_exit().has_value());
}

} // namespace
} // namespace chronos::wal
