#include "wal/wal_crash_test_support.hpp"

#include <cstdint>
#include <gtest/gtest.h>
#include <string>
#include <sys/wait.h>
#include <utility>

namespace chronos::wal {
namespace {

[[nodiscard]] test::CrashChildProcess start_ready_child(const test::CrashChildOptions& options) {
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

void submit(test::CrashChildProcess& child, const std::uint64_t request_id,
            const std::string_view mode) {
  ASSERT_TRUE(child.send("SUBMIT " + std::to_string(request_id) + " " + std::string{mode}).is_ok());
  const common::Result<test::CrashEvent> admitted = child.wait_for("ADMITTED", request_id);
  ASSERT_TRUE(admitted.has_value()) << admitted.error().to_string();
}

void expect_clean_exit(test::CrashChildProcess& child) {
  const common::Result<test::CrashEvent> shutdown = child.wait_for("SHUTDOWN");
  ASSERT_TRUE(shutdown.has_value()) << shutdown.error().to_string();
  ASSERT_EQ(shutdown->fields, std::vector<std::string>{"OK"});
  const common::Result<int> status = child.wait_for_exit();
  ASSERT_TRUE(status.has_value()) << status.error().to_string();
  ASSERT_TRUE(WIFEXITED(*status));
  EXPECT_EQ(WEXITSTATUS(*status), 0);
}

TEST(WalCrashHarnessTest, GracefulChildExercisesCreateMixedAcknowledgmentAndRecovery) {
  test::CrashWalDirectory directory{"chronos-wal-crash-graceful"};
  ASSERT_TRUE(directory.valid());
  test::CrashChildProcess child = start_ready_child(
      {.directory = directory.path(), .maximum_sync_batch_delay = std::chrono::seconds{30}});
  ASSERT_GT(child.process_id(), 0);

  submit(child, 101U, "ASYNC");
  common::Result<test::CrashEvent> completed = child.wait_for("COMPLETED", 101U);
  ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  ASSERT_GE(completed->fields.size(), 4U);
  EXPECT_EQ(completed->fields[1], "ASYNC");

  submit(child, 102U, "LOCAL_SYNC");
  ASSERT_TRUE(child.send("SHUTDOWN").is_ok());
  completed = child.wait_for("COMPLETED", 102U);
  ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  ASSERT_GE(completed->fields.size(), 4U);
  EXPECT_EQ(completed->fields[1], "LOCAL_SYNC");
  expect_clean_exit(child);

  const common::Result<test::CrashRecoveryResult> recovered =
      test::inspect_crash_wal(directory.path());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_TRUE(test::validate_crash_prefix(recovered->records).is_ok());
  EXPECT_EQ(recovered->records,
            (std::vector<test::RecoveredCrashRecord>{{.sequence = 1U, .request_id = 101U},
                                                     {.sequence = 2U, .request_id = 102U}}));
}

TEST(WalCrashHarnessTest, AsyncCompletionBeforeSigkillAllowsEitherRecoveryOutcome) {
  test::CrashWalDirectory directory{"chronos-wal-crash-async"};
  ASSERT_TRUE(directory.valid());
  test::CrashChildProcess child = start_ready_child({.directory = directory.path()});
  ASSERT_GT(child.process_id(), 0);

  submit(child, 201U, "ASYNC");
  const common::Result<test::CrashEvent> completed = child.wait_for("COMPLETED", 201U);
  ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  ASSERT_TRUE(child.kill_abruptly().is_ok());

  const common::Result<test::CrashRecoveryResult> recovered =
      test::inspect_crash_wal(directory.path());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_TRUE(test::validate_crash_prefix(recovered->records).is_ok());
  ASSERT_LE(recovered->records.size(), 1U);
  if (!recovered->records.empty()) {
    EXPECT_EQ(recovered->records.front(),
              (test::RecoveredCrashRecord{.sequence = 1U, .request_id = 201U}));
  }
}

} // namespace
} // namespace chronos::wal
