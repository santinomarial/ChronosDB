#include "chronos/ingest/raft_tablet_snapshot_storage.hpp"
#include "ingest/raft_tablet_snapshot_storage_fault.hpp"
#include "ingest/raft_tablet_snapshot_storage_internal.hpp"
#include "ingest/tablet_snapshot_install_crash_fixture.hpp"

#include <array>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <string_view>
#include <unistd.h>

namespace chronos::ingest {
namespace {

using test::OneShotSnapshotStorageFault;
using test::SnapshotStorageFault;

class TemporarySnapshotRoot {
public:
  TemporarySnapshotRoot() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-rtas-failure-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr) {
      path_ = created;
      std::error_code error;
      static_cast<void>(std::filesystem::create_directory(path_ / "snapshots", error));
      if (error) {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
        path_.clear();
      }
    }
  }

  ~TemporarySnapshotRoot() {
    std::error_code ignored;
    if (!path_.empty())
      std::filesystem::remove_all(path_, ignored);
  }

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

struct InstallFailureCase {
  SnapshotStorageFault fault;
  std::string_view name;
  bool final_visible;
  bool owner_poisoned;
};

constexpr std::array<InstallFailureCase, 11U> kInstallFailures{
    InstallFailureCase{SnapshotStorageFault::kPriorTemporaryUnlink, "prior_temporary_unlink", false,
                       false},
    InstallFailureCase{SnapshotStorageFault::kTemporaryCreate, "temporary_create", false, false},
    InstallFailureCase{SnapshotStorageFault::kTemporaryValidationStat, "temporary_validation_stat",
                       false, false},
    InstallFailureCase{SnapshotStorageFault::kTemporaryWrite, "temporary_write", false, false},
    InstallFailureCase{SnapshotStorageFault::kTemporaryPartialWrite, "temporary_partial_write",
                       false, false},
    InstallFailureCase{SnapshotStorageFault::kTemporarySizeStat, "temporary_size_stat", false,
                       false},
    InstallFailureCase{SnapshotStorageFault::kTemporaryReadback, "temporary_readback", false,
                       false},
    InstallFailureCase{SnapshotStorageFault::kTemporaryFileSync, "temporary_file_sync", false,
                       false},
    InstallFailureCase{SnapshotStorageFault::kTemporaryClose, "temporary_close", false, false},
    InstallFailureCase{SnapshotStorageFault::kFinalRename, "final_rename", false, false},
    InstallFailureCase{SnapshotStorageFault::kFinalDirectorySync, "final_directory_sync", true,
                       true},
};

class RaftTabletSnapshotStorageFailureTest : public ::testing::TestWithParam<InstallFailureCase> {};

TEST_P(RaftTabletSnapshotStorageFailureTest, FailsClosedAtEachInstallSyscallAndReopenConverges) {
  TemporarySnapshotRoot root;
  ASSERT_FALSE(root.path().empty());
  const InstallFailureCase failure = GetParam();
  const RaftTabletApplicationSnapshot expected = test::crash_application_snapshot();
  OneShotSnapshotStorageFault syscalls{failure.fault};
  {
    auto storage = detail::RaftTabletSnapshotStorageTestAccess::create(
        test::crash_snapshot_config(root.path()), syscalls);
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    syscalls.arm();

    auto installed = storage->install(expected);

    ASSERT_FALSE(installed.has_value());
    EXPECT_EQ(installed.error().code(), common::StatusCode::kIoError);
    EXPECT_TRUE(syscalls.fired());
    EXPECT_EQ(storage->is_usable(), !failure.owner_poisoned);
    EXPECT_EQ(storage->poison_status().is_ok(), !failure.owner_poisoned);
  }

  auto reopened =
      RaftTabletSnapshotStorage::open_existing(test::crash_snapshot_config(root.path()));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_FALSE(std::filesystem::exists(root.path() / "snapshots" /
                                       "snapshot-00000000000000000009.rtas.tmp"));
  auto before_retry = reopened->load(expected.raft_snapshot.last_included_index);
  EXPECT_EQ(before_retry.has_value(), failure.final_visible);
  if (!failure.final_visible)
    EXPECT_EQ(before_retry.error().code(), common::StatusCode::kNotFound);

  auto retried = reopened->install(expected);

  ASSERT_TRUE(retried.has_value()) << retried.error().to_string();
  EXPECT_EQ(retried->already_present, failure.final_visible);
  auto loaded = reopened->load(expected.raft_snapshot.last_included_index);
  ASSERT_TRUE(loaded.has_value()) << loaded.error().to_string();
  EXPECT_EQ(loaded->snapshot, expected);
}

INSTANTIATE_TEST_SUITE_P(EveryInstallSyscall, RaftTabletSnapshotStorageFailureTest,
                         ::testing::ValuesIn(kInstallFailures),
                         [](const ::testing::TestParamInfo<InstallFailureCase>& parameter) {
                           return std::string{parameter.param.name};
                         });

void leave_interrupted_temporary(const std::filesystem::path& root) {
  OneShotSnapshotStorageFault syscalls{SnapshotStorageFault::kTemporaryWrite};
  auto storage = detail::RaftTabletSnapshotStorageTestAccess::open_existing(
      test::crash_snapshot_config(root), syscalls);
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  syscalls.arm();
  auto failed = storage->install(test::crash_application_snapshot());
  ASSERT_FALSE(failed.has_value());
  ASSERT_TRUE(syscalls.fired());
}

TEST(RaftTabletSnapshotStorageCleanupFailureTest, FailedTemporaryUnlinkIsRetryableOnNextOpen) {
  TemporarySnapshotRoot root;
  ASSERT_FALSE(root.path().empty());
  {
    auto storage = RaftTabletSnapshotStorage::create(test::crash_snapshot_config(root.path()));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  }
  leave_interrupted_temporary(root.path());
  const std::filesystem::path temporary =
      root.path() / "snapshots" / "snapshot-00000000000000000009.rtas.tmp";
  ASSERT_TRUE(std::filesystem::exists(temporary));
  OneShotSnapshotStorageFault syscalls{SnapshotStorageFault::kPriorTemporaryUnlink};
  syscalls.arm();

  auto failed = detail::RaftTabletSnapshotStorageTestAccess::open_existing(
      test::crash_snapshot_config(root.path()), syscalls);

  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kIoError);
  EXPECT_TRUE(syscalls.fired());
  EXPECT_TRUE(std::filesystem::exists(temporary));
  auto reopened =
      RaftTabletSnapshotStorage::open_existing(test::crash_snapshot_config(root.path()));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_FALSE(std::filesystem::exists(temporary));
}

TEST(RaftTabletSnapshotStorageCleanupFailureTest, FailedCleanupSyncIsRetryableOnNextOpen) {
  TemporarySnapshotRoot root;
  ASSERT_FALSE(root.path().empty());
  {
    auto storage = RaftTabletSnapshotStorage::create(test::crash_snapshot_config(root.path()));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  }
  leave_interrupted_temporary(root.path());
  const std::filesystem::path temporary =
      root.path() / "snapshots" / "snapshot-00000000000000000009.rtas.tmp";
  ASSERT_TRUE(std::filesystem::exists(temporary));
  OneShotSnapshotStorageFault syscalls{SnapshotStorageFault::kFinalDirectorySync};
  syscalls.arm();

  auto failed = detail::RaftTabletSnapshotStorageTestAccess::open_existing(
      test::crash_snapshot_config(root.path()), syscalls);

  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kIoError);
  EXPECT_TRUE(syscalls.fired());
  EXPECT_FALSE(std::filesystem::exists(temporary));
  auto reopened =
      RaftTabletSnapshotStorage::open_existing(test::crash_snapshot_config(root.path()));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
}

} // namespace
} // namespace chronos::ingest
