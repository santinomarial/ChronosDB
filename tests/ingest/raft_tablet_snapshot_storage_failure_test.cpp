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
  if (!failure.final_visible) {
    EXPECT_EQ(before_retry.error().code(), common::StatusCode::kNotFound);
  }

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

[[nodiscard]] RaftTabletApplicationSnapshot reclamation_snapshot(const raft::LogIndex included) {
  RaftTabletApplicationSnapshot result = test::crash_application_snapshot();
  result.raft_snapshot.last_included_index = included;
  result.raft_snapshot.last_included_term = included == 9U ? 4U : 5U;
  result.raft_snapshot.manifest_generation = included;
  return result;
}

void install_reclamation_snapshots(RaftTabletSnapshotStorage& storage) {
  ASSERT_TRUE(storage.install(reclamation_snapshot(9U)).has_value());
  ASSERT_TRUE(storage.install(reclamation_snapshot(10U)).has_value());
  ASSERT_TRUE(storage.install(reclamation_snapshot(11U)).has_value());
}

[[nodiscard]] std::filesystem::path snapshot_path(const TemporarySnapshotRoot& root,
                                                  const raft::LogIndex index) {
  return root.path() / "snapshots" / raft_tablet_snapshot_file_name(index).value();
}

void expect_snapshot_presence(const TemporarySnapshotRoot& root,
                              const std::array<bool, 3U>& expected) {
  EXPECT_EQ(std::filesystem::exists(snapshot_path(root, 9U)), expected[0]);
  EXPECT_EQ(std::filesystem::exists(snapshot_path(root, 10U)), expected[1]);
  EXPECT_EQ(std::filesystem::exists(snapshot_path(root, 11U)), expected[2]);
}

struct AuthoritativeReclamationFailureCase {
  SnapshotStorageFault fault;
  std::string_view name;
  std::array<bool, 3U> present_after_failure;
  std::size_t reclaimed_on_retry;
};

constexpr std::array<AuthoritativeReclamationFailureCase, 8U> kAuthoritativeReclamationFailures{
    AuthoritativeReclamationFailureCase{
        SnapshotStorageFault::kReclamationAuthorityOpen, "authority_open", {true, true, true}, 2U},
    AuthoritativeReclamationFailureCase{SnapshotStorageFault::kReclamationAuthorityValidationStat,
                                        "authority_validation_stat",
                                        {true, true, true},
                                        2U},
    AuthoritativeReclamationFailureCase{SnapshotStorageFault::kReclamationAuthoritySizeStat,
                                        "authority_size_stat",
                                        {true, true, true},
                                        2U},
    AuthoritativeReclamationFailureCase{
        SnapshotStorageFault::kReclamationAuthorityRead, "authority_read", {true, true, true}, 2U},
    AuthoritativeReclamationFailureCase{
        SnapshotStorageFault::kReclamationList, "directory_list", {true, true, true}, 2U},
    AuthoritativeReclamationFailureCase{
        SnapshotStorageFault::kReclamationFirstUnlink, "first_unlink", {true, true, true}, 2U},
    AuthoritativeReclamationFailureCase{
        SnapshotStorageFault::kReclamationSecondUnlink, "second_unlink", {false, true, true}, 1U},
    AuthoritativeReclamationFailureCase{SnapshotStorageFault::kReclamationDirectorySync,
                                        "directory_sync",
                                        {false, true, false},
                                        0U},
};

class RaftTabletSnapshotAuthoritativeReclamationFailureTest
    : public ::testing::TestWithParam<AuthoritativeReclamationFailureCase> {};

TEST_P(RaftTabletSnapshotAuthoritativeReclamationFailureTest,
       PreservesAuthorityAndConvergesAfterEveryFailure) {
  TemporarySnapshotRoot root;
  ASSERT_FALSE(root.path().empty());
  const AuthoritativeReclamationFailureCase failure = GetParam();
  {
    OneShotSnapshotStorageFault syscalls{failure.fault};
    auto storage = detail::RaftTabletSnapshotStorageTestAccess::create(
        test::crash_snapshot_config(root.path()), syscalls);
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    install_reclamation_snapshots(*storage);
    syscalls.arm();

    auto failed = storage->reclaim_obsolete(10U);

    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error().code(), common::StatusCode::kIoError);
    EXPECT_TRUE(syscalls.fired());
    EXPECT_TRUE(storage->is_usable());
    EXPECT_TRUE(storage->poison_status().is_ok());
    EXPECT_EQ(
        storage->cleanup_metrics(),
        (RaftTabletSnapshotCleanupMetrics{.reclamation_attempts = 1U, .reclamation_failures = 1U}));
    expect_snapshot_presence(root, failure.present_after_failure);
    auto authority = storage->load(10U);
    ASSERT_TRUE(authority.has_value()) << authority.error().to_string();
    EXPECT_EQ(authority->snapshot, reclamation_snapshot(10U));

    auto retried = storage->reclaim_obsolete(10U);

    ASSERT_TRUE(retried.has_value()) << retried.error().to_string();
    EXPECT_EQ(retried->authoritative_index, 10U);
    EXPECT_EQ(retried->reclaimed_files, failure.reclaimed_on_retry);
    expect_snapshot_presence(root, {false, true, false});
    auto settled = storage->reclaim_obsolete(10U);
    ASSERT_TRUE(settled.has_value()) << settled.error().to_string();
    EXPECT_EQ(settled->reclaimed_files, 0U);
    EXPECT_EQ(storage->cleanup_metrics(),
              (RaftTabletSnapshotCleanupMetrics{
                  .reclamation_attempts = 3U,
                  .reclamation_failures = 1U,
                  .reclaimed_files = static_cast<std::uint64_t>(failure.reclaimed_on_retry),
                  .reclamation_directory_syncs = failure.reclaimed_on_retry == 0U ? 0U : 1U}));
  }

  auto reopened =
      RaftTabletSnapshotStorage::open_existing(test::crash_snapshot_config(root.path()));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  expect_snapshot_presence(root, {false, true, false});
  auto authority = reopened->load(10U);
  ASSERT_TRUE(authority.has_value()) << authority.error().to_string();
  EXPECT_EQ(authority->snapshot, reclamation_snapshot(10U));
}

INSTANTIATE_TEST_SUITE_P(
    EveryReclamationStage, RaftTabletSnapshotAuthoritativeReclamationFailureTest,
    ::testing::ValuesIn(kAuthoritativeReclamationFailures),
    [](const ::testing::TestParamInfo<AuthoritativeReclamationFailureCase>& parameter) {
      return std::string{parameter.param.name};
    });

struct OrphanReclamationFailureCase {
  SnapshotStorageFault fault;
  std::string_view name;
  std::array<bool, 3U> present_after_failure;
  std::size_t reclaimed_on_retry;
};

constexpr std::array<OrphanReclamationFailureCase, 5U> kOrphanReclamationFailures{
    OrphanReclamationFailureCase{
        SnapshotStorageFault::kReclamationList, "directory_list", {true, true, true}, 3U},
    OrphanReclamationFailureCase{
        SnapshotStorageFault::kReclamationFirstUnlink, "first_unlink", {true, true, true}, 3U},
    OrphanReclamationFailureCase{
        SnapshotStorageFault::kReclamationSecondUnlink, "second_unlink", {false, true, true}, 2U},
    OrphanReclamationFailureCase{
        SnapshotStorageFault::kReclamationThirdUnlink, "third_unlink", {false, false, true}, 1U},
    OrphanReclamationFailureCase{SnapshotStorageFault::kReclamationDirectorySync,
                                 "directory_sync",
                                 {false, false, false},
                                 0U},
};

class RaftTabletSnapshotOrphanReclamationFailureTest
    : public ::testing::TestWithParam<OrphanReclamationFailureCase> {};

TEST_P(RaftTabletSnapshotOrphanReclamationFailureTest,
       RemovesOnlyOrphansAndConvergesAfterEveryFailure) {
  TemporarySnapshotRoot root;
  ASSERT_FALSE(root.path().empty());
  const OrphanReclamationFailureCase failure = GetParam();
  {
    OneShotSnapshotStorageFault syscalls{failure.fault};
    auto storage = detail::RaftTabletSnapshotStorageTestAccess::create(
        test::crash_snapshot_config(root.path()), syscalls);
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    install_reclamation_snapshots(*storage);
    syscalls.arm();

    auto failed = storage->reclaim_obsolete(std::nullopt);

    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error().code(), common::StatusCode::kIoError);
    EXPECT_TRUE(syscalls.fired());
    EXPECT_TRUE(storage->is_usable());
    EXPECT_TRUE(storage->poison_status().is_ok());
    EXPECT_EQ(
        storage->cleanup_metrics(),
        (RaftTabletSnapshotCleanupMetrics{.reclamation_attempts = 1U, .reclamation_failures = 1U}));
    expect_snapshot_presence(root, failure.present_after_failure);

    auto retried = storage->reclaim_obsolete(std::nullopt);

    ASSERT_TRUE(retried.has_value()) << retried.error().to_string();
    EXPECT_EQ(retried->authoritative_index, std::nullopt);
    EXPECT_EQ(retried->reclaimed_files, failure.reclaimed_on_retry);
    expect_snapshot_presence(root, {false, false, false});
    auto settled = storage->reclaim_obsolete(std::nullopt);
    ASSERT_TRUE(settled.has_value()) << settled.error().to_string();
    EXPECT_EQ(settled->reclaimed_files, 0U);
    EXPECT_EQ(storage->cleanup_metrics(),
              (RaftTabletSnapshotCleanupMetrics{
                  .reclamation_attempts = 3U,
                  .reclamation_failures = 1U,
                  .reclaimed_files = static_cast<std::uint64_t>(failure.reclaimed_on_retry),
                  .reclamation_directory_syncs = failure.reclaimed_on_retry == 0U ? 0U : 1U}));
  }

  auto reopened =
      RaftTabletSnapshotStorage::open_existing(test::crash_snapshot_config(root.path()));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  auto latest = reopened->load_latest();
  ASSERT_TRUE(latest.has_value()) << latest.error().to_string();
  EXPECT_FALSE(latest->has_value());
}

INSTANTIATE_TEST_SUITE_P(
    EveryReclamationStage, RaftTabletSnapshotOrphanReclamationFailureTest,
    ::testing::ValuesIn(kOrphanReclamationFailures),
    [](const ::testing::TestParamInfo<OrphanReclamationFailureCase>& parameter) {
      return std::string{parameter.param.name};
    });

} // namespace
} // namespace chronos::ingest
