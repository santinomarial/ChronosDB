#include "chronos/common/status.hpp"
#include "chronos/ingest/raft_tablet_snapshot.hpp"
#include "chronos/ingest/raft_tablet_snapshot_storage.hpp"
#include "ingest/tablet_snapshot_install_crash_fixture.hpp"
#include "ingest/tablet_snapshot_install_crash_protocol.hpp"
#include "wal/wal_crash_protocol.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <utility>

namespace chronos::ingest {
namespace {

class ReclamationCrashDirectory {
public:
  ReclamationCrashDirectory() {
    std::string pattern = (std::filesystem::temp_directory_path() /
                           "chronos-tablet-snapshot-reclamation-crash-XXXXXX")
                              .string();
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

  ~ReclamationCrashDirectory() {
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

struct TabletSnapshotReclamationCrashPoint {
  std::string_view failpoint;
  std::uint64_t occurrence;
  std::string_view name;
  bool authoritative;
  std::array<bool, 3U> present_after_crash;
  std::size_t reclaimed_on_retry;
};

constexpr std::array<TabletSnapshotReclamationCrashPoint, 11U> kReclamationCrashPoints{
    TabletSnapshotReclamationCrashPoint{test::kAfterApplicationAuthoritativeReclamationList,
                                        1U,
                                        "authoritative_after_list",
                                        true,
                                        {true, true, true},
                                        2U},
    TabletSnapshotReclamationCrashPoint{test::kAfterApplicationAuthoritativeReclamationUnlink,
                                        1U,
                                        "authoritative_after_first_unlink",
                                        true,
                                        {false, true, true},
                                        1U},
    TabletSnapshotReclamationCrashPoint{test::kAfterApplicationAuthoritativeReclamationUnlink,
                                        2U,
                                        "authoritative_after_second_unlink",
                                        true,
                                        {false, true, false},
                                        0U},
    TabletSnapshotReclamationCrashPoint{
        test::kAfterApplicationAuthoritativeReclamationDirectorySync,
        1U,
        "authoritative_after_directory_sync",
        true,
        {false, true, false},
        0U},
    TabletSnapshotReclamationCrashPoint{test::kAfterApplicationAuthoritativeReclamationSuccess,
                                        1U,
                                        "authoritative_after_success",
                                        true,
                                        {false, true, false},
                                        0U},
    TabletSnapshotReclamationCrashPoint{test::kAfterApplicationOrphanReclamationList,
                                        1U,
                                        "orphan_after_list",
                                        false,
                                        {true, true, true},
                                        3U},
    TabletSnapshotReclamationCrashPoint{test::kAfterApplicationOrphanReclamationUnlink,
                                        1U,
                                        "orphan_after_first_unlink",
                                        false,
                                        {false, true, true},
                                        2U},
    TabletSnapshotReclamationCrashPoint{test::kAfterApplicationOrphanReclamationUnlink,
                                        2U,
                                        "orphan_after_second_unlink",
                                        false,
                                        {false, false, true},
                                        1U},
    TabletSnapshotReclamationCrashPoint{test::kAfterApplicationOrphanReclamationUnlink,
                                        3U,
                                        "orphan_after_third_unlink",
                                        false,
                                        {false, false, false},
                                        0U},
    TabletSnapshotReclamationCrashPoint{test::kAfterApplicationOrphanReclamationDirectorySync,
                                        1U,
                                        "orphan_after_directory_sync",
                                        false,
                                        {false, false, false},
                                        0U},
    TabletSnapshotReclamationCrashPoint{test::kAfterApplicationOrphanReclamationSuccess,
                                        1U,
                                        "orphan_after_success",
                                        false,
                                        {false, false, false},
                                        0U},
};

[[nodiscard]] std::filesystem::path snapshot_path(const ReclamationCrashDirectory& directory,
                                                  const raft::LogIndex index) {
  return directory.path() / "snapshots" / raft_tablet_snapshot_file_name(index).value();
}

void expect_snapshot_presence(const ReclamationCrashDirectory& directory,
                              const std::array<bool, 3U>& expected) {
  EXPECT_EQ(std::filesystem::exists(snapshot_path(directory, 9U)), expected[0]);
  EXPECT_EQ(std::filesystem::exists(snapshot_path(directory, 10U)), expected[1]);
  EXPECT_EQ(std::filesystem::exists(snapshot_path(directory, 11U)), expected[2]);
}

class TabletSnapshotReclamationCrashMatrixTest
    : public ::testing::TestWithParam<TabletSnapshotReclamationCrashPoint> {};

TEST_P(TabletSnapshotReclamationCrashMatrixTest,
       ReopensExactPartialDeletionAndConvergesAfterSigkill) {
  ReclamationCrashDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const TabletSnapshotReclamationCrashPoint point = GetParam();
  auto spawned = wal::test::CrashChildProcess::spawn({.directory = directory.path(),
                                                      .pause_after = std::string{point.failpoint},
                                                      .pause_occurrence = point.occurrence});
  ASSERT_TRUE(spawned.has_value()) << spawned.error().to_string();
  wal::test::CrashChildProcess child = std::move(*spawned);
  auto reached = child.wait_for("FAILPOINT");
  ASSERT_TRUE(reached.has_value()) << reached.error().to_string();
  ASSERT_EQ(reached->fields.size(), 1U);
  EXPECT_EQ(reached->fields.front(), point.failpoint);
  ASSERT_TRUE(child.kill_abruptly().is_ok());

  const RaftTabletApplicationSnapshot authority = test::crash_application_snapshot(10U);
  auto authority_bytes = encode_raft_tablet_application_snapshot_v1(authority);
  ASSERT_TRUE(authority_bytes.has_value()) << authority_bytes.error().to_string();
  {
    auto storage =
        RaftTabletSnapshotStorage::open_existing(test::crash_snapshot_config(directory.path()));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    expect_snapshot_presence(directory, point.present_after_crash);
    if (point.authoritative) {
      auto loaded = storage->load(10U);
      ASSERT_TRUE(loaded.has_value()) << loaded.error().to_string();
      EXPECT_EQ(loaded->snapshot, authority);
      EXPECT_EQ(loaded->bytes, *authority_bytes);
    }

    const std::optional<raft::LogIndex> authoritative_index =
        point.authoritative ? std::optional<raft::LogIndex>{10U} : std::nullopt;
    auto retried = storage->reclaim_obsolete(authoritative_index);

    ASSERT_TRUE(retried.has_value()) << retried.error().to_string();
    EXPECT_EQ(retried->authoritative_index, authoritative_index);
    EXPECT_EQ(retried->reclaimed_files, point.reclaimed_on_retry);
    expect_snapshot_presence(directory, point.authoritative ? std::array{false, true, false}
                                                            : std::array{false, false, false});
    auto settled = storage->reclaim_obsolete(authoritative_index);
    ASSERT_TRUE(settled.has_value()) << settled.error().to_string();
    EXPECT_EQ(settled->reclaimed_files, 0U);
  }

  auto repeated =
      RaftTabletSnapshotStorage::open_existing(test::crash_snapshot_config(directory.path()));
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  if (point.authoritative) {
    expect_snapshot_presence(directory, {false, true, false});
    auto loaded = repeated->load(10U);
    ASSERT_TRUE(loaded.has_value()) << loaded.error().to_string();
    EXPECT_EQ(loaded->snapshot, authority);
    EXPECT_EQ(loaded->bytes, *authority_bytes);
  } else {
    expect_snapshot_presence(directory, {false, false, false});
    auto latest = repeated->load_latest();
    ASSERT_TRUE(latest.has_value()) << latest.error().to_string();
    EXPECT_FALSE(latest->has_value());
  }
}

INSTANTIATE_TEST_SUITE_P(
    EveryDurabilityTransition, TabletSnapshotReclamationCrashMatrixTest,
    ::testing::ValuesIn(kReclamationCrashPoints),
    [](const ::testing::TestParamInfo<TabletSnapshotReclamationCrashPoint>& parameter) {
      return std::string{parameter.param.name};
    });

} // namespace
} // namespace chronos::ingest
