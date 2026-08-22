#include "chronos/common/status.hpp"
#include "chronos/raft/metadata_snapshot.hpp"
#include "chronos/raft/metadata_snapshot_storage.hpp"
#include "raft/metadata_snapshot_install_crash_fixture.hpp"
#include "raft/metadata_snapshot_install_crash_protocol.hpp"
#include "wal/wal_crash_protocol.hpp"

#include <array>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <utility>

namespace chronos::raft {
namespace {

class CrashDirectory {
public:
  CrashDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-metadata-snapshot-crash-XXXXXX")
            .string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }

  ~CrashDirectory() {
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

struct MetadataSnapshotCrashPoint {
  std::string_view failpoint;
  bool final_present;
};

constexpr std::array<MetadataSnapshotCrashPoint, 8U> kCrashPoints{
    MetadataSnapshotCrashPoint{test::kAfterMetadataTemporaryCreate, false},
    MetadataSnapshotCrashPoint{test::kAfterMetadataWrite, false},
    MetadataSnapshotCrashPoint{test::kAfterMetadataReadback, false},
    MetadataSnapshotCrashPoint{test::kAfterMetadataFileSync, false},
    MetadataSnapshotCrashPoint{test::kAfterMetadataTemporaryClose, false},
    MetadataSnapshotCrashPoint{test::kAfterMetadataRename, true},
    MetadataSnapshotCrashPoint{test::kAfterMetadataDirectorySync, true},
    MetadataSnapshotCrashPoint{test::kAfterMetadataSuccessRelease, true},
};

class MetadataSnapshotInstallCrashMatrixTest
    : public ::testing::TestWithParam<MetadataSnapshotCrashPoint> {};

TEST_P(MetadataSnapshotInstallCrashMatrixTest, RecoversExactAuthorityAndConvergesAfterSigkill) {
  CrashDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const MetadataSnapshotCrashPoint point = GetParam();
  auto spawned = wal::test::CrashChildProcess::spawn(
      {.directory = directory.path(), .pause_after = std::string{point.failpoint}});
  ASSERT_TRUE(spawned.has_value()) << spawned.error().to_string();
  wal::test::CrashChildProcess child = std::move(*spawned);
  auto reached = child.wait_for("FAILPOINT");
  ASSERT_TRUE(reached.has_value()) << reached.error().to_string();
  ASSERT_EQ(reached->fields.size(), 1U);
  EXPECT_EQ(reached->fields.front(), point.failpoint);
  ASSERT_TRUE(child.kill_abruptly().is_ok());

  const MetadataApplicationSnapshot expected = test::metadata_crash_snapshot();
  auto expected_bytes = encode_metadata_application_snapshot_v1(expected);
  ASSERT_TRUE(expected_bytes.has_value()) << expected_bytes.error().to_string();
  const std::filesystem::path temporary =
      directory.path() / "metadata-snapshot-00000000000000000007.rmas.tmp";
  {
    auto storage = MetadataSnapshotStorage::open_existing(
        test::metadata_crash_storage_config(directory.path()));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    EXPECT_FALSE(std::filesystem::exists(temporary));
    auto recovered = storage->load(expected.raft_snapshot.last_included_index);
    if (point.final_present) {
      ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
      EXPECT_EQ(recovered->snapshot, expected);
      EXPECT_EQ(recovered->bytes, *expected_bytes);
    } else {
      ASSERT_FALSE(recovered.has_value());
      EXPECT_EQ(recovered.error().code(), common::StatusCode::kNotFound);
    }

    auto retried = storage->install(expected);

    ASSERT_TRUE(retried.has_value()) << retried.error().to_string();
    EXPECT_EQ(retried->already_present, point.final_present);
    auto converged = storage->load(expected.raft_snapshot.last_included_index);
    ASSERT_TRUE(converged.has_value()) << converged.error().to_string();
    EXPECT_EQ(converged->snapshot, expected);
    EXPECT_EQ(converged->bytes, *expected_bytes);
  }

  auto repeated =
      MetadataSnapshotStorage::open_existing(test::metadata_crash_storage_config(directory.path()));
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  EXPECT_FALSE(std::filesystem::exists(temporary));
  auto recovered = repeated->load(expected.raft_snapshot.last_included_index);
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_EQ(recovered->snapshot, expected);
  EXPECT_EQ(recovered->bytes, *expected_bytes);
}

INSTANTIATE_TEST_SUITE_P(EveryAuthorityBoundary, MetadataSnapshotInstallCrashMatrixTest,
                         ::testing::ValuesIn(kCrashPoints),
                         [](const ::testing::TestParamInfo<MetadataSnapshotCrashPoint>& parameter) {
                           return std::string{parameter.param.failpoint};
                         });

} // namespace
} // namespace chronos::raft
