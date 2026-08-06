#include "chronos/manifest/storage.hpp"
#include "manifest/manifest_flush_crash_fixture.hpp"
#include "manifest/manifest_flush_crash_protocol.hpp"
#include "wal/wal_crash_protocol.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::manifest {
namespace {

class CrashDirectory {
public:
  CrashDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-manifest-flush-crash-XXXXXX").string();
    char* const created = ::mkdtemp(pattern.data());
    if (created != nullptr) {
      path_ = created;
    }
  }

  ~CrashDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  CrashDirectory(const CrashDirectory&) = delete;
  CrashDirectory& operator=(const CrashDirectory&) = delete;

  [[nodiscard]] bool valid() const noexcept {
    return !path_.empty();
  }
  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

struct ManifestCrashPoint {
  std::string_view failpoint;
  std::uint64_t selected_generation;
  bool final_part_present;
};

class ManifestFlushCrashMatrixTest : public ::testing::TestWithParam<ManifestCrashPoint> {};

TEST_P(ManifestFlushCrashMatrixTest, RecoverySelectsOneCompleteGenerationAndConverges) {
  CrashDirectory directory;
  ASSERT_TRUE(directory.valid());
  const ManifestCrashPoint point = GetParam();
  common::Result<wal::test::CrashChildProcess> spawned = wal::test::CrashChildProcess::spawn(
      {.directory = directory.path(), .pause_after = std::string{point.failpoint}});
  ASSERT_TRUE(spawned.has_value()) << spawned.error().to_string();
  wal::test::CrashChildProcess child = std::move(*spawned);
  const common::Result<wal::test::CrashEvent> reached = child.wait_for("FAILPOINT");
  ASSERT_TRUE(reached.has_value()) << reached.error().to_string();
  ASSERT_FALSE(reached->fields.empty());
  EXPECT_EQ(reached->fields.front(), point.failpoint);
  ASSERT_TRUE(child.kill_abruptly().is_ok());

  const test::ManifestFlushCrashFixture fixture;
  std::vector<std::byte> first_selected_bytes;
  {
    ManifestStorage storage =
        ManifestStorage::open_existing({.database_root = directory.path().string()}).value();
    const common::Result<ManifestNamespaceSnapshot> before_cleanup = storage.scan_namespace();
    ASSERT_TRUE(before_cleanup.has_value()) << before_cleanup.error().to_string();
    ASSERT_EQ(before_cleanup->generations.back(), point.selected_generation);
    EXPECT_EQ(std::ranges::contains(before_cleanup->final_parts, fixture.part_id),
              point.final_part_present);

    const common::Result<TemporaryCleanupReport> cleanup = storage.cleanup_temporaries();
    ASSERT_TRUE(cleanup.has_value()) << cleanup.error().to_string();
    const common::Result<ManifestNamespaceSnapshot> after_cleanup = storage.scan_namespace();
    ASSERT_TRUE(after_cleanup.has_value()) << after_cleanup.error().to_string();
    EXPECT_TRUE(after_cleanup->temporary_parts.empty());
    EXPECT_TRUE(after_cleanup->temporary_manifests.empty());

    const auto bindings = fixture.bindings();
    const std::span<const TabletSchemaBinding> selected_bindings =
        point.selected_generation == 1U ? std::span<const TabletSchemaBinding>{}
                                        : std::span<const TabletSchemaBinding>{bindings};
    const common::Result<LoadedManifestGeneration> loaded =
        storage.load_selected_manifest({.expected_database_id = fixture.database_id,
                                        .expected_wal_id = fixture.wal_id,
                                        .schema_bindings = selected_bindings,
                                        .decode_limits = {},
                                        .part_validation_limits = {}});
    ASSERT_TRUE(loaded.has_value()) << loaded.error().to_string();
    EXPECT_EQ(loaded->generation(), point.selected_generation);
    EXPECT_EQ(loaded->parts().size(), point.selected_generation == 1U ? 0U : 1U);
    EXPECT_EQ(loaded->orphan_parts().size(),
              point.selected_generation == 1U && point.final_part_present ? 1U : 0U);
    first_selected_bytes.assign(loaded->encoded_bytes().begin(), loaded->encoded_bytes().end());
  }

  ManifestStorage reopened =
      ManifestStorage::open_existing({.database_root = directory.path().string()}).value();
  const auto bindings = fixture.bindings();
  const std::span<const TabletSchemaBinding> selected_bindings =
      point.selected_generation == 1U ? std::span<const TabletSchemaBinding>{}
                                      : std::span<const TabletSchemaBinding>{bindings};
  const common::Result<LoadedManifestGeneration> repeated =
      reopened.load_selected_manifest({.expected_database_id = fixture.database_id,
                                       .expected_wal_id = fixture.wal_id,
                                       .schema_bindings = selected_bindings,
                                       .decode_limits = {},
                                       .part_validation_limits = {}});
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  EXPECT_EQ(repeated->generation(), point.selected_generation);
  EXPECT_TRUE(std::ranges::equal(repeated->encoded_bytes(), first_selected_bytes));
}

INSTANTIATE_TEST_SUITE_P(
    EveryDurabilityTransition, ManifestFlushCrashMatrixTest,
    ::testing::Values(ManifestCrashPoint{.failpoint = test::kAfterPartWrite,
                                         .selected_generation = 1U,
                                         .final_part_present = false},
                      ManifestCrashPoint{.failpoint = test::kAfterPartFileSync,
                                         .selected_generation = 1U,
                                         .final_part_present = false},
                      ManifestCrashPoint{.failpoint = test::kAfterPartRename,
                                         .selected_generation = 1U,
                                         .final_part_present = true},
                      ManifestCrashPoint{.failpoint = test::kAfterPartsDirectorySync,
                                         .selected_generation = 1U,
                                         .final_part_present = true},
                      ManifestCrashPoint{.failpoint = test::kAfterManifestWrite,
                                         .selected_generation = 1U,
                                         .final_part_present = true},
                      ManifestCrashPoint{.failpoint = test::kAfterManifestFileSync,
                                         .selected_generation = 1U,
                                         .final_part_present = true},
                      ManifestCrashPoint{.failpoint = test::kAfterManifestRename,
                                         .selected_generation = 2U,
                                         .final_part_present = true},
                      ManifestCrashPoint{.failpoint = test::kAfterManifestDirectorySync,
                                         .selected_generation = 2U,
                                         .final_part_present = true}),
    [](const ::testing::TestParamInfo<ManifestCrashPoint>& parameter) {
      return std::string{parameter.param.failpoint};
    });

} // namespace
} // namespace chronos::manifest
