#include "chronos/manifest/compaction_equivalence.hpp"
#include "chronos/manifest/storage.hpp"
#include "manifest/manifest_flush_crash_fixture.hpp"
#include "manifest/manifest_flush_crash_protocol.hpp"
#include "wal/wal_crash_protocol.hpp"

#include <algorithm>
#include <array>
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

class CompactionCrashDirectory {
public:
  CompactionCrashDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-compaction-crash-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr) {
      path_ = created;
    }
  }

  ~CompactionCrashDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  CompactionCrashDirectory(const CompactionCrashDirectory&) = delete;
  CompactionCrashDirectory& operator=(const CompactionCrashDirectory&) = delete;

  [[nodiscard]] bool valid() const noexcept {
    return !path_.empty();
  }
  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

struct CompactionCrashPoint {
  std::string_view failpoint;
  std::uint64_t selected_generation;
  bool output_final_present;
};

class CompactionCrashMatrixTest : public ::testing::TestWithParam<CompactionCrashPoint> {};

TEST_P(CompactionCrashMatrixTest, RecoverySelectsAnEquivalentOldOrNewGeneration) {
  CompactionCrashDirectory directory;
  ASSERT_TRUE(directory.valid());
  const CompactionCrashPoint point = GetParam();
  common::Result<wal::test::CrashChildProcess> spawned =
      wal::test::CrashChildProcess::spawn({.directory = directory.path(),
                                           .compaction = true,
                                           .pause_after = std::string{point.failpoint}});
  ASSERT_TRUE(spawned.has_value()) << spawned.error().to_string();
  wal::test::CrashChildProcess child = std::move(*spawned);
  const common::Result<wal::test::CrashEvent> reached = child.wait_for("FAILPOINT");
  ASSERT_TRUE(reached.has_value()) << reached.error().to_string();
  ASSERT_FALSE(reached->fields.empty());
  EXPECT_EQ(reached->fields.front(), point.failpoint);
  ASSERT_TRUE(child.kill_abruptly().is_ok());

  const test::ManifestFlushCrashFixture fixture;
  const cseg::PartId output_id = test::crash_id<cseg::PartId>(9U);
  std::vector<std::byte> first_selected_bytes;
  {
    ManifestStorage storage =
        ManifestStorage::open_existing({.database_root = directory.path().string()}).value();
    const common::Result<ManifestNamespaceSnapshot> before_cleanup = storage.scan_namespace();
    ASSERT_TRUE(before_cleanup.has_value()) << before_cleanup.error().to_string();
    ASSERT_EQ(before_cleanup->generations.back(), point.selected_generation);
    EXPECT_TRUE(std::ranges::contains(before_cleanup->final_parts, fixture.part_id));
    EXPECT_EQ(std::ranges::contains(before_cleanup->final_parts, output_id),
              point.output_final_present);

    const common::Result<TemporaryCleanupReport> cleanup = storage.cleanup_temporaries();
    ASSERT_TRUE(cleanup.has_value()) << cleanup.error().to_string();
    const common::Result<ManifestNamespaceSnapshot> after_cleanup = storage.scan_namespace();
    ASSERT_TRUE(after_cleanup.has_value()) << after_cleanup.error().to_string();
    EXPECT_TRUE(after_cleanup->temporary_parts.empty());
    EXPECT_TRUE(after_cleanup->temporary_manifests.empty());

    const auto bindings = fixture.bindings();
    common::Result<LoadedManifestGeneration> loaded =
        storage.load_selected_manifest({.expected_database_id = fixture.database_id,
                                        .expected_wal_id = fixture.wal_id,
                                        .schema_bindings = bindings,
                                        .decode_limits = {},
                                        .part_validation_limits = {}});
    ASSERT_TRUE(loaded.has_value()) << loaded.error().to_string();
    EXPECT_EQ(loaded->generation(), point.selected_generation);
    ASSERT_EQ(loaded->parts().size(), 1U);
    const cseg::PartId selected_part =
        point.selected_generation == 2U ? fixture.part_id : output_id;
    EXPECT_EQ(loaded->parts().front().part_id, selected_part);
    EXPECT_EQ(loaded->orphan_parts().size(), point.output_final_present ? 1U : 0U);
    if (!loaded->orphan_parts().empty()) {
      EXPECT_EQ(loaded->orphan_parts().front(),
                point.selected_generation == 2U ? output_id : fixture.part_id);
    }

    const std::array selected_ids{selected_part};
    common::Result<std::vector<LoadedPartImage>> selected_images =
        storage.load_selected_part_images(*loaded, selected_ids, bindings, {});
    ASSERT_TRUE(selected_images.has_value()) << selected_images.error().to_string();
    if (point.selected_generation == 2U) {
      EXPECT_TRUE(std::ranges::equal(selected_images->front().bytes(), fixture.encoded.bytes()));
    } else {
      const std::array original{
          CompactionPartImage{.part_id = fixture.part_id, .bytes = fixture.encoded.bytes()}};
      const std::array recovered{
          CompactionPartImage{.part_id = selected_part, .bytes = selected_images->front().bytes()}};
      const common::Status equivalence = validate_append_only_cseg_v1_equivalence(
          original, recovered, fixture.schema_value, fixture.tablet_id, fixture.wal_id, {});
      EXPECT_TRUE(equivalence.is_ok()) << equivalence.to_string();
    }
    first_selected_bytes.assign(loaded->encoded_bytes().begin(), loaded->encoded_bytes().end());
  }

  ManifestStorage reopened =
      ManifestStorage::open_existing({.database_root = directory.path().string()}).value();
  const auto bindings = fixture.bindings();
  const common::Result<LoadedManifestGeneration> repeated =
      reopened.load_selected_manifest({.expected_database_id = fixture.database_id,
                                       .expected_wal_id = fixture.wal_id,
                                       .schema_bindings = bindings,
                                       .decode_limits = {},
                                       .part_validation_limits = {}});
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  EXPECT_EQ(repeated->generation(), point.selected_generation);
  EXPECT_TRUE(std::ranges::equal(repeated->encoded_bytes(), first_selected_bytes));
}

INSTANTIATE_TEST_SUITE_P(
    EveryCompactionDurabilityTransition, CompactionCrashMatrixTest,
    ::testing::Values(CompactionCrashPoint{.failpoint = test::kAfterPartWrite,
                                           .selected_generation = 2U,
                                           .output_final_present = false},
                      CompactionCrashPoint{.failpoint = test::kAfterPartReadback,
                                           .selected_generation = 2U,
                                           .output_final_present = false},
                      CompactionCrashPoint{.failpoint = test::kAfterPartFileSync,
                                           .selected_generation = 2U,
                                           .output_final_present = false},
                      CompactionCrashPoint{.failpoint = test::kAfterPartRename,
                                           .selected_generation = 2U,
                                           .output_final_present = true},
                      CompactionCrashPoint{.failpoint = test::kAfterPartsDirectorySync,
                                           .selected_generation = 2U,
                                           .output_final_present = true},
                      CompactionCrashPoint{.failpoint = test::kAfterManifestWrite,
                                           .selected_generation = 2U,
                                           .output_final_present = true},
                      CompactionCrashPoint{.failpoint = test::kAfterManifestReadback,
                                           .selected_generation = 2U,
                                           .output_final_present = true},
                      CompactionCrashPoint{.failpoint = test::kAfterManifestFileSync,
                                           .selected_generation = 2U,
                                           .output_final_present = true},
                      CompactionCrashPoint{.failpoint = test::kAfterManifestRename,
                                           .selected_generation = 3U,
                                           .output_final_present = true},
                      CompactionCrashPoint{.failpoint = test::kAfterManifestDirectorySync,
                                           .selected_generation = 3U,
                                           .output_final_present = true},
                      CompactionCrashPoint{.failpoint = test::kAfterPublication,
                                           .selected_generation = 3U,
                                           .output_final_present = true}),
    [](const ::testing::TestParamInfo<CompactionCrashPoint>& parameter) {
      return std::string{parameter.param.failpoint};
    });

} // namespace
} // namespace chronos::manifest
