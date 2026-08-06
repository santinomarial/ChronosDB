#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/storage.hpp"
#include "manifest/manifest_flush_crash_fixture.hpp"
#include "manifest/manifest_flush_crash_protocol.hpp"
#include "wal/wal_crash_protocol.hpp"

#include <array>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>

namespace chronos::manifest {
namespace {

class PartReclamationCrashDirectory {
public:
  PartReclamationCrashDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-part-reclaim-crash-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr) {
      path_ = created;
    }
  }

  ~PartReclamationCrashDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  PartReclamationCrashDirectory(const PartReclamationCrashDirectory&) = delete;
  PartReclamationCrashDirectory& operator=(const PartReclamationCrashDirectory&) = delete;

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

class PartReclamationCrashMatrixTest : public ::testing::TestWithParam<std::string_view> {};

TEST_P(PartReclamationCrashMatrixTest, SelectedSuccessorReopensAfterEveryDeletionBoundary) {
  PartReclamationCrashDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const std::string_view failpoint = GetParam();
  common::Result<wal::test::CrashChildProcess> spawned =
      wal::test::CrashChildProcess::spawn({.directory = directory.path(),
                                           .part_reclamation = true,
                                           .pause_after = std::string{failpoint}});
  ASSERT_TRUE(spawned.has_value()) << spawned.error().to_string();
  wal::test::CrashChildProcess child = std::move(*spawned);
  const common::Result<wal::test::CrashEvent> reached = child.wait_for("FAILPOINT");
  ASSERT_TRUE(reached.has_value()) << reached.error().to_string();
  ASSERT_FALSE(reached->fields.empty());
  EXPECT_EQ(reached->fields.front(), failpoint);
  ASSERT_TRUE(child.kill_abruptly().is_ok());

  const test::ManifestFlushCrashFixture fixture;
  ManifestStorage storage =
      ManifestStorage::open_existing({.database_root = directory.path().string()}).value();
  const auto bindings = fixture.bindings();
  common::Result<LoadedManifestGeneration> selected =
      storage.load_selected_manifest({.expected_database_id = fixture.database_id,
                                      .expected_wal_id = fixture.wal_id,
                                      .schema_bindings = bindings,
                                      .decode_limits = {},
                                      .part_validation_limits = {}});
  ASSERT_TRUE(selected.has_value()) << selected.error().to_string();
  EXPECT_EQ(selected->generation(), 3U);
  ASSERT_EQ(selected->parts().size(), 1U);
  EXPECT_EQ(selected->parts().front().part_id, test::crash_id<cseg::PartId>(9U));
  EXPECT_FALSE(std::filesystem::exists(directory.path() / kPartsDirectoryName /
                                       part_file_name(fixture.part_id)));
  EXPECT_TRUE(std::filesystem::exists(directory.path() / kPartsDirectoryName /
                                      part_file_name(test::crash_id<cseg::PartId>(9U))));

  const std::array selected_ids{test::crash_id<cseg::PartId>(9U)};
  EXPECT_TRUE(storage.load_selected_part_images(*selected, selected_ids, bindings, {}).has_value());
}

INSTANTIATE_TEST_SUITE_P(EveryPartReclamationDurabilityTransition, PartReclamationCrashMatrixTest,
                         ::testing::Values(test::kAfterPartReclamationUnlink,
                                           test::kAfterPartReclamationDirectorySync),
                         [](const ::testing::TestParamInfo<std::string_view>& parameter) {
                           return std::string{parameter.param};
                         });

} // namespace
} // namespace chronos::manifest
