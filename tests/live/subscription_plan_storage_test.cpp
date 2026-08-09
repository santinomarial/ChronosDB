#include "chronos/live/subscription_plan_storage.hpp"
#include "chronos/query/catalog.hpp"
#include "query/snapshot_tablet_scan_test_fixture.hpp"

#include <cstddef>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace chronos::live {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-subscription-plans-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }

  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

[[nodiscard]] std::shared_ptr<const query::QueryCatalogSnapshot>
catalog(const query::test::SnapshotTabletScanFixture& fixture) {
  const std::vector<query::QueryCatalogTableInput> tables{
      {.name = "metrics", .quoted = false, .schema = fixture.schema_ptr()}};
  return std::make_shared<const query::QueryCatalogSnapshot>(
      query::QueryCatalogSnapshot::create(1U, tables).value());
}

[[nodiscard]] SubscriptionPlanStorageConfig
config(const TemporaryDirectory& directory, const query::test::SnapshotTabletScanFixture& fixture) {
  return {.directory_path = directory.path().string(),
          .database_id = fixture.snapshot().database_id().uuid()};
}

TEST(SubscriptionPlanStorageTest, InstallsReopensAndRepreparesExactFingerprint) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  query::test::SnapshotTabletScanFixture fixture{0U};
  constexpr std::string_view sql = "SUBSCRIBE SELECT event_time FROM metrics ORDER BY event_time";
  PlanFingerprint fingerprint{};
  {
    auto storage = SubscriptionPlanStorage::create(config(directory, fixture));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    auto locked = SubscriptionPlanStorage::open_existing(config(directory, fixture));
    ASSERT_FALSE(locked.has_value());
    EXPECT_EQ(locked.error().code(), common::StatusCode::kUnavailable);
    const auto installed = storage->install(sql, catalog(fixture));
    ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
    EXPECT_FALSE(installed->already_present);
    fingerprint = installed->plan_fingerprint;
    EXPECT_EQ(installed->file_name, subscription_plan_file_name(fingerprint));
    const auto repeated = storage->install(sql, catalog(fixture));
    ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
    EXPECT_TRUE(repeated->already_present);
    auto loaded = storage->load(fingerprint, catalog(fixture));
    ASSERT_TRUE(loaded.has_value()) << loaded.error().to_string();
    EXPECT_EQ(loaded->fingerprint(), fingerprint);
    EXPECT_EQ(loaded->schema_ptr(), fixture.schema_ptr());
  }

  auto reopened = SubscriptionPlanStorage::open_existing(config(directory, fixture));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  auto loaded = reopened->load(fingerprint, catalog(fixture));
  ASSERT_TRUE(loaded.has_value()) << loaded.error().to_string();
  EXPECT_EQ(loaded->fingerprint(), fingerprint);
  auto empty_catalog = std::make_shared<const query::QueryCatalogSnapshot>(
      query::QueryCatalogSnapshot::create(2U, {}).value());
  EXPECT_FALSE(reopened->load(fingerprint, std::move(empty_catalog)).has_value());
}

TEST(SubscriptionPlanStorageTest, RejectsCorruptInstalledDefinition) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  query::test::SnapshotTabletScanFixture fixture{0U};
  PlanFingerprint fingerprint{};
  std::filesystem::path path;
  {
    auto storage = SubscriptionPlanStorage::create(config(directory, fixture));
    ASSERT_TRUE(storage.has_value());
    auto installed = storage->install("SUBSCRIBE SELECT event_time FROM metrics", catalog(fixture));
    ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
    fingerprint = installed->plan_fingerprint;
    path = directory.path() / installed->file_name;
  }
  {
    std::fstream file{path, std::ios::binary | std::ios::in | std::ios::out};
    ASSERT_TRUE(file.good());
    file.seekp(100);
    file.put('x');
  }
  auto reopened = SubscriptionPlanStorage::open_existing(config(directory, fixture));
  ASSERT_TRUE(reopened.has_value());
  const auto loaded = reopened->load(fingerprint, catalog(fixture));
  ASSERT_FALSE(loaded.has_value());
  EXPECT_EQ(loaded.error().code(), common::StatusCode::kCorruption);
}

} // namespace
} // namespace chronos::live
