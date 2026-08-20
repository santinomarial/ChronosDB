#include "chronos/live/subscription_plan_storage.hpp"
#include "chronos/query/catalog.hpp"
#include "query/snapshot_tablet_scan_test_fixture.hpp"
#include "support/failing_allocator.hpp"

#include <array>
#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <utility>

namespace chronos::live {
namespace {

constexpr std::string_view kSql = "SUBSCRIBE SELECT event_time FROM metrics";

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-plan-storage-alloc-XXXXXX").string();
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
  const std::array tables{query::QueryCatalogTableInput{
      .name = "metrics", .quoted = false, .schema = fixture.schema_ptr()}};
  return std::make_shared<const query::QueryCatalogSnapshot>(
      query::QueryCatalogSnapshot::create(1U, tables).value());
}

[[nodiscard]] SubscriptionPlanStorageConfig
config(const TemporaryDirectory& directory, const query::test::SnapshotTabletScanFixture& fixture) {
  return {.directory_path = directory.path().string(),
          .database_id = fixture.snapshot().database_id().uuid()};
}

template <typename Operation>
[[nodiscard]] auto run_with_allocation_failure(const std::size_t fail_after, std::size_t& observed,
                                               Operation&& operation) {
  using Result = decltype(operation());
  std::optional<Result> result;
  {
    ::chronos::test::ScopedAllocationFailure failure{fail_after};
    result.emplace(operation());
    observed = failure.observed_allocations();
    failure.disable();
  }
  return std::move(*result);
}

void expect_exact_load(const SubscriptionPlanStorage& storage, const PlanFingerprint& fingerprint,
                       const std::shared_ptr<const query::QueryCatalogSnapshot>& query_catalog) {
  const auto loaded = storage.load(fingerprint, query_catalog);
  ASSERT_TRUE(loaded.has_value()) << loaded.error().to_string();
  EXPECT_EQ(loaded->fingerprint(), fingerprint);
}

TEST(SubscriptionPlanStorageAllocationFailureTest,
     InstallIsExactlyRetryableAtEveryOwnedAllocation) {
  query::test::SnapshotTabletScanFixture fixture{1U};
  const auto query_catalog = catalog(fixture);
  const auto prepared = prepare_subscription_plan(kSql, query_catalog);
  ASSERT_TRUE(prepared.has_value()) << prepared.error().status().to_string();
  const PlanFingerprint fingerprint = prepared->fingerprint();

  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 512U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    TemporaryDirectory directory;
    ASSERT_FALSE(directory.path().empty());
    auto storage = SubscriptionPlanStorage::create(config(directory, fixture));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();

    std::size_t observed = 0U;
    auto installed = run_with_allocation_failure(
        fail_after, observed, [&] { return storage->install(kSql, query_catalog); });
    EXPECT_GT(observed, 0U);
    if (installed.has_value()) {
      EXPECT_EQ(installed->plan_fingerprint, fingerprint);
      EXPECT_FALSE(installed->already_present);
      expect_exact_load(*storage, fingerprint, query_catalog);
      reached_success = true;
      break;
    }

    EXPECT_EQ(installed.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_TRUE(storage->is_usable());
    const auto recovered = storage->install(kSql, query_catalog);
    ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
    EXPECT_EQ(recovered->plan_fingerprint, fingerprint);
    expect_exact_load(*storage, fingerprint, query_catalog);
  }
  EXPECT_TRUE(reached_success);
}

TEST(SubscriptionPlanStorageAllocationFailureTest,
     LoadReprepareClassifiesEveryOwnedAllocationWithoutPoisoningStorage) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  query::test::SnapshotTabletScanFixture fixture{1U};
  const auto query_catalog = catalog(fixture);
  auto storage = SubscriptionPlanStorage::create(config(directory, fixture));
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  const auto installed = storage->install(kSql, query_catalog);
  ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
  const PlanFingerprint fingerprint = installed->plan_fingerprint;

  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 512U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    std::size_t observed = 0U;
    auto loaded = run_with_allocation_failure(
        fail_after, observed, [&] { return storage->load(fingerprint, query_catalog); });
    EXPECT_GT(observed, 0U);
    if (loaded.has_value()) {
      EXPECT_EQ(loaded->fingerprint(), fingerprint);
      reached_success = true;
      break;
    }

    EXPECT_EQ(loaded.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_TRUE(storage->is_usable());
    expect_exact_load(*storage, fingerprint, query_catalog);
  }
  EXPECT_TRUE(reached_success);
}

} // namespace
} // namespace chronos::live
