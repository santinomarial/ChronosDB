#include "chronos/live/subscription_plan.hpp"
#include "chronos/query/catalog.hpp"
#include "query/snapshot_tablet_scan_test_fixture.hpp"

#include <cstddef>
#include <gtest/gtest.h>
#include <memory>
#include <string_view>
#include <vector>

namespace chronos::live {
namespace {

[[nodiscard]] std::shared_ptr<const query::QueryCatalogSnapshot>
catalog(const query::test::SnapshotTabletScanFixture& fixture) {
  const std::vector<query::QueryCatalogTableInput> tables{
      {.name = "metrics", .quoted = false, .schema = fixture.schema_ptr()}};
  return std::make_shared<const query::QueryCatalogSnapshot>(
      query::QueryCatalogSnapshot::create(1U, tables).value());
}

TEST(SubscriptionPlanTest, PreparesDeterministicSchemaBoundExecutableIdentity) {
  query::test::SnapshotTabletScanFixture fixture{1U};
  constexpr std::string_view sql = "SUBSCRIBE SELECT event_time FROM metrics ORDER BY event_time";
  auto first = prepare_subscription_plan(sql, catalog(fixture));
  auto second = prepare_subscription_plan(sql, catalog(fixture));
  ASSERT_TRUE(first.has_value()) << first.error().status().to_string();
  ASSERT_TRUE(second.has_value()) << second.error().status().to_string();
  EXPECT_EQ(first->fingerprint(), second->fingerprint());
  EXPECT_EQ(first->schema_ptr(), fixture.schema_ptr());
  ASSERT_EQ(first->columns().size(), 1U);
  EXPECT_EQ(first->columns().front().name, "event_time");
  EXPECT_EQ(first->physical_plan().output_columns().size(), 1U);
  const common::Uuid subscription_id = fixture.snapshot().database_id().uuid();
  EXPECT_EQ(first->request(subscription_id).plan_fingerprint, first->fingerprint());
  ResumeTokenMacKey key{};
  key.fill(std::byte{9});
  const SubscriptionSource source =
      first->source(subscription_id, query::test::SnapshotTabletScanFixture::tablet_id(),
                    fixture.snapshot().wal_id(), key);
  EXPECT_EQ(source.plan_fingerprint, first->fingerprint());
  EXPECT_EQ(source.table_id, fixture.schema_ptr()->table_id());
  EXPECT_EQ(source.schema_id, fixture.schema_ptr()->schema_id());
  EXPECT_EQ(source.schema_version, fixture.schema_ptr()->version());

  auto textually_distinct = prepare_subscription_plan(
      "SUBSCRIBE  SELECT event_time FROM metrics ORDER BY event_time", catalog(fixture));
  ASSERT_TRUE(textually_distinct.has_value());
  EXPECT_NE(first->fingerprint(), textually_distinct->fingerprint());
}

TEST(SubscriptionPlanTest, RejectsNonSubscriptionHistoricalAndMultiSourceSql) {
  query::test::SnapshotTabletScanFixture fixture{0U};
  EXPECT_FALSE(
      prepare_subscription_plan("SELECT event_time FROM metrics", catalog(fixture)).has_value());
  EXPECT_FALSE(prepare_subscription_plan(
                   "SUBSCRIBE SELECT event_time FROM metrics FOR SYSTEM_TIME AS OF TIMESTAMP "
                   "'1970-01-01 00:00:00Z'",
                   catalog(fixture))
                   .has_value());
  EXPECT_FALSE(prepare_subscription_plan(
                   "SUBSCRIBE SELECT r.event_time FROM metrics AS l ASOF JOIN metrics AS r ON "
                   "l.event_time = r.event_time AND r.event_time <= l.event_time",
                   catalog(fixture))
                   .has_value());
}

} // namespace
} // namespace chronos::live
