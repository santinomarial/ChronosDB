#include "chronos/live/snapshot_subscription.hpp"
#include "chronos/live/subscription.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/network/subscription_messages.hpp"
#include "chronos/query/binder.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/physical_lowering.hpp"
#include "chronos/query/resource_context.hpp"
#include "query/snapshot_tablet_scan_test_fixture.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::live {
namespace {

[[nodiscard]] common::Uuid uuid(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return common::Uuid{bytes};
}

struct BoundPlan {
  query::PhysicalPipelinePlan plan;
  std::vector<SnapshotSubscriptionColumn> columns;
};

[[nodiscard]] BoundPlan lower(const query::test::SnapshotTabletScanFixture& fixture,
                              const std::string_view sql) {
  const std::vector<query::QueryCatalogTableInput> tables{
      {.name = "metrics", .quoted = false, .schema = fixture.schema_ptr()}};
  auto catalog = std::make_shared<const query::QueryCatalogSnapshot>(
      query::QueryCatalogSnapshot::create(1U, tables).value());
  auto parsed = query::parse_sql_v1_select(sql);
  auto bound = query::bind_sql_v1_select(std::move(*parsed), std::move(catalog));
  std::vector<SnapshotSubscriptionColumn> columns;
  columns.reserve(bound->outputs().size());
  for (const query::BoundOutputColumn& output : bound->outputs())
    columns.push_back({output.name, output.type, output.nullable});
  auto plan = query::lower_bound_sql_select(*bound);
  return {std::move(*plan), std::move(columns)};
}

[[nodiscard]] SubscriptionSource source(const query::test::SnapshotTabletScanFixture& fixture) {
  ResumeTokenMacKey key{};
  key.fill(std::byte{9});
  return {.database_id = fixture.snapshot().database_id().uuid(),
          .table_id = fixture.schema_ptr()->table_id(),
          .tablet_id = query::test::SnapshotTabletScanFixture::tablet_id(),
          .wal_id = fixture.snapshot().wal_id(),
          .token_key = key};
}

[[nodiscard]] SubscriptionRequest request(const query::test::SnapshotTabletScanFixture& fixture) {
  PlanFingerprint plan{};
  plan.fill(std::byte{7});
  return {uuid(std::byte{6}), plan, fixture.schema_ptr()->schema_id(),
          fixture.schema_ptr()->version()};
}

[[nodiscard]] CommittedChange change(const query::test::SnapshotTabletScanFixture& fixture,
                                     const std::uint64_t sequence) {
  return {.position = {query::test::SnapshotTabletScanFixture::tablet_id(),
                       fixture.snapshot().wal_id(), sequence},
          .schema_id = fixture.schema_ptr()->schema_id(),
          .schema_version = fixture.schema_ptr()->version(),
          .operation = LogicalChangeOperation::kUpsert,
          .result_key = {std::byte{1}},
          .payload = {std::byte{2}}};
}

TEST(SnapshotSubscriptionTest, ExecutesExactSnapshotThenOpensBufferedLiveSuffix) {
  query::test::SnapshotTabletScanFixture fixture{3U};
  const SubscriptionRequest subscription_request = request(fixture);
  SubscriptionSource configured = source(fixture);
  auto manager = SubscriptionManager::create(std::move(configured));
  ASSERT_TRUE(manager.has_value());
  ASSERT_TRUE(manager->publish_committed(change(fixture, 1U)).is_ok());
  BoundPlan bound = lower(fixture, "SELECT event_time FROM metrics ORDER BY event_time");
  query::QueryResourceContext resources =
      query::QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();

  auto subscription = SnapshotSubscription::start(
      *manager, subscription_request, resources, fixture.storage(), fixture.publisher(),
      query::test::SnapshotTabletScanFixture::tablet_id(), fixture.lineage(),
      fixture.schema_ptr()->schema_id(), bound.plan, std::move(bound.columns));
  ASSERT_TRUE(subscription.has_value()) << subscription.error().to_string();
  ASSERT_TRUE(manager->publish_committed(change(fixture, 2U)).is_ok());
  EXPECT_EQ(manager->poll(subscription_request.subscription_id, 8U).error().code(),
            common::StatusCode::kUnavailable);

  const auto rows = subscription->next();
  ASSERT_TRUE(rows.has_value()) << rows.error().to_string();
  EXPECT_EQ(rows->message_type, network::MessageType::kQueryResult);
  EXPECT_EQ(rows->flags, 0U);
  const auto decoded_rows = network::decode_query_result_batch(rows->payload);
  ASSERT_TRUE(decoded_rows.has_value());
  EXPECT_EQ(decoded_rows->row_count(), 3U);
  ASSERT_EQ(decoded_rows->columns().size(), 1U);
  EXPECT_EQ(decoded_rows->columns().front().name, "event_time");

  const auto end = subscription->next();
  ASSERT_TRUE(end.has_value());
  EXPECT_EQ(end->message_type, network::MessageType::kQueryResult);
  EXPECT_EQ(end->flags, network::kFrameFlagEndStream);
  EXPECT_EQ(network::decode_query_result_batch(end->payload)->row_count(), 0U);
  EXPECT_FALSE(subscription->ready());

  const auto ready = subscription->next();
  ASSERT_TRUE(ready.has_value()) << ready.error().to_string();
  EXPECT_EQ(ready->message_type, network::MessageType::kSubscriptionReady);
  EXPECT_TRUE(network::decode_subscription_ready(ready->payload).has_value());
  EXPECT_TRUE(subscription->ready());
  const auto live = manager->poll(subscription_request.subscription_id, 8U);
  ASSERT_TRUE(live.has_value());
  ASSERT_EQ(live->size(), 1U);
  EXPECT_EQ(live->front().change->position.record_sequence, 2U);
}

TEST(SnapshotSubscriptionTest, RejectsAndCancelsAStorageBoundaryMismatch) {
  query::test::SnapshotTabletScanFixture fixture{1U};
  const SubscriptionRequest subscription_request = request(fixture);
  auto manager = SubscriptionManager::create(source(fixture));
  ASSERT_TRUE(manager.has_value());
  BoundPlan bound = lower(fixture, "SELECT event_time FROM metrics");
  query::QueryResourceContext resources = query::QueryResourceContext::create(1U << 20U).value();

  auto rejected = SnapshotSubscription::start(
      *manager, subscription_request, resources, fixture.storage(), fixture.publisher(),
      query::test::SnapshotTabletScanFixture::tablet_id(), fixture.lineage(),
      fixture.schema_ptr()->schema_id(), bound.plan, std::move(bound.columns));
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kUnavailable);
  ASSERT_TRUE(manager->status(subscription_request.subscription_id).has_value());
  EXPECT_EQ(manager->status(subscription_request.subscription_id)->phase,
            SubscriptionPhase::kCancelled);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

} // namespace
} // namespace chronos::live
