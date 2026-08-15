#include "chronos/live/multi_tablet_snapshot_subscription.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/network/subscription_messages.hpp"
#include "chronos/query/binder.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/physical_lowering.hpp"
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

[[nodiscard]] ResumeTokenMacKey token_key() {
  ResumeTokenMacKey key{};
  key.fill(std::byte{9});
  return key;
}

[[nodiscard]] PlanFingerprint fingerprint() {
  PlanFingerprint plan{};
  plan.fill(std::byte{7});
  return plan;
}

[[nodiscard]] MultiTabletSubscriptionSource
source(const query::test::SnapshotTabletScanFixture& fixture, const std::uint64_t sequence_a,
       const std::uint64_t sequence_b) {
  return {.database_id = fixture.snapshot().database_id().uuid(),
          .table_id = fixture.schema_ptr()->table_id(),
          .plan_fingerprint = fingerprint(),
          .schema_id = fixture.schema_ptr()->schema_id(),
          .schema_version = fixture.schema_ptr()->version(),
          .members = {{query::test::SnapshotTabletScanFixture::second_tablet_id(),
                       fixture.snapshot().wal_id(), sequence_b},
                      {query::test::SnapshotTabletScanFixture::tablet_id(),
                       fixture.snapshot().wal_id(), sequence_a}},
          .token_key = token_key()};
}

[[nodiscard]] SubscriptionRequest request(const query::test::SnapshotTabletScanFixture& fixture) {
  return {uuid(std::byte{6}), fingerprint(), fixture.schema_ptr()->schema_id(),
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

[[nodiscard]] std::uint64_t decode_u64(const common::ByteView bytes) {
  std::uint64_t value = 0U;
  for (std::size_t index = 0U; index < bytes.size(); ++index)
    value |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(bytes[index]))
             << (index * 8U);
  return value;
}

TEST(MultiTabletSnapshotSubscriptionTest, ExecutesOneGlobalPlanBeforeOpeningLiveSuffix) {
  query::test::SnapshotTabletScanFixture fixture{2U, 3U};
  const auto* first =
      fixture.snapshot().find_tablet(query::test::SnapshotTabletScanFixture::tablet_id());
  const auto* second =
      fixture.snapshot().find_tablet(query::test::SnapshotTabletScanFixture::second_tablet_id());
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  EXPECT_TRUE(first->applied_position().has_value());
  EXPECT_TRUE(second->applied_position().has_value());
  auto manager = MultiTabletSubscriptionManager::create(source(fixture, 1U, 1U));
  ASSERT_TRUE(manager.has_value()) << manager.error().to_string();
  const SubscriptionRequest subscription_request = request(fixture);
  BoundPlan bound = lower(fixture, "SELECT count(*) AS total FROM metrics");
  auto resources = query::QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();

  auto subscription = MultiTabletSnapshotSubscription::start(
      *manager, subscription_request, resources, fixture.storage(), fixture.publisher(),
      fixture.lineage(), fixture.schema_ptr()->schema_id(), bound.plan, std::move(bound.columns));
  ASSERT_TRUE(subscription.has_value()) << subscription.error().to_string();
  ASSERT_TRUE(manager->publish_committed(change(fixture, 2U)).is_ok());
  EXPECT_EQ(manager->poll(subscription_request.subscription_id, 4U).error().code(),
            common::StatusCode::kUnavailable);

  const auto result = subscription->next();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_EQ(result->message_type, network::MessageType::kQueryResult);
  const auto decoded = network::decode_query_result_batch(result->payload);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  ASSERT_EQ(decoded->row_count(), 1U);
  ASSERT_NE(decoded->cell(0U, 0U), nullptr);
  EXPECT_EQ(decode_u64(decoded->cell(0U, 0U)->value), 5U);

  const auto end = subscription->next();
  ASSERT_TRUE(end.has_value());
  EXPECT_EQ(end->flags, network::kFrameFlagEndStream);
  const auto ready = subscription->next();
  ASSERT_TRUE(ready.has_value()) << ready.error().to_string();
  EXPECT_EQ(ready->message_type, network::MessageType::kSubscriptionReady);
  EXPECT_TRUE(network::decode_subscription_ready(ready->payload).has_value());
  EXPECT_TRUE(subscription->ready());
  const auto live = manager->poll(subscription_request.subscription_id, 4U);
  ASSERT_TRUE(live.has_value());
  ASSERT_EQ(live->size(), 1U);
  EXPECT_EQ(live->front().change->position.record_sequence, 2U);
}

TEST(MultiTabletSnapshotSubscriptionTest, CancelsWhenAnyTabletBoundaryDisagrees) {
  query::test::SnapshotTabletScanFixture fixture{1U, 1U};
  auto manager = MultiTabletSubscriptionManager::create(source(fixture, 1U, 0U));
  ASSERT_TRUE(manager.has_value());
  const SubscriptionRequest subscription_request = request(fixture);
  BoundPlan bound = lower(fixture, "SELECT event_time FROM metrics");
  auto resources = query::QueryResourceContext::create(std::size_t{8U} * 1024U * 1024U).value();
  auto rejected = MultiTabletSnapshotSubscription::start(
      *manager, subscription_request, resources, fixture.storage(), fixture.publisher(),
      fixture.lineage(), fixture.schema_ptr()->schema_id(), bound.plan, std::move(bound.columns));
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kUnavailable);
  const auto status = manager->status(subscription_request.subscription_id);
  ASSERT_TRUE(status.has_value());
  EXPECT_EQ(status->phase, SubscriptionPhase::kCancelled);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(MultiTabletSnapshotSubscriptionTest, AbandonsWithoutTokenEncodingWhenDriverIsDestroyed) {
  query::test::SnapshotTabletScanFixture fixture{1U, 1U};
  auto manager = MultiTabletSubscriptionManager::create(source(fixture, 1U, 1U));
  ASSERT_TRUE(manager.has_value());
  const SubscriptionRequest subscription_request = request(fixture);
  BoundPlan bound = lower(fixture, "SELECT event_time FROM metrics");
  auto resources = query::QueryResourceContext::create(std::size_t{8U} * 1024U * 1024U).value();
  {
    auto subscription = MultiTabletSnapshotSubscription::start(
        *manager, subscription_request, resources, fixture.storage(), fixture.publisher(),
        fixture.lineage(), fixture.schema_ptr()->schema_id(), bound.plan, std::move(bound.columns));
    ASSERT_TRUE(subscription.has_value()) << subscription.error().to_string();
  }
  const auto status = manager->status(subscription_request.subscription_id);
  ASSERT_TRUE(status.has_value());
  EXPECT_EQ(status->phase, SubscriptionPhase::kCancelled);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

} // namespace
} // namespace chronos::live
