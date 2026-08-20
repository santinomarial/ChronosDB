#include "chronos/live/snapshot_subscription.hpp"
#include "chronos/live/subscription.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/query/binder.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/physical_lowering.hpp"
#include "chronos/query/resource_context.hpp"
#include "query/snapshot_tablet_scan_test_fixture.hpp"
#include "support/failing_allocator.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
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

[[nodiscard]] BoundPlan lower(const query::test::SnapshotTabletScanFixture& fixture) {
  const std::vector<query::QueryCatalogTableInput> tables{
      {.name = "metrics", .quoted = false, .schema = fixture.schema_ptr()}};
  auto catalog = std::make_shared<const query::QueryCatalogSnapshot>(
      query::QueryCatalogSnapshot::create(1U, tables).value());
  auto parsed = query::parse_sql_v1_select("SELECT event_time FROM metrics");
  auto bound = query::bind_sql_v1_select(std::move(*parsed), std::move(catalog));
  std::vector<SnapshotSubscriptionColumn> columns;
  columns.reserve(bound->outputs().size());
  for (const query::BoundOutputColumn& output : bound->outputs())
    columns.push_back({output.name, output.type, output.nullable});
  auto plan = query::lower_bound_sql_select(*bound);
  return {std::move(*plan), std::move(columns)};
}

[[nodiscard]] SubscriptionRequest request(const query::test::SnapshotTabletScanFixture& fixture) {
  PlanFingerprint plan{};
  plan.fill(std::byte{7});
  return {uuid(std::byte{6}), plan, fixture.schema_ptr()->schema_id(),
          fixture.schema_ptr()->version()};
}

[[nodiscard]] SubscriptionSource source(const query::test::SnapshotTabletScanFixture& fixture,
                                        const PlanFingerprint& plan) {
  ResumeTokenMacKey key{};
  key.fill(std::byte{9});
  return {.database_id = fixture.snapshot().database_id().uuid(),
          .table_id = fixture.schema_ptr()->table_id(),
          .tablet_id = query::test::SnapshotTabletScanFixture::tablet_id(),
          .wal_id = fixture.snapshot().wal_id(),
          .plan_fingerprint = plan,
          .schema_id = fixture.schema_ptr()->schema_id(),
          .schema_version = fixture.schema_ptr()->version(),
          .token_key = key};
}

[[nodiscard]] CommittedChange
initial_change(const query::test::SnapshotTabletScanFixture& fixture) {
  return {.position = {query::test::SnapshotTabletScanFixture::tablet_id(),
                       fixture.snapshot().wal_id(), 1U},
          .schema_id = fixture.schema_ptr()->schema_id(),
          .schema_version = fixture.schema_ptr()->version(),
          .operation = LogicalChangeOperation::kUpsert,
          .result_key = {std::byte{1}},
          .payload = {std::byte{2}}};
}

[[nodiscard]] SnapshotSubscriptionLimits one_row_limits() {
  SnapshotSubscriptionLimits limits;
  limits.pipeline.scan.head.chunk.maximum_rows = 1U;
  return limits;
}

void expect_failed_owner_is_abandoned(SubscriptionManager& manager,
                                      const common::Uuid& subscription_id) {
  const auto status = manager.status(subscription_id);
  if (!status.has_value()) {
    EXPECT_EQ(status.error().code(), common::StatusCode::kNotFound);
    return;
  }
  EXPECT_EQ(status->phase, SubscriptionPhase::kCancelled);
  EXPECT_EQ(status->buffered_changes, 0U);
  EXPECT_EQ(status->buffered_bytes, 0U);
}

TEST(SnapshotSubscriptionAllocationFailureTest, StartClassifiesAndRollsBackEveryOwnedAllocation) {
  query::test::SnapshotTabletScanFixture fixture{3U};
  const SubscriptionRequest subscription_request = request(fixture);
  const BoundPlan bound = lower(fixture);
  bool reached_success = false;

  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    auto manager =
        SubscriptionManager::create(source(fixture, subscription_request.plan_fingerprint));
    ASSERT_TRUE(manager.has_value());
    ASSERT_TRUE(manager->publish_committed(initial_change(fixture)).is_ok());
    query::QueryResourceContext resources =
        query::QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
    std::vector<SnapshotSubscriptionColumn> columns = bound.columns;
    std::optional<common::Result<SnapshotSubscription>> result;
    std::size_t observed = 0U;
    {
      ::chronos::test::ScopedAllocationFailure failure{fail_after};
      result.emplace(SnapshotSubscription::start(
          *manager, subscription_request, resources, fixture.storage(), fixture.publisher(),
          query::test::SnapshotTabletScanFixture::tablet_id(), fixture.lineage(),
          fixture.schema_ptr()->schema_id(), bound.plan, std::move(columns), one_row_limits()));
      observed = failure.observed_allocations();
      failure.disable();
    }
    EXPECT_GT(observed, 0U);
    if (result->has_value()) {
      reached_success = true;
      result.reset();
      EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
      break;
    }
    EXPECT_EQ(result->error().code(), common::StatusCode::kResourceExhausted);
    expect_failed_owner_is_abandoned(*manager, subscription_request.subscription_id);
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }

  EXPECT_TRUE(reached_success);
}

struct OutputTransition {
  std::string_view name;
  std::size_t completed_outputs;
  network::MessageType expected_type;
  std::uint32_t expected_flags;
};

TEST(SnapshotSubscriptionAllocationFailureTest,
     PullEndAndReadyClassifyAndAbandonEveryOwnedAllocation) {
  constexpr std::array<OutputTransition, 3U> transitions{{
      {"first pull", 0U, network::MessageType::kQueryResult, 0U},
      {"end stream", 3U, network::MessageType::kQueryResult, network::kFrameFlagEndStream},
      {"ready", 4U, network::MessageType::kSubscriptionReady, 0U},
  }};
  query::test::SnapshotTabletScanFixture fixture{3U};
  const SubscriptionRequest subscription_request = request(fixture);
  const BoundPlan bound = lower(fixture);

  for (const OutputTransition& transition : transitions) {
    SCOPED_TRACE(transition.name);
    bool reached_success = false;
    for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
      SCOPED_TRACE(fail_after);
      auto manager =
          SubscriptionManager::create(source(fixture, subscription_request.plan_fingerprint));
      ASSERT_TRUE(manager.has_value());
      ASSERT_TRUE(manager->publish_committed(initial_change(fixture)).is_ok());
      query::QueryResourceContext resources =
          query::QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
      auto started = SnapshotSubscription::start(
          *manager, subscription_request, resources, fixture.storage(), fixture.publisher(),
          query::test::SnapshotTabletScanFixture::tablet_id(), fixture.lineage(),
          fixture.schema_ptr()->schema_id(), bound.plan, bound.columns, one_row_limits());
      ASSERT_TRUE(started.has_value()) << started.error().to_string();
      std::optional<SnapshotSubscription> subscription;
      subscription.emplace(std::move(*started));
      for (std::size_t output = 0U; output < transition.completed_outputs; ++output) {
        auto completed = subscription->next();
        ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
      }

      std::optional<common::Result<SnapshotSubscriptionOutput>> result;
      std::size_t observed = 0U;
      {
        ::chronos::test::ScopedAllocationFailure failure{fail_after};
        result.emplace(subscription->next());
        observed = failure.observed_allocations();
        failure.disable();
      }
      EXPECT_GT(observed, 0U);
      if (result->has_value()) {
        EXPECT_EQ(result->value().message_type, transition.expected_type);
        EXPECT_EQ(result->value().flags, transition.expected_flags);
        reached_success = true;
        result.reset();
        subscription.reset();
        EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
        break;
      }
      EXPECT_EQ(result->error().code(), common::StatusCode::kResourceExhausted);
      expect_failed_owner_is_abandoned(*manager, subscription_request.subscription_id);
      EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
    }
    EXPECT_TRUE(reached_success);
  }
}

} // namespace
} // namespace chronos::live
