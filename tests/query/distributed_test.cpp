#include "chronos/common/status.hpp"
#include "chronos/query/distributed.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

[[nodiscard]] schema::TabletId tablet(const std::uint8_t seed) {
  return schema::TabletId::from_uuid(uuid(seed)).value();
}

[[nodiscard]] std::vector<DistributedReadAdmission>
linearizable_admissions(const DistributedAggregatePlan& plan) {
  std::vector<DistributedReadAdmission> admissions;
  for (const DistributedTablet& fragment : plan.fragments) {
    admissions.push_back({fragment.tablet_id, fragment.leader_node, fragment.local_applied_position,
                          fragment.local_applied_position,
                          raft::ReadBarrier{1U, static_cast<std::uint64_t>(admissions.size()) + 1U,
                                            fragment.local_applied_position}});
  }
  return admissions;
}

TEST(DistributedQueryTest, PrunesTabletsMergesPartialStateAndRequiresEveryFragment) {
  const common::Uuid query_id = uuid(1U);
  const std::vector<DistributedTablet> tablets{
      {tablet(2U), 0, 99, 1U, 10U},
      {tablet(3U), 100, 199, 2U, 10U},
      {tablet(4U), 200, 299, 3U, 10U},
  };
  auto plan =
      plan_distributed_aggregation(query_id, tablets, DistributedEventTimePredicate{50, 250});
  ASSERT_TRUE(plan.has_value()) << plan.error().to_string();
  ASSERT_EQ(plan->fragments.size(), 3U);
  auto pruned =
      plan_distributed_aggregation(query_id, tablets, DistributedEventTimePredicate{100, 200});
  ASSERT_TRUE(pruned.has_value());
  ASSERT_EQ(pruned->fragments.size(), 1U);
  EXPECT_EQ(pruned->fragments.front().tablet_id, tablet(3U));

  auto admissions = linearizable_admissions(*plan);
  auto coordinator =
      DistributedAggregateCoordinator::create(std::move(*plan), std::move(admissions));
  ASSERT_TRUE(coordinator.has_value());
  MergeableAggregateState first;
  ASSERT_TRUE(first.add(1.0).is_ok());
  ASSERT_TRUE(first.add(2.0).is_ok());
  MergeableAggregateState second;
  ASSERT_TRUE(second.add(3.0).is_ok());
  MergeableAggregateState third;
  ASSERT_TRUE(third.add(4.0).is_ok());
  EXPECT_TRUE(coordinator->accept({query_id, tablet(2U), 1U, first, true}).is_ok());
  EXPECT_TRUE(coordinator->accept({query_id, tablet(3U), 1U, second, true}).is_ok());
  EXPECT_FALSE(coordinator->finish().has_value());
  EXPECT_TRUE(coordinator->accept({query_id, tablet(4U), 1U, third, true}).is_ok());
  const auto result = coordinator->finish();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->count, 4U);
  EXPECT_DOUBLE_EQ(result->sum, 10.0);
  EXPECT_DOUBLE_EQ(*result->minimum, 1.0);
  EXPECT_DOUBLE_EQ(*result->maximum, 4.0);
  EXPECT_NEAR(*result->variance_population(), 1.25, 1e-12);
}

TEST(DistributedQueryTest, BackpressureCancellationAndWorkerFailureFailClosed) {
  const common::Uuid query_id = uuid(1U);
  auto exchange = BoundedExchange::create(query_id, ExchangeLimits{1U, sizeof(ExchangeMessage)});
  ASSERT_TRUE(exchange.has_value());
  MergeableAggregateState partial;
  ASSERT_TRUE(partial.add(1.0).is_ok());
  EXPECT_TRUE(exchange->push({query_id, tablet(2U), 1U, partial, true}).is_ok());
  const auto full = exchange->push({query_id, tablet(2U), 2U, partial, true});
  EXPECT_EQ(full.code(), common::StatusCode::kResourceExhausted);
  EXPECT_TRUE(exchange->cancel().is_ok());
  EXPECT_EQ(exchange->queued_messages(), 0U);
  EXPECT_EQ(exchange->try_pop().error().code(), common::StatusCode::kCancelled);

  auto plan = plan_distributed_aggregation(query_id, {{tablet(2U), 0, 1, 1U, 1U}}, {});
  ASSERT_TRUE(plan.has_value());
  auto admissions = linearizable_admissions(*plan);
  auto coordinator =
      DistributedAggregateCoordinator::create(std::move(*plan), std::move(admissions));
  ASSERT_TRUE(coordinator.has_value());
  EXPECT_TRUE(coordinator
                  ->worker_failed(tablet(2U),
                                  common::Status{common::StatusCode::kUnavailable, "worker lost"})
                  .is_ok());
  const auto result = coordinator->finish();
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), common::StatusCode::kUnavailable);
}

TEST(DistributedQueryTest, EnforcesExplicitReadConsistencyEvidencePerFragment) {
  const common::Uuid query_id = uuid(5U);
  const DistributedTablet fragment{tablet(6U), 0, 99, 7U, 95U, 100U};
  EXPECT_EQ(plan_distributed_aggregation(query_id, {fragment}, {},
                                         DistributedReadConsistency::kFollowerBoundedStale)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  auto bounded = plan_distributed_aggregation(
      query_id, {fragment}, {},
      DistributedReadPolicy{DistributedReadConsistency::kFollowerBoundedStale, 5U});
  ASSERT_TRUE(bounded.has_value()) << bounded.error().to_string();
  const DistributedReadAdmission within_bound{fragment.tablet_id, 8U, 95U, 100U, std::nullopt};
  EXPECT_TRUE(validate_distributed_read_admission(*bounded, within_bound).is_ok());
  DistributedReadAdmission too_stale = within_bound;
  too_stale.applied_position = 94U;
  EXPECT_EQ(validate_distributed_read_admission(*bounded, too_stale).code(),
            common::StatusCode::kUnavailable);
  DistributedReadAdmission old_observation = within_bound;
  old_observation.observed_leader_commit_position = 99U;
  EXPECT_EQ(validate_distributed_read_admission(*bounded, old_observation).code(),
            common::StatusCode::kUnavailable);
  EXPECT_FALSE(DistributedAggregateCoordinator::create(*bounded, {too_stale}).has_value());

  auto eventual = plan_distributed_aggregation(
      query_id, {fragment}, {},
      DistributedReadPolicy{DistributedReadConsistency::kLocalEventual, std::nullopt});
  ASSERT_TRUE(eventual.has_value());
  EXPECT_TRUE(DistributedAggregateCoordinator::create(
                  std::move(*eventual), {{fragment.tablet_id, 8U, 1U, 0U, std::nullopt}})
                  .has_value());

  auto linearizable = plan_distributed_aggregation(query_id, {fragment}, {});
  ASSERT_TRUE(linearizable.has_value());
  const DistributedReadAdmission unapplied{fragment.tablet_id, fragment.leader_node, 99U, 100U,
                                           raft::ReadBarrier{2U, 9U, 100U}};
  EXPECT_EQ(validate_distributed_read_admission(*linearizable, unapplied).code(),
            common::StatusCode::kUnavailable);
}

} // namespace
} // namespace chronos::query
