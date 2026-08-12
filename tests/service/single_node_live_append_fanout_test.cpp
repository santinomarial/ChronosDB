#include "chronos/network/messages.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/service/single_node_live_append_fanout.hpp"
#include "columnar/columnar_test_support.hpp"

#include <array>
#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::service {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-live-fanout-XXXXXX").string();
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

[[nodiscard]] common::Uuid uuid(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return common::Uuid{bytes};
}

[[nodiscard]] schema::TabletId tablet_id() {
  return schema::TabletId::from_uuid(uuid(std::byte{3})).value();
}

[[nodiscard]] wal::WalId wal_id() {
  wal::WalId value{};
  value.bytes.fill(std::byte{4});
  return value;
}

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch> batch() {
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(columnar::test::batch_schema(),
                                           columnar::test::batch_columns())
          .value());
}

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch> successor_batch() {
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(columnar::test::successor_batch_schema(),
                                           columnar::test::successor_batch_columns())
          .value());
}

[[nodiscard]] std::shared_ptr<const query::QueryCatalogSnapshot>
catalog(const std::shared_ptr<const columnar::OwnedColumnarBatch>& input) {
  const std::array tables{query::QueryCatalogTableInput{
      .name = "events", .quoted = false, .schema = input->schema_ptr()}};
  return std::make_shared<const query::QueryCatalogSnapshot>(
      query::QueryCatalogSnapshot::create(1U, tables).value());
}

[[nodiscard]] live::DurableMultiTabletSubscriptionConfig
config(const std::filesystem::path& directory, const live::PreparedSubscriptionPlan& plan,
       live::SubscriptionLimits limits = {}) {
  live::ResumeTokenMacKey key{};
  key.fill(std::byte{5});
  live::MultiTabletSubscriptionSource source{.database_id = uuid(std::byte{1}),
                                             .table_id = plan.schema_ptr()->table_id(),
                                             .plan_fingerprint = plan.fingerprint(),
                                             .schema_id = plan.schema_ptr()->schema_id(),
                                             .schema_version = plan.schema_ptr()->version(),
                                             .members = {{tablet_id(), wal_id(), 0U}},
                                             .token_key = key};
  return {.storage = {.directory_path = directory.string(),
                      .identity = {source.database_id,
                                   source.table_id,
                                   source.plan_fingerprint,
                                   source.schema_id,
                                   source.schema_version,
                                   {{tablet_id(), wal_id()}}}},
          .source = std::move(source),
          .limits = limits};
}

[[nodiscard]] AppliedSingleNodeColumnarAppend
applied(const std::shared_ptr<const columnar::OwnedColumnarBatch>& input,
        const std::uint64_t sequence = 1U) {
  return {.tablet_id = tablet_id(),
          .position = {.wal_id = wal_id(), .record_sequence = sequence},
          .batch = input,
          .outcome = {}};
}

TEST(SingleNodeLiveAppendFanoutTest, EvaluatesAndPublishesOneAppliedAppend) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const auto input = batch();
  auto plan = live::prepare_subscription_plan("SUBSCRIBE SELECT tag FROM events WHERE enabled",
                                              catalog(input));
  ASSERT_TRUE(plan.has_value()) << plan.error().status().to_string();
  auto coordinator =
      live::DurableMultiTabletSubscription::create_new(config(directory.path(), *plan));
  ASSERT_TRUE(coordinator.has_value()) << coordinator.error().to_string();
  const common::Uuid subscription_id = uuid(std::byte{6});
  ASSERT_TRUE(coordinator->register_subscription(plan->request(subscription_id)).has_value());
  ASSERT_TRUE(coordinator->complete_snapshot(subscription_id).is_ok());
  query::QueryResourceContext resources = query::QueryResourceContext::create(1U << 20U).value();
  auto fanout = SingleNodeLiveAppendFanout::create({{&*plan, &*coordinator, &resources, {}}});
  ASSERT_TRUE(fanout.has_value()) << fanout.error().to_string();

  (*fanout)->on_applied(applied(input));

  auto delivery = coordinator->poll(subscription_id, 1U);
  ASSERT_TRUE(delivery.has_value()) << delivery.error().to_string();
  ASSERT_EQ(delivery->size(), 1U);
  ASSERT_NE(delivery->front().change, nullptr);
  EXPECT_EQ(delivery->front().change->position.record_sequence, 1U);
  auto decoded = network::decode_query_result_batch(delivery->front().change->payload);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->row_count(), 1U);
  const SingleNodeLiveAppendFanoutMetrics metrics = (*fanout)->metrics();
  EXPECT_EQ(metrics.observed_appends, 1U);
  EXPECT_EQ(metrics.evaluated_plans, 1U);
  EXPECT_EQ(metrics.published_changes, 1U);
  EXPECT_EQ(metrics.continuity_losses, 0U);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(SingleNodeLiveAppendFanoutTest, ContainsPostApplyPublicationFailureAsContinuityLoss) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const auto input = batch();
  auto plan = live::prepare_subscription_plan("SUBSCRIBE SELECT tag FROM events", catalog(input));
  ASSERT_TRUE(plan.has_value());
  live::SubscriptionLimits limits;
  limits.maximum_change_bytes = 1U;
  auto coordinator =
      live::DurableMultiTabletSubscription::create_new(config(directory.path(), *plan, limits));
  ASSERT_TRUE(coordinator.has_value()) << coordinator.error().to_string();
  const common::Uuid subscription_id = uuid(std::byte{7});
  ASSERT_TRUE(coordinator->register_subscription(plan->request(subscription_id)).has_value());
  ASSERT_TRUE(coordinator->complete_snapshot(subscription_id).is_ok());
  query::QueryResourceContext resources = query::QueryResourceContext::create(1U << 20U).value();
  auto fanout = SingleNodeLiveAppendFanout::create({{&*plan, &*coordinator, &resources, {}}});
  ASSERT_TRUE(fanout.has_value());

  (*fanout)->on_applied(applied(input));

  auto status = coordinator->status(subscription_id);
  ASSERT_TRUE(status.has_value()) << status.error().to_string();
  EXPECT_EQ(status->phase, live::SubscriptionPhase::kOverflowed);
  auto positions = coordinator->latest_positions();
  ASSERT_TRUE(positions.has_value());
  ASSERT_EQ(positions->size(), 1U);
  EXPECT_EQ(positions->front().record_sequence, 1U);
  const SingleNodeLiveAppendFanoutMetrics metrics = (*fanout)->metrics();
  EXPECT_EQ(metrics.publication_failures, 1U);
  EXPECT_EQ(metrics.continuity_losses, 1U);
  EXPECT_EQ(metrics.containment_failures, 0U);
  EXPECT_EQ(metrics.disabled_plans, 0U);
}

TEST(SingleNodeLiveAppendFanoutTest, ContainsPostApplyEvaluationFailureAsContinuityLoss) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const auto input = batch();
  auto plan = live::prepare_subscription_plan("SUBSCRIBE SELECT tag FROM events", catalog(input));
  ASSERT_TRUE(plan.has_value());
  auto coordinator =
      live::DurableMultiTabletSubscription::create_new(config(directory.path(), *plan));
  ASSERT_TRUE(coordinator.has_value()) << coordinator.error().to_string();
  const common::Uuid subscription_id = uuid(std::byte{9});
  ASSERT_TRUE(coordinator->register_subscription(plan->request(subscription_id)).has_value());
  ASSERT_TRUE(coordinator->complete_snapshot(subscription_id).is_ok());
  query::QueryResourceContext resources = query::QueryResourceContext::create(1U).value();
  auto fanout = SingleNodeLiveAppendFanout::create({{&*plan, &*coordinator, &resources, {}}});
  ASSERT_TRUE(fanout.has_value());

  (*fanout)->on_applied(applied(input));

  auto status = coordinator->status(subscription_id);
  ASSERT_TRUE(status.has_value()) << status.error().to_string();
  EXPECT_EQ(status->phase, live::SubscriptionPhase::kOverflowed);
  const SingleNodeLiveAppendFanoutMetrics metrics = (*fanout)->metrics();
  EXPECT_EQ(metrics.evaluation_failures, 1U);
  EXPECT_EQ(metrics.continuity_losses, 1U);
  EXPECT_EQ(metrics.publication_failures, 0U);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(SingleNodeLiveAppendFanoutTest, TerminatesOldPlanWhenAppliedSchemaChanges) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const auto input = batch();
  auto plan = live::prepare_subscription_plan("SUBSCRIBE SELECT tag FROM events", catalog(input));
  ASSERT_TRUE(plan.has_value());
  auto coordinator =
      live::DurableMultiTabletSubscription::create_new(config(directory.path(), *plan));
  ASSERT_TRUE(coordinator.has_value()) << coordinator.error().to_string();
  const common::Uuid subscription_id = uuid(std::byte{8});
  ASSERT_TRUE(coordinator->register_subscription(plan->request(subscription_id)).has_value());
  ASSERT_TRUE(coordinator->complete_snapshot(subscription_id).is_ok());
  query::QueryResourceContext resources = query::QueryResourceContext::create(1U << 20U).value();
  auto fanout = SingleNodeLiveAppendFanout::create({{&*plan, &*coordinator, &resources, {}}});
  ASSERT_TRUE(fanout.has_value());

  (*fanout)->on_applied(applied(successor_batch()));

  auto status = coordinator->status(subscription_id);
  ASSERT_TRUE(status.has_value()) << status.error().to_string();
  EXPECT_EQ(status->phase, live::SubscriptionPhase::kSchemaChanged);
  const SingleNodeLiveAppendFanoutMetrics metrics = (*fanout)->metrics();
  EXPECT_EQ(metrics.schema_invalidations, 1U);
  EXPECT_EQ(metrics.evaluated_plans, 0U);
  EXPECT_EQ(metrics.continuity_losses, 0U);
}

TEST(SingleNodeLiveAppendFanoutTest, RejectsUnsupportedAndDuplicateBindingsAtAdmission) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const auto input = batch();
  auto aggregate =
      live::prepare_subscription_plan("SUBSCRIBE SELECT count(*) FROM events", catalog(input));
  ASSERT_TRUE(aggregate.has_value());
  auto coordinator =
      live::DurableMultiTabletSubscription::create_new(config(directory.path(), *aggregate));
  ASSERT_TRUE(coordinator.has_value()) << coordinator.error().to_string();
  query::QueryResourceContext resources = query::QueryResourceContext::create(1U << 20U).value();
  auto rejected =
      SingleNodeLiveAppendFanout::create({{&*aggregate, &*coordinator, &resources, {}}});
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kNotSupported);

  TemporaryDirectory row_directory;
  auto row = live::prepare_subscription_plan("SUBSCRIBE SELECT tag FROM events", catalog(input));
  ASSERT_TRUE(row.has_value());
  auto row_coordinator =
      live::DurableMultiTabletSubscription::create_new(config(row_directory.path(), *row));
  ASSERT_TRUE(row_coordinator.has_value());
  rejected = SingleNodeLiveAppendFanout::create(
      {{&*row, &*row_coordinator, &resources, {}}, {&*row, &*row_coordinator, &resources, {}}});
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::service
