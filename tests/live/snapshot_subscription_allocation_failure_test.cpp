#include "chronos/live/multi_tablet_snapshot_subscription.hpp"
#include "chronos/live/snapshot_subscription.hpp"
#include "chronos/live/subscription.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/query/binder.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/physical_lowering.hpp"
#include "chronos/query/resource_context.hpp"
#include "columnar/columnar_test_support.hpp"
#include "query/snapshot_tablet_scan_test_fixture.hpp"
#include "support/failing_allocator.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
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
                              const std::string_view sql = "SELECT event_time FROM metrics") {
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

template <typename Manager>
void expect_failed_owner_is_abandoned(Manager& manager, const common::Uuid& subscription_id) {
  const auto status = manager.status(subscription_id);
  if (!status.has_value()) {
    EXPECT_EQ(status.error().code(), common::StatusCode::kNotFound);
    return;
  }
  EXPECT_EQ(status->phase, SubscriptionPhase::kCancelled);
  EXPECT_EQ(status->buffered_changes, 0U);
  EXPECT_EQ(status->buffered_bytes, 0U);
}

[[nodiscard]] MultiTabletSubscriptionSource
multi_source(const query::test::SnapshotTabletScanFixture& fixture) {
  return {.database_id = fixture.snapshot().database_id().uuid(),
          .table_id = fixture.schema_ptr()->table_id(),
          .plan_fingerprint = request(fixture).plan_fingerprint,
          .schema_id = fixture.schema_ptr()->schema_id(),
          .schema_version = fixture.schema_ptr()->version(),
          .members = {{query::test::SnapshotTabletScanFixture::second_tablet_id(),
                       fixture.snapshot().wal_id(), 1U},
                      {query::test::SnapshotTabletScanFixture::tablet_id(),
                       fixture.snapshot().wal_id(), 1U}},
          .token_key = source(fixture, request(fixture).plan_fingerprint).token_key};
}

[[nodiscard]] raft::GroupId raft_group_id() {
  return uuid(std::byte{0x71U});
}

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-live-allocation-XXXXXX").string();
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

[[nodiscard]] schema::TabletId raft_tablet_id() {
  return columnar::test::id<schema::TabletId>(81U);
}

[[nodiscard]] std::shared_ptr<ingest::AsyncRaftTabletApplication>
empty_raft_application(const query::test::SnapshotTabletScanFixture& fixture) {
  auto tablet = ingest::TabletState::create(
      fixture.schema_ptr(), raft_tablet_id(),
      {.head_capacity = {.row_capacity = 4U, .variable_value_bytes = {0U}},
       .maximum_schema_versions = 1U,
       .maximum_sealed_generations = 1U,
       .maximum_retry_entries = 4U});
  if (!tablet.has_value())
    std::abort();
  auto retries = ingest::RetryDirectory::create({.maximum_entries = 4U});
  if (!retries.has_value())
    std::abort();
  std::vector<ingest::AsyncRaftTabletApplicationConfig> configured;
  configured.push_back({.group_id = raft_group_id(),
                        .snapshot_storage = std::nullopt,
                        .retry_directory = std::move(*retries),
                        .tablet = std::move(*tablet),
                        .retained_schemas = {fixture.schema_ptr()},
                        .decode_limits = {}});
  auto application = ingest::AsyncRaftTabletApplication::create(std::move(configured));
  if (!application.has_value())
    std::abort();
  return std::move(*application);
}

[[nodiscard]] MultiTabletSubscriptionSource
raft_source(const query::test::SnapshotTabletScanFixture& fixture) {
  return {.database_id = fixture.snapshot().database_id().uuid(),
          .table_id = fixture.schema_ptr()->table_id(),
          .plan_fingerprint = request(fixture).plan_fingerprint,
          .schema_id = fixture.schema_ptr()->schema_id(),
          .schema_version = fixture.schema_ptr()->version(),
          .members = {MultiTabletSubscriptionMember::raft(raft_tablet_id(), raft_group_id(), 0U)},
          .token_key = source(fixture, request(fixture).plan_fingerprint).token_key};
}

[[nodiscard]] MultiTabletSubscriptionSource
mixed_source(const query::test::SnapshotTabletScanFixture& fixture) {
  return {.database_id = fixture.snapshot().database_id().uuid(),
          .table_id = fixture.schema_ptr()->table_id(),
          .plan_fingerprint = request(fixture).plan_fingerprint,
          .schema_id = fixture.schema_ptr()->schema_id(),
          .schema_version = fixture.schema_ptr()->version(),
          .members = {{query::test::SnapshotTabletScanFixture::tablet_id(),
                       fixture.snapshot().wal_id(), 1U},
                      MultiTabletSubscriptionMember::raft(raft_tablet_id(), raft_group_id(), 0U)},
          .token_key = source(fixture, request(fixture).plan_fingerprint).token_key};
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

TEST(MultiTabletSnapshotSubscriptionAllocationFailureTest,
     StartClassifiesAndRollsBackEveryOwnedAllocation) {
  query::test::SnapshotTabletScanFixture fixture{2U, 3U};
  const SubscriptionRequest subscription_request = request(fixture);
  const BoundPlan bound = lower(fixture, "SELECT count(*) AS total FROM metrics");
  bool reached_success = false;

  for (std::size_t fail_after = 0U; fail_after < 512U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    auto manager = MultiTabletSubscriptionManager::create(multi_source(fixture));
    ASSERT_TRUE(manager.has_value());
    query::QueryResourceContext resources =
        query::QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
    std::vector<SnapshotSubscriptionColumn> columns = bound.columns;
    std::optional<common::Result<MultiTabletSnapshotSubscription>> result;
    std::size_t observed = 0U;
    {
      ::chronos::test::ScopedAllocationFailure failure{fail_after};
      result.emplace(MultiTabletSnapshotSubscription::start(
          *manager, subscription_request, resources, fixture.storage(), fixture.publisher(),
          fixture.lineage(), fixture.schema_ptr()->schema_id(), bound.plan, std::move(columns),
          one_row_limits()));
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

TEST(MultiTabletSnapshotSubscriptionAllocationFailureTest,
     PullEndAndReadyClassifyAndAbandonEveryOwnedAllocation) {
  constexpr std::array<OutputTransition, 3U> transitions{{
      {"first pull", 0U, network::MessageType::kQueryResult, 0U},
      {"end stream", 1U, network::MessageType::kQueryResult, network::kFrameFlagEndStream},
      {"ready", 2U, network::MessageType::kSubscriptionReady, 0U},
  }};
  query::test::SnapshotTabletScanFixture fixture{2U, 3U};
  const SubscriptionRequest subscription_request = request(fixture);
  const BoundPlan bound = lower(fixture, "SELECT count(*) AS total FROM metrics");

  for (const OutputTransition& transition : transitions) {
    SCOPED_TRACE(transition.name);
    bool reached_success = false;
    for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
      SCOPED_TRACE(fail_after);
      auto manager = MultiTabletSubscriptionManager::create(multi_source(fixture));
      ASSERT_TRUE(manager.has_value());
      query::QueryResourceContext resources =
          query::QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
      auto started = MultiTabletSnapshotSubscription::start(
          *manager, subscription_request, resources, fixture.storage(), fixture.publisher(),
          fixture.lineage(), fixture.schema_ptr()->schema_id(), bound.plan, bound.columns,
          one_row_limits());
      ASSERT_TRUE(started.has_value()) << started.error().to_string();
      std::optional<MultiTabletSnapshotSubscription> subscription;
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

TEST(MultiTabletSnapshotSubscriptionAllocationFailureTest,
     RaftStartClassifiesAndRollsBackEveryOwnedAllocation) {
  query::test::SnapshotTabletScanFixture fixture{1U};
  const auto application = empty_raft_application(fixture);
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  auto runtime = raft::AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{raft_group_id(), {1U}}}, {},
      application);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  const SubscriptionRequest subscription_request = request(fixture);
  const BoundPlan bound = lower(fixture, "SELECT count(*) AS total FROM metrics");
  bool reached_success = false;

  for (std::size_t fail_after = 0U; fail_after < 512U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    auto manager = MultiTabletSubscriptionManager::create(raft_source(fixture));
    ASSERT_TRUE(manager.has_value()) << manager.error().to_string();
    query::QueryResourceContext resources =
        query::QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
    std::vector<SnapshotSubscriptionColumn> columns = bound.columns;
    std::optional<common::Result<MultiTabletSnapshotSubscription>> result;
    std::size_t observed = 0U;
    {
      ::chronos::test::ScopedAllocationFailure failure{fail_after};
      result.emplace(MultiTabletSnapshotSubscription::start_raft(
          *manager, subscription_request, resources, *application, fixture.lineage(),
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
  EXPECT_TRUE(runtime->shutdown().is_ok());
}

TEST(MultiTabletSnapshotSubscriptionAllocationFailureTest,
     MixedStartClassifiesAndRollsBackEveryOwnedAllocation) {
  query::test::SnapshotTabletScanFixture fixture{3U};
  const auto application = empty_raft_application(fixture);
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  auto runtime = raft::AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{raft_group_id(), {1U}}}, {},
      application);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  const SubscriptionRequest subscription_request = request(fixture);
  const BoundPlan bound = lower(fixture, "SELECT count(*) AS total FROM metrics");
  bool reached_success = false;

  for (std::size_t fail_after = 0U; fail_after < 512U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    auto manager = MultiTabletSubscriptionManager::create(mixed_source(fixture));
    ASSERT_TRUE(manager.has_value()) << manager.error().to_string();
    query::QueryResourceContext resources =
        query::QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
    std::vector<SnapshotSubscriptionColumn> columns = bound.columns;
    std::optional<common::Result<MultiTabletSnapshotSubscription>> result;
    std::size_t observed = 0U;
    {
      ::chronos::test::ScopedAllocationFailure failure{fail_after};
      result.emplace(MultiTabletSnapshotSubscription::start_mixed(
          *manager, subscription_request, resources, fixture.storage(), fixture.publisher(),
          *application, fixture.lineage(), fixture.schema_ptr()->schema_id(), bound.plan,
          std::move(columns), one_row_limits()));
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
  EXPECT_TRUE(runtime->shutdown().is_ok());
}

} // namespace
} // namespace chronos::live
