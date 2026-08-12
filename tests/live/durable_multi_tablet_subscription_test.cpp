#include "chronos/live/durable_multi_tablet_subscription.hpp"
#include "chronos/live/subscription_plan_storage.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/query/catalog.hpp"
#include "query/snapshot_tablet_scan_test_fixture.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace chronos::live {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-durable-subscription-XXXXXX").string();
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

template <typename Identifier> [[nodiscard]] Identifier identifier(const std::byte seed) {
  return Identifier::from_uuid(uuid(seed)).value();
}

[[nodiscard]] wal::WalId wal_id(const std::byte seed) {
  wal::WalId value{};
  value.bytes.fill(seed);
  return value;
}

struct Fixture {
  common::Uuid database_id{uuid(std::byte{1})};
  schema::TableId table_id{identifier<schema::TableId>(std::byte{2})};
  schema::TabletId tablet_a{identifier<schema::TabletId>(std::byte{3})};
  schema::TabletId tablet_b{identifier<schema::TabletId>(std::byte{4})};
  wal::WalId wal_a{wal_id(std::byte{5})};
  wal::WalId wal_b{wal_id(std::byte{6})};
  schema::SchemaId schema_id{identifier<schema::SchemaId>(std::byte{7})};
  common::Uuid subscription_id{uuid(std::byte{8})};
  PlanFingerprint plan{};
  ResumeTokenMacKey key{};

  Fixture() {
    plan.fill(std::byte{9});
    key.fill(std::byte{10});
  }

  [[nodiscard]] DurableMultiTabletSubscriptionConfig
  config(const std::filesystem::path& directory) const {
    SubscriptionLimits limits;
    limits.maximum_retained_changes = 2U;
    return {.storage = {.directory_path = directory.string(),
                        .identity = {database_id,
                                     table_id,
                                     plan,
                                     schema_id,
                                     schema::SchemaVersion::initial(),
                                     {{tablet_a, wal_a}, {tablet_b, wal_b}}}},
            .source = {.database_id = database_id,
                       .table_id = table_id,
                       .plan_fingerprint = plan,
                       .schema_id = schema_id,
                       .schema_version = schema::SchemaVersion::initial(),
                       .members = {{tablet_b, wal_b, 0U}, {tablet_a, wal_a, 0U}},
                       .token_key = key},
            .limits = limits};
  }

  [[nodiscard]] SubscriptionRequest request() const {
    return {subscription_id, plan, schema_id, schema::SchemaVersion::initial()};
  }

  [[nodiscard]] CommittedChange change(const schema::TabletId& tablet, const wal::WalId& wal,
                                       const std::uint64_t sequence) const {
    return {.position = {tablet, wal, sequence},
            .schema_id = schema_id,
            .schema_version = schema::SchemaVersion::initial(),
            .operation = LogicalChangeOperation::kUpsert,
            .result_key = {std::byte{static_cast<unsigned char>(sequence)}},
            .payload = {std::byte{static_cast<unsigned char>(sequence + 16U)}}};
  }
};

TEST(DurableMultiTabletSubscriptionTest, RecoversExactGenerationAndPublishesOnlyDurableFrontiers) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  Fixture fixture;
  std::vector<std::byte> safe_token;
  {
    auto owner = DurableMultiTabletSubscription::create_new(fixture.config(directory.path()));
    ASSERT_TRUE(owner.has_value()) << owner.error().to_string();
    EXPECT_TRUE(owner->has_uncheckpointed_changes());
    const auto no_frontiers = owner->durable_retention_frontiers();
    ASSERT_TRUE(no_frontiers.has_value());
    EXPECT_FALSE(no_frontiers->has_value());

    const auto registration = owner->register_subscription(fixture.request());
    ASSERT_TRUE(registration.has_value()) << registration.error().to_string();
    ASSERT_TRUE(owner->complete_snapshot(fixture.subscription_id).is_ok());
    ASSERT_TRUE(
        owner->publish_committed(fixture.change(fixture.tablet_a, fixture.wal_a, 1U)).is_ok());
    ASSERT_TRUE(
        owner->publish_committed(fixture.change(fixture.tablet_b, fixture.wal_b, 1U)).is_ok());
    const auto first_delivery = owner->poll(fixture.subscription_id, 2U);
    ASSERT_TRUE(first_delivery.has_value());
    const auto acknowledged = owner->acknowledge(fixture.subscription_id, 2U);
    ASSERT_TRUE(acknowledged.has_value());
    safe_token = *acknowledged;

    ASSERT_TRUE(
        owner->publish_committed(fixture.change(fixture.tablet_a, fixture.wal_a, 2U)).is_ok());
    ASSERT_TRUE(
        owner->publish_committed(fixture.change(fixture.tablet_b, fixture.wal_b, 2U)).is_ok());
    const auto installed = owner->checkpoint();
    ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
    EXPECT_EQ(installed->checkpoint_generation, 1U);
    EXPECT_FALSE(owner->has_uncheckpointed_changes());
    const auto durable = owner->durable_retention_frontiers();
    ASSERT_TRUE(durable.has_value());
    ASSERT_TRUE(durable->has_value());
    ASSERT_EQ((*durable)->size(), 2U);
    EXPECT_EQ((**durable)[0].record_sequence, 1U);
    EXPECT_EQ((**durable)[1].record_sequence, 1U);

    const auto repeated = owner->checkpoint();
    ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
    EXPECT_TRUE(repeated->already_present);
    ASSERT_TRUE(
        owner->publish_committed(fixture.change(fixture.tablet_a, fixture.wal_a, 3U)).is_ok());
    EXPECT_TRUE(owner->has_uncheckpointed_changes());
    const auto unchanged = owner->durable_retention_frontiers();
    ASSERT_TRUE(unchanged.has_value());
    EXPECT_EQ(*unchanged, *durable);
  }

  auto reopened = DurableMultiTabletSubscription::open_existing(fixture.config(directory.path()));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_EQ(reopened->checkpoint_generation(), 1U);
  EXPECT_FALSE(reopened->has_uncheckpointed_changes());
  const auto latest = reopened->latest_positions();
  ASSERT_TRUE(latest.has_value());
  ASSERT_EQ(latest->size(), 2U);
  EXPECT_EQ((*latest)[0].record_sequence, 2U);
  EXPECT_EQ((*latest)[1].record_sequence, 2U);

  const auto resumed = reopened->resume_subscription(safe_token);
  ASSERT_TRUE(resumed.has_value()) << resumed.error().to_string();
  const auto replayed = reopened->poll(fixture.subscription_id, 2U);
  ASSERT_TRUE(replayed.has_value());
  ASSERT_EQ(replayed->size(), 2U);
  EXPECT_EQ((*replayed)[0].change->position.tablet_id, fixture.tablet_a);
  EXPECT_EQ((*replayed)[0].change->position.record_sequence, 2U);
  EXPECT_EQ((*replayed)[1].change->position.tablet_id, fixture.tablet_b);
  EXPECT_EQ((*replayed)[1].change->position.record_sequence, 2U);
  EXPECT_TRUE(
      reopened->publish_committed(fixture.change(fixture.tablet_a, fixture.wal_a, 3U)).is_ok());
}

TEST(DurableMultiTabletSubscriptionTest, DoesNotAdvanceFrontierWhenInstallationFails) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  Fixture fixture;
  auto owner = DurableMultiTabletSubscription::create_new(fixture.config(directory.path()));
  ASSERT_TRUE(owner.has_value()) << owner.error().to_string();
  ASSERT_TRUE(
      owner->publish_committed(fixture.change(fixture.tablet_a, fixture.wal_a, 1U)).is_ok());
  {
    std::ofstream corrupt{directory.path() / "generation-00000000000000000001.subc",
                          std::ios::binary};
    corrupt.put('x');
  }
  const auto failed = owner->checkpoint();
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kCorruption);
  EXPECT_EQ(owner->checkpoint_generation(), 0U);
  EXPECT_TRUE(owner->has_uncheckpointed_changes());
  const auto frontiers = owner->durable_retention_frontiers();
  ASSERT_TRUE(frontiers.has_value());
  EXPECT_FALSE(frontiers->has_value());
}

TEST(DurableMultiTabletSubscriptionTest, PersistsRecoveryReplayRebaseBeforeReopen) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  Fixture fixture;
  std::vector<std::byte> initial_token;
  {
    auto owner = DurableMultiTabletSubscription::create_new(fixture.config(directory.path()));
    ASSERT_TRUE(owner.has_value()) << owner.error().to_string();
    const auto registration = owner->register_subscription(fixture.request());
    ASSERT_TRUE(registration.has_value()) << registration.error().to_string();
    initial_token = registration->initial_resume_token;

    auto current = owner->latest_positions();
    ASSERT_TRUE(current.has_value()) << current.error().to_string();
    ASSERT_EQ(current->size(), 2U);
    (*current)[0].record_sequence = 4U;
    (*current)[1].record_sequence = 7U;
    ASSERT_TRUE(owner->mark_replay_unavailable_through(*current).is_ok());
    EXPECT_TRUE(owner->has_uncheckpointed_changes());
    const auto installed = owner->checkpoint();
    ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
  }

  auto reopened = DurableMultiTabletSubscription::open_existing(fixture.config(directory.path()));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  const auto latest = reopened->latest_positions();
  ASSERT_TRUE(latest.has_value()) << latest.error().to_string();
  ASSERT_EQ(latest->size(), 2U);
  EXPECT_EQ((*latest)[0].record_sequence, 4U);
  EXPECT_EQ((*latest)[1].record_sequence, 7U);
  EXPECT_EQ(reopened->resume_subscription(initial_token).error().code(),
            common::StatusCode::kNotFound);
}

TEST(DurableMultiTabletSubscriptionTest, RecoversTerminalSchemaInvalidation) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  Fixture fixture;
  std::vector<std::byte> initial_token;
  {
    auto owner = DurableMultiTabletSubscription::create_new(fixture.config(directory.path()));
    ASSERT_TRUE(owner.has_value()) << owner.error().to_string();
    const auto registration = owner->register_subscription(fixture.request());
    ASSERT_TRUE(registration.has_value());
    initial_token = registration->initial_resume_token;
    CommittedChange incompatible = fixture.change(fixture.tablet_a, fixture.wal_a, 1U);
    incompatible.schema_id = identifier<schema::SchemaId>(std::byte{15});
    ASSERT_TRUE(owner->publish_committed(std::move(incompatible)).is_ok());
    const auto status = owner->status(fixture.subscription_id);
    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(status->phase, SubscriptionPhase::kSchemaChanged);
    const auto installed = owner->checkpoint();
    ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
    const auto frontiers = owner->durable_retention_frontiers();
    ASSERT_TRUE(frontiers.has_value());
    ASSERT_TRUE(frontiers->has_value());
    EXPECT_EQ((**frontiers)[0].record_sequence, 1U);
    EXPECT_EQ((**frontiers)[1].record_sequence, 0U);
  }

  auto reopened = DurableMultiTabletSubscription::open_existing(fixture.config(directory.path()));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_EQ(reopened->resume_subscription(initial_token).error().code(),
            common::StatusCode::kNotSupported);
  EXPECT_EQ(reopened->register_subscription(fixture.request()).error().code(),
            common::StatusCode::kNotSupported);
}

TEST(DurableMultiTabletSubscriptionTest, StartsExactSnapshotFromRecoveredExecutablePlan) {
  TemporaryDirectory plan_directory;
  TemporaryDirectory checkpoint_directory;
  ASSERT_FALSE(plan_directory.path().empty());
  ASSERT_FALSE(checkpoint_directory.path().empty());
  query::test::SnapshotTabletScanFixture snapshot{2U, 3U};
  const std::vector<query::QueryCatalogTableInput> tables{
      {.name = "metrics", .quoted = false, .schema = snapshot.schema_ptr()}};
  auto catalog = std::make_shared<const query::QueryCatalogSnapshot>(
      query::QueryCatalogSnapshot::create(1U, tables).value());
  auto plans =
      SubscriptionPlanStorage::create({.directory_path = plan_directory.path().string(),
                                       .database_id = snapshot.snapshot().database_id().uuid()});
  ASSERT_TRUE(plans.has_value()) << plans.error().to_string();
  const auto installed = plans->install("SUBSCRIBE SELECT count(*) AS total FROM metrics", catalog);
  ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
  auto prepared = plans->load(installed->plan_fingerprint, catalog);
  ASSERT_TRUE(prepared.has_value()) << prepared.error().to_string();

  ResumeTokenMacKey key{};
  key.fill(std::byte{13});
  const schema::TabletId tablet_a = query::test::SnapshotTabletScanFixture::tablet_id();
  const schema::TabletId tablet_b = query::test::SnapshotTabletScanFixture::second_tablet_id();
  const wal::WalId wal = snapshot.snapshot().wal_id();
  DurableMultiTabletSubscriptionConfig owner_config{
      .storage = {.directory_path = checkpoint_directory.path().string(),
                  .identity = {snapshot.snapshot().database_id().uuid(),
                               snapshot.schema_ptr()->table_id(),
                               prepared->fingerprint(),
                               snapshot.schema_ptr()->schema_id(),
                               snapshot.schema_ptr()->version(),
                               {{tablet_a, wal}, {tablet_b, wal}}}},
      .source = {.database_id = snapshot.snapshot().database_id().uuid(),
                 .table_id = snapshot.schema_ptr()->table_id(),
                 .plan_fingerprint = prepared->fingerprint(),
                 .schema_id = snapshot.schema_ptr()->schema_id(),
                 .schema_version = snapshot.schema_ptr()->version(),
                 .members = {{tablet_b, wal, 1U}, {tablet_a, wal, 1U}},
                 .token_key = key}};
  auto owner = DurableMultiTabletSubscription::create_new(std::move(owner_config));
  ASSERT_TRUE(owner.has_value()) << owner.error().to_string();
  auto resources = query::QueryResourceContext::create(32U * 1024U * 1024U).value();
  auto subscription =
      owner->start_snapshot(*prepared, uuid(std::byte{14}), resources, snapshot.storage(),
                            snapshot.publisher(), snapshot.lineage());
  ASSERT_TRUE(subscription.has_value()) << subscription.error().to_string();
  const auto rows = subscription->next();
  ASSERT_TRUE(rows.has_value()) << rows.error().to_string();
  const auto decoded = network::decode_query_result_batch(rows->payload);
  ASSERT_TRUE(decoded.has_value());
  ASSERT_EQ(decoded->row_count(), 1U);
  ASSERT_NE(decoded->cell(0U, 0U), nullptr);
  EXPECT_EQ(std::to_integer<std::uint8_t>(decoded->cell(0U, 0U)->value.front()), 5U);
  ASSERT_TRUE(subscription->next().has_value());
  ASSERT_TRUE(subscription->next().has_value());
  EXPECT_TRUE(subscription->ready());
}

} // namespace
} // namespace chronos::live
