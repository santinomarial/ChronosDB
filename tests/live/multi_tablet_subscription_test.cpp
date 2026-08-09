#include "chronos/live/multi_tablet_subscription.hpp"
#include "chronos/live/subscription_protocol.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <vector>

namespace chronos::live {
namespace {

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

[[nodiscard]] ResumeTokenMacKey token_key() {
  ResumeTokenMacKey value{};
  value.fill(std::byte{9});
  return value;
}

struct Fixture {
  schema::TabletId tablet_a{identifier<schema::TabletId>(std::byte{1})};
  schema::TabletId tablet_b{identifier<schema::TabletId>(std::byte{2})};
  wal::WalId wal_a{wal_id(std::byte{3})};
  wal::WalId wal_b{wal_id(std::byte{4})};
  schema::SchemaId schema_id{identifier<schema::SchemaId>(std::byte{5})};
  common::Uuid subscription_id{uuid(std::byte{6})};
  PlanFingerprint plan{};

  Fixture() {
    plan.fill(std::byte{7});
  }

  [[nodiscard]] MultiTabletSubscriptionSource source(const std::uint64_t initial_a = 0U,
                                                     const std::uint64_t initial_b = 0U) const {
    return {.database_id = uuid(std::byte{8}),
            .table_id = identifier<schema::TableId>(std::byte{10}),
            .plan_fingerprint = plan,
            .schema_id = schema_id,
            .schema_version = schema::SchemaVersion::initial(),
            .members = {{tablet_b, wal_b, initial_b}, {tablet_a, wal_a, initial_a}},
            .token_key = token_key()};
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

TEST(MultiTabletSubscriptionTest, CapturesVectorAndReplaysRecordedCrossTabletOrder) {
  Fixture fixture;
  auto manager = MultiTabletSubscriptionManager::create(fixture.source(10U, 20U));
  ASSERT_TRUE(manager.has_value()) << manager.error().to_string();
  const auto registration = manager->register_subscription(fixture.request());
  ASSERT_TRUE(registration.has_value()) << registration.error().to_string();
  ASSERT_EQ(registration->snapshot_boundaries.size(), 2U);
  EXPECT_TRUE(network::decode_subscription_ready(*encode_subscription_registration(*registration))
                  .has_value());
  EXPECT_EQ(registration->snapshot_boundaries[0].tablet_id, fixture.tablet_a);
  EXPECT_EQ(registration->snapshot_boundaries[0].record_sequence, 10U);
  EXPECT_EQ(registration->snapshot_boundaries[1].tablet_id, fixture.tablet_b);
  EXPECT_EQ(registration->snapshot_boundaries[1].record_sequence, 20U);

  ASSERT_TRUE(
      manager->publish_committed(fixture.change(fixture.tablet_a, fixture.wal_a, 11U)).is_ok());
  ASSERT_TRUE(
      manager->publish_committed(fixture.change(fixture.tablet_b, fixture.wal_b, 21U)).is_ok());
  ASSERT_TRUE(
      manager->publish_committed(fixture.change(fixture.tablet_a, fixture.wal_a, 12U)).is_ok());
  EXPECT_EQ(manager->poll(fixture.subscription_id, 8U).error().code(),
            common::StatusCode::kUnavailable);
  ASSERT_TRUE(manager->complete_snapshot(fixture.subscription_id).is_ok());
  const auto delivered = manager->poll(fixture.subscription_id, 8U);
  ASSERT_TRUE(delivered.has_value());
  ASSERT_EQ(delivered->size(), 3U);
  EXPECT_EQ((*delivered)[0].delivery_sequence, 1U);
  EXPECT_EQ((*delivered)[0].change->position.tablet_id, fixture.tablet_a);
  EXPECT_EQ((*delivered)[1].delivery_sequence, 2U);
  EXPECT_EQ((*delivered)[1].change->position.tablet_id, fixture.tablet_b);
  EXPECT_EQ((*delivered)[2].delivery_sequence, 3U);
  EXPECT_EQ((*delivered)[2].change->position.tablet_id, fixture.tablet_a);

  const auto safe_token = manager->acknowledge(fixture.subscription_id, 2U);
  ASSERT_TRUE(safe_token.has_value());
  const auto decoded = decode_resume_token_v1(*safe_token, token_key());
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->safe_delivery_sequence, 2U);
  EXPECT_EQ(decoded->source_positions[0].record_sequence, 11U);
  EXPECT_EQ(decoded->source_positions[1].record_sequence, 21U);

  ASSERT_TRUE(
      manager->publish_committed(fixture.change(fixture.tablet_b, fixture.wal_b, 22U)).is_ok());
  ASSERT_TRUE(manager->cancel(fixture.subscription_id).has_value());
  const auto resumed = manager->resume_subscription(*safe_token);
  ASSERT_TRUE(resumed.has_value()) << resumed.error().to_string();
  const auto replayed = manager->poll(fixture.subscription_id, 8U);
  ASSERT_TRUE(replayed.has_value());
  ASSERT_EQ(replayed->size(), 2U);
  EXPECT_EQ((*replayed)[0].delivery_sequence, 3U);
  EXPECT_EQ((*replayed)[0].change->position.tablet_id, fixture.tablet_a);
  EXPECT_EQ((*replayed)[0].change->position.record_sequence, 12U);
  EXPECT_EQ((*replayed)[1].delivery_sequence, 4U);
  EXPECT_EQ((*replayed)[1].change->position.tablet_id, fixture.tablet_b);
  EXPECT_EQ((*replayed)[1].change->position.record_sequence, 22U);
}

TEST(MultiTabletSubscriptionTest, RejectsPerSourceGapsPlanMismatchAndSourceSetMismatch) {
  Fixture fixture;
  auto manager = MultiTabletSubscriptionManager::create(fixture.source());
  ASSERT_TRUE(manager.has_value());
  SubscriptionRequest wrong = fixture.request();
  wrong.plan_fingerprint.front() ^= std::byte{1};
  EXPECT_FALSE(manager->register_subscription(wrong).has_value());
  const auto registration = manager->register_subscription(fixture.request());
  ASSERT_TRUE(registration.has_value());
  EXPECT_FALSE(
      manager->publish_committed(fixture.change(fixture.tablet_a, fixture.wal_a, 2U)).is_ok());
  EXPECT_TRUE(
      manager->publish_committed(fixture.change(fixture.tablet_b, fixture.wal_b, 1U)).is_ok());
  ASSERT_TRUE(manager->cancel(fixture.subscription_id).has_value());

  MultiTabletSubscriptionSource changed = fixture.source();
  changed.members[0].wal_id = wal_id(std::byte{11});
  auto other = MultiTabletSubscriptionManager::create(std::move(changed));
  ASSERT_TRUE(other.has_value());
  EXPECT_FALSE(other->resume_subscription(registration->initial_resume_token).has_value());
}

TEST(MultiTabletSubscriptionTest, FailsResumeWhenAnyRequiredSourceSuffixExpired) {
  Fixture fixture;
  SubscriptionLimits limits{};
  limits.maximum_retained_changes = 2U;
  auto manager = MultiTabletSubscriptionManager::create(fixture.source(), limits);
  ASSERT_TRUE(manager.has_value());
  const auto registration = manager->register_subscription(fixture.request());
  ASSERT_TRUE(registration.has_value());
  ASSERT_TRUE(manager->complete_snapshot(fixture.subscription_id).is_ok());
  ASSERT_TRUE(
      manager->publish_committed(fixture.change(fixture.tablet_a, fixture.wal_a, 1U)).is_ok());
  ASSERT_TRUE(
      manager->publish_committed(fixture.change(fixture.tablet_b, fixture.wal_b, 1U)).is_ok());
  ASSERT_TRUE(
      manager->publish_committed(fixture.change(fixture.tablet_a, fixture.wal_a, 2U)).is_ok());
  ASSERT_TRUE(manager->cancel(fixture.subscription_id).has_value());
  const auto resumed = manager->resume_subscription(registration->initial_resume_token);
  ASSERT_FALSE(resumed.has_value());
  EXPECT_EQ(resumed.error().code(), common::StatusCode::kNotFound);
}

TEST(MultiTabletSubscriptionTest, CheckpointsAndRestoresExactAdmissionOrderForResume) {
  Fixture fixture;
  auto manager = MultiTabletSubscriptionManager::create(fixture.source());
  ASSERT_TRUE(manager.has_value());
  const auto registration = manager->register_subscription(fixture.request());
  ASSERT_TRUE(registration.has_value());
  ASSERT_TRUE(manager->complete_snapshot(fixture.subscription_id).is_ok());
  ASSERT_TRUE(
      manager->publish_committed(fixture.change(fixture.tablet_a, fixture.wal_a, 1U)).is_ok());
  ASSERT_TRUE(
      manager->publish_committed(fixture.change(fixture.tablet_b, fixture.wal_b, 1U)).is_ok());
  const auto delivered = manager->poll(fixture.subscription_id, 8U);
  ASSERT_TRUE(delivered.has_value());
  const auto safe_token = manager->acknowledge(fixture.subscription_id, 1U);
  ASSERT_TRUE(safe_token.has_value());
  ASSERT_TRUE(
      manager->publish_committed(fixture.change(fixture.tablet_a, fixture.wal_a, 2U)).is_ok());
  ASSERT_TRUE(
      manager->publish_committed(fixture.change(fixture.tablet_b, fixture.wal_b, 2U)).is_ok());

  const auto checkpoint = manager->checkpoint();
  ASSERT_TRUE(checkpoint.has_value()) << checkpoint.error().to_string();
  ASSERT_EQ(checkpoint->retained_changes.size(), 4U);
  MultiTabletSubscriptionSource restored_source = fixture.source(2U, 2U);
  auto restored = MultiTabletSubscriptionManager::restore(std::move(restored_source), *checkpoint);
  ASSERT_TRUE(restored.has_value()) << restored.error().to_string();
  const auto resumed = restored->resume_subscription(*safe_token);
  ASSERT_TRUE(resumed.has_value()) << resumed.error().to_string();
  const auto replayed = restored->poll(fixture.subscription_id, 8U);
  ASSERT_TRUE(replayed.has_value());
  ASSERT_EQ(replayed->size(), 3U);
  EXPECT_EQ((*replayed)[0].change->position.tablet_id, fixture.tablet_b);
  EXPECT_EQ((*replayed)[0].change->position.record_sequence, 1U);
  EXPECT_EQ((*replayed)[1].change->position.tablet_id, fixture.tablet_a);
  EXPECT_EQ((*replayed)[1].change->position.record_sequence, 2U);
  EXPECT_EQ((*replayed)[2].change->position.tablet_id, fixture.tablet_b);
  EXPECT_EQ((*replayed)[2].change->position.record_sequence, 2U);

  MultiTabletSubscriptionCheckpoint corrupted = *checkpoint;
  corrupted.retained_changes.erase(corrupted.retained_changes.begin() + 1);
  EXPECT_FALSE(
      MultiTabletSubscriptionManager::restore(fixture.source(2U, 2U), corrupted).has_value());
}

} // namespace
} // namespace chronos::live
