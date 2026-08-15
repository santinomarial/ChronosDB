#include "chronos/common/status.hpp"
#include "chronos/live/subscription.hpp"
#include "chronos/live/subscription_protocol.hpp"

#include <cstddef>
#include <gtest/gtest.h>

namespace chronos::live {
namespace {

[[nodiscard]] common::Uuid uuid(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return common::Uuid{bytes};
}

template <typename Identifier> [[nodiscard]] Identifier identifier(const std::byte seed) {
  auto value = Identifier::from_uuid(uuid(seed));
  EXPECT_TRUE(value.has_value());
  return *value;
}

[[nodiscard]] wal::WalId make_wal_id() {
  wal::WalId value{};
  value.bytes.fill(std::byte{0x44});
  return value;
}

[[nodiscard]] ResumeTokenMacKey key() {
  ResumeTokenMacKey value{};
  value.fill(std::byte{0x55});
  return value;
}

struct Fixture {
  common::Uuid database_id{uuid(std::byte{1})};
  schema::TableId table_id{identifier<schema::TableId>(std::byte{2})};
  schema::TabletId tablet_id{identifier<schema::TabletId>(std::byte{3})};
  schema::SchemaId schema_id{identifier<schema::SchemaId>(std::byte{4})};
  wal::WalId wal{make_wal_id()};
  common::Uuid subscription_id{uuid(std::byte{5})};
  PlanFingerprint plan{};

  [[nodiscard]] SubscriptionSource source() const {
    return {.database_id = database_id,
            .table_id = table_id,
            .tablet_id = tablet_id,
            .wal_id = wal,
            .plan_fingerprint = plan,
            .schema_id = schema_id,
            .schema_version = schema::SchemaVersion::initial(),
            .token_key = key()};
  }
  [[nodiscard]] SubscriptionRequest request() const {
    return SubscriptionRequest{subscription_id, plan, schema_id, schema::SchemaVersion::initial()};
  }
  [[nodiscard]] CommittedChange change(const std::uint64_t sequence) const {
    return CommittedChange{SourcePosition{tablet_id, wal, sequence},
                           schema_id,
                           schema::SchemaVersion::initial(),
                           LogicalChangeOperation::kUpsert,
                           {std::byte{static_cast<unsigned char>(sequence)}},
                           {std::byte{0xa0}}};
  }
};

TEST(SubscriptionTest, BuffersEveryPostBoundaryCommitUntilSnapshotCompletes) {
  Fixture fixture;
  auto manager = SubscriptionManager::create(fixture.source());
  ASSERT_TRUE(manager.has_value()) << manager.error().to_string();
  SubscriptionRequest wrong_plan = fixture.request();
  wrong_plan.plan_fingerprint.front() ^= std::byte{1};
  EXPECT_FALSE(manager->register_subscription(wrong_plan).has_value());
  auto registration = manager->register_subscription(fixture.request());
  ASSERT_TRUE(registration.has_value());
  EXPECT_EQ(registration->snapshot_boundary.record_sequence, 0U);

  EXPECT_TRUE(manager->publish_committed(fixture.change(1U)).is_ok());
  const auto early = manager->poll(fixture.subscription_id, 8U);
  ASSERT_FALSE(early.has_value());
  EXPECT_EQ(early.error().code(), common::StatusCode::kUnavailable);
  EXPECT_TRUE(manager->complete_snapshot(fixture.subscription_id).is_ok());

  const auto delivered = manager->poll(fixture.subscription_id, 8U);
  ASSERT_TRUE(delivered.has_value());
  ASSERT_EQ(delivered->size(), 1U);
  EXPECT_EQ(delivered->front().delivery_sequence, 1U);
  EXPECT_EQ(delivered->front().change->position.record_sequence, 1U);
  const auto token = manager->acknowledge(fixture.subscription_id, 1U);
  ASSERT_TRUE(token.has_value());
  const auto decoded = decode_resume_token_v1(*token, key());
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->safe_delivery_sequence, 1U);
  EXPECT_EQ(decoded->source_positions.front().record_sequence, 1U);
}

TEST(SubscriptionTest, ResumeReplaysRetainedSuffixAtLeastOnce) {
  Fixture fixture;
  auto manager = SubscriptionManager::create(fixture.source());
  ASSERT_TRUE(manager.has_value());
  ASSERT_TRUE(manager->register_subscription(fixture.request()).has_value());
  ASSERT_TRUE(manager->complete_snapshot(fixture.subscription_id).is_ok());
  ASSERT_TRUE(manager->publish_committed(fixture.change(1U)).is_ok());
  ASSERT_TRUE(manager->poll(fixture.subscription_id, 1U).has_value());
  auto checkpoint = manager->acknowledge(fixture.subscription_id, 1U);
  ASSERT_TRUE(checkpoint.has_value());
  ASSERT_TRUE(manager->publish_committed(fixture.change(2U)).is_ok());
  ASSERT_TRUE(manager->publish_committed(fixture.change(3U)).is_ok());
  ASSERT_TRUE(manager->cancel(fixture.subscription_id).has_value());

  const auto resumed = manager->resume_subscription(*checkpoint);
  ASSERT_TRUE(resumed.has_value()) << resumed.error().to_string();
  const auto replayed = manager->poll(fixture.subscription_id, 8U);
  ASSERT_TRUE(replayed.has_value());
  ASSERT_EQ(replayed->size(), 2U);
  EXPECT_EQ((*replayed)[0].delivery_sequence, 2U);
  EXPECT_EQ((*replayed)[0].change->position.record_sequence, 2U);
  EXPECT_EQ((*replayed)[1].delivery_sequence, 3U);
  EXPECT_EQ((*replayed)[1].change->position.record_sequence, 3U);
}

TEST(SubscriptionTest, SlowSubscriberOverflowDoesNotRejectCommittedChange) {
  Fixture fixture;
  SubscriptionLimits limits{};
  limits.maximum_buffered_changes_per_subscription = 1U;
  auto manager = SubscriptionManager::create(fixture.source(), limits);
  ASSERT_TRUE(manager.has_value());
  ASSERT_TRUE(manager->register_subscription(fixture.request()).has_value());
  EXPECT_TRUE(manager->publish_committed(fixture.change(1U)).is_ok());
  EXPECT_TRUE(manager->publish_committed(fixture.change(2U)).is_ok());
  const auto status = manager->status(fixture.subscription_id);
  ASSERT_TRUE(status.has_value());
  EXPECT_EQ(status->phase, SubscriptionPhase::kOverflowed);
  EXPECT_EQ(status->last_assigned_sequence, 1U);
  EXPECT_EQ(status->buffered_changes, 0U);
  EXPECT_EQ(status->buffered_bytes, 0U);
}

TEST(SubscriptionTest, SchemaChangeTerminatesPlanAndRejectsResumePrecisely) {
  Fixture fixture;
  auto manager = SubscriptionManager::create(fixture.source());
  ASSERT_TRUE(manager.has_value());
  const auto registration = manager->register_subscription(fixture.request());
  ASSERT_TRUE(registration.has_value());
  ASSERT_TRUE(manager->complete_snapshot(fixture.subscription_id).is_ok());
  ASSERT_TRUE(manager->publish_committed(fixture.change(1U)).is_ok());
  ASSERT_TRUE(manager->poll(fixture.subscription_id, 1U).has_value());
  const auto safe_token = manager->acknowledge(fixture.subscription_id, 1U);
  ASSERT_TRUE(safe_token.has_value());

  CommittedChange incompatible = fixture.change(2U);
  incompatible.schema_id = identifier<schema::SchemaId>(std::byte{9});
  ASSERT_TRUE(manager->publish_committed(std::move(incompatible)).is_ok());
  const auto status = manager->status(fixture.subscription_id);
  ASSERT_TRUE(status.has_value());
  EXPECT_EQ(status->phase, SubscriptionPhase::kSchemaChanged);
  EXPECT_EQ(manager->poll(fixture.subscription_id, 1U).error().code(),
            common::StatusCode::kNotSupported);
  EXPECT_FALSE(manager->acknowledge(fixture.subscription_id, 1U).has_value());
  EXPECT_FALSE(terminate_subscription(*manager, fixture.subscription_id,
                                      network::SubscriptionEndReason::kOverflowed)
                   .has_value());

  const auto terminal = terminate_subscription(*manager, fixture.subscription_id,
                                               network::SubscriptionEndReason::kSchemaChanged);
  ASSERT_TRUE(terminal.has_value());
  const auto decoded = network::decode_subscription_end(*terminal);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->reason, network::SubscriptionEndReason::kSchemaChanged);
  EXPECT_EQ(decoded->safe_delivery_sequence, 1U);
  EXPECT_EQ(manager->resume_subscription(*safe_token).error().code(),
            common::StatusCode::kNotSupported);

  SubscriptionRequest replacement = fixture.request();
  replacement.subscription_id = uuid(std::byte{10});
  EXPECT_EQ(manager->register_subscription(replacement).error().code(),
            common::StatusCode::kNotSupported);
}

} // namespace
} // namespace chronos::live
