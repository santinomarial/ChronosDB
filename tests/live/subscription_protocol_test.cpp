#include "chronos/live/subscription_protocol.hpp"

#include <algorithm>
#include <cstddef>
#include <gtest/gtest.h>
#include <memory>

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

[[nodiscard]] wal::WalId wal_id() {
  wal::WalId value{};
  value.bytes.fill(std::byte{4});
  return value;
}

[[nodiscard]] ResumeTokenMacKey key() {
  ResumeTokenMacKey value{};
  value.fill(std::byte{5});
  return value;
}

TEST(SubscriptionProtocolTest, BridgesManagerDeliveryAcknowledgementAndTermination) {
  const common::Uuid subscription_id = uuid(std::byte{6});
  const schema::TabletId tablet_id = identifier<schema::TabletId>(std::byte{3});
  const schema::SchemaId schema_id = identifier<schema::SchemaId>(std::byte{7});
  const PlanFingerprint plan{};
  auto manager = SubscriptionManager::create({.database_id = uuid(std::byte{1}),
                                              .table_id = identifier<schema::TableId>(std::byte{2}),
                                              .tablet_id = tablet_id,
                                              .wal_id = wal_id(),
                                              .plan_fingerprint = plan,
                                              .schema_id = schema_id,
                                              .schema_version = schema::SchemaVersion::initial(),
                                              .token_key = key()});
  ASSERT_TRUE(manager.has_value()) << manager.error().to_string();
  const SubscriptionRequest request{subscription_id, plan, schema_id,
                                    schema::SchemaVersion::initial()};
  const auto registration = manager->register_subscription(request);
  ASSERT_TRUE(registration.has_value());
  const auto ready_payload = encode_subscription_registration(*registration);
  ASSERT_TRUE(ready_payload.has_value());
  const auto ready = network::decode_subscription_ready(*ready_payload);
  ASSERT_TRUE(ready.has_value());
  EXPECT_TRUE(std::ranges::equal(ready->resume_token, registration->initial_resume_token));

  ASSERT_TRUE(manager->complete_snapshot(subscription_id).is_ok());
  ASSERT_TRUE(manager
                  ->publish_committed({.position = {tablet_id, wal_id(), 1U},
                                       .schema_id = schema_id,
                                       .schema_version = schema::SchemaVersion::initial(),
                                       .operation = LogicalChangeOperation::kUpsert,
                                       .result_key = {std::byte{8}},
                                       .payload = {std::byte{9}}})
                  .is_ok());
  const auto delivery = manager->poll(subscription_id, 1U);
  ASSERT_TRUE(delivery.has_value());
  ASSERT_EQ(delivery->size(), 1U);
  const auto change_payload = encode_subscription_delivery(delivery->front());
  ASSERT_TRUE(change_payload.has_value()) << change_payload.error().to_string();
  const auto change = network::decode_subscription_change(*change_payload);
  ASSERT_TRUE(change.has_value());
  EXPECT_EQ(change->delivery_sequence, 1U);
  EXPECT_EQ(change->tablet_id, tablet_id);
  EXPECT_EQ(change->record_sequence, 1U);

  const auto checkpoint_payload = acknowledge_subscription_delivery(*manager, subscription_id, 1U);
  ASSERT_TRUE(checkpoint_payload.has_value());
  const auto checkpoint = network::decode_subscription_checkpoint(*checkpoint_payload);
  ASSERT_TRUE(checkpoint.has_value());
  EXPECT_EQ(checkpoint->acknowledged_delivery_sequence, 1U);
  const auto decoded_token = decode_resume_token(checkpoint->resume_token, key());
  ASSERT_TRUE(decoded_token.has_value());
  EXPECT_EQ(decoded_token->safe_delivery_sequence, 1U);
  EXPECT_EQ(decoded_token->source_positions.front().record_sequence, 1U);

  const auto end_payload =
      terminate_subscription(*manager, subscription_id, network::SubscriptionEndReason::kCancelled);
  ASSERT_TRUE(end_payload.has_value());
  const auto end = network::decode_subscription_end(*end_payload);
  ASSERT_TRUE(end.has_value());
  EXPECT_EQ(end->reason, network::SubscriptionEndReason::kCancelled);
  EXPECT_EQ(end->safe_delivery_sequence, 1U);
}

TEST(SubscriptionProtocolTest, ManagerRejectsNoncanonicalDeleteBeforeWireDelivery) {
  const schema::TabletId tablet_id = identifier<schema::TabletId>(std::byte{3});
  const schema::SchemaId schema_id = identifier<schema::SchemaId>(std::byte{7});
  auto manager = SubscriptionManager::create({.database_id = uuid(std::byte{1}),
                                              .table_id = identifier<schema::TableId>(std::byte{2}),
                                              .tablet_id = tablet_id,
                                              .wal_id = wal_id(),
                                              .plan_fingerprint = {},
                                              .schema_id = schema_id,
                                              .schema_version = schema::SchemaVersion::initial(),
                                              .token_key = key()});
  ASSERT_TRUE(manager.has_value());
  EXPECT_FALSE(manager
                   ->publish_committed({.position = {tablet_id, wal_id(), 1U},
                                        .schema_id = schema_id,
                                        .schema_version = schema::SchemaVersion::initial(),
                                        .operation = LogicalChangeOperation::kDelete,
                                        .result_key = {std::byte{8}},
                                        .payload = {std::byte{9}}})
                   .is_ok());
}

TEST(SubscriptionProtocolTest, ProtocolOnePointOneRejectsRaftSourceWithoutAliasingGroupBytes) {
  const schema::TabletId tablet_id = identifier<schema::TabletId>(std::byte{3});
  const schema::SchemaId schema_id = identifier<schema::SchemaId>(std::byte{7});
  const auto change = std::make_shared<const CommittedChange>(
      CommittedChange{.position = SourcePosition::raft(tablet_id, uuid(std::byte{4}), 1U),
                      .schema_id = schema_id,
                      .schema_version = schema::SchemaVersion::initial(),
                      .operation = LogicalChangeOperation::kUpsert,
                      .result_key = {std::byte{8}},
                      .payload = {std::byte{9}}});
  const auto encoded = encode_subscription_delivery({1U, change});
  ASSERT_FALSE(encoded.has_value());
  EXPECT_EQ(encoded.error().code(), common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::live
