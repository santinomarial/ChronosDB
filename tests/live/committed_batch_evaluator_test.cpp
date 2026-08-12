#include "chronos/live/committed_batch_evaluator.hpp"
#include "chronos/live/subscription_protocol.hpp"
#include "chronos/query/catalog.hpp"
#include "columnar/columnar_test_support.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <gtest/gtest.h>
#include <memory>
#include <ranges>
#include <span>
#include <string_view>
#include <vector>

namespace chronos::live {
namespace {

[[nodiscard]] common::Uuid uuid(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return common::Uuid{bytes};
}

[[nodiscard]] wal::WalId wal_id() {
  wal::WalId id{};
  id.bytes.fill(std::byte{9});
  return id;
}

[[nodiscard]] schema::TabletId tablet_id() {
  return schema::TabletId::from_uuid(uuid(std::byte{8})).value();
}

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch> batch() {
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(columnar::test::batch_schema(),
                                           columnar::test::batch_columns())
          .value());
}

[[nodiscard]] std::shared_ptr<const query::QueryCatalogSnapshot>
catalog(const std::shared_ptr<const columnar::OwnedColumnarBatch>& input) {
  const std::array tables{query::QueryCatalogTableInput{
      .name = "events", .quoted = false, .schema = input->schema_ptr()}};
  return std::make_shared<const query::QueryCatalogSnapshot>(
      query::QueryCatalogSnapshot::create(1U, tables).value());
}

TEST(CommittedBatchEvaluatorTest, PublishesOneDeterministicMultiChunkResultPerCommit) {
  const auto input = batch();
  auto plan =
      prepare_subscription_plan("SUBSCRIBE SELECT tag FROM events WHERE enabled", catalog(input));
  ASSERT_TRUE(plan.has_value()) << plan.error().status().to_string();
  const SourcePosition position{tablet_id(), wal_id(), 1U};
  query::QueryResourceContext resources = query::QueryResourceContext::create(1U << 20U).value();
  CommittedBatchEvaluatorLimits limits;
  limits.scan.maximum_rows_per_chunk = 1U;
  limits.scan.chunk.maximum_rows = 1U;
  auto change = evaluate_committed_batch(*plan, position, input, resources, limits);
  ASSERT_TRUE(change.has_value()) << change.error().to_string();
  EXPECT_EQ(change->position, position);
  EXPECT_EQ(change->schema_id, input->schema().schema_id());
  EXPECT_EQ(change->operation, LogicalChangeOperation::kUpsert);
  ASSERT_EQ(change->result_key.size(), kCommittedBatchResultKeySize);
  EXPECT_EQ(change->result_key[0], std::byte{'C'});
  const std::span<const std::byte> key_bytes{change->result_key};
  EXPECT_TRUE(
      std::ranges::equal(key_bytes.subspan(8U, plan->fingerprint().size()), plan->fingerprint()));
  EXPECT_TRUE(std::ranges::equal(key_bytes.subspan(40U, 16U), position.tablet_id.bytes()));
  EXPECT_TRUE(std::ranges::equal(key_bytes.subspan(56U, 16U), position.wal_id.bytes));
  EXPECT_EQ(key_bytes[72U], std::byte{1});
  auto decoded = network::decode_query_result_batch(change->payload);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  ASSERT_EQ(decoded->row_count(), 1U);
  ASSERT_EQ(decoded->columns().size(), 1U);
  EXPECT_EQ(decoded->columns().front().name, "tag");
  const std::array expected{std::byte{'x'}};
  EXPECT_TRUE(std::ranges::equal(decoded->cell(0U, 0U)->value, expected));

  auto repeated = evaluate_committed_batch(*plan, position, input, resources, limits);
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  EXPECT_EQ(repeated->result_key, change->result_key);
  EXPECT_EQ(repeated->payload, change->payload);
  SourcePosition next = position;
  next.record_sequence = 2U;
  auto distinct = evaluate_committed_batch(*plan, next, input, resources, limits);
  ASSERT_TRUE(distinct.has_value());
  EXPECT_NE(distinct->result_key, change->result_key);

  auto empty_plan = prepare_subscription_plan(
      "SUBSCRIBE SELECT tag FROM events WHERE enabled = TRUE AND enabled = FALSE", catalog(input));
  ASSERT_TRUE(empty_plan.has_value()) << empty_plan.error().status().to_string();
  auto empty = evaluate_committed_batch(*empty_plan, position, input, resources, limits);
  ASSERT_TRUE(empty.has_value()) << empty.error().to_string();
  auto empty_payload = network::decode_query_result_batch(empty->payload);
  ASSERT_TRUE(empty_payload.has_value());
  EXPECT_EQ(empty_payload->row_count(), 0U);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(CommittedBatchEvaluatorTest, ResultFitsTheExistingGapFreeManagerContract) {
  const auto input = batch();
  auto plan =
      prepare_subscription_plan("SUBSCRIBE SELECT tag FROM events WHERE enabled", catalog(input));
  ASSERT_TRUE(plan.has_value());
  ResumeTokenMacKey key{};
  key.fill(std::byte{7});
  const common::Uuid database_id = uuid(std::byte{6});
  const common::Uuid subscription_id = uuid(std::byte{5});
  auto manager = SubscriptionManager::create(plan->source(database_id, tablet_id(), wal_id(), key));
  ASSERT_TRUE(manager.has_value()) << manager.error().to_string();
  ASSERT_TRUE(manager->register_subscription(plan->request(subscription_id)).has_value());
  query::QueryResourceContext resources = query::QueryResourceContext::create(1U << 20U).value();
  auto change = evaluate_committed_batch(*plan, {tablet_id(), wal_id(), 1U}, input, resources);
  ASSERT_TRUE(change.has_value()) << change.error().to_string();
  ASSERT_TRUE(manager->publish_committed(std::move(*change)).is_ok());
  EXPECT_FALSE(manager->poll(subscription_id, 1U).has_value());
  ASSERT_TRUE(manager->complete_snapshot(subscription_id).is_ok());
  auto delivery = manager->poll(subscription_id, 1U);
  ASSERT_TRUE(delivery.has_value());
  ASSERT_EQ(delivery->size(), 1U);
  EXPECT_EQ(delivery->front().change->position.record_sequence, 1U);
  EXPECT_EQ(network::decode_query_result_batch(delivery->front().change->payload)->row_count(), 1U);
  auto encoded_delivery = encode_subscription_delivery(delivery->front());
  ASSERT_TRUE(encoded_delivery.has_value()) << encoded_delivery.error().to_string();
  auto decoded_delivery = network::decode_subscription_change(*encoded_delivery);
  ASSERT_TRUE(decoded_delivery.has_value()) << decoded_delivery.error().to_string();
  EXPECT_EQ(network::decode_query_result_batch(decoded_delivery->payload)->row_count(), 1U);
}

TEST(CommittedBatchEvaluatorTest, RejectsStatefulPlansAndResourceFailureWithoutPublishing) {
  const auto input = batch();
  auto aggregate =
      prepare_subscription_plan("SUBSCRIBE SELECT count(*) FROM events", catalog(input));
  ASSERT_TRUE(aggregate.has_value());
  query::QueryResourceContext resources = query::QueryResourceContext::create(1U << 20U).value();
  auto rejected =
      evaluate_committed_batch(*aggregate, {tablet_id(), wal_id(), 1U}, input, resources);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kNotSupported);

  auto row_plan = prepare_subscription_plan("SUBSCRIBE SELECT tag FROM events", catalog(input));
  ASSERT_TRUE(row_plan.has_value());
  query::QueryResourceContext tiny = query::QueryResourceContext::create(1U).value();
  rejected = evaluate_committed_batch(*row_plan, {tablet_id(), wal_id(), 1U}, input, tiny);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_EQ(tiny.reserved_memory_bytes(), 0U);

  CommittedBatchEvaluatorLimits narrow;
  narrow.subscription.protocol.maximum_payload_size = 200U;
  narrow.subscription.maximum_resume_token_bytes = 80U;
  narrow.subscription.maximum_result_key_bytes = kCommittedBatchResultKeySize;
  rejected =
      evaluate_committed_batch(*row_plan, {tablet_id(), wal_id(), 1U}, input, resources, narrow);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

} // namespace
} // namespace chronos::live
