#include "chronos/ingest/sha256.hpp"
#include "chronos/live/subscription.hpp"
#include "chronos/query/distributed.hpp"
#include "chronos/query/temporal_snapshot.hpp"
#include "chronos/query/value.hpp"
#include "chronos/raft/metadata.hpp"
#include "chronos/raft/multi_raft.hpp"
#include "chronos/raft/rebalancing.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/table_schema.hpp"
#include "chronos/tiering/tiered_parts.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace chronos {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

[[nodiscard]] std::shared_ptr<const schema::TableSchema> make_schema() {
  const auto timestamp = id<schema::ColumnId>(4U);
  const auto value = id<schema::ColumnId>(5U);
  const auto key = id<schema::ColumnId>(6U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(
                        timestamp, "ts", type(schema::LogicalTypeKind::kTimestampNs), false)
                        .value());
  columns.push_back(
      schema::ColumnDefinition::create(value, "value", type(schema::LogicalTypeKind::kInt64), false)
          .value());
  columns.push_back(
      schema::ColumnDefinition::create(key, "key", type(schema::LogicalTypeKind::kString), false)
          .value());
  return std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(id<schema::TableId>(1U), id<schema::SchemaId>(2U),
                                  schema::SchemaVersion::initial(), std::nullopt,
                                  std::move(columns),
                                  {.event_time_column = timestamp,
                                   .physical_ordering_key = {key, timestamp},
                                   .partition_columns = {timestamp},
                                   .shard_key = {key},
                                   .deduplication_key = {key}})
          .value());
}

[[nodiscard]] wal::WalId wal_id() {
  wal::WalId value{};
  value.bytes.front() = std::byte{9U};
  return value;
}

TEST(FeatureCompletionSmokeTest, CommittedDataFlowsAcrossTemporalLiveDistributedAndColdBoundaries) {
  const auto table_schema = make_schema();
  const auto tablet = id<schema::TabletId>(3U);

  auto metadata = raft::MetadataStateMachine::create();
  ASSERT_TRUE(metadata.has_value());
  ASSERT_TRUE(metadata->apply_committed(1U, raft::ClusterNodeMetadata{1U, "node-1"}).is_ok());
  ASSERT_TRUE(metadata
                  ->apply_committed(2U, raft::SchemaMetadata{table_schema->table_id(),
                                                             table_schema->schema_id(),
                                                             table_schema->version()})
                  .is_ok());
  ASSERT_TRUE(metadata
                  ->apply_committed(3U,
                                    raft::TabletPlacementMetadata{
                                        table_schema->table_id(), tablet, 1U, {1U, 2U, 3U}, 1U})
                  .is_ok());

  auto temporal = query::TemporalSnapshotProvider::create(table_schema);
  ASSERT_TRUE(temporal.has_value());
  query::TemporalMutation mutation{
      .logical_identity = {std::byte{1U}},
      .columns =
          {query::ScalarValue::signed_value(type(schema::LogicalTypeKind::kTimestampNs), 100)
               .value(),
           query::ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), 7).value(),
           query::ScalarValue::text(type(schema::LogicalTypeKind::kString), "a").value()},
      .event_time_ns = 100,
      .receive_time_ns = 110,
      .wal_id = uuid(9U),
      .record_sequence = 1U,
      .row_ordinal = 0U,
      .kind = query::TemporalMutationKind::kOriginal,
  };
  ASSERT_TRUE((*temporal)->apply_committed(1U, 1000, {std::move(mutation)}).is_ok());
  const auto historical = (*temporal)->resolve(table_schema, 1000);
  ASSERT_TRUE(historical.has_value());
  ASSERT_EQ((*historical)->rows().size(), 1U);

  live::ResumeTokenMacKey token_key{};
  token_key.fill(std::byte{0xa5});
  auto subscriptions = live::SubscriptionManager::create(
      {uuid(10U), table_schema->table_id(), tablet, wal_id(), token_key});
  ASSERT_TRUE(subscriptions.has_value());
  live::PlanFingerprint fingerprint{};
  const common::Uuid subscription_id = uuid(11U);
  ASSERT_TRUE(subscriptions
                  ->register_subscription({subscription_id, fingerprint, table_schema->schema_id(),
                                           table_schema->version()})
                  .has_value());
  ASSERT_TRUE(subscriptions
                  ->publish_committed({{tablet, wal_id(), 1U},
                                       table_schema->schema_id(),
                                       table_schema->version(),
                                       live::LogicalChangeOperation::kUpsert,
                                       {std::byte{1U}},
                                       {std::byte{7U}}})
                  .is_ok());
  ASSERT_TRUE(subscriptions->complete_snapshot(subscription_id).is_ok());
  ASSERT_EQ(subscriptions->poll(subscription_id, 1U)->size(), 1U);

  auto plan = query::plan_distributed_aggregation(uuid(12U), {{tablet, 0, 200, 1U, 1U}}, {0, 200});
  ASSERT_TRUE(plan.has_value());
  auto coordinator = query::DistributedAggregateCoordinator::create(std::move(*plan));
  ASSERT_TRUE(coordinator.has_value());
  query::MergeableAggregateState partial;
  ASSERT_TRUE(partial.add(7.0).is_ok());
  ASSERT_TRUE(coordinator->accept({uuid(12U), tablet, 1U, partial, true}).is_ok());
  ASSERT_EQ(coordinator->finish()->count, 1U);

  auto local_group = raft::MultiRaftRuntime::create(1U);
  ASSERT_TRUE(local_group.has_value());
  ASSERT_TRUE(local_group->add_group(uuid(13U), {1U}).is_ok());
  ASSERT_TRUE(local_group->start_election(uuid(13U)).has_value());
  const auto replicated = local_group->propose(uuid(13U), 1U, {std::byte{1U}});
  ASSERT_TRUE(replicated.has_value());
  ASSERT_EQ(local_group->find_group(uuid(13U))->commit_index(), 1U);

  tiering::MemoryObjectStore object_store;
  auto tiering = tiering::TieredPartManager::create(object_store);
  ASSERT_TRUE(tiering.has_value()) << tiering.error().to_string();
  const std::vector<std::byte> immutable_part{std::byte{1U}, std::byte{2U}, std::byte{3U}};
  const auto part_id = id<cseg::PartId>(14U);
  tiering::ColdPartDescriptor cold{
      manifest::PartDescriptor{part_id, table_schema->table_id(), tablet, table_schema->schema_id(),
                               table_schema->version(), immutable_part.size(), 1U, 1U, 1U, 100,
                               100},
      "parts/smoke", ingest::sha256(immutable_part).value()};
  const auto tiered = tiering->upload_and_install(
      std::move(cold), immutable_part,
      [](const tiering::ColdPartDescriptor&) { return common::Status::ok(); });
  ASSERT_TRUE(tiered.has_value());
  const auto reread = tiering->read_range(part_id, 0U, immutable_part.size());
  ASSERT_TRUE(reread.has_value());
  EXPECT_EQ(*reread, immutable_part);
}

} // namespace
} // namespace chronos
