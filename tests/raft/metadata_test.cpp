#include "chronos/raft/metadata.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::raft {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] GroupId group(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return GroupId{bytes};
}

TEST(MetadataStateMachineTest, AppliesSchemaPlacementNodesAndRetentionOnlyInCommitOrder) {
  auto metadata = MetadataStateMachine::create();
  ASSERT_TRUE(metadata.has_value());
  EXPECT_TRUE(metadata->apply_committed(1U, ClusterNodeMetadata{1U, "node-1"}).is_ok());
  const auto table = id<schema::TableId>(1U);
  const auto schema_id = id<schema::SchemaId>(2U);
  const auto tablet = id<schema::TabletId>(3U);
  EXPECT_TRUE(
      metadata
          ->apply_committed(2U, SchemaMetadata{table, schema_id, schema::SchemaVersion::initial()})
          .is_ok());
  EXPECT_TRUE(
      metadata->apply_committed(3U, TabletPlacementMetadata{table, tablet, 1U, {3U, 1U, 2U}, 1U})
          .is_ok());
  const GroupId tablet_group = group(4U);
  EXPECT_TRUE(metadata
                  ->apply_committed_tablet_group_binding(
                      4U, TabletGroupBindingMetadata{tablet, tablet_group})
                  .is_ok());
  EXPECT_FALSE(
      metadata
          ->apply_committed_tablet_group_binding(5U, TabletGroupBindingMetadata{tablet, group(5U)})
          .is_ok());
  EXPECT_TRUE(metadata
                  ->apply_committed_tablet_group_binding(
                      5U, TabletGroupBindingMetadata{tablet, tablet_group})
                  .is_ok());
  EXPECT_TRUE(metadata->apply_committed(6U, RetentionMetadata{table, 1000, 100U}).is_ok());
  EXPECT_EQ(metadata->applied_index(), 6U);
  EXPECT_EQ(metadata->find_node(1U)->endpoint, "node-1");
  EXPECT_EQ(metadata->find_schema(schema_id)->table_id, table);
  EXPECT_EQ(metadata->find_tablet(tablet)->leader_hint, 1U);
  EXPECT_EQ(metadata->find_tablet(tablet)->replicas, (std::vector<NodeId>{1U, 2U, 3U}));
  EXPECT_EQ(metadata->find_tablet_group_binding(tablet)->group_id, tablet_group);
  EXPECT_EQ(metadata->find_retention(table)->system_history_ns, 1000);
  EXPECT_FALSE(metadata->apply_committed(8U, ClusterNodeMetadata{2U, "node-2"}).is_ok());
  EXPECT_EQ(metadata->applied_index(), 6U);
  EXPECT_TRUE(metadata->apply_internal_noop(7U).is_ok());
  EXPECT_TRUE(metadata->apply_committed(8U, ClusterNodeMetadata{2U, "node-2"}).is_ok());
  EXPECT_EQ(metadata->applied_index(), 8U);
}

TEST(MetadataStateMachineTest, AppliesCompleteSchemasInLinearCommittedOrder) {
  auto metadata = MetadataStateMachine::create();
  ASSERT_TRUE(metadata.has_value());
  const auto table = id<schema::TableId>(10U);
  EXPECT_FALSE(
      metadata->apply_committed(1U, TablePolicyMetadata{table, 100, 1000, 500, 10, 100U}).is_ok());
  EXPECT_EQ(metadata->applied_index(), 0U);
  const auto first_schema = id<schema::SchemaId>(11U);
  const auto timestamp = id<schema::ColumnId>(12U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(
                        timestamp, "ts",
                        schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(),
                        false)
                        .value());
  auto first = schema::TableSchema::create(table, first_schema, schema::SchemaVersion::initial(),
                                           std::nullopt, std::move(columns),
                                           {.event_time_column = timestamp,
                                            .physical_ordering_key = {timestamp},
                                            .partition_columns = {timestamp},
                                            .shard_key = {timestamp},
                                            .deduplication_key = {timestamp}});
  ASSERT_TRUE(first.has_value());
  auto first_shared = std::make_shared<const schema::TableSchema>(std::move(*first));
  EXPECT_TRUE(metadata
                  ->apply_committed_schema_definition(
                      1U, {.name = "events", .quoted = false, .schema = first_shared})
                  .is_ok());
  ASSERT_NE(metadata->find_schema_definition(first_schema), nullptr);
  EXPECT_EQ(metadata->find_active_table_definition(table)->schema.get(), first_shared.get());

  std::vector<schema::ColumnDefinition> successor_columns;
  successor_columns.push_back(
      schema::ColumnDefinition::create(
          timestamp, "event_time",
          schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(), false)
          .value());
  successor_columns.push_back(
      schema::ColumnDefinition::create(
          id<schema::ColumnId>(13U), "note",
          schema::LogicalType::create(schema::LogicalTypeKind::kString).value(), true)
          .value());
  const auto second_version = schema::SchemaVersion::initial().next().value();
  auto second = schema::TableSchema::create(table, id<schema::SchemaId>(14U), second_version,
                                            first_schema, std::move(successor_columns),
                                            {.event_time_column = timestamp,
                                             .physical_ordering_key = {timestamp},
                                             .partition_columns = {timestamp},
                                             .shard_key = {timestamp},
                                             .deduplication_key = {timestamp}});
  ASSERT_TRUE(second.has_value());
  const auto second_id = second->schema_id();
  EXPECT_TRUE(
      metadata
          ->apply_committed_schema_definition(
              2U, {.name = "events",
                   .quoted = false,
                   .schema = std::make_shared<const schema::TableSchema>(std::move(*second))})
          .is_ok());
  EXPECT_EQ(metadata->find_active_table_definition(table)->schema->schema_id(), second_id);
  EXPECT_EQ(metadata->find_active_table_definition("events", false)->schema->schema_id(),
            second_id);
  EXPECT_EQ(metadata->find_active_table_definition("events", true), nullptr);
  EXPECT_TRUE(
      metadata
          ->apply_committed(3U, TablePolicyMetadata{table, 60'000'000'000LL, 86'400'000'000'000LL,
                                                    3'600'000'000'000LL, 5'000'000'000LL, 8192U})
          .is_ok());
  ASSERT_NE(metadata->find_table_policy(table), nullptr);
  EXPECT_EQ(metadata->find_table_policy(table)->allowed_lateness_ns, 5'000'000'000LL);
  EXPECT_EQ(metadata->find_retention(table)->system_history_ns, 3'600'000'000'000LL);
  EXPECT_FALSE(
      metadata->apply_committed(4U, RetentionMetadata{table, 3'600'000'000'001LL, 8192U}).is_ok());
  EXPECT_EQ(metadata->applied_index(), 3U);
}

TEST(MetadataStateMachineTest, RejectsOneRaftGroupBoundToDifferentTablets) {
  auto metadata = MetadataStateMachine::create();
  ASSERT_TRUE(metadata.has_value());
  const auto table = id<schema::TableId>(20U);
  const auto first = id<schema::TabletId>(21U);
  const auto second = id<schema::TabletId>(22U);
  const GroupId first_group = group(23U);
  EXPECT_FALSE(
      metadata
          ->apply_committed_tablet_group_binding(1U, TabletGroupBindingMetadata{first, first_group})
          .is_ok());
  EXPECT_EQ(metadata->applied_index(), 0U);
  EXPECT_TRUE(
      metadata->apply_committed(1U, TabletPlacementMetadata{table, first, 1U, {1U}, 1U}).is_ok());
  EXPECT_TRUE(
      metadata
          ->apply_committed_tablet_group_binding(2U, TabletGroupBindingMetadata{first, first_group})
          .is_ok());
  EXPECT_TRUE(
      metadata->apply_committed(3U, TabletPlacementMetadata{table, second, 1U, {1U}, 1U}).is_ok());
  EXPECT_FALSE(metadata
                   ->apply_committed_tablet_group_binding(
                       4U, TabletGroupBindingMetadata{second, first_group})
                   .is_ok());
  EXPECT_EQ(metadata->applied_index(), 3U);
  EXPECT_TRUE(
      metadata
          ->apply_committed_tablet_group_binding(4U, TabletGroupBindingMetadata{second, group(24U)})
          .is_ok());
}

} // namespace
} // namespace chronos::raft
