#include "chronos/raft/metadata.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>

namespace chronos::raft {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
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
      metadata->apply_committed(3U, TabletPlacementMetadata{table, tablet, 1U, {1U, 2U, 3U}, 1U})
          .is_ok());
  EXPECT_TRUE(metadata->apply_committed(4U, RetentionMetadata{table, 1000, 100U}).is_ok());
  EXPECT_EQ(metadata->applied_index(), 4U);
  EXPECT_EQ(metadata->find_node(1U)->endpoint, "node-1");
  EXPECT_EQ(metadata->find_schema(schema_id)->table_id, table);
  EXPECT_EQ(metadata->find_tablet(tablet)->leader_hint, 1U);
  EXPECT_EQ(metadata->find_retention(table)->system_history_ns, 1000);
  EXPECT_FALSE(metadata->apply_committed(6U, ClusterNodeMetadata{2U, "node-2"}).is_ok());
  EXPECT_EQ(metadata->applied_index(), 4U);
}

} // namespace
} // namespace chronos::raft
