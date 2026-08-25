#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_authority.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  return Identifier::from_uuid(uuid(seed)).value();
}

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

[[nodiscard]] std::vector<query::VectorGroupKeyDefinition> keys() {
  return {{.column_ordinal = 0U, .type = type(schema::LogicalTypeKind::kString), .nullable = false},
          {.column_ordinal = 1U, .type = type(schema::LogicalTypeKind::kBool), .nullable = true}};
}

[[nodiscard]] std::vector<query::VectorAggregateDefinition> aggregates() {
  return {{.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
}

[[nodiscard]] std::vector<DistributedVectorGroupedAggregateShuffleSource> sources() {
  return {{.tablet_id = id<schema::TabletId>(1U), .node_id = 10U},
          {.tablet_id = id<schema::TabletId>(2U), .node_id = 20U}};
}

[[nodiscard]] std::vector<DistributedVectorGroupedAggregateShuffleDestination> destinations() {
  return {{.partition_id = 0U, .node_id = 30U},
          {.partition_id = 1U, .node_id = 40U},
          {.partition_id = 2U, .node_id = 30U},
          {.partition_id = 3U, .node_id = 40U}};
}

[[nodiscard]] query::DistributedMutableVectorFragment fragment(const std::uint8_t tablet_seed,
                                                               const raft::NodeId node_id) {
  const auto int64 = type(schema::LogicalTypeKind::kInt64);
  return {.query_id = uuid(9U),
          .database_id = id<manifest::DatabaseId>(50U),
          .table_id = id<schema::TableId>(51U),
          .tablet_id = id<schema::TabletId>(tablet_seed),
          .destination_schema_id = id<schema::SchemaId>(52U),
          .raft_group_id = uuid(53U),
          .serving_node = node_id,
          .applied_position = 10U,
          .observed_leader_commit_position = 10U,
          .placement_epoch = 8U,
          .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
          .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
          .destination_column_ordinals = {0U},
          .plan = {.mode = query::DistributedVectorPlanMode::kRows, .row_output_indices = {0U}},
          .result_schema = {.columns = {{"value", int64, false}}}};
}

TEST(DistributedVectorGroupedAggregateShuffleAuthorityTest,
     OwnsCompleteSourceOrderAndCanonicalPartitionDestinations) {
  const auto expected_sources = sources();
  const auto expected_destinations = destinations();
  const auto expected_aggregates = aggregates();
  auto authority = DistributedVectorGroupedAggregateShuffleAuthority::create(
      uuid(9U), expected_sources, expected_destinations, keys(), expected_aggregates);
  ASSERT_TRUE(authority.has_value()) << authority.error().to_string();
  EXPECT_EQ(authority->query_id(), uuid(9U));
  EXPECT_EQ(authority->hash_version(), 1U);
  EXPECT_EQ(authority->partition_count(), 4U);
  EXPECT_TRUE(std::ranges::equal(authority->sources(), expected_sources));
  EXPECT_TRUE(std::ranges::equal(authority->destinations(), expected_destinations));
  EXPECT_EQ(authority->key_definitions().size(), 2U);
  EXPECT_TRUE(std::ranges::equal(authority->aggregate_definitions(), expected_aggregates));
  EXPECT_GT(authority->retained_configuration_bytes(), 0U);
  EXPECT_EQ(authority->source_node(id<schema::TabletId>(1U)).value(), 10U);
  EXPECT_EQ(authority->destination_node(3U).value(), 40U);
  EXPECT_TRUE(
      authority
          ->validate_edge({.tablet_id = id<schema::TabletId>(2U),
                           .partition_id = 2U,
                           .source_node_id = 20U,
                           .target_node_id = 30U,
                           .hash_version = kDistributedVectorGroupedAggregateShuffleHashVersionV1})
          .is_ok());
}

TEST(DistributedVectorGroupedAggregateShuffleAuthorityTest,
     RejectsIdentityCountCanonicalityAndConfigurationDamage) {
  auto duplicate_sources = sources();
  duplicate_sources[1].tablet_id = duplicate_sources[0].tablet_id;
  EXPECT_EQ(DistributedVectorGroupedAggregateShuffleAuthority::create(
                uuid(9U), std::move(duplicate_sources), destinations(), keys(), aggregates())
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  auto noncanonical_destinations = destinations();
  noncanonical_destinations[2].partition_id = 3U;
  EXPECT_EQ(DistributedVectorGroupedAggregateShuffleAuthority::create(
                uuid(9U), sources(), std::move(noncanonical_destinations), keys(), aggregates())
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  auto zero_node_sources = sources();
  zero_node_sources[0].node_id = 0U;
  EXPECT_EQ(DistributedVectorGroupedAggregateShuffleAuthority::create(
                uuid(9U), std::move(zero_node_sources), destinations(), keys(), aggregates())
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  EXPECT_EQ(DistributedVectorGroupedAggregateShuffleAuthority::create(
                common::Uuid{}, sources(), destinations(), keys(), aggregates())
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  DistributedVectorGroupedAggregateShuffleAuthorityLimits limits;
  limits.maximum_sources = 1U;
  EXPECT_EQ(DistributedVectorGroupedAggregateShuffleAuthority::create(
                uuid(9U), sources(), destinations(), keys(), aggregates(), limits)
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
  limits.maximum_sources = kMaximumDistributedVectorGroupedAggregateShuffleSources;
  limits.maximum_retained_configuration_bytes = 1U;
  EXPECT_EQ(DistributedVectorGroupedAggregateShuffleAuthority::create(
                uuid(9U), sources(), destinations(), keys(), aggregates(), limits)
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
}

TEST(DistributedVectorGroupedAggregateShuffleAuthorityTest,
     FailsClosedForEveryUnboundSourceDestinationAndHashVersion) {
  auto authority = DistributedVectorGroupedAggregateShuffleAuthority::create(
                       uuid(9U), sources(), destinations(), keys(), aggregates())
                       .value();
  EXPECT_EQ(authority.source_node(id<schema::TabletId>(8U)).error().code(),
            common::StatusCode::kNotFound);
  EXPECT_EQ(authority.destination_node(4U).error().code(), common::StatusCode::kNotFound);
  EXPECT_EQ(authority.validate_edge({id<schema::TabletId>(1U), 0U, 11U, 30U, 1U}).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(authority.validate_edge({id<schema::TabletId>(1U), 0U, 10U, 31U, 1U}).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(authority.validate_edge({id<schema::TabletId>(1U), 0U, 10U, 30U, 2U}).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(authority.validate_edge({id<schema::TabletId>(8U), 0U, 10U, 30U, 1U}).code(),
            common::StatusCode::kNotFound);
}

TEST(DistributedVectorGroupedAggregateShuffleAuthorityTest,
     DerivesPlanOrderSourcesAndSortedUniqueServingNodePartitions) {
  std::vector fragments{fragment(1U, 40U), fragment(2U, 20U), fragment(3U, 40U)};
  auto authority = DistributedVectorGroupedAggregateShuffleAuthority::create_from_mutable_fragments(
      fragments, keys(), aggregates());
  ASSERT_TRUE(authority.has_value()) << authority.error().to_string();
  ASSERT_EQ(authority->sources().size(), 3U);
  EXPECT_EQ(authority->sources()[0].tablet_id, id<schema::TabletId>(1U));
  EXPECT_EQ(authority->sources()[0].node_id, 40U);
  EXPECT_EQ(authority->sources()[1].tablet_id, id<schema::TabletId>(2U));
  ASSERT_EQ(authority->destinations().size(), 2U);
  EXPECT_EQ(authority->destinations()[0],
            (DistributedVectorGroupedAggregateShuffleDestination{0U, 20U}));
  EXPECT_EQ(authority->destinations()[1],
            (DistributedVectorGroupedAggregateShuffleDestination{1U, 40U}));

  fragments[1].query_id = uuid(8U);
  EXPECT_EQ(DistributedVectorGroupedAggregateShuffleAuthority::create_from_mutable_fragments(
                fragments, keys(), aggregates())
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  fragments[1].query_id = uuid(9U);
  fragments[1].serving_node = 0U;
  EXPECT_EQ(DistributedVectorGroupedAggregateShuffleAuthority::create_from_mutable_fragments(
                fragments, keys(), aggregates())
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  EXPECT_EQ(DistributedVectorGroupedAggregateShuffleAuthority::create_from_mutable_fragments(
                {}, keys(), aggregates())
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  fragments[1].serving_node = 20U;
  DistributedVectorGroupedAggregateShuffleAuthorityLimits limits;
  limits.maximum_partitions = 1U;
  EXPECT_EQ(DistributedVectorGroupedAggregateShuffleAuthority::create_from_mutable_fragments(
                fragments, keys(), aggregates(), limits)
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
}

} // namespace
} // namespace chronos::cluster
