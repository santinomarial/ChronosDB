#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_transport.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

[[nodiscard]] schema::TabletId tablet() {
  return schema::TabletId::from_uuid(uuid(2U)).value();
}

[[nodiscard]] schema::LogicalType string_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
}

[[nodiscard]] std::vector<query::VectorGroupKeyDefinition> keys() {
  return {{.column_ordinal = 0U, .type = string_type(), .nullable = false}};
}

[[nodiscard]] std::vector<query::VectorAggregateDefinition> aggregates() {
  return {{.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleAuthority
authority(const common::Uuid query_id = uuid(1U), const raft::NodeId first_destination = 3U,
          const raft::NodeId second_destination = 4U) {
  return DistributedVectorGroupedAggregateShuffleAuthority::create(
             query_id, {{.tablet_id = tablet(), .node_id = 2U}},
             {{.partition_id = 0U, .node_id = first_destination},
              {.partition_id = 1U, .node_id = second_destination}},
             keys(), aggregates())
      .value();
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleFrameV1
frame(const DistributedVectorGroupedAggregateShuffleAuthority& expected, std::string key) {
  std::vector<query::ScalarValue> values;
  values.push_back(query::ScalarValue::text(string_type(), std::move(key)).value());
  const auto hash = query::canonical_vector_group_key_hash_v1(expected.key_definitions(), values);
  const auto partition = static_cast<std::uint32_t>(*hash % expected.partition_count());
  auto state = query::MergeableVectorAggregateState::create(aggregates().front()).value();
  EXPECT_TRUE(state.accumulate_count_star().has_value());
  std::vector<query::MergeableVectorAggregateState> states;
  states.push_back(std::move(state));
  return {.query_id = expected.query_id(),
          .edge = {.tablet_id = tablet(),
                   .partition_id = partition,
                   .source_node_id = 2U,
                   .target_node_id = expected.destination_node(partition).value(),
                   .hash_version = expected.hash_version()},
          .partition_count = expected.partition_count(),
          .payload = {{.query_id = expected.query_id(),
                       .tablet_id = tablet(),
                       .sequence = 1U,
                       .group_ordinal = 0U,
                       .group_count = 1U,
                       .terminal = true,
                       .empty = false},
                      std::move(values),
                      std::move(states)}};
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleFrameV1
empty_frame(const DistributedVectorGroupedAggregateShuffleAuthority& expected,
            const std::uint32_t partition) {
  return {.query_id = expected.query_id(),
          .edge = {.tablet_id = tablet(),
                   .partition_id = partition,
                   .source_node_id = 2U,
                   .target_node_id = expected.destination_node(partition).value(),
                   .hash_version = expected.hash_version()},
          .partition_count = expected.partition_count(),
          .payload = {{.query_id = expected.query_id(),
                       .tablet_id = tablet(),
                       .sequence = 1U,
                       .group_ordinal = 0U,
                       .group_count = 0U,
                       .terminal = true,
                       .empty = true},
                      {},
                      {}}};
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
}

TEST(DistributedVectorGroupedAggregateShuffleTransportTest,
     RoundTripsAuthorityBoundGroupAndCanonicalEmptyPartition) {
  auto expected = authority();
  const auto message = frame(expected, "partition-key-larger-than-SSO");
  auto encoded = encode_distributed_vector_grouped_aggregate_shuffle_frame_v1(message, expected);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  const std::array<std::byte, 8U> magic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                        std::byte{'V'}, std::byte{'G'}, std::byte{'S'},
                                        std::byte{'F'}, std::byte{'1'}};
  EXPECT_TRUE(std::ranges::equal(std::span{*encoded}.first(magic.size()), magic));

  query::QueryResourceContext resources = query::QueryResourceContext::create(4U << 20U).value();
  {
    auto decoded = decode_distributed_vector_grouped_aggregate_shuffle_frame_v1_exact(
        *encoded, expected, resources);
    ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
    EXPECT_EQ(decoded->query_id, expected.query_id());
    EXPECT_EQ(decoded->edge.partition_id, message.edge.partition_id);
    EXPECT_EQ(decoded->edge.target_node_id, message.edge.target_node_id);
    EXPECT_EQ(decoded->partition_count, 2U);
    EXPECT_EQ(std::get<std::string>(decoded->payload.keys()[0].storage()),
              "partition-key-larger-than-SSO");
    EXPECT_GT(resources.reserved_memory_bytes(), 0U);
  }
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);

  const auto empty = empty_frame(expected, 1U);
  const auto encoded_empty =
      encode_distributed_vector_grouped_aggregate_shuffle_frame_v1(empty, expected).value();
  auto decoded_empty = decode_distributed_vector_grouped_aggregate_shuffle_frame_v1_exact(
      encoded_empty, expected, resources);
  ASSERT_TRUE(decoded_empty.has_value());
  EXPECT_TRUE(decoded_empty->payload.position().empty);
  EXPECT_EQ(decoded_empty->edge.partition_id, 1U);
}

TEST(DistributedVectorGroupedAggregateShuffleTransportTest,
     RejectsWrongPartitionAuthorityDamageVersionAndNonexactBytes) {
  auto expected = authority();
  auto message = frame(expected, "routed-key");
  const std::uint32_t wrong_partition = message.edge.partition_id == 0U ? 1U : 0U;
  message.edge.partition_id = wrong_partition;
  message.edge.target_node_id = expected.destination_node(wrong_partition).value();
  EXPECT_EQ(encode_distributed_vector_grouped_aggregate_shuffle_frame_v1(message, expected)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  auto valid = frame(expected, "routed-key");
  const auto encoded =
      encode_distributed_vector_grouped_aggregate_shuffle_frame_v1(valid, expected).value();
  query::QueryResourceContext resources = query::QueryResourceContext::create(4U << 20U).value();
  auto different_destination = authority(uuid(1U), 9U, 10U);
  EXPECT_EQ(decode_distributed_vector_grouped_aggregate_shuffle_frame_v1_exact(
                encoded, different_destination, resources)
                .error()
                .code(),
            common::StatusCode::kCorruption);
  EXPECT_EQ(decode_distributed_vector_grouped_aggregate_shuffle_frame_v1_exact(
                std::span{encoded}.first(encoded.size() - 1U), expected, resources)
                .error()
                .code(),
            common::StatusCode::kCorruption);

  auto damaged = encoded;
  damaged[kDistributedVectorGroupedAggregateShuffleFrameV1HeaderSize] ^= std::byte{1U};
  EXPECT_EQ(decode_distributed_vector_grouped_aggregate_shuffle_frame_v1_exact(damaged, expected,
                                                                               resources)
                .error()
                .code(),
            common::StatusCode::kCorruption);

  auto unknown = encoded;
  unknown[8U] = std::byte{2U};
  store_u32(unknown, 124U, common::crc32c(std::span{unknown}.first(124U)));
  store_u32(unknown, unknown.size() - 4U,
            common::crc32c(std::span{unknown}.first(unknown.size() - 4U)));
  EXPECT_EQ(decode_distributed_vector_grouped_aggregate_shuffle_frame_v1_exact(unknown, expected,
                                                                               resources)
                .error()
                .code(),
            common::StatusCode::kNotSupported);
}

TEST(DistributedVectorGroupedAggregateShuffleTransportTest,
     ReaderOwnsEverySplitAndCursorOwnsShortWriteProgress) {
  auto expected = authority();
  const auto message = frame(expected, "fragmented-partition-key");
  const auto encoded =
      encode_distributed_vector_grouped_aggregate_shuffle_frame_v1(message, expected).value();
  for (std::size_t split = 0U; split < encoded.size(); ++split) {
    SCOPED_TRACE(split);
    query::QueryResourceContext resources = query::QueryResourceContext::create(4U << 20U).value();
    DistributedVectorGroupedAggregateShuffleFrameV1Reader reader{expected, resources};
    auto first = reader.consume(std::span{encoded}.first(split));
    ASSERT_TRUE(first.has_value()) << first.error().to_string();
    EXPECT_EQ(first->consumed_bytes, split);
    EXPECT_FALSE(first->frame.has_value());
    std::vector<std::byte> suffix(encoded.begin() + static_cast<std::ptrdiff_t>(split),
                                  encoded.end());
    suffix.push_back(std::byte{0x5aU});
    auto second = reader.consume(suffix);
    ASSERT_TRUE(second.has_value()) << second.error().to_string();
    EXPECT_EQ(second->consumed_bytes, encoded.size() - split);
    ASSERT_TRUE(second->frame.has_value());
    EXPECT_EQ(second->frame->edge.partition_id, message.edge.partition_id);
    EXPECT_EQ(reader.buffered_bytes(), 0U);
  }

  auto cursor =
      DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor::create(message, expected).value();
  std::size_t consumed{};
  while (!cursor.complete()) {
    const std::size_t step = std::min<std::size_t>(7U, cursor.pending_write().size());
    EXPECT_TRUE(cursor.consume_written(step).is_ok());
    consumed += step;
  }
  EXPECT_EQ(cursor.written_bytes(), encoded.size());
  EXPECT_EQ(consumed, encoded.size());
  EXPECT_EQ(cursor.consume_written(1U).code(), common::StatusCode::kInvalidArgument);
  auto moved = std::move(cursor);
  EXPECT_TRUE(cursor.complete());
  EXPECT_TRUE(moved.complete());

  auto damaged = encoded;
  damaged[0U] ^= std::byte{1U};
  query::QueryResourceContext resources = query::QueryResourceContext::create(4U << 20U).value();
  DistributedVectorGroupedAggregateShuffleFrameV1Reader failed{expected, resources};
  EXPECT_EQ(failed.consume(damaged).error().code(), common::StatusCode::kCorruption);
  EXPECT_TRUE(failed.failed());
  EXPECT_EQ(failed.consume(encoded).error().code(), common::StatusCode::kCorruption);
}

} // namespace
} // namespace chronos::cluster
