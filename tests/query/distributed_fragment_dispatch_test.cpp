#include "chronos/common/byte_reader.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/query/distributed_fragment_dispatch.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

[[nodiscard]] DistributedAggregateFragmentDispatch dispatch() {
  return DistributedAggregateFragmentDispatch{
      .raft_group_id = uuid(9U),
      .fragment = {.query_id = uuid(1U),
                   .database_id = manifest::DatabaseId::from_uuid(uuid(2U)).value(),
                   .table_id = schema::TableId::from_uuid(uuid(3U)).value(),
                   .tablet_id = schema::TabletId::from_uuid(uuid(4U)).value(),
                   .destination_schema_id = schema::SchemaId::from_uuid(uuid(5U)).value(),
                   .snapshot_generation = 6U,
                   .serving_node = 7U,
                   .applied_position = 10U,
                   .observed_leader_commit_position = 10U,
                   .placement_epoch = 8U,
                   .read_policy = {.consistency = DistributedReadConsistency::kLeaderLinearizable,
                                   .maximum_staleness_positions = std::nullopt},
                   .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
                   .destination_column_ordinals = {1U},
                   .aggregate_input_index = 0U,
                   .event_time_predicate = std::nullopt}};
}

[[nodiscard]] DistributedGroupedFloat64FragmentDispatch grouped_dispatch() {
  DistributedAggregateFragmentDispatch aggregate = dispatch();
  return {.raft_group_id = aggregate.raft_group_id,
          .fragment = {.aggregate = std::move(aggregate.fragment), .group_key_input_index = 0U}};
}

void store_u16_le(std::vector<std::byte>& bytes, const std::size_t offset,
                  const std::uint16_t value) {
  bytes[offset] = static_cast<std::byte>(value & 0xffU);
  bytes[offset + 1U] = static_cast<std::byte>((value >> 8U) & 0xffU);
}

void store_u32_le(std::vector<std::byte>& bytes, const std::size_t offset,
                  const std::uint32_t value) {
  for (std::size_t index = 0U; index < 4U; ++index)
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
}

void rewrite_checksums(std::vector<std::byte>& bytes) {
  store_u32_le(bytes, distributed_fragment_dispatch_format::kHeaderLength - 4U,
               common::crc32c(common::ByteView{bytes}.first(
                   distributed_fragment_dispatch_format::kHeaderLength - 4U)));
  store_u32_le(bytes, bytes.size() - 4U,
               common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

void rewrite_grouped_checksums(std::vector<std::byte>& bytes) {
  store_u32_le(bytes, distributed_grouped_float64_fragment_dispatch_format::kHeaderLength - 4U,
               common::crc32c(common::ByteView{bytes}.first(
                   distributed_grouped_float64_fragment_dispatch_format::kHeaderLength - 4U)));
  store_u32_le(bytes, bytes.size() - 4U,
               common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

TEST(DistributedFragmentDispatchTest, RoundTripsExactGroupAndInnerFragment) {
  const DistributedAggregateFragmentDispatch expected = dispatch();
  const auto encoded = encode_distributed_aggregate_fragment_dispatch(expected);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  EXPECT_EQ(encoded->bytes().size(), 308U);
  EXPECT_EQ(encoded->bytes()[24U], std::byte{9U});
  common::ByteReader lengths{encoded->bytes().subspan(12U)};
  EXPECT_EQ(lengths.read_u32_le().value(), distributed_fragment_dispatch_format::kHeaderLength);
  EXPECT_EQ(lengths.read_u64_le().value(), encoded->bytes().size());
  common::ByteReader inner_length{encoded->bytes().subspan(40U)};
  EXPECT_EQ(inner_length.read_u64_le().value(), 224U);
  common::ByteReader header_crc{encoded->bytes().subspan(76U, 4U)};
  EXPECT_EQ(header_crc.read_u32_le().value(), 0xbc4f864dU);
  common::ByteReader frame_crc{encoded->bytes().last(4U)};
  EXPECT_EQ(frame_crc.read_u32_le().value(), 0x28572978U);

  const auto decoded = decode_distributed_aggregate_fragment_dispatch_exact(encoded->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, expected);
}

TEST(DistributedFragmentDispatchTest, RejectsDamageUnknownVersionAndNilGroup) {
  const auto encoded = encode_distributed_aggregate_fragment_dispatch(dispatch());
  ASSERT_TRUE(encoded.has_value());
  std::vector<std::byte> bytes(encoded->bytes().begin(), encoded->bytes().end());
  EXPECT_EQ(decode_distributed_aggregate_fragment_dispatch_exact(
                common::ByteView{bytes}.first(bytes.size() - 1U))
                .error()
                .code(),
            common::StatusCode::kCorruption);

  std::vector<std::byte> corrupt = bytes;
  corrupt[24U] ^= std::byte{1U};
  EXPECT_EQ(decode_distributed_aggregate_fragment_dispatch_exact(corrupt).error().code(),
            common::StatusCode::kCorruption);
  corrupt = bytes;
  corrupt[80U] ^= std::byte{1U};
  EXPECT_EQ(decode_distributed_aggregate_fragment_dispatch_exact(corrupt).error().code(),
            common::StatusCode::kCorruption);

  std::vector<std::byte> future = bytes;
  store_u16_le(future, 8U, distributed_fragment_dispatch_format::kMajor + 1U);
  rewrite_checksums(future);
  EXPECT_EQ(decode_distributed_aggregate_fragment_dispatch_exact(future).error().code(),
            common::StatusCode::kNotSupported);

  DistributedAggregateFragmentDispatch nil_group = dispatch();
  nil_group.raft_group_id = common::Uuid{};
  EXPECT_EQ(encode_distributed_aggregate_fragment_dispatch(nil_group).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedFragmentDispatchTest, RoundTripsDistinctGroupedIntentAndExactGroup) {
  const DistributedGroupedFloat64FragmentDispatch expected = grouped_dispatch();
  const auto encoded = encode_distributed_grouped_float64_fragment_dispatch(expected);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  EXPECT_EQ(encoded->bytes().size(), 352U);
  const std::array<std::byte, 8U> magic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                        std::byte{'G'}, std::byte{'D'}, std::byte{'S'},
                                        std::byte{'P'}, std::byte{'1'}};
  EXPECT_TRUE(std::ranges::equal(encoded->bytes().first(magic.size()), magic));
  EXPECT_EQ(encoded->bytes()[24U], std::byte{9U});
  common::ByteReader lengths{encoded->bytes().subspan(12U)};
  EXPECT_EQ(lengths.read_u32_le().value(),
            distributed_grouped_float64_fragment_dispatch_format::kHeaderLength);
  EXPECT_EQ(lengths.read_u64_le().value(), encoded->bytes().size());
  common::ByteReader inner_length{encoded->bytes().subspan(40U)};
  EXPECT_EQ(inner_length.read_u64_le().value(), 268U);
  common::ByteReader header_crc{encoded->bytes().subspan(76U, 4U)};
  EXPECT_EQ(header_crc.read_u32_le().value(), common::crc32c(encoded->bytes().first(76U)));
  common::ByteReader frame_crc{encoded->bytes().last(4U)};
  EXPECT_EQ(frame_crc.read_u32_le().value(),
            common::crc32c(encoded->bytes().first(encoded->bytes().size() - 4U)));

  const auto decoded = decode_distributed_grouped_float64_fragment_dispatch_exact(encoded->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, expected);
  EXPECT_EQ(decode_distributed_aggregate_fragment_dispatch_exact(encoded->bytes()).error().code(),
            common::StatusCode::kCorruption);
  const auto ungrouped = encode_distributed_aggregate_fragment_dispatch(dispatch());
  ASSERT_TRUE(ungrouped.has_value());
  EXPECT_EQ(
      decode_distributed_grouped_float64_fragment_dispatch_exact(ungrouped->bytes()).error().code(),
      common::StatusCode::kCorruption);
}

TEST(DistributedFragmentDispatchTest, RejectsGroupedDamageVersionsBoundsAndNilGroup) {
  const auto encoded = encode_distributed_grouped_float64_fragment_dispatch(grouped_dispatch());
  ASSERT_TRUE(encoded.has_value());
  const std::vector<std::byte> canonical(encoded->bytes().begin(), encoded->bytes().end());
  EXPECT_EQ(decode_distributed_grouped_float64_fragment_dispatch_exact(
                common::ByteView{canonical}.first(canonical.size() - 1U))
                .error()
                .code(),
            common::StatusCode::kCorruption);
  std::vector<std::byte> trailing = canonical;
  trailing.push_back(std::byte{0U});
  EXPECT_EQ(decode_distributed_grouped_float64_fragment_dispatch_exact(trailing).error().code(),
            common::StatusCode::kCorruption);
  std::vector<std::byte> corrupt = canonical;
  corrupt[24U] ^= std::byte{1U};
  EXPECT_EQ(decode_distributed_grouped_float64_fragment_dispatch_exact(corrupt).error().code(),
            common::StatusCode::kCorruption);
  corrupt = canonical;
  corrupt[80U] ^= std::byte{1U};
  EXPECT_EQ(decode_distributed_grouped_float64_fragment_dispatch_exact(corrupt).error().code(),
            common::StatusCode::kCorruption);
  std::vector<std::byte> future = canonical;
  store_u16_le(future, 8U, distributed_grouped_float64_fragment_dispatch_format::kMajor + 1U);
  rewrite_grouped_checksums(future);
  EXPECT_EQ(decode_distributed_grouped_float64_fragment_dispatch_exact(future).error().code(),
            common::StatusCode::kNotSupported);

  DistributedGroupedFloat64FragmentDispatch nil_group = grouped_dispatch();
  nil_group.raft_group_id = {};
  EXPECT_EQ(encode_distributed_grouped_float64_fragment_dispatch(nil_group).error().code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::query
