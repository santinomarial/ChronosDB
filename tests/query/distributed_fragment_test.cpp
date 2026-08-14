#include "chronos/common/byte_reader.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/query/distributed_fragment.hpp"

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

[[nodiscard]] DistributedAggregateFragment linearizable_fragment() {
  return DistributedAggregateFragment{
      .query_id = uuid(1U),
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
      .linearizable_barrier = raft::ReadBarrier{9U, 11U, 10U},
      .destination_column_ordinals = {4U, 1U},
      .aggregate_input_index = 1U,
      .event_time_predicate =
          cseg::EventTimePredicate{.lower = cseg::EventTimeBound{.value = -5, .inclusive = false},
                                   .upper = cseg::EventTimeBound{.value = 20, .inclusive = true}}};
}

void store_u16_le(std::vector<std::byte>& bytes, const std::size_t offset,
                  const std::uint16_t value) {
  bytes[offset] = static_cast<std::byte>(value & 0xffU);
  bytes[offset + 1U] = static_cast<std::byte>((value >> 8U) & 0xffU);
}

void store_u32_le(std::vector<std::byte>& bytes, const std::size_t offset,
                  const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
}

void rewrite_checksums(std::vector<std::byte>& bytes, const bool header) {
  if (header) {
    store_u32_le(bytes, distributed_fragment_format::kHeaderLength - 4U,
                 common::crc32c(common::ByteView{bytes}.first(
                     distributed_fragment_format::kHeaderLength - 4U)));
  }
  store_u32_le(bytes, bytes.size() - 4U,
               common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

[[nodiscard]] std::vector<std::byte>
copy_encoded(const EncodedDistributedAggregateFragment& encoded) {
  return {encoded.bytes().begin(), encoded.bytes().end()};
}

void rewrite_grouped_checksums(std::vector<std::byte>& bytes, const bool header) {
  if (header) {
    store_u32_le(bytes, distributed_grouped_float64_fragment_format::kHeaderLength - 4U,
                 common::crc32c(common::ByteView{bytes}.first(
                     distributed_grouped_float64_fragment_format::kHeaderLength - 4U)));
  }
  store_u32_le(bytes, bytes.size() - 4U,
               common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

[[nodiscard]] std::vector<std::byte>
copy_encoded(const EncodedDistributedGroupedFloat64Fragment& encoded) {
  return {encoded.bytes().begin(), encoded.bytes().end()};
}

TEST(DistributedFragmentTest, FreezesLayoutAndRoundTripsSnapshotProjectionAndAdmission) {
  const DistributedAggregateFragment fragment = linearizable_fragment();
  const auto encoded = encode_distributed_aggregate_fragment(fragment);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  ASSERT_EQ(encoded->bytes().size(), 228U);

  common::ByteReader reader{encoded->bytes()};
  const auto magic = reader.read_exact(8U);
  ASSERT_TRUE(magic.has_value());
  const std::array<std::byte, 8U> expected_magic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                                 std::byte{'F'}, std::byte{'R'}, std::byte{'A'},
                                                 std::byte{'G'}, std::byte{'1'}};
  EXPECT_TRUE(std::ranges::equal(*magic, expected_magic));
  EXPECT_EQ(reader.read_u16_le().value(), distributed_fragment_format::kMajor);
  EXPECT_EQ(reader.read_u16_le().value(), distributed_fragment_format::kMinor);
  EXPECT_EQ(reader.read_u32_le().value(), distributed_fragment_format::kHeaderLength);
  EXPECT_EQ(reader.read_u64_le().value(), encoded->bytes().size());
  EXPECT_EQ(encoded->bytes()[24U], std::byte{1U});
  EXPECT_EQ(encoded->bytes()[40U], std::byte{2U});
  EXPECT_EQ(encoded->bytes()[56U], std::byte{3U});
  EXPECT_EQ(encoded->bytes()[72U], std::byte{4U});
  EXPECT_EQ(encoded->bytes()[88U], std::byte{5U});
  common::ByteReader fields{encoded->bytes().subspan(104U)};
  EXPECT_EQ(fields.read_u64_le().value(), 6U);
  EXPECT_EQ(fields.read_u64_le().value(), 7U);
  EXPECT_EQ(fields.read_u64_le().value(), 10U);
  EXPECT_EQ(fields.read_u64_le().value(), 10U);
  EXPECT_EQ(fields.read_u64_le().value(), 8U);
  EXPECT_EQ(fields.read_u64_le().value(), 0U);
  EXPECT_EQ(fields.read_u64_le().value(), 9U);
  EXPECT_EQ(fields.read_u64_le().value(), 11U);
  EXPECT_EQ(fields.read_u64_le().value(), 10U);
  EXPECT_EQ(fields.read_u32_le().value(), 2U);
  EXPECT_EQ(fields.read_u32_le().value(), 1U);
  EXPECT_EQ(fields.read_u32_le().value(), 0x2dU);
  EXPECT_EQ(fields.read_u8().value(),
            static_cast<std::uint8_t>(DistributedReadConsistency::kLeaderLinearizable));
  common::ByteReader header_crc{encoded->bytes().subspan(212U, 4U)};
  EXPECT_EQ(header_crc.read_u32_le().value(), 0x949b69dfU);
  EXPECT_EQ(encoded->bytes()[216U], std::byte{4U});
  EXPECT_EQ(encoded->bytes()[220U], std::byte{1U});
  common::ByteReader frame_crc{encoded->bytes().last(4U)};
  EXPECT_EQ(frame_crc.read_u32_le().value(), 0xd7389d88U);

  const auto decoded = decode_distributed_aggregate_fragment_exact(encoded->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, fragment);
}

TEST(DistributedFragmentTest, RejectsCorruptionVersionsCanonicalViolationsAndLimits) {
  const auto encoded = encode_distributed_aggregate_fragment(linearizable_fragment());
  ASSERT_TRUE(encoded.has_value());
  const std::vector<std::byte> canonical = copy_encoded(*encoded);

  EXPECT_EQ(decode_distributed_aggregate_fragment_exact(
                common::ByteView{canonical}.first(canonical.size() - 1U))
                .error()
                .code(),
            common::StatusCode::kCorruption);
  std::vector<std::byte> trailing = canonical;
  trailing.push_back(std::byte{0});
  EXPECT_EQ(decode_distributed_aggregate_fragment_exact(trailing).error().code(),
            common::StatusCode::kCorruption);

  std::vector<std::byte> header_corrupt = canonical;
  header_corrupt[104U] ^= std::byte{1U};
  EXPECT_EQ(decode_distributed_aggregate_fragment_exact(header_corrupt).error().code(),
            common::StatusCode::kCorruption);
  std::vector<std::byte> body_corrupt = canonical;
  body_corrupt[216U] ^= std::byte{1U};
  EXPECT_EQ(decode_distributed_aggregate_fragment_exact(body_corrupt).error().code(),
            common::StatusCode::kCorruption);

  std::vector<std::byte> future = canonical;
  store_u16_le(future, 8U, distributed_fragment_format::kMajor + 1U);
  rewrite_checksums(future, true);
  EXPECT_EQ(decode_distributed_aggregate_fragment_exact(future).error().code(),
            common::StatusCode::kNotSupported);
  std::vector<std::byte> unknown_consistency = canonical;
  unknown_consistency[188U] = std::byte{0xffU};
  rewrite_checksums(unknown_consistency, true);
  EXPECT_EQ(decode_distributed_aggregate_fragment_exact(unknown_consistency).error().code(),
            common::StatusCode::kCorruption);

  std::vector<std::byte> duplicate = canonical;
  store_u32_le(duplicate, 220U, 4U);
  rewrite_checksums(duplicate, false);
  EXPECT_EQ(decode_distributed_aggregate_fragment_exact(duplicate).error().code(),
            common::StatusCode::kCorruption);
  EXPECT_EQ(
      decode_distributed_aggregate_fragment_exact(canonical, DistributedFragmentDecodeLimits{1U})
          .error()
          .code(),
      common::StatusCode::kResourceExhausted);
}

TEST(DistributedFragmentTest, EncoderRejectsNoncanonicalRoutesProjectionAndReadProofs) {
  DistributedAggregateFragment fragment = linearizable_fragment();
  fragment.destination_column_ordinals = {1U, 1U};
  EXPECT_EQ(encode_distributed_aggregate_fragment(fragment).error().code(),
            common::StatusCode::kInvalidArgument);
  fragment = linearizable_fragment();
  fragment.aggregate_input_index = 2U;
  EXPECT_EQ(encode_distributed_aggregate_fragment(fragment).error().code(),
            common::StatusCode::kInvalidArgument);
  fragment = linearizable_fragment();
  fragment.linearizable_barrier = raft::ReadBarrier{9U, 11U, 11U};
  EXPECT_EQ(encode_distributed_aggregate_fragment(fragment).error().code(),
            common::StatusCode::kInvalidArgument);
  fragment = linearizable_fragment();
  fragment.placement_epoch = 0U;
  EXPECT_EQ(encode_distributed_aggregate_fragment(fragment).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedFragmentTest, FreezesDistinctGroupedIntentAroundExactAggregateFragment) {
  const DistributedGroupedFloat64Fragment fragment{.aggregate = linearizable_fragment(),
                                                   .group_key_input_index = 0U};
  const auto encoded = encode_distributed_grouped_float64_fragment(fragment);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  ASSERT_EQ(encoded->bytes().size(), 272U);

  common::ByteReader reader{encoded->bytes()};
  const std::array<std::byte, 8U> expected_magic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                                 std::byte{'F'}, std::byte{'G'}, std::byte{'R'},
                                                 std::byte{'P'}, std::byte{'1'}};
  EXPECT_TRUE(std::ranges::equal(reader.read_exact(8U).value(), expected_magic));
  EXPECT_EQ(reader.read_u16_le().value(), distributed_grouped_float64_fragment_format::kMajor);
  EXPECT_EQ(reader.read_u16_le().value(), distributed_grouped_float64_fragment_format::kMinor);
  EXPECT_EQ(reader.read_u32_le().value(),
            distributed_grouped_float64_fragment_format::kHeaderLength);
  EXPECT_EQ(reader.read_u64_le().value(), encoded->bytes().size());
  EXPECT_EQ(reader.read_u32_le().value(), 0U);
  EXPECT_EQ(reader.read_u32_le().value(), 228U);
  const auto reserved = reader.read_exact(4U);
  ASSERT_TRUE(reserved.has_value());
  EXPECT_TRUE(
      std::ranges::all_of(*reserved, [](const std::byte value) { return value == std::byte{0U}; }));
  EXPECT_EQ(reader.read_u32_le().value(),
            common::crc32c(encoded->bytes().first(
                distributed_grouped_float64_fragment_format::kHeaderLength - 4U)));
  const std::array<std::byte, 8U> expected_inner_magic{
      std::byte{'C'}, std::byte{'H'}, std::byte{'D'}, std::byte{'F'},
      std::byte{'R'}, std::byte{'A'}, std::byte{'G'}, std::byte{'1'}};
  EXPECT_TRUE(std::ranges::equal(
      encoded->bytes().subspan(distributed_grouped_float64_fragment_format::kHeaderLength,
                               expected_inner_magic.size()),
      expected_inner_magic));
  common::ByteReader frame_crc{encoded->bytes().last(4U)};
  EXPECT_EQ(frame_crc.read_u32_le().value(),
            common::crc32c(encoded->bytes().first(encoded->bytes().size() - 4U)));

  const auto decoded = decode_distributed_grouped_float64_fragment_exact(encoded->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, fragment);
}

TEST(DistributedFragmentTest, RejectsGroupedEnvelopeDamageVersionsLengthsAndKeyBounds) {
  const DistributedGroupedFloat64Fragment fragment{.aggregate = linearizable_fragment(),
                                                   .group_key_input_index = 0U};
  const auto encoded = encode_distributed_grouped_float64_fragment(fragment);
  ASSERT_TRUE(encoded.has_value());
  const std::vector<std::byte> canonical = copy_encoded(*encoded);

  EXPECT_EQ(decode_distributed_grouped_float64_fragment_exact(
                common::ByteView{canonical}.first(canonical.size() - 1U))
                .error()
                .code(),
            common::StatusCode::kCorruption);
  std::vector<std::byte> trailing = canonical;
  trailing.push_back(std::byte{0U});
  EXPECT_EQ(decode_distributed_grouped_float64_fragment_exact(trailing).error().code(),
            common::StatusCode::kCorruption);

  std::vector<std::byte> damaged_header = canonical;
  damaged_header[24U] ^= std::byte{1U};
  EXPECT_EQ(decode_distributed_grouped_float64_fragment_exact(damaged_header).error().code(),
            common::StatusCode::kCorruption);
  std::vector<std::byte> damaged_inner = canonical;
  damaged_inner[distributed_grouped_float64_fragment_format::kHeaderLength + 104U] ^= std::byte{1U};
  rewrite_grouped_checksums(damaged_inner, false);
  EXPECT_EQ(decode_distributed_grouped_float64_fragment_exact(damaged_inner).error().code(),
            common::StatusCode::kCorruption);

  std::vector<std::byte> future = canonical;
  store_u16_le(future, 8U, distributed_grouped_float64_fragment_format::kMajor + 1U);
  rewrite_grouped_checksums(future, true);
  EXPECT_EQ(decode_distributed_grouped_float64_fragment_exact(future).error().code(),
            common::StatusCode::kNotSupported);
  std::vector<std::byte> reserved = canonical;
  reserved[32U] = std::byte{1U};
  rewrite_grouped_checksums(reserved, true);
  EXPECT_EQ(decode_distributed_grouped_float64_fragment_exact(reserved).error().code(),
            common::StatusCode::kCorruption);
  std::vector<std::byte> bad_inner_length = canonical;
  store_u32_le(bad_inner_length, 28U, 227U);
  rewrite_grouped_checksums(bad_inner_length, true);
  EXPECT_EQ(decode_distributed_grouped_float64_fragment_exact(bad_inner_length).error().code(),
            common::StatusCode::kCorruption);
  std::vector<std::byte> bad_key = canonical;
  store_u32_le(bad_key, 24U, 2U);
  rewrite_grouped_checksums(bad_key, true);
  EXPECT_EQ(decode_distributed_grouped_float64_fragment_exact(bad_key).error().code(),
            common::StatusCode::kCorruption);
  EXPECT_EQ(decode_distributed_grouped_float64_fragment_exact(canonical,
                                                              DistributedFragmentDecodeLimits{1U})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);

  DistributedGroupedFloat64Fragment invalid = fragment;
  invalid.group_key_input_index = 2U;
  EXPECT_EQ(encode_distributed_grouped_float64_fragment(invalid).error().code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::query
