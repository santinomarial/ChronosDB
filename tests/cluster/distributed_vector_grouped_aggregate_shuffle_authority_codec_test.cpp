#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_authority_codec.hpp"
#include "chronos/common/crc32c.hpp"

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

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleAuthority authority() {
  return DistributedVectorGroupedAggregateShuffleAuthority::create(
             uuid(1U),
             {{schema::TabletId::from_uuid(uuid(2U)).value(), 3U},
              {schema::TabletId::from_uuid(uuid(3U)).value(), 4U}},
             {{0U, 3U}, {1U, 4U}},
             {{0U, type(schema::LogicalTypeKind::kString), true},
              {1U, type(schema::LogicalTypeKind::kInt64), false}},
             {{query::VectorAggregateOperation::kCountStar, std::nullopt},
              {query::VectorAggregateOperation::kSum,
               query::VectorAggregateInput{2U, type(schema::LogicalTypeKind::kFloat64), true}}})
      .value();
}

void store_u16(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint16_t value) {
  bytes[offset] = static_cast<std::byte>(value & 0xffU);
  bytes[offset + 1U] = static_cast<std::byte>((value >> 8U) & 0xffU);
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < 4U; ++index)
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
}

void refresh_checksums(std::vector<std::byte>& bytes) {
  store_u32(bytes, 68U, common::crc32c(common::ByteView{bytes}.first(68U)));
  store_u32(bytes, bytes.size() - 4U,
            common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

TEST(DistributedVectorGroupedAggregateShuffleAuthorityCodecTest,
     RoundTripsCompleteNodeKeyAndAggregateAuthority) {
  auto expected = authority();
  auto encoded = encode_distributed_vector_grouped_aggregate_shuffle_authority(expected);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  auto decoded =
      decode_distributed_vector_grouped_aggregate_shuffle_authority_exact(encoded->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->query_id(), expected.query_id());
  EXPECT_EQ(decoded->hash_version(), expected.hash_version());
  EXPECT_TRUE(std::ranges::equal(decoded->sources(), expected.sources()));
  EXPECT_TRUE(std::ranges::equal(decoded->destinations(), expected.destinations()));
  ASSERT_EQ(decoded->key_definitions().size(), expected.key_definitions().size());
  for (std::size_t index = 0U; index < expected.key_definitions().size(); ++index) {
    EXPECT_EQ(decoded->key_definitions()[index].column_ordinal,
              expected.key_definitions()[index].column_ordinal);
    EXPECT_EQ(decoded->key_definitions()[index].type, expected.key_definitions()[index].type);
    EXPECT_EQ(decoded->key_definitions()[index].nullable,
              expected.key_definitions()[index].nullable);
  }
  EXPECT_TRUE(
      std::ranges::equal(decoded->aggregate_definitions(), expected.aggregate_definitions()));
}

TEST(DistributedVectorGroupedAggregateShuffleAuthorityCodecTest,
     RejectsDamageUnknownVersionNoncanonicalDescriptorsAndCallerLimits) {
  auto expected = authority();
  auto encoded = encode_distributed_vector_grouped_aggregate_shuffle_authority(expected).value();
  std::vector<std::byte> bytes(encoded.bytes().begin(), encoded.bytes().end());

  auto damaged = bytes;
  damaged.back() ^= std::byte{1U};
  EXPECT_EQ(
      decode_distributed_vector_grouped_aggregate_shuffle_authority_exact(damaged).error().code(),
      common::StatusCode::kCorruption);

  auto future = bytes;
  store_u16(future, 8U, 2U);
  refresh_checksums(future);
  EXPECT_EQ(
      decode_distributed_vector_grouped_aggregate_shuffle_authority_exact(future).error().code(),
      common::StatusCode::kNotSupported);

  auto reserved = bytes;
  reserved[72U] = std::byte{1U};
  refresh_checksums(reserved);
  EXPECT_EQ(
      decode_distributed_vector_grouped_aggregate_shuffle_authority_exact(reserved).error().code(),
      common::StatusCode::kCorruption);

  DistributedVectorGroupedAggregateShuffleAuthorityDecodeLimits limits;
  limits.authority.maximum_sources = 1U;
  EXPECT_EQ(decode_distributed_vector_grouped_aggregate_shuffle_authority_exact(bytes, limits)
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
}

} // namespace
} // namespace chronos::cluster
