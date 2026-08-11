#include "chronos/cluster/tablet_physical_part_chunk.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <ranges>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

template <typename Identity> [[nodiscard]] Identity id(const std::uint8_t seed) {
  return Identity::from_uuid(uuid(seed)).value();
}

[[nodiscard]] ingest::Sha256Digest digest(const std::uint8_t seed) {
  ingest::Sha256Digest::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return ingest::Sha256Digest{bytes};
}

[[nodiscard]] TabletPhysicalPartTransferSession session() {
  return {.table_id = id<schema::TableId>(1U),
          .tablet_id = id<schema::TabletId>(2U),
          .group_id = uuid(3U),
          .placement_epoch = 4U,
          .source_node = 5U,
          .target_node = 6U,
          .manifest_generation = 7U,
          .part_id = id<cseg::PartId>(8U),
          .total_bytes = 5U,
          .content_sha256 = digest(9U)};
}

[[nodiscard]] TabletPhysicalPartChunk chunk() {
  return {.session = session(),
          .offset = 2U,
          .bytes = {std::byte{0x10U}, std::byte{0x20U}, std::byte{0x30U}}};
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
}

TEST(TabletPhysicalPartChunkTest, RoundTripsExactMovementAndObjectIdentity) {
  const TabletPhysicalPartChunk expected = chunk();
  const auto first = encode_tablet_physical_part_chunk_v1(expected);
  const auto second = encode_tablet_physical_part_chunk_v1(expected);
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  ASSERT_TRUE(second.has_value()) << second.error().to_string();
  EXPECT_TRUE(std::ranges::equal(*first, *second));
  EXPECT_EQ(first->size(), kTabletPhysicalPartChunkHeaderSize + expected.bytes.size() +
                               kTabletPhysicalPartChunkTrailerSize);

  const auto decoded = decode_tablet_physical_part_chunk_v1(*first);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, expected);
}

TEST(TabletPhysicalPartChunkTest, RejectsInvalidSessionsBoundsAndResourceLimits) {
  TabletPhysicalPartChunk value = chunk();
  value.session.group_id = {};
  EXPECT_EQ(encode_tablet_physical_part_chunk_v1(value).error().code(),
            common::StatusCode::kInvalidArgument);
  value = chunk();
  value.offset = 3U;
  EXPECT_EQ(encode_tablet_physical_part_chunk_v1(value).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(encode_tablet_physical_part_chunk_v1(
                chunk(), {.maximum_object_bytes = 4U,
                          .maximum_chunk_bytes = 3U,
                          .maximum_encoded_bytes = kMaximumTabletPhysicalPartChunkSize})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  auto encoded = encode_tablet_physical_part_chunk_v1(chunk()).value();
  EXPECT_EQ(decode_tablet_physical_part_chunk_v1(
                encoded, {.maximum_object_bytes = 5U,
                          .maximum_chunk_bytes = 2U,
                          .maximum_encoded_bytes = kMaximumTabletPhysicalPartChunkSize})
                .error()
                .code(),
            common::StatusCode::kCorruption);
}

TEST(TabletPhysicalPartChunkTest, RejectsHeaderPayloadTrailerAndUnknownVersionDamage) {
  const auto encoded = encode_tablet_physical_part_chunk_v1(chunk()).value();
  for (const std::size_t offset :
       {std::size_t{24U}, kTabletPhysicalPartChunkHeaderSize, encoded.size() - 1U}) {
    std::vector<std::byte> damaged = encoded;
    damaged[offset] ^= std::byte{0x80U};
    EXPECT_EQ(decode_tablet_physical_part_chunk_v1(damaged).error().code(),
              common::StatusCode::kCorruption);
  }

  std::vector<std::byte> future = encoded;
  future[8U] = std::byte{2U};
  store_u32(future, 176U, 0U);
  store_u32(future, 176U,
            common::crc32c(common::ByteView{future}.first(kTabletPhysicalPartChunkHeaderSize)));
  const std::size_t trailer = future.size() - kTabletPhysicalPartChunkTrailerSize;
  store_u32(future, trailer, common::crc32c(common::ByteView{future}.first(trailer)));
  EXPECT_EQ(decode_tablet_physical_part_chunk_v1(future).error().code(),
            common::StatusCode::kNotSupported);
}

} // namespace
} // namespace chronos::cluster
