#include "chronos/common/crc32c.hpp"
#include "chronos/raft/tablet_group_binding_codec.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>

namespace chronos::raft {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] GroupId group(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return GroupId{bytes};
}

TEST(TabletGroupBindingCodecTest, CanonicallyRoundTripsExactIdentities) {
  const TabletGroupBindingMetadata expected{id<schema::TabletId>(1U), group(2U)};
  auto encoded = encode_tablet_group_binding_v1(expected);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  EXPECT_EQ(encoded->size(), kTabletGroupBindingSize);
  auto decoded = decode_tablet_group_binding_v1(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, expected);
  EXPECT_EQ(encode_tablet_group_binding_v1(*decoded), encoded);
}

TEST(TabletGroupBindingCodecTest, RejectsNilDamageTruncationAndUnknownVersion) {
  EXPECT_EQ(encode_tablet_group_binding_v1({id<schema::TabletId>(3U), GroupId{}}).error().code(),
            common::StatusCode::kInvalidArgument);
  auto encoded = encode_tablet_group_binding_v1({id<schema::TabletId>(5U), group(4U)}).value();
  for (std::size_t size = 0U; size < encoded.size(); ++size) {
    auto decoded = decode_tablet_group_binding_v1({encoded.data(), size});
    EXPECT_FALSE(decoded.has_value()) << size;
  }
  auto damaged = encoded;
  damaged[60U] ^= std::byte{1U};
  EXPECT_EQ(decode_tablet_group_binding_v1(damaged).error().code(),
            common::StatusCode::kCorruption);
  auto unknown = encoded;
  unknown[8U] = std::byte{2U};
  // Restore the header checksum so version rejection is distinguished from damage.
  unknown[36U] = std::byte{0U};
  unknown[37U] = std::byte{0U};
  unknown[38U] = std::byte{0U};
  unknown[39U] = std::byte{0U};
  const std::uint32_t crc = common::crc32c(common::ByteView{unknown}.first(48U));
  for (std::size_t index = 0U; index < sizeof(crc); ++index)
    unknown[36U + index] = static_cast<std::byte>(crc >> (index * 8U));
  EXPECT_EQ(decode_tablet_group_binding_v1(unknown).error().code(),
            common::StatusCode::kNotSupported);
}

} // namespace
} // namespace chronos::raft
