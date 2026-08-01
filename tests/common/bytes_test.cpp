#include "chronos/common/bytes.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <type_traits>

namespace chronos::common {
namespace {

static_assert(std::is_same_v<ByteView, std::span<const std::byte>>);
static_assert(std::is_same_v<MutableByteView, std::span<std::byte>>);

TEST(BytesTest, CreatesViewsOnlyFromExplicitByteOrientedBuffers) {
  std::array<std::uint8_t, 3> buffer{0x01U, 0x80U, 0xffU};
  const MutableByteView mutable_view = mutable_byte_view(std::span{buffer});
  ASSERT_EQ(mutable_view.size(), buffer.size());
  mutable_view[1] = std::byte{0x22};
  EXPECT_EQ(buffer[1], 0x22U);

  const ByteView immutable_view = byte_view(std::span<const std::uint8_t>{buffer});
  EXPECT_EQ(std::to_integer<std::uint8_t>(immutable_view[0]), 0x01U);
  EXPECT_EQ(std::to_integer<std::uint8_t>(immutable_view[1]), 0x22U);
  EXPECT_EQ(std::to_integer<std::uint8_t>(immutable_view[2]), 0xffU);
}

TEST(BytesTest, EmptyViewsAreWellDefined) {
  const ByteView immutable;
  const MutableByteView mutable_view;
  EXPECT_TRUE(immutable.empty());
  EXPECT_TRUE(mutable_view.empty());
}

} // namespace
} // namespace chronos::common
