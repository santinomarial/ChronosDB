#include "chronos/common/uuid.hpp"

#include <array>
#include <cstddef>
#include <gtest/gtest.h>
#include <type_traits>

namespace chronos::common {
namespace {

static_assert(sizeof(Uuid::Bytes) == Uuid::kSize);
static_assert(sizeof(Uuid) == Uuid::kSize);
static_assert(std::is_trivially_copyable_v<Uuid>);

TEST(UuidTest, PreservesExactNetworkOrderBytes) {
  Uuid::Bytes bytes{};
  for (std::size_t index = 0; index < bytes.size(); ++index) {
    bytes[index] = static_cast<std::byte>(index);
  }

  const Uuid value{bytes};
  EXPECT_EQ(value.bytes(), bytes);
  EXPECT_FALSE(value.is_nil());
}

TEST(UuidTest, DefaultValueIsTheReusableNilUuid) {
  const Uuid nil;
  EXPECT_TRUE(nil.is_nil());
  EXPECT_EQ(nil, Uuid{Uuid::Bytes{}});
}

TEST(UuidTest, ComparisonUsesUnsignedNetworkByteOrder) {
  Uuid::Bytes smaller_bytes{};
  Uuid::Bytes larger_bytes{};
  smaller_bytes[15] = std::byte{0x7f};
  larger_bytes[15] = std::byte{0x80};

  EXPECT_LT(Uuid{smaller_bytes}, Uuid{larger_bytes});
}

} // namespace
} // namespace chronos::common
