#include "chronos/cseg/types.hpp"
#include "chronos/schema/identity.hpp"

#include <gtest/gtest.h>
#include <type_traits>

namespace chronos::cseg {
namespace {

static_assert(!std::is_default_constructible_v<PartId>);
static_assert(!std::is_same_v<PartId, schema::TableId>);
static_assert(!std::is_convertible_v<PartId, schema::TableId>);

TEST(CsegPartIdTest, RejectsNilAndPreservesNetworkOrderBytes) {
  common::Uuid::Bytes bytes{};
  const common::Result<PartId> nil = PartId::from_bytes(bytes);
  ASSERT_FALSE(nil.has_value());
  EXPECT_EQ(nil.error().code(), common::StatusCode::kInvalidArgument);

  bytes.front() = std::byte{0x12};
  bytes.back() = std::byte{0xfe};
  const common::Result<PartId> part = PartId::from_bytes(bytes);
  ASSERT_TRUE(part.has_value());
  EXPECT_EQ(part->bytes(), bytes);
  EXPECT_EQ(part->uuid(), common::Uuid{bytes});
}

TEST(CsegPartIdTest, OrdersByTheExactDurableByteSequence) {
  common::Uuid::Bytes first_bytes{};
  first_bytes.back() = std::byte{1U};
  common::Uuid::Bytes second_bytes{};
  second_bytes.back() = std::byte{2U};
  EXPECT_LT(PartId::from_bytes(first_bytes).value(), PartId::from_bytes(second_bytes).value());
}

} // namespace
} // namespace chronos::cseg
