#include "chronos/cseg/types.hpp"
#include "chronos/manifest/types.hpp"
#include "chronos/schema/identity.hpp"

#include <gtest/gtest.h>
#include <type_traits>

namespace chronos::manifest {
namespace {

static_assert(!std::is_default_constructible_v<DatabaseId>);
static_assert(!std::is_same_v<DatabaseId, cseg::PartId>);
static_assert(!std::is_same_v<DatabaseId, schema::TableId>);
static_assert(!std::is_convertible_v<DatabaseId, cseg::PartId>);

TEST(ManifestDatabaseIdTest, RejectsNilAndPreservesNetworkOrderBytes) {
  common::Uuid::Bytes bytes{};
  const common::Result<DatabaseId> nil = DatabaseId::from_bytes(bytes);
  ASSERT_FALSE(nil.has_value());
  EXPECT_EQ(nil.error().code(), common::StatusCode::kInvalidArgument);

  bytes.front() = std::byte{0x12};
  bytes.back() = std::byte{0xfe};
  const common::Result<DatabaseId> database = DatabaseId::from_bytes(bytes);
  ASSERT_TRUE(database.has_value());
  EXPECT_EQ(database->bytes(), bytes);
  EXPECT_EQ(database->uuid(), common::Uuid{bytes});
}

TEST(ManifestDatabaseIdTest, OrdersByTheExactDurableByteSequence) {
  common::Uuid::Bytes first_bytes{};
  first_bytes.back() = std::byte{1U};
  common::Uuid::Bytes second_bytes{};
  second_bytes.back() = std::byte{2U};
  EXPECT_LT(DatabaseId::from_bytes(first_bytes).value(),
            DatabaseId::from_bytes(second_bytes).value());
}

} // namespace
} // namespace chronos::manifest
