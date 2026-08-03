#include "chronos/schema/identity.hpp"

#include "schema/schema_test_support.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <limits>
#include <type_traits>

namespace chronos::schema {
namespace {

static_assert(!std::is_same_v<TableId, ColumnId>);
static_assert(!std::is_same_v<TableId, SchemaId>);
static_assert(!std::is_same_v<TableId, TabletId>);
static_assert(!std::is_convertible_v<TableId, ColumnId>);
static_assert(!std::is_default_constructible_v<TableId>);
static_assert(!std::is_default_constructible_v<SchemaVersion>);

TEST(IdentifierTest, RejectsNilAndPreservesExactUuidBytes) {
  const common::Result<TableId> nil = TableId::from_uuid(common::Uuid{});
  ASSERT_FALSE(nil.has_value());
  EXPECT_EQ(nil.error().code(), common::StatusCode::kInvalidArgument);

  common::Uuid::Bytes bytes{};
  bytes[0] = std::byte{0x12};
  bytes[15] = std::byte{0xfe};
  const common::Result<TableId> id = TableId::from_bytes(bytes);
  ASSERT_TRUE(id.has_value());
  EXPECT_EQ(id->bytes(), bytes);
  EXPECT_EQ(id->uuid(), common::Uuid{bytes});
}

TEST(IdentifierTest, EachNominalTypeUsesTheSameDurableByteSequenceWithoutConversion) {
  common::Uuid::Bytes bytes{};
  bytes[7] = std::byte{0x80};

  const TableId table = TableId::from_bytes(bytes).value();
  const ColumnId column = ColumnId::from_bytes(bytes).value();
  const SchemaId schema = SchemaId::from_bytes(bytes).value();
  const TabletId tablet = TabletId::from_bytes(bytes).value();

  EXPECT_EQ(table.bytes(), bytes);
  EXPECT_EQ(column.bytes(), bytes);
  EXPECT_EQ(schema.bytes(), bytes);
  EXPECT_EQ(tablet.bytes(), bytes);
}

TEST(IdentifierTest, OrderingIsDeterministicNetworkByteOrder) {
  EXPECT_LT(test::make_id<TableId>(1), test::make_id<TableId>(2));
  EXPECT_EQ(test::make_id<TableId>(513), test::make_id<TableId>(513));
}

TEST(SchemaVersionTest, RequiresPositiveValueAndAdvancesExactlyOnce) {
  const common::Result<SchemaVersion> zero = SchemaVersion::from_value(0);
  ASSERT_FALSE(zero.has_value());
  EXPECT_EQ(zero.error().code(), common::StatusCode::kInvalidArgument);

  const SchemaVersion initial = SchemaVersion::initial();
  EXPECT_EQ(initial.value(), 1U);
  const common::Result<SchemaVersion> next = initial.next();
  ASSERT_TRUE(next.has_value());
  EXPECT_EQ(next->value(), 2U);
  EXPECT_GT(*next, initial);
}

TEST(SchemaVersionTest, ReportsExhaustionWithoutWrapping) {
  const SchemaVersion maximum =
      SchemaVersion::from_value(std::numeric_limits<std::uint64_t>::max()).value();
  const common::Result<SchemaVersion> next = maximum.next();
  ASSERT_FALSE(next.has_value());
  EXPECT_EQ(next.error().code(), common::StatusCode::kOutOfRange);
}

} // namespace
} // namespace chronos::schema
