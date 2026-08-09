#include "chronos/tiering/object_store.hpp"

#include "chronos/ingest/sha256.hpp"

#include <cstddef>
#include <gtest/gtest.h>
#include <vector>

namespace chronos::tiering {
namespace {

TEST(MemoryObjectStoreTest, ImmutablePutIsIdempotentAndRangesAreBounded) {
  MemoryObjectStore store;
  const std::vector<std::byte> bytes{std::byte{1U}, std::byte{2U}, std::byte{3U}};
  const auto checksum = ingest::sha256(bytes).value();
  EXPECT_TRUE(store.put_if_absent("part/a", bytes, checksum).has_value());
  EXPECT_TRUE(store.put_if_absent("part/a", bytes, checksum).has_value());
  EXPECT_EQ(store.object_count(), 1U);
  const auto range = store.get_range("part/a", 1U, 2U);
  ASSERT_TRUE(range.has_value());
  EXPECT_EQ(*range, (std::vector<std::byte>{std::byte{2U}, std::byte{3U}}));
  const std::vector<std::byte> other{std::byte{9U}};
  EXPECT_FALSE(store.put_if_absent("part/a", other, ingest::sha256(other).value()).has_value());
}

} // namespace
} // namespace chronos::tiering
