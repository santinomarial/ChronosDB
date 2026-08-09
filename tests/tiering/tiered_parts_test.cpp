#include "chronos/ingest/sha256.hpp"
#include "chronos/tiering/tiered_parts.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <vector>

namespace chronos::tiering {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] ColdPartDescriptor descriptor(const std::uint8_t seed,
                                            const std::vector<std::byte>& bytes) {
  return ColdPartDescriptor{
      manifest::PartDescriptor{id<cseg::PartId>(seed), id<schema::TableId>(10U),
                               id<schema::TabletId>(11U), id<schema::SchemaId>(12U),
                               schema::SchemaVersion::initial(), bytes.size(), 1U, 1U, 1U, 0, 1},
      "parts/" + std::to_string(seed), ingest::sha256(bytes).value()};
}

TEST(TieredPartManagerTest, VerifiesUploadBeforeManifestInstallAndCachesFullObject) {
  MemoryObjectStore store;
  auto manager = TieredPartManager::create(store, TieringLimits{8U, 1024U, 6U, 1U});
  ASSERT_TRUE(manager.has_value());
  const std::vector<std::byte> first{std::byte{1U}, std::byte{2U}, std::byte{3U}, std::byte{4U}};
  const std::vector<std::byte> second{std::byte{5U}, std::byte{6U}, std::byte{7U}, std::byte{8U}};
  bool installed = false;
  auto receipt =
      manager->upload_and_install(descriptor(1U, first), first, [&](const ColdPartDescriptor&) {
        installed = true;
        return common::Status::ok();
      });
  ASSERT_TRUE(receipt.has_value()) << receipt.error().to_string();
  EXPECT_TRUE(installed);
  EXPECT_TRUE(receipt->local_source_may_be_released);
  auto range = manager->read_range(receipt->installed.part.part_id, 1U, 2U);
  ASSERT_TRUE(range.has_value());
  EXPECT_EQ(*range, (std::vector<std::byte>{std::byte{2U}, std::byte{3U}}));
  EXPECT_EQ(manager->cached_entries(), 1U);

  auto second_receipt =
      manager->upload_and_install(descriptor(2U, second), second,
                                  [](const ColdPartDescriptor&) { return common::Status::ok(); });
  ASSERT_TRUE(second_receipt.has_value());
  ASSERT_TRUE(manager->read_range(second_receipt->installed.part.part_id, 0U, 1U).has_value());
  EXPECT_EQ(manager->cached_entries(), 1U);
  EXPECT_LE(manager->cached_bytes(), 6U);
}

TEST(TieredPartManagerTest, LargeObjectUsesAuthenticatedRangeWithoutWholeDownload) {
  MemoryObjectStore store;
  auto manager = TieredPartManager::create(store, TieringLimits{8U, 1024U, 2U, 1U});
  ASSERT_TRUE(manager.has_value());
  const std::vector<std::byte> bytes{std::byte{1U}, std::byte{2U}, std::byte{3U}, std::byte{4U}};
  auto receipt = manager->upload_and_install(
      descriptor(3U, bytes), bytes, [](const ColdPartDescriptor&) { return common::Status::ok(); });
  ASSERT_TRUE(receipt.has_value());
  const std::vector<std::byte> expected{std::byte{2U}, std::byte{3U}};
  auto range = manager->read_range(receipt->installed.part.part_id, 1U, 2U,
                                   ingest::sha256(expected).value());
  ASSERT_TRUE(range.has_value());
  EXPECT_EQ(*range, expected);
  EXPECT_EQ(manager->cached_entries(), 0U);
}

} // namespace
} // namespace chronos::tiering
