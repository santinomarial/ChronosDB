#include "chronos/ingest/sha256.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/table_schema.hpp"
#include "chronos/tiering/tiered_parts.hpp"
#include "cseg/cseg_test_fixture.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace chronos::tiering {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] wal::WalId wal_id(const std::uint8_t seed = 0x70U) {
  wal::WalId value{};
  value.bytes.front() = std::byte{seed};
  return value;
}

[[nodiscard]] schema::TableSchema schema_value() {
  const schema::ColumnId event_id = id<schema::ColumnId>(5U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(
                        event_id, "event_time",
                        schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(),
                        false)
                        .value());
  return schema::TableSchema::create(id<schema::TableId>(2U), id<schema::SchemaId>(4U),
                                     schema::SchemaVersion::initial(), std::nullopt,
                                     std::move(columns),
                                     {.event_time_column = event_id,
                                      .physical_ordering_key = {event_id},
                                      .partition_columns = {event_id},
                                      .shard_key = {event_id},
                                      .deduplication_key = {}})
      .value();
}

struct PartFixture {
  explicit PartFixture(const std::uint8_t seed)
      : part(cseg::test::make_valid_part_with_rows(
            2U, 2U, cseg::PageCompression::kNone,
            {.part_id_seed = seed, .first_event_time = -5, .record_sequence = 7U})),
        schema(schema_value()), descriptor{.part_id = id<cseg::PartId>(seed),
                                           .table_id = id<schema::TableId>(2U),
                                           .tablet_id = id<schema::TabletId>(3U),
                                           .schema_id = id<schema::SchemaId>(4U),
                                           .schema_version = schema::SchemaVersion::initial(),
                                           .file_length = part.size(),
                                           .row_count = 2U,
                                           .minimum_record_sequence = 7U,
                                           .maximum_record_sequence = 7U,
                                           .minimum_event_time = -5,
                                           .maximum_event_time = -4} {}

  [[nodiscard]] ColdPartDescriptor cold() const {
    return {descriptor,
            "parts/" +
                std::to_string(std::to_integer<unsigned char>(descriptor.part_id.bytes().front())),
            ingest::sha256(part.bytes()).value()};
  }

  [[nodiscard]] TieredPartAdmission admission(const wal::WalId& source = wal_id()) const {
    return {.wal_id = source, .schema = std::cref(schema), .validation_limits = {}};
  }

  cseg::EncodedCsegPart part;
  schema::TableSchema schema;
  manifest::PartDescriptor descriptor;
};

TEST(TieredPartManagerTest, VerifiesUploadBeforeManifestInstallAndCachesFullObject) {
  MemoryObjectStore store;
  auto manager = TieredPartManager::create(store, TieringLimits{8U, 1U << 20U, 1U << 20U, 1U});
  ASSERT_TRUE(manager.has_value());
  const PartFixture first{1U};
  const PartFixture second{2U};
  bool installed = false;
  auto receipt = manager->upload_and_install(first.cold(), first.part.bytes(), first.admission(),
                                             [&](const ColdPartDescriptor&) {
                                               installed = true;
                                               return common::Status::ok();
                                             });
  ASSERT_TRUE(receipt.has_value()) << receipt.error().to_string();
  EXPECT_TRUE(installed);
  EXPECT_TRUE(receipt->local_source_may_be_released);
  auto range = manager->read_range(receipt->installed.part.part_id, 1U, 2U);
  ASSERT_TRUE(range.has_value());
  EXPECT_EQ(*range, (std::vector<std::byte>{first.part.bytes()[1], first.part.bytes()[2]}));
  EXPECT_EQ(manager->cached_entries(), 1U);

  auto second_receipt =
      manager->upload_and_install(second.cold(), second.part.bytes(), second.admission(),
                                  [](const ColdPartDescriptor&) { return common::Status::ok(); });
  ASSERT_TRUE(second_receipt.has_value());
  ASSERT_TRUE(manager->read_range(second_receipt->installed.part.part_id, 0U, 1U).has_value());
  EXPECT_EQ(manager->cached_entries(), 1U);
  EXPECT_LE(manager->cached_bytes(), 1U << 20U);
}

TEST(TieredPartManagerTest, LargeObjectUsesAuthenticatedRangeWithoutWholeDownload) {
  MemoryObjectStore store;
  auto manager = TieredPartManager::create(store, TieringLimits{8U, 1U << 20U, 2U, 1U});
  ASSERT_TRUE(manager.has_value());
  const PartFixture fixture{3U};
  auto receipt =
      manager->upload_and_install(fixture.cold(), fixture.part.bytes(), fixture.admission(),
                                  [](const ColdPartDescriptor&) { return common::Status::ok(); });
  ASSERT_TRUE(receipt.has_value());
  const std::vector<std::byte> expected{fixture.part.bytes()[1], fixture.part.bytes()[2]};
  auto range = manager->read_range(receipt->installed.part.part_id, 1U, 2U,
                                   ingest::sha256(expected).value());
  ASSERT_TRUE(range.has_value());
  EXPECT_EQ(*range, expected);
  EXPECT_EQ(manager->cached_entries(), 0U);
}

TEST(TieredPartManagerTest, RejectsInvalidCsegAndWrongWalBeforeRemoteMutation) {
  MemoryObjectStore store;
  auto manager = TieredPartManager::create(store);
  ASSERT_TRUE(manager.has_value());
  const PartFixture fixture{4U};
  std::vector<std::byte> corrupt{fixture.part.bytes().begin(), fixture.part.bytes().end()};
  corrupt.back() ^= std::byte{1U};
  ColdPartDescriptor corrupt_descriptor = fixture.cold();
  corrupt_descriptor.checksum = ingest::sha256(corrupt).value();
  bool installed = false;
  auto rejected = manager->upload_and_install(std::move(corrupt_descriptor), corrupt,
                                              fixture.admission(), [&](const ColdPartDescriptor&) {
                                                installed = true;
                                                return common::Status::ok();
                                              });
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kCorruption);
  EXPECT_FALSE(installed);
  EXPECT_EQ(store.object_count(), 0U);

  auto wrong_source =
      manager->upload_and_install(fixture.cold(), fixture.part.bytes(),
                                  fixture.admission(wal_id(0x71U)), [&](const ColdPartDescriptor&) {
                                    installed = true;
                                    return common::Status::ok();
                                  });
  ASSERT_FALSE(wrong_source.has_value());
  EXPECT_EQ(wrong_source.error().code(), common::StatusCode::kCorruption);
  EXPECT_FALSE(installed);
  EXPECT_EQ(store.object_count(), 0U);
}

TEST(TieredPartManagerTest, ConcurrentReadersShareBoundedEvictingCacheSafely) {
  MemoryObjectStore store;
  const std::array fixtures{PartFixture{5U}, PartFixture{6U}, PartFixture{7U}};
  const std::size_t cache_limit = fixtures.front().part.size() * 2U;
  auto manager = TieredPartManager::create(store, TieringLimits{8U, 1U << 20U, cache_limit, 2U});
  ASSERT_TRUE(manager.has_value());
  for (const auto& fixture : fixtures) {
    auto receipt =
        manager->upload_and_install(fixture.cold(), fixture.part.bytes(), fixture.admission(),
                                    [](const ColdPartDescriptor&) { return common::Status::ok(); });
    ASSERT_TRUE(receipt.has_value()) << receipt.error().to_string();
  }

  std::atomic_bool failed{};
  std::vector<std::jthread> readers;
  for (std::size_t worker = 0U; worker < 8U; ++worker) {
    readers.emplace_back([&, worker] {
      for (std::size_t iteration = 0U; iteration < 100U; ++iteration) {
        const auto& fixture = fixtures[(worker + iteration) % fixtures.size()];
        auto bytes = manager->read_range(fixture.descriptor.part_id, 0U, fixture.part.size());
        if (!bytes.has_value() || !std::ranges::equal(*bytes, fixture.part.bytes())) {
          failed.store(true, std::memory_order_relaxed);
          return;
        }
      }
    });
  }
  readers.clear();

  EXPECT_FALSE(failed.load(std::memory_order_relaxed));
  EXPECT_LE(manager->cached_entries(), 2U);
  EXPECT_LE(manager->cached_bytes(), cache_limit);
}

TEST(TieredPartManagerTest, RestoresExactCatalogWithAnEmptyRebuildableCache) {
  MemoryObjectStore store;
  const std::array fixtures{PartFixture{8U}, PartFixture{9U}};
  std::vector<ColdPartDescriptor> descriptors;
  for (const auto& fixture : fixtures) {
    const ColdPartDescriptor descriptor = fixture.cold();
    ASSERT_TRUE(
        store.put_if_absent(descriptor.object_key, fixture.part.bytes(), descriptor.checksum)
            .has_value());
    descriptors.push_back(descriptor);
  }

  auto restarted = TieredPartManager::create(store);
  ASSERT_TRUE(restarted.has_value());
  const common::Status restored = restarted->restore_catalog(descriptors);
  ASSERT_TRUE(restored.is_ok()) << restored.to_string();
  EXPECT_EQ(restarted->cached_entries(), 0U);
  EXPECT_EQ(restarted->cached_bytes(), 0U);
  for (const auto& fixture : fixtures) {
    auto bytes = restarted->read_range(fixture.descriptor.part_id, 0U, fixture.part.size());
    ASSERT_TRUE(bytes.has_value()) << bytes.error().to_string();
    EXPECT_TRUE(std::ranges::equal(*bytes, fixture.part.bytes()));
  }
  EXPECT_EQ(restarted->cached_entries(), fixtures.size());

  auto rejected = TieredPartManager::create(store);
  ASSERT_TRUE(rejected.has_value());
  auto damaged = descriptors;
  ingest::Sha256Digest::Bytes wrong_bytes = damaged.back().checksum.bytes();
  wrong_bytes.back() ^= std::byte{1U};
  damaged.back().checksum = ingest::Sha256Digest{wrong_bytes};
  const common::Status mismatch = rejected->restore_catalog(damaged);
  EXPECT_EQ(mismatch.code(), common::StatusCode::kCorruption);
  EXPECT_FALSE(rejected->find(descriptors.front().part.part_id).has_value());
}

} // namespace
} // namespace chronos::tiering
