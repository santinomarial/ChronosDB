#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/manifest/temporal_publication.hpp"
#include "chronos/tiering/tiered_publication.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <vector>

namespace chronos::tiering {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-tiered-publication-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  TemporaryDirectory(const TemporaryDirectory&) = delete;
  TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

[[nodiscard]] common::Uuid nonce(const std::uint8_t seed) {
  return uuid(seed);
}

[[nodiscard]] manifest::EncodedTemporalManifest
empty_manifest(const manifest::DatabaseId database_id, const std::uint64_t generation) {
  return manifest::encode_manifest_v2_temporal({.generation = generation,
                                                .database_id = database_id,
                                                .wal_reclaim_checkpoint = std::nullopt,
                                                .tablets = {},
                                                .parts = {},
                                                .retries = {}})
      .value();
}

void write_bytes(const std::filesystem::path& path, const common::ByteView bytes) {
  std::ofstream output{path, std::ios::binary};
  ASSERT_TRUE(output.good());
  for (const std::byte value : bytes)
    output.put(static_cast<char>(std::to_integer<unsigned char>(value)));
  output.close();
  ASSERT_TRUE(output.good());
}

void establish_layout(const TemporaryDirectory& directory, const manifest::DatabaseId database_id) {
  ASSERT_TRUE(std::filesystem::create_directory(directory.path() / manifest::kPartsDirectoryName));
  ASSERT_TRUE(
      std::filesystem::create_directory(directory.path() / manifest::kManifestDirectoryName));
  ASSERT_TRUE(std::filesystem::create_directory(directory.path() / "cold-manifest"));
  {
    std::ofstream lock{directory.path() / manifest::kManifestDirectoryName /
                           manifest::kManifestLockFileName,
                       std::ios::binary};
    ASSERT_TRUE(lock.good());
  }
  const auto initial = empty_manifest(database_id, 1U);
  write_bytes(directory.path() / manifest::kManifestDirectoryName /
                  *manifest::manifest_file_name(1U),
              initial.bytes());
}

[[nodiscard]] std::shared_ptr<const manifest::LoadedTemporalManifestGeneration>
load_manifest_owner(manifest::ManifestStorage& storage, const manifest::DatabaseId database_id) {
  auto loaded = storage.load_selected_temporal_manifest({.expected_database_id = database_id,
                                                         .schema_bindings = {},
                                                         .source_bindings = {},
                                                         .decode_limits = {},
                                                         .part_validation_limits = {}});
  EXPECT_TRUE(loaded.has_value()) << loaded.error().to_string();
  return loaded.has_value() ? std::make_shared<const manifest::LoadedTemporalManifestGeneration>(
                                  std::move(*loaded))
                            : nullptr;
}

[[nodiscard]] std::shared_ptr<const LoadedColdLocationManifest>
load_cold_owner(ColdLocationManifestStorage& storage,
                const manifest::TemporalDatabaseStorageSnapshot& base) {
  auto decoded = manifest::decode_manifest_v2_temporal_exact(base.manifest_bytes());
  EXPECT_TRUE(decoded.has_value());
  if (!decoded.has_value())
    return nullptr;
  auto loaded = storage.load_selected(*decoded);
  EXPECT_TRUE(loaded.has_value()) << loaded.error().to_string();
  EXPECT_TRUE(loaded.has_value() && loaded->has_value());
  return loaded.has_value() && loaded->has_value()
             ? std::make_shared<const LoadedColdLocationManifest>(std::move(**loaded))
             : nullptr;
}

void install_empty_cold(ColdLocationManifestStorage& storage,
                        const manifest::TemporalDatabaseStorageSnapshot& base,
                        const common::Uuid object_store_id, const std::uint64_t generation) {
  auto decoded = manifest::decode_manifest_v2_temporal_exact(base.manifest_bytes());
  ASSERT_TRUE(decoded.has_value());
  auto encoded = encode_cold_location_manifest_v1({.generation = generation,
                                                   .base_manifest_generation = base.generation(),
                                                   .database_id = base.database_id(),
                                                   .object_store_id = object_store_id,
                                                   .locations = {}});
  ASSERT_TRUE(encoded.has_value());
  auto installed = storage.install(std::cref(*encoded), *decoded);
  ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
}

[[nodiscard]] manifest::TemporalDatabaseStorageSnapshot
advance_manifest(manifest::ManifestStorage& storage,
                 manifest::TemporalDatabaseStoragePublisher& publisher,
                 const manifest::DatabaseId database_id, const std::uint64_t generation,
                 const std::uint8_t nonce_seed) {
  const auto encoded = empty_manifest(database_id, generation);
  auto installed = storage.install_temporal_manifest({.encoded_manifest = std::cref(encoded),
                                                      .schema_bindings = {},
                                                      .nonce = nonce(nonce_seed),
                                                      .decode_limits = {},
                                                      .part_validation_limits = {}});
  EXPECT_TRUE(installed.has_value()) << installed.error().to_string();
  auto selected = load_manifest_owner(storage, database_id);
  auto published = publisher.publish_manifest(
      {.selected_manifest = std::move(selected), .schema_bindings = {}, .decode_limits = {}});
  EXPECT_TRUE(published.has_value()) << published.error().to_string();
  return std::move(*published);
}

TEST(TieredDatabaseStoragePublisherTest,
     AtomicallyPublishesCompatiblePairsAndRetainsPredecessorReaders) {
  TemporaryDirectory directory;
  const manifest::DatabaseId database_id{id<manifest::DatabaseId>(1U)};
  const common::Uuid object_store_id{uuid(2U)};
  establish_layout(directory, database_id);
  auto manifest_storage =
      manifest::ManifestStorage::open_existing({.database_root = directory.path().string()});
  ASSERT_TRUE(manifest_storage.has_value()) << manifest_storage.error().to_string();
  auto manifest_publisher = manifest::TemporalDatabaseStoragePublisher::create(
      load_manifest_owner(*manifest_storage, database_id), {});
  ASSERT_TRUE(manifest_publisher.has_value());
  auto base1 = manifest_publisher->snapshot();
  ASSERT_TRUE(base1.has_value());
  auto cold_storage = ColdLocationManifestStorage::create(
      {.directory_path = (directory.path() / "cold-manifest").string(),
       .expected_database_id = database_id,
       .expected_object_store_id = object_store_id});
  ASSERT_TRUE(cold_storage.has_value()) << cold_storage.error().to_string();
  install_empty_cold(*cold_storage, *base1, object_store_id, 1U);
  auto cold1 = load_cold_owner(*cold_storage, *base1);
  ASSERT_NE(cold1, nullptr);
  std::weak_ptr<const LoadedColdLocationManifest> cold1_lifetime = cold1;

  auto publisher = TieredDatabaseStoragePublisher::create(*base1, cold1);
  ASSERT_TRUE(publisher.has_value()) << publisher.error().to_string();
  auto held = publisher->snapshot();
  ASSERT_TRUE(held.has_value());
  EXPECT_EQ(held->manifest_generation(), 1U);
  ASSERT_NE(held->cold_manifest(), nullptr);
  EXPECT_EQ(held->cold_manifest()->manifest().generation(), 1U);
  cold1.reset();

  install_empty_cold(*cold_storage, *base1, object_store_id, 2U);
  auto cold2 = load_cold_owner(*cold_storage, *base1);
  ASSERT_NE(cold2, nullptr);
  auto cold_advanced = publisher->publish({.manifest_snapshot = *base1,
                                           .cold_manifest = cold2,
                                           .schema_bindings = {},
                                           .manifest_decode_limits = {},
                                           .cold_decode_limits = {}});
  ASSERT_TRUE(cold_advanced.has_value()) << cold_advanced.error().to_string();
  EXPECT_EQ(cold_advanced->cold_manifest()->manifest().generation(), 2U);
  EXPECT_FALSE(cold1_lifetime.expired());

  const auto base2 = advance_manifest(*manifest_storage, *manifest_publisher, database_id, 2U, 9U);
  install_empty_cold(*cold_storage, base2, object_store_id, 3U);
  auto cold3 = load_cold_owner(*cold_storage, base2);
  ASSERT_NE(cold3, nullptr);
  auto pair_advanced = publisher->publish({.manifest_snapshot = base2,
                                           .cold_manifest = cold3,
                                           .schema_bindings = {},
                                           .manifest_decode_limits = {},
                                           .cold_decode_limits = {}});
  ASSERT_TRUE(pair_advanced.has_value()) << pair_advanced.error().to_string();
  EXPECT_EQ(pair_advanced->manifest_generation(), 2U);
  EXPECT_EQ(pair_advanced->cold_manifest()->manifest().base_manifest_generation(), 2U);
  EXPECT_EQ(held->manifest_generation(), 1U);
  EXPECT_EQ(held->cold_manifest()->manifest().generation(), 1U);
  held = std::move(pair_advanced);
  EXPECT_TRUE(cold1_lifetime.expired());
  EXPECT_TRUE(publisher->is_usable());
}

TEST(TieredDatabaseStoragePublisherTest, ConcurrentReadersSeeOnlyCompleteOldOrNewPairs) {
  TemporaryDirectory directory;
  const manifest::DatabaseId database_id{id<manifest::DatabaseId>(11U)};
  const common::Uuid object_store_id{uuid(12U)};
  establish_layout(directory, database_id);
  auto manifest_storage =
      manifest::ManifestStorage::open_existing({.database_root = directory.path().string()});
  ASSERT_TRUE(manifest_storage.has_value());
  auto manifest_publisher = manifest::TemporalDatabaseStoragePublisher::create(
      load_manifest_owner(*manifest_storage, database_id), {});
  ASSERT_TRUE(manifest_publisher.has_value());
  auto base1 = manifest_publisher->snapshot();
  ASSERT_TRUE(base1.has_value());
  auto cold_storage = ColdLocationManifestStorage::create(
      {.directory_path = (directory.path() / "cold-manifest").string(),
       .expected_database_id = database_id,
       .expected_object_store_id = object_store_id});
  ASSERT_TRUE(cold_storage.has_value());
  install_empty_cold(*cold_storage, *base1, object_store_id, 1U);
  auto publisher =
      TieredDatabaseStoragePublisher::create(*base1, load_cold_owner(*cold_storage, *base1));
  ASSERT_TRUE(publisher.has_value());

  const auto base2 = advance_manifest(*manifest_storage, *manifest_publisher, database_id, 2U, 13U);
  install_empty_cold(*cold_storage, base2, object_store_id, 2U);
  auto cold2 = load_cold_owner(*cold_storage, base2);
  ASSERT_NE(cold2, nullptr);
  std::atomic<bool> stop{false};
  std::atomic<bool> invalid_pair{false};
  std::jthread reader([&] {
    while (!stop.load(std::memory_order_acquire)) {
      auto snapshot = publisher->snapshot();
      if (!snapshot.has_value() || snapshot->cold_manifest() == nullptr) {
        invalid_pair.store(true, std::memory_order_release);
        continue;
      }
      const std::uint64_t manifest_generation = snapshot->manifest_generation();
      const std::uint64_t cold_base =
          snapshot->cold_manifest()->manifest().base_manifest_generation();
      if (manifest_generation != cold_base ||
          (manifest_generation != 1U && manifest_generation != 2U))
        invalid_pair.store(true, std::memory_order_release);
    }
  });
  auto published = publisher->publish({.manifest_snapshot = base2,
                                       .cold_manifest = cold2,
                                       .schema_bindings = {},
                                       .manifest_decode_limits = {},
                                       .cold_decode_limits = {}});
  ASSERT_TRUE(published.has_value()) << published.error().to_string();
  stop.store(true, std::memory_order_release);
  reader.join();
  EXPECT_FALSE(invalid_pair.load(std::memory_order_acquire));
}

TEST(TieredDatabaseStoragePublisherTest, FailsClosedOnIncompatibleDurablePair) {
  TemporaryDirectory directory;
  const manifest::DatabaseId database_id{id<manifest::DatabaseId>(21U)};
  const common::Uuid object_store_id{uuid(22U)};
  establish_layout(directory, database_id);
  auto manifest_storage =
      manifest::ManifestStorage::open_existing({.database_root = directory.path().string()});
  ASSERT_TRUE(manifest_storage.has_value());
  auto manifest_publisher = manifest::TemporalDatabaseStoragePublisher::create(
      load_manifest_owner(*manifest_storage, database_id), {});
  ASSERT_TRUE(manifest_publisher.has_value());
  auto base1 = manifest_publisher->snapshot();
  ASSERT_TRUE(base1.has_value());
  auto cold_storage = ColdLocationManifestStorage::create(
      {.directory_path = (directory.path() / "cold-manifest").string(),
       .expected_database_id = database_id,
       .expected_object_store_id = object_store_id});
  ASSERT_TRUE(cold_storage.has_value());
  install_empty_cold(*cold_storage, *base1, object_store_id, 1U);
  auto cold1 = load_cold_owner(*cold_storage, *base1);
  ASSERT_NE(cold1, nullptr);
  auto publisher = TieredDatabaseStoragePublisher::create(*base1, cold1);
  ASSERT_TRUE(publisher.has_value());

  const auto base2 = advance_manifest(*manifest_storage, *manifest_publisher, database_id, 2U, 23U);
  auto rejected = publisher->publish({.manifest_snapshot = base2,
                                      .cold_manifest = cold1,
                                      .schema_bindings = {},
                                      .manifest_decode_limits = {},
                                      .cold_decode_limits = {}});
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kUnavailable);
  EXPECT_FALSE(publisher->is_usable());
  EXPECT_EQ(publisher->snapshot().error().code(), common::StatusCode::kUnavailable);
}

} // namespace
} // namespace chronos::tiering
