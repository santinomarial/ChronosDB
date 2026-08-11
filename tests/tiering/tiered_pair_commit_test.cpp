#include "chronos/common/crc32c.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/tiering/tiered_pair_commit.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace chronos::tiering {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-tiered-pair-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
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

[[nodiscard]] ingest::Sha256Digest digest(const std::uint8_t seed) {
  ingest::Sha256Digest::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return ingest::Sha256Digest{bytes};
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

void store_u16(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint16_t value) {
  bytes[offset] = std::byte{static_cast<std::uint8_t>(value)};
  bytes[offset + 1U] = std::byte{static_cast<std::uint8_t>(value >> 8U)};
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = std::byte{static_cast<std::uint8_t>(value >> (index * 8U))};
}

void refresh_checksums(std::vector<std::byte>& bytes) {
  store_u32(bytes, 248U, common::crc32c(common::ByteView{bytes}.first(248U)));
  store_u32(bytes, 252U, common::crc32c(common::ByteView{bytes}.first(252U)));
}

void establish_layout(const TemporaryDirectory& directory, const manifest::DatabaseId database_id) {
  ASSERT_TRUE(std::filesystem::create_directory(directory.path() / manifest::kPartsDirectoryName));
  ASSERT_TRUE(
      std::filesystem::create_directory(directory.path() / manifest::kManifestDirectoryName));
  ASSERT_TRUE(std::filesystem::create_directory(directory.path() / "cold-manifest"));
  ASSERT_TRUE(std::filesystem::create_directory(directory.path() / "tiered-pair"));
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
  ASSERT_TRUE(storage.install(std::cref(*encoded), *decoded).has_value());
}

TEST(TieredPairCommitCodecTest, RoundTripsAndRejectsEveryTruncationAndDamage) {
  const TieredPairCommitRecord record{.generation = 3U,
                                      .previous_generation = 2U,
                                      .database_id = id<manifest::DatabaseId>(1U),
                                      .object_store_id = uuid(2U),
                                      .manifest_generation = 5U,
                                      .cold_generation = 4U,
                                      .manifest_length = 4096U,
                                      .cold_length = 512U,
                                      .manifest_sha256 = digest(3U),
                                      .cold_sha256 = digest(4U)};
  auto encoded = encode_tiered_pair_commit_v1(record);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  EXPECT_EQ(encoded->size(), kTieredPairCommitV1Size);
  auto decoded = decode_tiered_pair_commit_v1_exact(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, record);
  for (std::size_t size = 0U; size < encoded->size(); ++size)
    EXPECT_FALSE(
        decode_tiered_pair_commit_v1_exact(common::ByteView{*encoded}.first(size)).has_value());
  (*encoded)[100U] ^= std::byte{1U};
  EXPECT_EQ(decode_tiered_pair_commit_v1_exact(*encoded).error().code(),
            common::StatusCode::kCorruption);

  auto unknown = encode_tiered_pair_commit_v1(record).value();
  store_u16(unknown, 8U, 2U);
  refresh_checksums(unknown);
  EXPECT_EQ(decode_tiered_pair_commit_v1_exact(unknown).error().code(),
            common::StatusCode::kNotSupported);
  unknown = encode_tiered_pair_commit_v1(record).value();
  store_u32(unknown, 16U, 3U);
  refresh_checksums(unknown);
  EXPECT_EQ(decode_tiered_pair_commit_v1_exact(unknown).error().code(),
            common::StatusCode::kNotSupported);
}

TEST(TieredPairCommitStorageTest, IgnoresUncommittedFinalsThenRecoversNewCommittedPair) {
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
  auto cold1 = load_cold_owner(*cold_storage, *base1);
  ASSERT_NE(cold1, nullptr);
  auto pair_storage = TieredPairCommitStorage::create(
      {.directory_path = (directory.path() / "tiered-pair").string(),
       .expected_database_id = database_id,
       .expected_object_store_id = object_store_id});
  ASSERT_TRUE(pair_storage.has_value()) << pair_storage.error().to_string();
  auto first = pair_storage->commit(*base1, cold1);
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  EXPECT_FALSE(first->already_present);
  auto retry = pair_storage->commit(*base1, cold1);
  ASSERT_TRUE(retry.has_value());
  EXPECT_TRUE(retry->already_present);

  const auto base2_encoded = empty_manifest(database_id, 2U);
  auto installed_base2 =
      manifest_storage->install_temporal_manifest({.encoded_manifest = std::cref(base2_encoded),
                                                   .schema_bindings = {},
                                                   .nonce = uuid(13U),
                                                   .decode_limits = {},
                                                   .part_validation_limits = {}});
  ASSERT_TRUE(installed_base2.has_value()) << installed_base2.error().to_string();
  auto base2_owner = load_manifest_owner(*manifest_storage, database_id);
  auto base2 = manifest_publisher->publish_manifest(
      {.selected_manifest = base2_owner, .schema_bindings = {}, .decode_limits = {}});
  ASSERT_TRUE(base2.has_value());
  install_empty_cold(*cold_storage, *base2, object_store_id, 2U);
  auto cold2 = load_cold_owner(*cold_storage, *base2);
  ASSERT_NE(cold2, nullptr);

  const TieredPairRecoveryRequest recovery{.manifest_request = {.expected_database_id = database_id,
                                                                .schema_bindings = {},
                                                                .source_bindings = {},
                                                                .decode_limits = {},
                                                                .part_validation_limits = {}}};
  auto before_commit = pair_storage->recover(*manifest_storage, *cold_storage, recovery);
  ASSERT_TRUE(before_commit.has_value()) << before_commit.error().to_string();
  ASSERT_TRUE(before_commit->has_value());
  EXPECT_EQ((*before_commit)->manifest_snapshot.generation(), 1U);
  EXPECT_EQ((*before_commit)->cold_manifest->manifest().generation(), 1U);

  auto second = pair_storage->commit(*base2, cold2);
  ASSERT_TRUE(second.has_value()) << second.error().to_string();
  EXPECT_EQ(second->record.generation, 2U);
  auto after_commit = pair_storage->recover(*manifest_storage, *cold_storage, recovery);
  ASSERT_TRUE(after_commit.has_value()) << after_commit.error().to_string();
  ASSERT_TRUE(after_commit->has_value());
  EXPECT_EQ((*after_commit)->manifest_snapshot.generation(), 2U);
  EXPECT_EQ((*after_commit)->cold_manifest->manifest().generation(), 2U);
}

TEST(TieredPairCommitStorageTest, NeverFallsBackFromDamagedHighestCommit) {
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
  auto base = manifest_publisher->snapshot();
  ASSERT_TRUE(base.has_value());
  auto cold_storage = ColdLocationManifestStorage::create(
      {.directory_path = (directory.path() / "cold-manifest").string(),
       .expected_database_id = database_id,
       .expected_object_store_id = object_store_id});
  ASSERT_TRUE(cold_storage.has_value());
  install_empty_cold(*cold_storage, *base, object_store_id, 1U);
  auto cold = load_cold_owner(*cold_storage, *base);
  auto pair_storage = TieredPairCommitStorage::create(
      {.directory_path = (directory.path() / "tiered-pair").string(),
       .expected_database_id = database_id,
       .expected_object_store_id = object_store_id});
  ASSERT_TRUE(pair_storage.has_value());
  ASSERT_TRUE(pair_storage->commit(*base, cold).has_value());
  install_empty_cold(*cold_storage, *base, object_store_id, 2U);
  auto cold2 = load_cold_owner(*cold_storage, *base);
  ASSERT_NE(cold2, nullptr);
  ASSERT_TRUE(pair_storage->commit(*base, cold2).has_value());
  const auto name = tiered_pair_commit_file_name(2U).value();
  {
    std::fstream file{directory.path() / "tiered-pair" / name,
                      std::ios::binary | std::ios::in | std::ios::out};
    ASSERT_TRUE(file.good());
    file.seekp(100);
    file.put('x');
  }
  const TieredPairRecoveryRequest recovery{.manifest_request = {.expected_database_id = database_id,
                                                                .schema_bindings = {},
                                                                .source_bindings = {},
                                                                .decode_limits = {},
                                                                .part_validation_limits = {}}};
  auto recovered = pair_storage->recover(*manifest_storage, *cold_storage, recovery);
  ASSERT_FALSE(recovered.has_value());
  EXPECT_EQ(recovered.error().code(), common::StatusCode::kCorruption);
}

} // namespace
} // namespace chronos::tiering
