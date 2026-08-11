#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/manifest/temporal_publication.hpp"
#include "manifest/manifest_test_support.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <system_error>
#include <unistd.h>

namespace chronos::manifest {
namespace {

class TemporalPublicationDirectory {
public:
  TemporalPublicationDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-temporal-publication-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }
  ~TemporalPublicationDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

[[nodiscard]] common::Uuid nonce(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

[[nodiscard]] EncodedTemporalManifest empty_manifest(const DatabaseId database_id,
                                                     const std::uint64_t generation) {
  return encode_manifest_v2_temporal({.generation = generation,
                                      .database_id = database_id,
                                      .wal_reclaim_checkpoint = std::nullopt,
                                      .tablets = {},
                                      .parts = {},
                                      .retries = {}})
      .value();
}

void establish_layout(const std::filesystem::path& root, const DatabaseId database_id) {
  ASSERT_TRUE(std::filesystem::create_directory(root / kPartsDirectoryName));
  ASSERT_TRUE(std::filesystem::create_directory(root / kManifestDirectoryName));
  {
    std::ofstream lock{root / kManifestDirectoryName / kManifestLockFileName, std::ios::binary};
    ASSERT_TRUE(lock.good());
  }
  const EncodedTemporalManifest initial = empty_manifest(database_id, 1U);
  std::ofstream output{root / kManifestDirectoryName / *manifest_file_name(1U), std::ios::binary};
  ASSERT_TRUE(output.good());
  for (const std::byte value : initial.bytes())
    output.put(static_cast<char>(std::to_integer<unsigned char>(value)));
  output.close();
  ASSERT_TRUE(output.good());
}

[[nodiscard]] std::shared_ptr<const LoadedTemporalManifestGeneration>
load_selected(ManifestStorage& storage, const DatabaseId database_id) {
  auto loaded = storage.load_selected_temporal_manifest({.expected_database_id = database_id,
                                                         .schema_bindings = {},
                                                         .source_bindings = {},
                                                         .decode_limits = {},
                                                         .part_validation_limits = {}});
  EXPECT_TRUE(loaded.has_value()) << loaded.error().to_string();
  return loaded.has_value()
             ? std::make_shared<const LoadedTemporalManifestGeneration>(std::move(*loaded))
             : nullptr;
}

TEST(TemporalDatabaseStoragePublisherTest, AtomicallyPublishesDurableSuccessorAndRetainsOldEpoch) {
  TemporalPublicationDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const DatabaseId database_id = test::make_id<DatabaseId>(41U);
  establish_layout(directory.path(), database_id);
  auto storage = ManifestStorage::open_existing({.database_root = directory.path().string()});
  ASSERT_TRUE(storage.has_value());
  auto initial = load_selected(*storage, database_id);
  ASSERT_NE(initial, nullptr);
  auto publisher = TemporalDatabaseStoragePublisher::create(initial, {});
  ASSERT_TRUE(publisher.has_value()) << publisher.error().to_string();
  auto held = publisher->snapshot();
  ASSERT_TRUE(held.has_value());
  EXPECT_EQ(held->generation(), 1U);

  const EncodedTemporalManifest candidate = empty_manifest(database_id, 2U);
  auto installed = storage->install_temporal_manifest({.encoded_manifest = std::cref(candidate),
                                                       .schema_bindings = {},
                                                       .nonce = nonce(1U),
                                                       .decode_limits = {},
                                                       .part_validation_limits = {}});
  ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
  auto successor = load_selected(*storage, database_id);
  ASSERT_NE(successor, nullptr);
  auto published = publisher->publish_manifest(
      {.selected_manifest = successor, .schema_bindings = {}, .decode_limits = {}});
  ASSERT_TRUE(published.has_value()) << published.error().to_string();
  EXPECT_EQ(published->generation(), 2U);
  EXPECT_EQ(publisher->snapshot()->generation(), 2U);
  EXPECT_EQ(held->generation(), 1U);
  EXPECT_TRUE(publisher->is_usable());
}

TEST(TemporalDatabaseStoragePublisherTest, FailsClosedWhenDurableNamespaceSkipsLiveEpoch) {
  TemporalPublicationDirectory directory;
  const DatabaseId database_id = test::make_id<DatabaseId>(42U);
  establish_layout(directory.path(), database_id);
  auto storage = ManifestStorage::open_existing({.database_root = directory.path().string()});
  ASSERT_TRUE(storage.has_value());
  auto publisher =
      TemporalDatabaseStoragePublisher::create(load_selected(*storage, database_id), {});
  ASSERT_TRUE(publisher.has_value());

  const EncodedTemporalManifest generation_two = empty_manifest(database_id, 2U);
  ASSERT_TRUE(storage
                  ->install_temporal_manifest({.encoded_manifest = std::cref(generation_two),
                                               .schema_bindings = {},
                                               .nonce = nonce(2U),
                                               .decode_limits = {},
                                               .part_validation_limits = {}})
                  .has_value());
  const EncodedTemporalManifest generation_three = empty_manifest(database_id, 3U);
  ASSERT_TRUE(storage
                  ->install_temporal_manifest({.encoded_manifest = std::cref(generation_three),
                                               .schema_bindings = {},
                                               .nonce = nonce(3U),
                                               .decode_limits = {},
                                               .part_validation_limits = {}})
                  .has_value());
  auto generation_three_owner = load_selected(*storage, database_id);
  ASSERT_NE(generation_three_owner, nullptr);
  EXPECT_EQ(publisher
                ->publish_manifest({.selected_manifest = generation_three_owner,
                                    .schema_bindings = {},
                                    .decode_limits = {}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_FALSE(publisher->is_usable());
  EXPECT_EQ(publisher->snapshot().error().code(), common::StatusCode::kUnavailable);
  EXPECT_FALSE(publisher->poison_status().is_ok());
}

} // namespace
} // namespace chronos::manifest
