#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/tiering/cold_manifest_storage.hpp"
#include "io/posix_syscalls.hpp"

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::tiering {
namespace detail {

class ColdLocationManifestStorageTestAccess {
public:
  [[nodiscard]] static common::Result<ColdLocationManifestStorage>
  open_existing(ColdLocationManifestStorageConfig config, io::detail::PosixSyscalls& syscalls) {
    return ColdLocationManifestStorage::open(std::move(config), false, syscalls);
  }
};

} // namespace detail
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-cold-manifest-XXXXXX").string();
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

[[nodiscard]] ingest::Sha256Digest digest(const std::uint8_t seed) {
  ingest::Sha256Digest::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return ingest::Sha256Digest{bytes};
}

struct Fixture {
  manifest::DatabaseId database_id{id<manifest::DatabaseId>(1U)};
  common::Uuid object_store_id{uuid(2U)};
  schema::TableId table_id{id<schema::TableId>(3U)};
  schema::TabletId tablet_id{id<schema::TabletId>(4U)};
  schema::SchemaId schema_id{id<schema::SchemaId>(5U)};
  common::Uuid source_id{uuid(6U)};
  std::vector<manifest::TemporalTabletDescriptor> tablets{{
      .table_id = table_id,
      .tablet_id = tablet_id,
      .recovery_schema_id = schema_id,
      .recovery_schema_version = schema::SchemaVersion::initial(),
      .source_id = source_id,
      .durable_position = 2U,
      .reclaim_position = 1U,
      .first_part_index = 0U,
      .part_count = 2U,
      .durable_version_count = 2U,
      .commit_source = manifest::ManifestCommitSource::kRaft,
  }};
  std::vector<manifest::TemporalPartDescriptor> parts{
      {.part_id = id<cseg::PartId>(10U),
       .table_id = table_id,
       .tablet_id = tablet_id,
       .schema_id = schema_id,
       .schema_version = schema::SchemaVersion::initial(),
       .file_length = 4'096U,
       .row_count = 1U,
       .minimum_commit_position = 1U,
       .maximum_commit_position = 1U,
       .minimum_event_time = 10,
       .maximum_event_time = 10,
       .minimum_system_time = 100,
       .maximum_system_time = 100,
       .source_id = source_id,
       .content_sha256 = digest(20U),
       .commit_source = manifest::ManifestCommitSource::kRaft},
      {.part_id = id<cseg::PartId>(11U),
       .table_id = table_id,
       .tablet_id = tablet_id,
       .schema_id = schema_id,
       .schema_version = schema::SchemaVersion::initial(),
       .file_length = 8'192U,
       .row_count = 1U,
       .minimum_commit_position = 2U,
       .maximum_commit_position = 2U,
       .minimum_event_time = 20,
       .maximum_event_time = 20,
       .minimum_system_time = 200,
       .maximum_system_time = 200,
       .source_id = source_id,
       .content_sha256 = digest(21U),
       .commit_source = manifest::ManifestCommitSource::kRaft}};
  std::vector<ColdPartLocationDescriptor> locations{
      {parts[0].part_id, parts[0].file_length, parts[0].content_sha256, "cold/a"},
      {parts[1].part_id, parts[1].file_length, parts[1].content_sha256, "cold/b"}};

  [[nodiscard]] ColdLocationManifestStorageConfig
  config(const TemporaryDirectory& directory) const {
    return {.directory_path = directory.path().string(),
            .expected_database_id = database_id,
            .expected_object_store_id = object_store_id};
  }

  [[nodiscard]] common::Result<manifest::EncodedTemporalManifest>
  encode_base(const std::uint64_t generation) const {
    return manifest::encode_manifest_v2_temporal({.generation = generation,
                                                  .database_id = database_id,
                                                  .wal_reclaim_checkpoint = std::nullopt,
                                                  .tablets = tablets,
                                                  .parts = parts,
                                                  .retries = {}});
  }

  [[nodiscard]] common::Result<EncodedColdLocationManifest>
  encode_cold(const std::uint64_t generation, const std::uint64_t base_generation) const {
    return encode_cold_location_manifest_v1({.generation = generation,
                                             .base_manifest_generation = base_generation,
                                             .database_id = database_id,
                                             .object_store_id = object_store_id,
                                             .locations = locations});
  }
};

class FailingFsyncSyscalls final : public io::detail::PosixSyscalls {
public:
  explicit FailingFsyncSyscalls(const std::size_t fail_call)
      : delegate_(io::detail::system_posix_syscalls()), fail_call_(fail_call) {}

  int open_directory(const char* path, const int flags) override {
    return delegate_.open_directory(path, flags);
  }
  int open_at(const io::detail::OpenAtRequest& request) override {
    return delegate_.open_at(request);
  }
  int mkdir_at(const io::detail::MkdirAtRequest& request) override {
    return delegate_.mkdir_at(request);
  }
  ssize_t pread(const io::detail::ReadAtRequest& request) override {
    return delegate_.pread(request);
  }
  ssize_t pwrite(const io::detail::WriteAtRequest& request) override {
    return delegate_.pwrite(request);
  }
  int fstat(const int descriptor, struct stat* metadata) override {
    return delegate_.fstat(descriptor, metadata);
  }
  int ftruncate(const io::detail::TruncateRequest& request) override {
    return delegate_.ftruncate(request);
  }
  int fdatasync(const int descriptor) override {
    return delegate_.fdatasync(descriptor);
  }
  int fsync(const int descriptor) override {
    ++fsync_calls_;
    if (fsync_calls_ == fail_call_) {
      errno = EIO;
      return -1;
    }
    return delegate_.fsync(descriptor);
  }
  int rename_no_replace(const io::detail::RenameAtRequest& request) override {
    return delegate_.rename_no_replace(request);
  }
  int try_lock_exclusive(const int descriptor) override {
    return delegate_.try_lock_exclusive(descriptor);
  }
  int list_directory_entries(const int descriptor,
                             std::vector<io::DirectoryEntry>& entries) override {
    return delegate_.list_directory_entries(descriptor, entries);
  }
  int unlink_at(const int descriptor, const char* name) override {
    return delegate_.unlink_at(descriptor, name);
  }
  int close(const int descriptor) override {
    return delegate_.close(descriptor);
  }

private:
  io::detail::PosixSyscalls& delegate_;
  std::size_t fail_call_{};
  std::size_t fsync_calls_{};
};

TEST(ColdLocationManifestStorageTest, InstallsIdempotentlyAndRecoversHighestBoundGeneration) {
  TemporaryDirectory directory;
  Fixture fixture;
  auto base5_bytes = fixture.encode_base(5U);
  auto base6_bytes = fixture.encode_base(6U);
  ASSERT_TRUE(base5_bytes.has_value());
  ASSERT_TRUE(base6_bytes.has_value());
  auto base5 = manifest::decode_manifest_v2_temporal_exact(base5_bytes->bytes());
  auto base6 = manifest::decode_manifest_v2_temporal_exact(base6_bytes->bytes());
  ASSERT_TRUE(base5.has_value());
  ASSERT_TRUE(base6.has_value());
  auto cold1 = fixture.encode_cold(1U, 5U);
  auto cold2 = fixture.encode_cold(2U, 6U);
  ASSERT_TRUE(cold1.has_value());
  ASSERT_TRUE(cold2.has_value());

  {
    auto storage = ColdLocationManifestStorage::create(fixture.config(directory));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    auto locked = ColdLocationManifestStorage::open_existing(fixture.config(directory));
    ASSERT_FALSE(locked.has_value());
    EXPECT_EQ(locked.error().code(), common::StatusCode::kUnavailable);

    auto first = storage->install(std::cref(*cold1), *base5);
    ASSERT_TRUE(first.has_value()) << first.error().to_string();
    EXPECT_EQ(first->file_name, "generation-00000000000000000001.clm");
    EXPECT_FALSE(first->already_present);
    auto retry = storage->install(std::cref(*cold1), *base5);
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    EXPECT_TRUE(retry->already_present);
    auto second = storage->install(std::cref(*cold2), *base6);
    ASSERT_TRUE(second.has_value()) << second.error().to_string();

    auto selected = storage->load_selected(*base6);
    ASSERT_TRUE(selected.has_value()) << selected.error().to_string();
    const auto* selected_manifest = selected
                                        ->transform([](const LoadedColdLocationManifest& loaded) {
                                          return std::addressof(loaded);
                                        })
                                        .value_or(nullptr);
    ASSERT_NE(selected_manifest, nullptr);
    EXPECT_EQ(selected_manifest->manifest().generation(), 2U);
    EXPECT_EQ(storage->metrics().install_attempts, 3U);
    EXPECT_EQ(storage->metrics().installed_generations, 2U);
    EXPECT_EQ(storage->metrics().file_syncs, 2U);
    EXPECT_EQ(storage->metrics().directory_syncs, 2U);
  }

  auto reopened = ColdLocationManifestStorage::open_existing(fixture.config(directory));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  auto selected = reopened->load_selected(*base6);
  ASSERT_TRUE(selected.has_value()) << selected.error().to_string();
  const auto* selected_manifest = selected
                                      ->transform([](const LoadedColdLocationManifest& loaded) {
                                        return std::addressof(loaded);
                                      })
                                      .value_or(nullptr);
  ASSERT_NE(selected_manifest, nullptr);
  EXPECT_EQ(selected_manifest->manifest().generation(), 2U);
  EXPECT_EQ(reopened->load_selected(*base5).error().code(), common::StatusCode::kUnavailable);
}

TEST(ColdLocationManifestStorageTest, CleansTemporaryAndNeverFallsBackFromDamagedHighest) {
  TemporaryDirectory directory;
  Fixture fixture;
  auto base5_bytes = fixture.encode_base(5U);
  auto base6_bytes = fixture.encode_base(6U);
  ASSERT_TRUE(base5_bytes.has_value());
  ASSERT_TRUE(base6_bytes.has_value());
  auto base5 = manifest::decode_manifest_v2_temporal_exact(base5_bytes->bytes());
  auto base6 = manifest::decode_manifest_v2_temporal_exact(base6_bytes->bytes());
  ASSERT_TRUE(base5.has_value());
  ASSERT_TRUE(base6.has_value());
  auto cold1 = fixture.encode_cold(1U, 5U);
  auto cold2 = fixture.encode_cold(2U, 6U);
  ASSERT_TRUE(cold1.has_value());
  ASSERT_TRUE(cold2.has_value());
  {
    auto storage = ColdLocationManifestStorage::create(fixture.config(directory));
    ASSERT_TRUE(storage.has_value());
    ASSERT_TRUE(storage->install(std::cref(*cold1), *base5).has_value());
    ASSERT_TRUE(storage->install(std::cref(*cold2), *base6).has_value());
  }
  const auto temporary = directory.path() / "generation-00000000000000000003.clm.tmp";
  {
    std::ofstream output{temporary, std::ios::binary};
    output.put('x');
  }
  {
    auto storage = ColdLocationManifestStorage::open_existing(fixture.config(directory));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    EXPECT_FALSE(std::filesystem::exists(temporary));
  }
  auto highest_name = cold_location_manifest_file_name(2U);
  ASSERT_TRUE(highest_name.has_value());
  {
    std::fstream file{directory.path() / *highest_name,
                      std::ios::binary | std::ios::in | std::ios::out};
    ASSERT_TRUE(file.good());
    file.seekp(300);
    file.put('x');
  }
  auto storage = ColdLocationManifestStorage::open_existing(fixture.config(directory));
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  auto selected = storage->load_selected(*base6);
  ASSERT_FALSE(selected.has_value());
  EXPECT_EQ(selected.error().code(), common::StatusCode::kCorruption);
}

TEST(ColdLocationManifestStorageTest, RejectsGenerationGapsAndLocationRewrites) {
  TemporaryDirectory directory;
  Fixture fixture;
  auto base5_bytes = fixture.encode_base(5U);
  auto base6_bytes = fixture.encode_base(6U);
  ASSERT_TRUE(base5_bytes.has_value());
  ASSERT_TRUE(base6_bytes.has_value());
  auto base5 = manifest::decode_manifest_v2_temporal_exact(base5_bytes->bytes());
  auto base6 = manifest::decode_manifest_v2_temporal_exact(base6_bytes->bytes());
  ASSERT_TRUE(base5.has_value());
  ASSERT_TRUE(base6.has_value());
  auto cold1 = fixture.encode_cold(1U, 5U);
  ASSERT_TRUE(cold1.has_value());
  {
    auto storage = ColdLocationManifestStorage::create(fixture.config(directory));
    ASSERT_TRUE(storage.has_value());
    ASSERT_TRUE(storage->install(std::cref(*cold1), *base5).has_value());

    fixture.locations[0].object_key = "cold/changed";
    auto rewritten = fixture.encode_cold(2U, 6U);
    ASSERT_TRUE(rewritten.has_value());
    auto rejected = storage->install(std::cref(*rewritten), *base6);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error().code(), common::StatusCode::kInvalidArgument);
    auto skipped = fixture.encode_cold(3U, 6U);
    ASSERT_TRUE(skipped.has_value());
    EXPECT_EQ(storage->install(std::cref(*skipped), *base6).error().code(),
              common::StatusCode::kInvalidArgument);
  }

  auto first_name = cold_location_manifest_file_name(1U);
  auto second_name = cold_location_manifest_file_name(2U);
  ASSERT_TRUE(first_name.has_value());
  ASSERT_TRUE(second_name.has_value());
  std::filesystem::rename(directory.path() / *first_name, directory.path() / *second_name);
  auto gap = ColdLocationManifestStorage::open_existing(fixture.config(directory));
  ASSERT_FALSE(gap.has_value());
  EXPECT_EQ(gap.error().code(), common::StatusCode::kCorruption);
}

TEST(ColdLocationManifestStorageTest, DropsOnlyRoutesRetiredByANewerBaseManifest) {
  Fixture fixture;
  auto base5_bytes = fixture.encode_base(5U);
  auto cold1 = fixture.encode_cold(1U, 5U);
  ASSERT_TRUE(base5_bytes.has_value());
  ASSERT_TRUE(cold1.has_value());
  auto base5 = manifest::decode_manifest_v2_temporal_exact(base5_bytes->bytes());
  ASSERT_TRUE(base5.has_value());

  Fixture successor = fixture;
  successor.parts.erase(successor.parts.begin());
  successor.locations.erase(successor.locations.begin());
  successor.tablets.front().part_count = 1U;
  successor.tablets.front().durable_version_count = 1U;
  auto same_base_cold = successor.encode_cold(2U, 5U);
  auto base6_bytes = successor.encode_base(6U);
  auto cold2 = successor.encode_cold(2U, 6U);
  ASSERT_TRUE(same_base_cold.has_value());
  ASSERT_TRUE(base6_bytes.has_value());
  ASSERT_TRUE(cold2.has_value());
  auto base6 = manifest::decode_manifest_v2_temporal_exact(base6_bytes->bytes());
  ASSERT_TRUE(base6.has_value());

  TemporaryDirectory same_base_directory;
  auto same_base_storage = ColdLocationManifestStorage::create(fixture.config(same_base_directory));
  ASSERT_TRUE(same_base_storage.has_value());
  ASSERT_TRUE(same_base_storage->install(std::cref(*cold1), *base5).has_value());
  auto premature = same_base_storage->install(std::cref(*same_base_cold), *base5);
  ASSERT_FALSE(premature.has_value());
  EXPECT_EQ(premature.error().code(), common::StatusCode::kInvalidArgument);

  Fixture still_logical = fixture;
  still_logical.locations.erase(still_logical.locations.begin());
  auto still_logical_base6_bytes = fixture.encode_base(6U);
  auto missing_still_logical = still_logical.encode_cold(2U, 6U);
  ASSERT_TRUE(still_logical_base6_bytes.has_value());
  ASSERT_TRUE(missing_still_logical.has_value());
  auto still_logical_base6 =
      manifest::decode_manifest_v2_temporal_exact(still_logical_base6_bytes->bytes());
  ASSERT_TRUE(still_logical_base6.has_value());
  TemporaryDirectory still_logical_directory;
  auto still_logical_storage =
      ColdLocationManifestStorage::create(fixture.config(still_logical_directory));
  ASSERT_TRUE(still_logical_storage.has_value());
  ASSERT_TRUE(still_logical_storage->install(std::cref(*cold1), *base5).has_value());
  auto missing =
      still_logical_storage->install(std::cref(*missing_still_logical), *still_logical_base6);
  ASSERT_FALSE(missing.has_value());
  EXPECT_EQ(missing.error().code(), common::StatusCode::kInvalidArgument);

  TemporaryDirectory advanced_directory;
  auto advanced_storage = ColdLocationManifestStorage::create(fixture.config(advanced_directory));
  ASSERT_TRUE(advanced_storage.has_value());
  ASSERT_TRUE(advanced_storage->install(std::cref(*cold1), *base5).has_value());
  auto installed = advanced_storage->install(std::cref(*cold2), *base6);
  ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
  auto selected = advanced_storage->load_selected(*base6);
  ASSERT_TRUE(selected.has_value());
  const auto* selected_manifest = selected
                                      ->transform([](const LoadedColdLocationManifest& loaded) {
                                        return std::addressof(loaded);
                                      })
                                      .value_or(nullptr);
  ASSERT_NE(selected_manifest, nullptr);
  ASSERT_EQ(selected_manifest->manifest().locations().size(), 1U);
  EXPECT_EQ(selected_manifest->manifest().locations().front(), successor.locations.front());
}

TEST(ColdLocationManifestStorageTest, PoisonsAfterUncertainDirectorySync) {
  TemporaryDirectory directory;
  Fixture fixture;
  {
    auto creator = ColdLocationManifestStorage::create(fixture.config(directory));
    ASSERT_TRUE(creator.has_value());
  }
  auto base_bytes = fixture.encode_base(5U);
  ASSERT_TRUE(base_bytes.has_value());
  auto base = manifest::decode_manifest_v2_temporal_exact(base_bytes->bytes());
  ASSERT_TRUE(base.has_value());
  auto cold = fixture.encode_cold(1U, 5U);
  ASSERT_TRUE(cold.has_value());

  FailingFsyncSyscalls syscalls{2U};
  auto storage = detail::ColdLocationManifestStorageTestAccess::open_existing(
      fixture.config(directory), syscalls);
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  auto installed = storage->install(std::cref(*cold), *base);
  ASSERT_FALSE(installed.has_value());
  EXPECT_EQ(installed.error().code(), common::StatusCode::kIoError);
  EXPECT_FALSE(storage->is_usable());
  EXPECT_EQ(storage->install(std::cref(*cold), *base).error().code(),
            common::StatusCode::kUnavailable);
}

} // namespace
} // namespace chronos::tiering
