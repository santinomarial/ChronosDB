#include "chronos/common/status.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/io/posix_io.hpp"
#include "chronos/manifest/compaction.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/publication.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/schema/table_schema.hpp"
#include "cseg/cseg_test_fixture.hpp"
#include "manifest/storage_internal.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::manifest {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-manifest-storage-XXXXXX").string();
    char* const created = ::mkdtemp(pattern.data());
    if (created != nullptr) {
      path_ = created;
    }
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
  [[nodiscard]] bool valid() const noexcept {
    return !path_.empty();
  }

private:
  std::filesystem::path path_;
};

struct InjectedSyscallFaults {
  std::size_t fail_fsync_call{};
  std::size_t corrupt_pread_call{};
  std::size_t fail_unlink_call{};
};

class FailingFsyncSyscalls final : public io::detail::PosixSyscalls {
public:
  explicit FailingFsyncSyscalls(const InjectedSyscallFaults faults)
      : delegate_(io::detail::system_posix_syscalls()), fail_call_(faults.fail_fsync_call),
        corrupt_pread_call_(faults.corrupt_pread_call), fail_unlink_call_(faults.fail_unlink_call) {
  }

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
    ++pread_calls_;
    const ssize_t result = delegate_.pread(request);
    if (result > 0 && pread_calls_ == corrupt_pread_call_) {
      auto* const destination = static_cast<std::byte*>(request.destination);
      destination[0] ^= std::byte{0x01U};
    }
    return result;
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
    ++unlink_calls_;
    if (unlink_calls_ == fail_unlink_call_) {
      errno = EIO;
      return -1;
    }
    return delegate_.unlink_at(descriptor, name);
  }
  int close(const int descriptor) override {
    return delegate_.close(descriptor);
  }

private:
  io::detail::PosixSyscalls& delegate_;
  std::size_t fail_call_;
  std::size_t corrupt_pread_call_;
  std::size_t fail_unlink_call_;
  std::size_t fsync_calls_{};
  std::size_t pread_calls_{};
  std::size_t unlink_calls_{};
};

void establish_layout(const std::filesystem::path& path, const bool create_lock = true) {
  common::Result<io::PosixDirectory> root = io::PosixDirectory::open(path.string());
  ASSERT_TRUE(root.has_value()) << root.error().to_string();
  ASSERT_TRUE(root->create_exclusive_directory(kPartsDirectoryName).is_ok());
  ASSERT_TRUE(root->create_exclusive_directory(kManifestDirectoryName).is_ok());
  ASSERT_TRUE(root->sync().is_ok());
  if (create_lock) {
    common::Result<io::PosixDirectory> manifests = root->open_directory(kManifestDirectoryName);
    ASSERT_TRUE(manifests.has_value()) << manifests.error().to_string();
    common::Result<io::PosixAdvisoryLock> lock =
        manifests->acquire_exclusive_lock(kManifestLockFileName);
    ASSERT_TRUE(lock.has_value()) << lock.error().to_string();
    ASSERT_TRUE(manifests->sync().is_ok());
    ASSERT_TRUE(lock->close().is_ok());
  }
}

// Both paths are intrinsic to this descriptor-relative test helper.
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
void create_entry(const std::filesystem::path& root_path,
                  const std::filesystem::path& relative_path, const common::ByteView bytes = {}) {
  common::Result<io::PosixDirectory> root = io::PosixDirectory::open(root_path.string());
  ASSERT_TRUE(root.has_value()) << root.error().to_string();
  const std::string directory = relative_path.parent_path().string();
  const std::string name = relative_path.filename().string();
  common::Result<io::PosixDirectory> child = root->open_directory(directory);
  ASSERT_TRUE(child.has_value()) << child.error().to_string();
  common::Result<io::PosixFile> file = child->create_exclusive_regular_file(name);
  ASSERT_TRUE(file.has_value()) << file.error().to_string();
  ASSERT_TRUE(file->write_all_at(0U, bytes).is_ok());
  ASSERT_TRUE(file->sync_all().is_ok());
  ASSERT_TRUE(file->close().is_ok());
  ASSERT_TRUE(child->sync().is_ok());
}

struct PartFixture {
  cseg::EncodedCsegPart encoded{cseg::test::make_valid_part(cseg::PageCompression::kZstd)};
  schema::TableId table_id{id<schema::TableId>(2U)};
  schema::TabletId tablet_id{id<schema::TabletId>(3U)};
  schema::SchemaId schema_id{id<schema::SchemaId>(4U)};
  cseg::PartId part_id{id<cseg::PartId>(1U)};
  schema::TableSchema schema_value{make_schema()};
  schema::SchemaLineage lineage{schema::SchemaLineage::create(schema_value).value()};
  PartDescriptor descriptor{.part_id = part_id,
                            .table_id = table_id,
                            .tablet_id = tablet_id,
                            .schema_id = schema_id,
                            .schema_version = schema::SchemaVersion::initial(),
                            .file_length = encoded.size(),
                            .row_count = 2U,
                            .minimum_record_sequence = 7U,
                            .maximum_record_sequence = 7U,
                            .minimum_event_time = -5,
                            .maximum_event_time = 10};
  wal::WalId wal_id{make_wal_id()};
  DatabaseId database_id{id<DatabaseId>(6U)};
  common::Uuid nonce{make_nonce(0xa0U)};

  [[nodiscard]] schema::TableSchema make_schema() const {
    const schema::ColumnId event_id = id<schema::ColumnId>(5U);
    std::vector<schema::ColumnDefinition> columns;
    columns.push_back(
        schema::ColumnDefinition::create(
            event_id, "event_time",
            schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(), false)
            .value());
    return schema::TableSchema::create(table_id, schema_id, schema::SchemaVersion::initial(),
                                       std::nullopt, std::move(columns),
                                       {.event_time_column = event_id,
                                        .physical_ordering_key = {event_id},
                                        .partition_columns = {event_id},
                                        .shard_key = {event_id},
                                        .deduplication_key = {}})
        .value();
  }

  [[nodiscard]] static wal::WalId make_wal_id() {
    wal::WalId value{};
    value.bytes.front() = std::byte{0x70U};
    return value;
  }

  [[nodiscard]] static common::Uuid make_nonce(const std::uint8_t seed) {
    common::Uuid::Bytes bytes{};
    bytes.front() = std::byte{seed};
    return common::Uuid{bytes};
  }

  [[nodiscard]] PartInstallRequest request(const common::Uuid& request_nonce) const {
    return {.encoded_part = std::cref(encoded),
            .descriptor = descriptor,
            .wal_id = wal_id,
            .schema = std::cref(schema_value),
            .nonce = request_nonce,
            .validation_limits = {}};
  }

  [[nodiscard]] EncodedManifest manifest(const std::uint64_t generation) const {
    const std::array tablets{
        TabletDescriptor{.table_id = table_id,
                         .tablet_id = tablet_id,
                         .recovery_schema_id = schema_id,
                         .recovery_schema_version = schema::SchemaVersion::initial(),
                         .durable_record_sequence = 7U,
                         .first_part_index = 0U,
                         .part_count = 1U,
                         .durable_row_count = 2U}};
    const std::array parts{descriptor};
    return encode_manifest_v1({.generation = generation,
                               .database_id = database_id,
                               .wal_id = wal_id,
                               .reclaim_checkpoint = {.record_sequence = 0U,
                                                      .segment_number = 1U,
                                                      .byte_offset = 64U},
                               .tablets = tablets,
                               .parts = parts,
                               .retries = {}})
        .value();
  }

  [[nodiscard]] std::array<TabletSchemaBinding, 1> bindings() const {
    return {TabletSchemaBinding{.tablet_id = tablet_id, .lineage = std::cref(lineage)}};
  }
};

struct TemporalStorageFixture {
  cseg::EncodedCsegPart encoded{cseg::test::make_valid_temporal_part(cseg::PageCompression::kZstd)};
  schema::TableId table_id{id<schema::TableId>(2U)};
  schema::TabletId tablet_id{id<schema::TabletId>(3U)};
  schema::SchemaId schema_id{id<schema::SchemaId>(4U)};
  cseg::PartId part_id{id<cseg::PartId>(1U)};
  common::Uuid source{common::Uuid{id<schema::SchemaId>(8U).bytes()}};
  schema::TableSchema schema_value{make_schema()};
  schema::SchemaLineage lineage{schema::SchemaLineage::create(schema_value).value()};
  TemporalTabletDescriptor owner{.table_id = table_id,
                                 .tablet_id = tablet_id,
                                 .recovery_schema_id = schema_id,
                                 .recovery_schema_version = schema::SchemaVersion::initial(),
                                 .source_id = source,
                                 .durable_position = 9U,
                                 .reclaim_position = 0U,
                                 .first_part_index = 0U,
                                 .part_count = 1U,
                                 .durable_version_count = 2U,
                                 .commit_source = ManifestCommitSource::kWal};
  TemporalPartDescriptor descriptor{
      describe_manifest_v2_temporal_part_image(encoded.bytes(), schema_value, tablet_id,
                                               ManifestCommitSource::kWal, source)
          .value()};
  DatabaseId database_id{id<DatabaseId>(6U)};
  common::Uuid nonce{PartFixture::make_nonce(0xd0U)};

  [[nodiscard]] schema::TableSchema make_schema() const {
    const schema::ColumnId event_id = id<schema::ColumnId>(5U);
    std::vector<schema::ColumnDefinition> columns;
    columns.push_back(
        schema::ColumnDefinition::create(
            event_id, "event_time",
            schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(), false)
            .value());
    return schema::TableSchema::create(table_id, schema_id, schema::SchemaVersion::initial(),
                                       std::nullopt, std::move(columns),
                                       {.event_time_column = event_id,
                                        .physical_ordering_key = {event_id},
                                        .partition_columns = {event_id},
                                        .shard_key = {event_id},
                                        .deduplication_key = {event_id}})
        .value();
  }

  [[nodiscard]] TemporalPartInstallRequest part_request(const common::Uuid& request_nonce) const {
    return {.encoded_part = std::cref(encoded),
            .descriptor = descriptor,
            .owner = owner,
            .schema = std::cref(schema_value),
            .nonce = request_nonce,
            .validation_limits = {}};
  }

  [[nodiscard]] EncodedTemporalManifest manifest(const std::uint64_t generation) const {
    const std::array tablets{owner};
    const std::array parts{descriptor};
    return encode_manifest_v2_temporal({.generation = generation,
                                        .database_id = database_id,
                                        .wal_reclaim_checkpoint = std::nullopt,
                                        .tablets = tablets,
                                        .parts = parts,
                                        .retries = {}})
        .value();
  }

  [[nodiscard]] std::array<TabletSchemaBinding, 1> bindings() const {
    return {TabletSchemaBinding{.tablet_id = tablet_id, .lineage = std::cref(lineage)}};
  }

  [[nodiscard]] std::array<TemporalTabletSourceBinding, 1> source_bindings() const {
    return {TemporalTabletSourceBinding{
        .tablet_id = tablet_id, .commit_source = ManifestCommitSource::kWal, .source_id = source}};
  }
};

TEST(ManifestStorageTest, RequiresExactExistingDirectoryAndLockLayout) {
  TemporaryDirectory missing_directories;
  ASSERT_TRUE(missing_directories.valid());
  EXPECT_EQ(ManifestStorage::open_existing({.database_root = missing_directories.path().string()})
                .error()
                .code(),
            common::StatusCode::kNotFound);

  TemporaryDirectory missing_lock;
  ASSERT_TRUE(missing_lock.valid());
  establish_layout(missing_lock.path(), false);
  EXPECT_EQ(ManifestStorage::open_existing({.database_root = missing_lock.path().string()})
                .error()
                .code(),
            common::StatusCode::kNotFound);

  EXPECT_EQ(ManifestStorage::open_existing(
                {.database_root = missing_lock.path().string(), .file_permissions = 01000U})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

TEST(ManifestStorageTest, HoldsTheManifestLockForItsCompleteLifetime) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  establish_layout(temporary.path());
  common::Result<ManifestStorage> owner =
      ManifestStorage::open_existing({.database_root = temporary.path().string()});
  ASSERT_TRUE(owner.has_value()) << owner.error().to_string();
  EXPECT_EQ(
      ManifestStorage::open_existing({.database_root = temporary.path().string()}).error().code(),
      common::StatusCode::kUnavailable);
}

TEST(ManifestStorageTest, InstallsExactValidatedPartAndReportsDurabilityMetrics) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  establish_layout(temporary.path());
  ManifestStorage owner =
      ManifestStorage::open_existing({.database_root = temporary.path().string()}).value();
  const PartFixture fixture;

  const common::Result<InstalledPart> installed =
      owner.install_part(fixture.request(fixture.nonce));
  ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
  EXPECT_EQ(installed->file_name, part_file_name(fixture.part_id));
  EXPECT_EQ(installed->descriptor, fixture.descriptor);
  const std::filesystem::path final_path =
      temporary.path() / kPartsDirectoryName / installed->file_name;
  EXPECT_TRUE(std::filesystem::is_regular_file(final_path));
  EXPECT_EQ(std::filesystem::file_size(final_path), fixture.encoded.size());
  EXPECT_FALSE(std::filesystem::exists(temporary.path() / kPartsDirectoryName /
                                       temporary_part_file_name(fixture.part_id, fixture.nonce)));
  EXPECT_EQ(owner.metrics(), (PartInstallationMetrics{.attempts = 1U,
                                                      .failures = 0U,
                                                      .installed_parts = 1U,
                                                      .installed_bytes = fixture.encoded.size(),
                                                      .file_syncs = 1U,
                                                      .directory_syncs = 1U}));
  EXPECT_TRUE(owner.is_usable());
  EXPECT_TRUE(owner.poison_status().is_ok());
}

TEST(TemporalManifestStorageTest, InstallsValidatedPartAndExactSuccessorGeneration) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  establish_layout(temporary.path());
  const TemporalStorageFixture fixture;
  const EncodedTemporalManifest predecessor = fixture.manifest(1U);
  create_entry(temporary.path(),
               std::filesystem::path{kManifestDirectoryName} / *manifest_file_name(1U),
               predecessor.bytes());
  ManifestStorage owner =
      ManifestStorage::open_existing({.database_root = temporary.path().string()}).value();

  const auto installed_part = owner.install_temporal_part(fixture.part_request(fixture.nonce));
  ASSERT_TRUE(installed_part.has_value()) << installed_part.error().to_string();
  EXPECT_EQ(installed_part->file_name, part_file_name(fixture.part_id));
  EXPECT_EQ(installed_part->descriptor, fixture.descriptor);
  EXPECT_TRUE(std::filesystem::is_regular_file(temporary.path() / kPartsDirectoryName /
                                               installed_part->file_name));

  const EncodedTemporalManifest candidate = fixture.manifest(2U);
  const auto bindings = fixture.bindings();
  const common::Uuid manifest_nonce = PartFixture::make_nonce(0xd1U);
  const auto installed_manifest =
      owner.install_temporal_manifest({.encoded_manifest = std::cref(candidate),
                                       .schema_bindings = bindings,
                                       .nonce = manifest_nonce,
                                       .decode_limits = {},
                                       .part_validation_limits = {}});
  ASSERT_TRUE(installed_manifest.has_value()) << installed_manifest.error().to_string();
  EXPECT_EQ(installed_manifest->file_name, *manifest_file_name(2U));
  EXPECT_EQ(installed_manifest->generation, 2U);
  EXPECT_FALSE(installed_manifest->wal_reclaim_checkpoint.has_value());
  EXPECT_EQ(installed_manifest->tablet_count, 1U);
  EXPECT_EQ(installed_manifest->part_count, 1U);
  EXPECT_EQ(installed_manifest->retry_count, 0U);
  EXPECT_TRUE(std::filesystem::is_regular_file(temporary.path() / kManifestDirectoryName /
                                               *manifest_file_name(2U)));
  EXPECT_FALSE(std::filesystem::exists(temporary.path() / kManifestDirectoryName /
                                       *temporary_manifest_file_name(2U, manifest_nonce)));
  EXPECT_EQ(owner.metrics().installed_parts, 1U);
  EXPECT_EQ(owner.manifest_metrics().installed_generations, 1U);
  EXPECT_EQ(owner.manifest_metrics().referenced_parts_validated, 1U);
  EXPECT_EQ(owner.manifest_metrics().file_syncs, 1U);
  EXPECT_EQ(owner.manifest_metrics().directory_syncs, 1U);
}

TEST(TemporalManifestStorageTest, RejectsDescriptorMismatchBeforeFilesystemMutation) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  establish_layout(temporary.path());
  ManifestStorage owner =
      ManifestStorage::open_existing({.database_root = temporary.path().string()}).value();
  const TemporalStorageFixture fixture;
  TemporalPartInstallRequest request = fixture.part_request(fixture.nonce);
  --request.descriptor.minimum_system_time;
  EXPECT_EQ(owner.install_temporal_part(request).error().code(), common::StatusCode::kCorruption);
  EXPECT_TRUE(std::filesystem::is_empty(temporary.path() / kPartsDirectoryName));
  EXPECT_TRUE(owner.is_usable());
}

TEST(TemporalManifestStorageTest, PartDirectorySyncFailurePoisonsOwnerAfterRename) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  establish_layout(temporary.path());
  FailingFsyncSyscalls syscalls{{.fail_fsync_call = 2U}};
  ManifestStorage owner = detail::ManifestStorageTestAccess::open_existing(
                              {.database_root = temporary.path().string()}, syscalls)
                              .value();
  const TemporalStorageFixture fixture;
  const auto installed = owner.install_temporal_part(fixture.part_request(fixture.nonce));
  ASSERT_FALSE(installed.has_value());
  EXPECT_EQ(installed.error().code(), common::StatusCode::kIoError);
  EXPECT_FALSE(owner.is_usable());
  EXPECT_TRUE(std::filesystem::exists(temporary.path() / kPartsDirectoryName /
                                      part_file_name(fixture.part_id)));
  EXPECT_EQ(owner.metrics().file_syncs, 1U);
  EXPECT_EQ(owner.metrics().directory_syncs, 0U);
}

TEST(TemporalManifestStorageTest, CorruptPartReadbackFailsBeforeFileSync) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  establish_layout(temporary.path());
  FailingFsyncSyscalls syscalls{{.corrupt_pread_call = 1U}};
  ManifestStorage owner = detail::ManifestStorageTestAccess::open_existing(
                              {.database_root = temporary.path().string()}, syscalls)
                              .value();
  const TemporalStorageFixture fixture;
  const auto installed = owner.install_temporal_part(fixture.part_request(fixture.nonce));
  ASSERT_FALSE(installed.has_value());
  EXPECT_EQ(installed.error().code(), common::StatusCode::kCorruption);
  EXPECT_TRUE(owner.is_usable());
  EXPECT_EQ(owner.metrics().file_syncs, 0U);
  EXPECT_FALSE(std::filesystem::exists(temporary.path() / kPartsDirectoryName /
                                       part_file_name(fixture.part_id)));
  EXPECT_TRUE(std::filesystem::exists(temporary.path() / kPartsDirectoryName /
                                      temporary_part_file_name(fixture.part_id, fixture.nonce)));
}

TEST(TemporalManifestStorageTest, CorruptManifestReadbackFailsBeforeFileSync) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  establish_layout(temporary.path());
  const TemporalStorageFixture fixture;
  const EncodedTemporalManifest predecessor = fixture.manifest(1U);
  create_entry(temporary.path(),
               std::filesystem::path{kManifestDirectoryName} / *manifest_file_name(1U),
               predecessor.bytes());
  create_entry(temporary.path(),
               std::filesystem::path{kPartsDirectoryName} / part_file_name(fixture.part_id),
               fixture.encoded.bytes());
  FailingFsyncSyscalls syscalls{{.corrupt_pread_call = 3U}};
  ManifestStorage owner = detail::ManifestStorageTestAccess::open_existing(
                              {.database_root = temporary.path().string()}, syscalls)
                              .value();
  const EncodedTemporalManifest candidate = fixture.manifest(2U);
  const auto bindings = fixture.bindings();
  const common::Uuid nonce = PartFixture::make_nonce(0xd2U);

  const auto installed = owner.install_temporal_manifest({.encoded_manifest = std::cref(candidate),
                                                          .schema_bindings = bindings,
                                                          .nonce = nonce,
                                                          .decode_limits = {},
                                                          .part_validation_limits = {}});
  ASSERT_FALSE(installed.has_value());
  EXPECT_EQ(installed.error().code(), common::StatusCode::kCorruption);
  EXPECT_TRUE(owner.is_usable());
  EXPECT_EQ(owner.manifest_metrics().file_syncs, 0U);
  EXPECT_FALSE(
      std::filesystem::exists(temporary.path() / kManifestDirectoryName / *manifest_file_name(2U)));
  EXPECT_TRUE(std::filesystem::exists(temporary.path() / kManifestDirectoryName /
                                      *temporary_manifest_file_name(2U, nonce)));
}

TEST(TemporalManifestStorageTest, ManifestDirectorySyncFailurePoisonsOwnerAfterRename) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  establish_layout(temporary.path());
  const TemporalStorageFixture fixture;
  const EncodedTemporalManifest predecessor = fixture.manifest(1U);
  create_entry(temporary.path(),
               std::filesystem::path{kManifestDirectoryName} / *manifest_file_name(1U),
               predecessor.bytes());
  create_entry(temporary.path(),
               std::filesystem::path{kPartsDirectoryName} / part_file_name(fixture.part_id),
               fixture.encoded.bytes());
  FailingFsyncSyscalls syscalls{{.fail_fsync_call = 2U}};
  ManifestStorage owner = detail::ManifestStorageTestAccess::open_existing(
                              {.database_root = temporary.path().string()}, syscalls)
                              .value();
  const EncodedTemporalManifest candidate = fixture.manifest(2U);
  const auto bindings = fixture.bindings();
  const common::Uuid nonce = PartFixture::make_nonce(0xd3U);

  const auto installed = owner.install_temporal_manifest({.encoded_manifest = std::cref(candidate),
                                                          .schema_bindings = bindings,
                                                          .nonce = nonce,
                                                          .decode_limits = {},
                                                          .part_validation_limits = {}});
  ASSERT_FALSE(installed.has_value());
  EXPECT_EQ(installed.error().code(), common::StatusCode::kIoError);
  EXPECT_FALSE(owner.is_usable());
  EXPECT_EQ(owner.manifest_metrics().file_syncs, 1U);
  EXPECT_EQ(owner.manifest_metrics().directory_syncs, 0U);
  EXPECT_TRUE(
      std::filesystem::exists(temporary.path() / kManifestDirectoryName / *manifest_file_name(2U)));
}

TEST(TemporalManifestStorageTest, RejectsMissingFinalPartAndV1Predecessor) {
  TemporaryDirectory missing_part;
  ASSERT_TRUE(missing_part.valid());
  establish_layout(missing_part.path());
  const TemporalStorageFixture fixture;
  const EncodedTemporalManifest temporal_predecessor = fixture.manifest(1U);
  create_entry(missing_part.path(),
               std::filesystem::path{kManifestDirectoryName} / *manifest_file_name(1U),
               temporal_predecessor.bytes());
  ManifestStorage owner =
      ManifestStorage::open_existing({.database_root = missing_part.path().string()}).value();
  const EncodedTemporalManifest candidate = fixture.manifest(2U);
  const auto bindings = fixture.bindings();
  EXPECT_EQ(owner
                .install_temporal_manifest({.encoded_manifest = std::cref(candidate),
                                            .schema_bindings = bindings,
                                            .nonce = PartFixture::make_nonce(0xd4U),
                                            .decode_limits = {},
                                            .part_validation_limits = {}})
                .error()
                .code(),
            common::StatusCode::kCorruption);

  TemporaryDirectory v1_predecessor;
  ASSERT_TRUE(v1_predecessor.valid());
  establish_layout(v1_predecessor.path());
  const PartFixture v1_fixture;
  const EncodedManifest v1 = v1_fixture.manifest(1U);
  create_entry(v1_predecessor.path(),
               std::filesystem::path{kManifestDirectoryName} / *manifest_file_name(1U), v1.bytes());
  ManifestStorage v1_owner =
      ManifestStorage::open_existing({.database_root = v1_predecessor.path().string()}).value();
  EXPECT_EQ(v1_owner
                .install_temporal_manifest({.encoded_manifest = std::cref(candidate),
                                            .schema_bindings = bindings,
                                            .nonce = PartFixture::make_nonce(0xd5U),
                                            .decode_limits = {},
                                            .part_validation_limits = {}})
                .error()
                .code(),
            common::StatusCode::kNotSupported);
}

TEST(TemporalManifestStorageTest, LoadsHighestValidatedGenerationAndReportsNamespaceState) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  establish_layout(temporary.path());
  const TemporalStorageFixture fixture;
  const EncodedTemporalManifest selected = fixture.manifest(1U);
  create_entry(temporary.path(),
               std::filesystem::path{kManifestDirectoryName} / *manifest_file_name(1U),
               selected.bytes());
  create_entry(temporary.path(),
               std::filesystem::path{kPartsDirectoryName} / part_file_name(fixture.part_id),
               fixture.encoded.bytes());
  const cseg::PartId orphan = id<cseg::PartId>(99U);
  create_entry(temporary.path(),
               std::filesystem::path{kPartsDirectoryName} / part_file_name(orphan));
  const common::Uuid temporary_nonce = PartFixture::make_nonce(0xe0U);
  create_entry(temporary.path(),
               std::filesystem::path{kPartsDirectoryName} /
                   temporary_part_file_name(id<cseg::PartId>(98U), temporary_nonce));
  create_entry(temporary.path(), std::filesystem::path{kManifestDirectoryName} /
                                     *temporary_manifest_file_name(2U, temporary_nonce));
  ManifestStorage owner =
      ManifestStorage::open_existing({.database_root = temporary.path().string()}).value();
  const auto bindings = fixture.bindings();
  const auto sources = fixture.source_bindings();

  auto loaded = owner.load_selected_temporal_manifest({.expected_database_id = fixture.database_id,
                                                       .schema_bindings = bindings,
                                                       .source_bindings = sources,
                                                       .decode_limits = {},
                                                       .part_validation_limits = {}});
  ASSERT_TRUE(loaded.has_value()) << loaded.error().to_string();
  EXPECT_EQ(loaded->generation(), 1U);
  EXPECT_EQ(loaded->previous_generation(), 0U);
  EXPECT_EQ(loaded->database_id(), fixture.database_id);
  EXPECT_FALSE(loaded->wal_reclaim_checkpoint().has_value());
  EXPECT_EQ(loaded->tablets().size(), 1U);
  EXPECT_EQ(loaded->parts().size(), 1U);
  EXPECT_TRUE(loaded->retries().empty());
  EXPECT_TRUE(std::ranges::equal(loaded->encoded_bytes(), selected.bytes()));
  ASSERT_EQ(loaded->orphan_parts().size(), 1U);
  EXPECT_EQ(loaded->orphan_parts().front(), orphan);
  EXPECT_EQ(loaded->temporary_parts().size(), 1U);
  EXPECT_EQ(loaded->temporary_manifests().size(), 1U);
  EXPECT_GT(loaded->retained_buffer_bytes(), loaded->encoded_bytes().size());

  auto selected_owner =
      std::make_shared<const LoadedTemporalManifestGeneration>(std::move(*loaded));
  const std::array part_ids{fixture.part_id};
  const auto images = owner.load_temporal_part_images(selected_owner, part_ids, bindings, {});
  ASSERT_TRUE(images.has_value()) << images.error().to_string();
  ASSERT_EQ(images->size(), 1U);
  EXPECT_EQ(images->front().generation(), 1U);
  EXPECT_EQ(images->front().descriptor(), fixture.descriptor);
  EXPECT_TRUE(std::ranges::equal(images->front().bytes(), fixture.encoded.bytes()));
  EXPECT_GT(images->front().retained_buffer_bytes(), images->front().bytes().size());
  const std::array duplicate_ids{fixture.part_id, fixture.part_id};
  EXPECT_EQ(
      owner.load_temporal_part_images(selected_owner, duplicate_ids, bindings, {}).error().code(),
      common::StatusCode::kInvalidArgument);

  auto wrong_sources = sources;
  wrong_sources[0].source_id = common::Uuid{id<schema::SchemaId>(97U).bytes()};
  EXPECT_EQ(owner
                .load_selected_temporal_manifest({.expected_database_id = fixture.database_id,
                                                  .schema_bindings = bindings,
                                                  .source_bindings = wrong_sources,
                                                  .decode_limits = {},
                                                  .part_validation_limits = {}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

TEST(TemporalManifestStorageTest, HighestCorruptionFailsWithoutFallback) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  establish_layout(temporary.path());
  const TemporalStorageFixture fixture;
  const EncodedTemporalManifest generation_one = fixture.manifest(1U);
  const EncodedTemporalManifest generation_two = fixture.manifest(2U);
  create_entry(temporary.path(),
               std::filesystem::path{kManifestDirectoryName} / *manifest_file_name(1U),
               generation_one.bytes());
  std::vector<std::byte> damaged(generation_two.bytes().begin(), generation_two.bytes().end());
  damaged.back() ^= std::byte{1U};
  create_entry(temporary.path(),
               std::filesystem::path{kManifestDirectoryName} / *manifest_file_name(2U), damaged);
  create_entry(temporary.path(),
               std::filesystem::path{kPartsDirectoryName} / part_file_name(fixture.part_id),
               fixture.encoded.bytes());
  ManifestStorage owner =
      ManifestStorage::open_existing({.database_root = temporary.path().string()}).value();
  const auto bindings = fixture.bindings();
  const auto sources = fixture.source_bindings();
  EXPECT_EQ(owner
                .load_selected_temporal_manifest({.expected_database_id = fixture.database_id,
                                                  .schema_bindings = bindings,
                                                  .source_bindings = sources,
                                                  .decode_limits = {},
                                                  .part_validation_limits = {}})
                .error()
                .code(),
            common::StatusCode::kCorruption);
}

TEST(ManifestStorageTest, RejectsBeforeMutationAndNeverReplacesFinalIdentity) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  establish_layout(temporary.path());
  ManifestStorage owner =
      ManifestStorage::open_existing({.database_root = temporary.path().string()}).value();
  PartFixture fixture;

  fixture.descriptor.maximum_record_sequence = 8U;
  EXPECT_EQ(owner.install_part(fixture.request(fixture.nonce)).error().code(),
            common::StatusCode::kCorruption);
  EXPECT_TRUE(std::filesystem::is_empty(temporary.path() / kPartsDirectoryName));
  fixture.descriptor.maximum_record_sequence = 7U;
  ASSERT_TRUE(owner.install_part(fixture.request(fixture.nonce)).has_value());

  const common::Uuid second_nonce = PartFixture::make_nonce(0xa1U);
  EXPECT_EQ(owner.install_part(fixture.request(second_nonce)).error().code(),
            common::StatusCode::kAlreadyExists);
  EXPECT_TRUE(std::filesystem::exists(temporary.path() / kPartsDirectoryName /
                                      temporary_part_file_name(fixture.part_id, second_nonce)));
  EXPECT_EQ(owner.metrics().attempts, 3U);
  EXPECT_EQ(owner.metrics().failures, 2U);
  EXPECT_EQ(owner.metrics().installed_parts, 1U);
  EXPECT_TRUE(owner.is_usable());
}

TEST(ManifestStorageTest, RejectsZeroNonceWithoutCreatingCandidate) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  establish_layout(temporary.path());
  ManifestStorage owner =
      ManifestStorage::open_existing({.database_root = temporary.path().string()}).value();
  const PartFixture fixture;
  EXPECT_EQ(owner.install_part(fixture.request(common::Uuid{})).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(std::filesystem::is_empty(temporary.path() / kPartsDirectoryName));
}

TEST(ManifestStorageTest, DirectorySyncFailurePoisonsOwnerAfterFinalRename) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  establish_layout(temporary.path());
  FailingFsyncSyscalls syscalls{{.fail_fsync_call = 2U}};
  ManifestStorage owner = detail::ManifestStorageTestAccess::open_existing(
                              {.database_root = temporary.path().string()}, syscalls)
                              .value();
  const PartFixture fixture;

  const common::Result<InstalledPart> installed =
      owner.install_part(fixture.request(fixture.nonce));
  ASSERT_FALSE(installed.has_value());
  EXPECT_EQ(installed.error().code(), common::StatusCode::kIoError);
  EXPECT_FALSE(owner.is_usable());
  EXPECT_EQ(owner.poison_status().code(), common::StatusCode::kIoError);
  EXPECT_TRUE(std::filesystem::exists(temporary.path() / kPartsDirectoryName /
                                      part_file_name(fixture.part_id)));
  EXPECT_EQ(owner.metrics().file_syncs, 1U);
  EXPECT_EQ(owner.metrics().directory_syncs, 0U);
  EXPECT_EQ(owner.metrics().installed_parts, 0U);

  const common::Uuid retry_nonce = PartFixture::make_nonce(0xa2U);
  EXPECT_EQ(owner.install_part(fixture.request(retry_nonce)).error().code(),
            common::StatusCode::kUnavailable);
}

TEST(ManifestStorageTest, InstallsExactNextManifestAfterRevalidatingReferencedParts) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  establish_layout(temporary.path());
  const PartFixture fixture;
  const EncodedManifest predecessor = fixture.manifest(1U);
  create_entry(temporary.path(),
               std::filesystem::path{kManifestDirectoryName} / *manifest_file_name(1U),
               predecessor.bytes());
  ManifestStorage owner =
      ManifestStorage::open_existing({.database_root = temporary.path().string()}).value();
  ASSERT_TRUE(owner.install_part(fixture.request(fixture.nonce)).has_value());
  const EncodedManifest candidate = fixture.manifest(2U);
  const auto bindings = fixture.bindings();
  const common::Uuid manifest_nonce = PartFixture::make_nonce(0xc0U);

  const common::Result<InstalledManifest> installed =
      owner.install_manifest({.encoded_manifest = std::cref(candidate),
                              .schema_bindings = bindings,
                              .nonce = manifest_nonce,
                              .decode_limits = {},
                              .part_validation_limits = {},
                              .compaction_equivalence_limits = {}});
  ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
  EXPECT_EQ(installed->file_name, *manifest_file_name(2U));
  EXPECT_EQ(installed->generation, 2U);
  EXPECT_EQ(installed->reclaim_checkpoint,
            (WalCheckpoint{.record_sequence = 0U, .segment_number = 1U, .byte_offset = 64U}));
  EXPECT_EQ(installed->tablet_count, 1U);
  EXPECT_EQ(installed->part_count, 1U);
  EXPECT_EQ(installed->retry_count, 0U);
  EXPECT_TRUE(std::filesystem::is_regular_file(temporary.path() / kManifestDirectoryName /
                                               *manifest_file_name(2U)));
  EXPECT_EQ(std::filesystem::file_size(temporary.path() / kManifestDirectoryName /
                                       *manifest_file_name(2U)),
            candidate.size());
  EXPECT_FALSE(std::filesystem::exists(temporary.path() / kManifestDirectoryName /
                                       *temporary_manifest_file_name(2U, manifest_nonce)));
  EXPECT_EQ(owner.manifest_metrics(),
            (ManifestInstallationMetrics{.attempts = 1U,
                                         .failures = 0U,
                                         .installed_generations = 1U,
                                         .installed_bytes = candidate.size(),
                                         .referenced_parts_validated = 1U,
                                         .file_syncs = 1U,
                                         .directory_syncs = 1U}));
  const common::Result<ManifestNamespaceSnapshot> snapshot = owner.scan_namespace();
  ASSERT_TRUE(snapshot.has_value());
  EXPECT_EQ(snapshot->generations, (std::vector<std::uint64_t>{1U, 2U}));
}

TEST(ManifestStorageTest, LoadsSnapshotBoundPartAfterNewerManifestBecomesSelected) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  establish_layout(temporary.path());
  const PartFixture fixture;
  const EncodedManifest predecessor = fixture.manifest(1U);
  create_entry(temporary.path(),
               std::filesystem::path{kManifestDirectoryName} / *manifest_file_name(1U),
               predecessor.bytes());
  create_entry(temporary.path(),
               std::filesystem::path{kPartsDirectoryName} / part_file_name(fixture.part_id),
               fixture.encoded.bytes());
  ManifestStorage owner =
      ManifestStorage::open_existing({.database_root = temporary.path().string()}).value();
  const auto bindings = fixture.bindings();
  auto selected_one = std::make_shared<const LoadedManifestGeneration>(
      owner
          .load_selected_manifest({.expected_database_id = fixture.database_id,
                                   .expected_wal_id = fixture.wal_id,
                                   .schema_bindings = bindings,
                                   .decode_limits = {},
                                   .part_validation_limits = {}})
          .value());
  DatabaseStoragePublisher publisher = DatabaseStoragePublisher::create(selected_one, {}).value();
  const DatabaseStorageSnapshot old_snapshot = publisher.snapshot().value();

  const EncodedManifest successor = fixture.manifest(2U);
  ASSERT_TRUE(owner
                  .install_manifest({.encoded_manifest = std::cref(successor),
                                     .schema_bindings = bindings,
                                     .nonce = PartFixture::make_nonce(0xc1U),
                                     .decode_limits = {},
                                     .part_validation_limits = {},
                                     .compaction_equivalence_limits = {}})
                  .has_value());
  auto selected_two = std::make_shared<const LoadedManifestGeneration>(
      owner
          .load_selected_manifest({.expected_database_id = fixture.database_id,
                                   .expected_wal_id = fixture.wal_id,
                                   .schema_bindings = bindings,
                                   .decode_limits = {},
                                   .part_validation_limits = {}})
          .value());
  ASSERT_TRUE(publisher.publish_manifest({.selected_manifest = selected_two, .replacements = {}})
                  .has_value());
  ASSERT_EQ(publisher.snapshot()->generation(), 2U);

  const std::array part_ids{fixture.part_id};
  common::Result<std::vector<SnapshotPartImage>> images =
      owner.load_snapshot_part_images(old_snapshot, part_ids, bindings, {});
  ASSERT_TRUE(images.has_value()) << images.error().to_string();
  ASSERT_EQ(images->size(), 1U);
  EXPECT_EQ(images->front().database_id(), fixture.database_id);
  EXPECT_EQ(images->front().wal_id(), fixture.wal_id);
  EXPECT_EQ(images->front().snapshot_generation(), 1U);
  EXPECT_EQ(images->front().descriptor(), fixture.descriptor);
  EXPECT_TRUE(std::ranges::equal(images->front().bytes(), fixture.encoded.bytes()));
  EXPECT_GE(images->front().retained_buffer_bytes(), images->front().bytes().size());
  EXPECT_GT(old_snapshot.retained_buffer_bytes(), old_snapshot.manifest_bytes().size());

  EXPECT_EQ(owner.load_snapshot_part_images(old_snapshot, {}, bindings, {}).error().code(),
            common::StatusCode::kInvalidArgument);
  const std::array duplicate_ids{fixture.part_id, fixture.part_id};
  EXPECT_EQ(
      owner.load_snapshot_part_images(old_snapshot, duplicate_ids, bindings, {}).error().code(),
      common::StatusCode::kInvalidArgument);
  const std::array unknown_ids{id<cseg::PartId>(0xfeU)};
  EXPECT_EQ(owner.load_snapshot_part_images(old_snapshot, unknown_ids, bindings, {}).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(owner.load_snapshot_part_images(old_snapshot, part_ids, {}, {}).error().code(),
            common::StatusCode::kInvalidArgument);

  std::fstream damaged{temporary.path() / kPartsDirectoryName / part_file_name(fixture.part_id),
                       std::ios::binary | std::ios::in | std::ios::out};
  ASSERT_TRUE(damaged.good());
  char first{};
  damaged.read(&first, 1);
  ASSERT_TRUE(damaged.good());
  first = static_cast<char>(static_cast<unsigned char>(first) ^ 0x01U);
  damaged.seekp(0);
  damaged.write(&first, 1);
  damaged.close();
  EXPECT_EQ(owner.load_snapshot_part_images(old_snapshot, part_ids, bindings, {}).error().code(),
            common::StatusCode::kCorruption);
}

TEST(ManifestStorageTest, CompactionAuthorityReprovesDiskImagesBeforeAtomicReplacement) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  establish_layout(temporary.path());
  const PartFixture fixture;
  const EncodedManifest predecessor_bytes = fixture.manifest(1U);
  create_entry(temporary.path(),
               std::filesystem::path{kManifestDirectoryName} / *manifest_file_name(1U),
               predecessor_bytes.bytes());
  create_entry(temporary.path(),
               std::filesystem::path{kPartsDirectoryName} / part_file_name(fixture.part_id),
               fixture.encoded.bytes());
  ManifestStorage owner =
      ManifestStorage::open_existing({.database_root = temporary.path().string()}).value();
  const std::array input_images{
      CompactionPartImage{.part_id = fixture.part_id, .bytes = fixture.encoded.bytes()}};
  common::Result<EncodedCompactionPart> merged =
      merge_append_only_cseg_v1({.inputs = input_images,
                                 .schema = std::cref(fixture.schema_value),
                                 .tablet_id = fixture.tablet_id,
                                 .wal_id = fixture.wal_id,
                                 .output_part_id = id<cseg::PartId>(9U),
                                 .limits = {}});
  ASSERT_TRUE(merged.has_value()) << merged.error().to_string();
  const auto bindings = fixture.bindings();
  const DecodedManifestView predecessor =
      decode_manifest_v1_exact(predecessor_bytes.bytes()).value();
  common::Result<EncodedManifest> candidate =
      build_manifest_v1_for_append_only_compaction({.predecessor = predecessor,
                                                    .inputs = input_images,
                                                    .output = std::cref(*merged),
                                                    .schema = std::cref(fixture.schema_value),
                                                    .schema_bindings = bindings,
                                                    .equivalence_limits = {},
                                                    .part_validation_limits = {}});
  ASSERT_TRUE(candidate.has_value()) << candidate.error().to_string();
  ASSERT_TRUE(owner
                  .install_part({.encoded_part = std::cref(merged->encoded_part),
                                 .descriptor = merged->descriptor,
                                 .wal_id = merged->wal_id,
                                 .schema = std::cref(fixture.schema_value),
                                 .nonce = PartFixture::make_nonce(0xd0U),
                                 .validation_limits = {}})
                  .has_value());

  EXPECT_EQ(owner
                .install_manifest({.encoded_manifest = std::cref(*candidate),
                                   .schema_bindings = bindings,
                                   .nonce = PartFixture::make_nonce(0xd1U),
                                   .decode_limits = {},
                                   .part_validation_limits = {},
                                   .compaction_equivalence_limits = {}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  const std::array input_ids{fixture.part_id};
  const std::array output_ids{merged->descriptor.part_id};
  const ManifestCompactionReplacement replacement{
      .tablet_id = fixture.tablet_id, .input_part_ids = input_ids, .output_part_ids = output_ids};
  const common::Result<InstalledManifest> installed =
      owner.install_manifest({.encoded_manifest = std::cref(*candidate),
                              .schema_bindings = bindings,
                              .nonce = PartFixture::make_nonce(0xd2U),
                              .decode_limits = {},
                              .part_validation_limits = {},
                              .compaction_replacement = &replacement,
                              .compaction_equivalence_limits = {}});
  ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
  EXPECT_EQ(installed->generation, 2U);
  EXPECT_EQ(installed->part_count, 1U);
  EXPECT_TRUE(std::filesystem::exists(temporary.path() / kPartsDirectoryName /
                                      part_file_name(fixture.part_id)));
  EXPECT_TRUE(std::filesystem::exists(temporary.path() / kPartsDirectoryName /
                                      part_file_name(merged->descriptor.part_id)));
  EXPECT_EQ(owner.manifest_metrics().attempts, 2U);
  EXPECT_EQ(owner.manifest_metrics().failures, 1U);
  EXPECT_EQ(owner.manifest_metrics().installed_generations, 1U);
}

TEST(ManifestStorageTest, LoadsOwnedHighestManifestAndReportsOrphansAndTemporaries) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  establish_layout(temporary.path());
  const PartFixture fixture;
  const EncodedManifest selected = fixture.manifest(1U);
  const cseg::PartId orphan_id = id<cseg::PartId>(9U);
  const common::Uuid nonce = PartFixture::make_nonce(0xd0U);
  const std::string temporary_part = temporary_part_file_name(orphan_id, nonce);
  const std::string temporary_manifest = *temporary_manifest_file_name(2U, nonce);
  create_entry(temporary.path(),
               std::filesystem::path{kManifestDirectoryName} / *manifest_file_name(1U),
               selected.bytes());
  create_entry(temporary.path(),
               std::filesystem::path{kManifestDirectoryName} / temporary_manifest);
  create_entry(temporary.path(),
               std::filesystem::path{kPartsDirectoryName} / part_file_name(fixture.part_id),
               fixture.encoded.bytes());
  create_entry(temporary.path(),
               std::filesystem::path{kPartsDirectoryName} / part_file_name(orphan_id));
  create_entry(temporary.path(), std::filesystem::path{kPartsDirectoryName} / temporary_part);
  ManifestStorage owner =
      ManifestStorage::open_existing({.database_root = temporary.path().string()}).value();
  const auto bindings = fixture.bindings();

  common::Result<LoadedManifestGeneration> loaded =
      owner.load_selected_manifest({.expected_database_id = fixture.database_id,
                                    .expected_wal_id = fixture.wal_id,
                                    .schema_bindings = bindings,
                                    .decode_limits = {},
                                    .part_validation_limits = {}});
  ASSERT_TRUE(loaded.has_value()) << loaded.error().to_string();
  EXPECT_EQ(loaded->generation(), 1U);
  EXPECT_EQ(loaded->database_id(), fixture.database_id);
  EXPECT_EQ(loaded->wal_id(), fixture.wal_id);
  EXPECT_EQ(loaded->tablets().size(), 1U);
  ASSERT_EQ(loaded->parts().size(), 1U);
  EXPECT_EQ(loaded->parts().front(), fixture.descriptor);
  EXPECT_TRUE(std::equal(loaded->encoded_bytes().begin(), loaded->encoded_bytes().end(),
                         selected.bytes().begin(), selected.bytes().end()));
  EXPECT_NE(loaded->encoded_bytes().data(), selected.bytes().data());
  ASSERT_EQ(loaded->orphan_parts().size(), 1U);
  EXPECT_EQ(loaded->orphan_parts().front(), orphan_id);
  ASSERT_EQ(loaded->temporary_parts().size(), 1U);
  EXPECT_EQ(loaded->temporary_parts().front(), temporary_part);
  ASSERT_EQ(loaded->temporary_manifests().size(), 1U);
  EXPECT_EQ(loaded->temporary_manifests().front(), temporary_manifest);
  EXPECT_TRUE(std::filesystem::exists(temporary.path() / kPartsDirectoryName / temporary_part));
  EXPECT_TRUE(
      std::filesystem::exists(temporary.path() / kManifestDirectoryName / temporary_manifest));

  LoadedManifestGeneration moved = std::move(*loaded);
  EXPECT_EQ(moved.generation(), 1U);
  EXPECT_TRUE(std::equal(moved.encoded_bytes().begin(), moved.encoded_bytes().end(),
                         selected.bytes().begin(), selected.bytes().end()));
}

TEST(ManifestStorageTest, LoadedManifestRequiresExactRecoveryIdentityAndReferencedParts) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  establish_layout(temporary.path());
  const PartFixture fixture;
  const EncodedManifest selected = fixture.manifest(1U);
  create_entry(temporary.path(),
               std::filesystem::path{kManifestDirectoryName} / *manifest_file_name(1U),
               selected.bytes());
  ManifestStorage owner =
      ManifestStorage::open_existing({.database_root = temporary.path().string()}).value();
  const auto bindings = fixture.bindings();

  EXPECT_EQ(owner
                .load_selected_manifest({.expected_database_id = id<DatabaseId>(7U),
                                         .expected_wal_id = fixture.wal_id,
                                         .schema_bindings = bindings,
                                         .decode_limits = {},
                                         .part_validation_limits = {}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(owner
                .load_selected_manifest({.expected_database_id = fixture.database_id,
                                         .expected_wal_id = fixture.wal_id,
                                         .schema_bindings = bindings,
                                         .decode_limits = {},
                                         .part_validation_limits = {}})
                .error()
                .code(),
            common::StatusCode::kCorruption);
  EXPECT_TRUE(owner.is_usable());
}

TEST(ManifestStorageTest, RejectsStaleTransitionAndMissingPartBeforeManifestMutation) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  establish_layout(temporary.path());
  const PartFixture fixture;
  const EncodedManifest predecessor = fixture.manifest(1U);
  create_entry(temporary.path(),
               std::filesystem::path{kManifestDirectoryName} / *manifest_file_name(1U),
               predecessor.bytes());
  ManifestStorage owner =
      ManifestStorage::open_existing({.database_root = temporary.path().string()}).value();
  const auto bindings = fixture.bindings();
  const EncodedManifest stale = fixture.manifest(3U);
  const common::Uuid stale_nonce = PartFixture::make_nonce(0xc1U);
  EXPECT_EQ(owner
                .install_manifest({.encoded_manifest = std::cref(stale),
                                   .schema_bindings = bindings,
                                   .nonce = stale_nonce,
                                   .decode_limits = {},
                                   .part_validation_limits = {},
                                   .compaction_equivalence_limits = {}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_FALSE(std::filesystem::exists(temporary.path() / kManifestDirectoryName /
                                       *temporary_manifest_file_name(3U, stale_nonce)));

  const EncodedManifest candidate = fixture.manifest(2U);
  const common::Uuid missing_nonce = PartFixture::make_nonce(0xc2U);
  EXPECT_EQ(owner
                .install_manifest({.encoded_manifest = std::cref(candidate),
                                   .schema_bindings = bindings,
                                   .nonce = missing_nonce,
                                   .decode_limits = {},
                                   .part_validation_limits = {},
                                   .compaction_equivalence_limits = {}})
                .error()
                .code(),
            common::StatusCode::kCorruption);
  EXPECT_FALSE(std::filesystem::exists(temporary.path() / kManifestDirectoryName /
                                       *temporary_manifest_file_name(2U, missing_nonce)));
  EXPECT_TRUE(owner.is_usable());
  EXPECT_EQ(owner.manifest_metrics().attempts, 2U);
  EXPECT_EQ(owner.manifest_metrics().failures, 2U);
  EXPECT_EQ(owner.manifest_metrics().file_syncs, 0U);
}

TEST(ManifestStorageTest, RejectsCorruptSelectedManifestWithoutFallingBackOrWriting) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  establish_layout(temporary.path());
  const PartFixture fixture;
  const EncodedManifest predecessor = fixture.manifest(1U);
  std::vector<std::byte> corrupted(predecessor.bytes().begin(), predecessor.bytes().end());
  corrupted.back() ^= std::byte{0x01U};
  create_entry(temporary.path(),
               std::filesystem::path{kManifestDirectoryName} / *manifest_file_name(1U), corrupted);
  create_entry(temporary.path(),
               std::filesystem::path{kPartsDirectoryName} / part_file_name(fixture.part_id),
               fixture.encoded.bytes());
  ManifestStorage owner =
      ManifestStorage::open_existing({.database_root = temporary.path().string()}).value();
  const EncodedManifest candidate = fixture.manifest(2U);
  const auto bindings = fixture.bindings();
  const common::Uuid nonce = PartFixture::make_nonce(0xc3U);

  EXPECT_EQ(owner
                .install_manifest({.encoded_manifest = std::cref(candidate),
                                   .schema_bindings = bindings,
                                   .nonce = nonce,
                                   .decode_limits = {},
                                   .part_validation_limits = {},
                                   .compaction_equivalence_limits = {}})
                .error()
                .code(),
            common::StatusCode::kCorruption);
  EXPECT_FALSE(std::filesystem::exists(temporary.path() / kManifestDirectoryName /
                                       *temporary_manifest_file_name(2U, nonce)));
  EXPECT_FALSE(
      std::filesystem::exists(temporary.path() / kManifestDirectoryName / *manifest_file_name(2U)));
}

TEST(ManifestStorageTest, CorruptManifestReadbackFailsBeforeSyncAndLeavesOnlyTemporary) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  establish_layout(temporary.path());
  const PartFixture fixture;
  const EncodedManifest predecessor = fixture.manifest(1U);
  create_entry(temporary.path(),
               std::filesystem::path{kManifestDirectoryName} / *manifest_file_name(1U),
               predecessor.bytes());
  create_entry(temporary.path(),
               std::filesystem::path{kPartsDirectoryName} / part_file_name(fixture.part_id),
               fixture.encoded.bytes());
  FailingFsyncSyscalls syscalls{{.corrupt_pread_call = 3U}};
  ManifestStorage owner = detail::ManifestStorageTestAccess::open_existing(
                              {.database_root = temporary.path().string()}, syscalls)
                              .value();
  const EncodedManifest candidate = fixture.manifest(2U);
  const auto bindings = fixture.bindings();
  const common::Uuid nonce = PartFixture::make_nonce(0xc4U);

  EXPECT_EQ(owner
                .install_manifest({.encoded_manifest = std::cref(candidate),
                                   .schema_bindings = bindings,
                                   .nonce = nonce,
                                   .decode_limits = {},
                                   .part_validation_limits = {},
                                   .compaction_equivalence_limits = {}})
                .error()
                .code(),
            common::StatusCode::kCorruption);
  EXPECT_TRUE(std::filesystem::exists(temporary.path() / kManifestDirectoryName /
                                      *temporary_manifest_file_name(2U, nonce)));
  EXPECT_FALSE(
      std::filesystem::exists(temporary.path() / kManifestDirectoryName / *manifest_file_name(2U)));
  EXPECT_TRUE(owner.is_usable());
  EXPECT_EQ(owner.manifest_metrics().referenced_parts_validated, 1U);
  EXPECT_EQ(owner.manifest_metrics().file_syncs, 0U);
}

TEST(ManifestStorageTest, ManifestDirectorySyncFailurePoisonsOwnerAfterFinalRename) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  establish_layout(temporary.path());
  const PartFixture fixture;
  const EncodedManifest predecessor = fixture.manifest(1U);
  create_entry(temporary.path(),
               std::filesystem::path{kManifestDirectoryName} / *manifest_file_name(1U),
               predecessor.bytes());
  create_entry(temporary.path(),
               std::filesystem::path{kPartsDirectoryName} / part_file_name(fixture.part_id),
               fixture.encoded.bytes());
  FailingFsyncSyscalls syscalls{{.fail_fsync_call = 2U}};
  ManifestStorage owner = detail::ManifestStorageTestAccess::open_existing(
                              {.database_root = temporary.path().string()}, syscalls)
                              .value();
  const EncodedManifest candidate = fixture.manifest(2U);
  const auto bindings = fixture.bindings();
  const common::Uuid nonce = PartFixture::make_nonce(0xc5U);

  const common::Result<InstalledManifest> installed =
      owner.install_manifest({.encoded_manifest = std::cref(candidate),
                              .schema_bindings = bindings,
                              .nonce = nonce,
                              .decode_limits = {},
                              .part_validation_limits = {},
                              .compaction_equivalence_limits = {}});
  ASSERT_FALSE(installed.has_value());
  EXPECT_EQ(installed.error().code(), common::StatusCode::kIoError);
  EXPECT_FALSE(owner.is_usable());
  EXPECT_EQ(owner.poison_status().code(), common::StatusCode::kIoError);
  EXPECT_TRUE(
      std::filesystem::exists(temporary.path() / kManifestDirectoryName / *manifest_file_name(2U)));
  EXPECT_EQ(owner.manifest_metrics().file_syncs, 1U);
  EXPECT_EQ(owner.manifest_metrics().directory_syncs, 0U);
  EXPECT_EQ(owner.manifest_metrics().installed_generations, 0U);
  EXPECT_EQ(owner
                .install_manifest({.encoded_manifest = std::cref(candidate),
                                   .schema_bindings = bindings,
                                   .nonce = PartFixture::make_nonce(0xc6U),
                                   .decode_limits = {},
                                   .part_validation_limits = {},
                                   .compaction_equivalence_limits = {}})
                .error()
                .code(),
            common::StatusCode::kUnavailable);
}

TEST(ManifestStorageTest, ScansExactConsecutiveNamespaceAndRetainsFinalParts) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  establish_layout(temporary.path());
  const cseg::PartId part_id = id<cseg::PartId>(1U);
  const common::Uuid nonce = PartFixture::make_nonce(0xb0U);
  create_entry(temporary.path(),
               std::filesystem::path{kManifestDirectoryName} / *manifest_file_name(1U));
  create_entry(temporary.path(),
               std::filesystem::path{kManifestDirectoryName} / *manifest_file_name(2U));
  create_entry(temporary.path(), std::filesystem::path{kManifestDirectoryName} /
                                     *temporary_manifest_file_name(3U, nonce));
  create_entry(temporary.path(),
               std::filesystem::path{kPartsDirectoryName} / part_file_name(part_id));
  create_entry(temporary.path(), std::filesystem::path{kPartsDirectoryName} /
                                     temporary_part_file_name(part_id, nonce));
  ManifestStorage owner =
      ManifestStorage::open_existing({.database_root = temporary.path().string()}).value();

  const common::Result<ManifestNamespaceSnapshot> snapshot = owner.scan_namespace();
  ASSERT_TRUE(snapshot.has_value()) << snapshot.error().to_string();
  EXPECT_EQ(snapshot->generations, (std::vector<std::uint64_t>{1U, 2U}));
  EXPECT_EQ(snapshot->final_parts, (std::vector<cseg::PartId>{part_id}));
  EXPECT_EQ(snapshot->temporary_parts,
            (std::vector<std::string>{temporary_part_file_name(part_id, nonce)}));
  EXPECT_EQ(snapshot->temporary_manifests,
            (std::vector<std::string>{*temporary_manifest_file_name(3U, nonce)}));
}

TEST(ManifestStorageTest, RejectsGapsMalformedNamesAndNonregularEntries) {
  TemporaryDirectory gap;
  ASSERT_TRUE(gap.valid());
  establish_layout(gap.path());
  create_entry(gap.path(), std::filesystem::path{kManifestDirectoryName} / *manifest_file_name(1U));
  create_entry(gap.path(), std::filesystem::path{kManifestDirectoryName} / *manifest_file_name(3U));
  ManifestStorage gap_owner =
      ManifestStorage::open_existing({.database_root = gap.path().string()}).value();
  EXPECT_EQ(gap_owner.scan_namespace().error().code(), common::StatusCode::kCorruption);

  TemporaryDirectory malformed;
  ASSERT_TRUE(malformed.valid());
  establish_layout(malformed.path());
  create_entry(malformed.path(),
               std::filesystem::path{kManifestDirectoryName} / *manifest_file_name(1U));
  create_entry(malformed.path(), std::filesystem::path{kPartsDirectoryName} / "unrelated");
  ManifestStorage malformed_owner =
      ManifestStorage::open_existing({.database_root = malformed.path().string()}).value();
  EXPECT_EQ(malformed_owner.scan_namespace().error().code(), common::StatusCode::kCorruption);

  TemporaryDirectory symlinked;
  ASSERT_TRUE(symlinked.valid());
  establish_layout(symlinked.path());
  create_entry(symlinked.path(),
               std::filesystem::path{kManifestDirectoryName} / *manifest_file_name(1U));
  std::error_code symlink_error;
  std::filesystem::create_symlink(
      symlinked.path() / kManifestDirectoryName / std::string{kManifestLockFileName},
      symlinked.path() / kPartsDirectoryName / part_file_name(id<cseg::PartId>(2U)), symlink_error);
  ASSERT_FALSE(symlink_error);
  ManifestStorage symlink_owner =
      ManifestStorage::open_existing({.database_root = symlinked.path().string()}).value();
  EXPECT_EQ(symlink_owner.scan_namespace().error().code(), common::StatusCode::kCorruption);
}

TEST(ManifestStorageTest, CleansOnlyRecognizedTemporariesAndSynchronizesChangedDirectories) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  establish_layout(temporary.path());
  const cseg::PartId part_id = id<cseg::PartId>(1U);
  const common::Uuid nonce = PartFixture::make_nonce(0xb1U);
  const std::string final_part = part_file_name(part_id);
  const std::string temporary_part = temporary_part_file_name(part_id, nonce);
  const std::string temporary_manifest = *temporary_manifest_file_name(2U, nonce);
  create_entry(temporary.path(),
               std::filesystem::path{kManifestDirectoryName} / *manifest_file_name(1U));
  create_entry(temporary.path(),
               std::filesystem::path{kManifestDirectoryName} / temporary_manifest);
  create_entry(temporary.path(), std::filesystem::path{kPartsDirectoryName} / final_part);
  create_entry(temporary.path(), std::filesystem::path{kPartsDirectoryName} / temporary_part);
  ManifestStorage owner =
      ManifestStorage::open_existing({.database_root = temporary.path().string()}).value();

  const common::Result<TemporaryCleanupReport> report = owner.cleanup_temporaries();
  ASSERT_TRUE(report.has_value()) << report.error().to_string();
  EXPECT_EQ(*report, (TemporaryCleanupReport{
                         .removed_parts = 1U, .removed_manifests = 1U, .directory_syncs = 2U}));
  EXPECT_TRUE(std::filesystem::exists(temporary.path() / kPartsDirectoryName / final_part));
  EXPECT_FALSE(std::filesystem::exists(temporary.path() / kPartsDirectoryName / temporary_part));
  EXPECT_TRUE(
      std::filesystem::exists(temporary.path() / kManifestDirectoryName / *manifest_file_name(1U)));
  EXPECT_FALSE(
      std::filesystem::exists(temporary.path() / kManifestDirectoryName / temporary_manifest));
  const common::Result<TemporaryCleanupReport> repeated = owner.cleanup_temporaries();
  ASSERT_TRUE(repeated.has_value());
  EXPECT_EQ(*repeated, TemporaryCleanupReport{});
}

TEST(ManifestStorageTest, CleanupDirectorySyncFailurePoisonsOwnerWithoutPromotingCandidates) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  establish_layout(temporary.path());
  const cseg::PartId part_id = id<cseg::PartId>(1U);
  const common::Uuid nonce = PartFixture::make_nonce(0xb2U);
  const std::string temporary_part = temporary_part_file_name(part_id, nonce);
  const std::string temporary_manifest = *temporary_manifest_file_name(2U, nonce);
  create_entry(temporary.path(),
               std::filesystem::path{kManifestDirectoryName} / *manifest_file_name(1U));
  create_entry(temporary.path(),
               std::filesystem::path{kManifestDirectoryName} / temporary_manifest);
  create_entry(temporary.path(), std::filesystem::path{kPartsDirectoryName} / temporary_part);
  FailingFsyncSyscalls syscalls{{.fail_fsync_call = 1U}};
  ManifestStorage owner = detail::ManifestStorageTestAccess::open_existing(
                              {.database_root = temporary.path().string()}, syscalls)
                              .value();

  const common::Result<TemporaryCleanupReport> cleanup = owner.cleanup_temporaries();
  ASSERT_FALSE(cleanup.has_value());
  EXPECT_EQ(cleanup.error().code(), common::StatusCode::kIoError);
  EXPECT_FALSE(owner.is_usable());
  EXPECT_EQ(owner.poison_status().code(), common::StatusCode::kIoError);
  EXPECT_FALSE(std::filesystem::exists(temporary.path() / kPartsDirectoryName / temporary_part));
  EXPECT_TRUE(
      std::filesystem::exists(temporary.path() / kManifestDirectoryName / temporary_manifest));
  EXPECT_EQ(owner.scan_namespace().error().code(), common::StatusCode::kUnavailable);
}

TEST(ManifestStorageTest, ReclamationRejectsCurrentReferencesAndUnlinkFailureBeforeMutation) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  establish_layout(temporary.path());
  const PartFixture fixture;
  create_entry(temporary.path(),
               std::filesystem::path{kManifestDirectoryName} / *manifest_file_name(1U),
               fixture.manifest(1U).bytes());
  create_entry(temporary.path(),
               std::filesystem::path{kManifestDirectoryName} / *manifest_file_name(2U),
               fixture.manifest(2U).bytes());
  create_entry(temporary.path(),
               std::filesystem::path{kPartsDirectoryName} / part_file_name(fixture.part_id),
               fixture.encoded.bytes());
  const cseg::PartId orphan = id<cseg::PartId>(0x10U);
  const std::array orphan_bytes{std::byte{0x7fU}};
  create_entry(temporary.path(),
               std::filesystem::path{kPartsDirectoryName} / part_file_name(orphan), orphan_bytes);
  FailingFsyncSyscalls syscalls{{.fail_unlink_call = 1U}};
  ManifestStorage owner = detail::ManifestStorageTestAccess::open_existing(
                              {.database_root = temporary.path().string()}, syscalls)
                              .value();
  const auto bindings = fixture.bindings();
  LoadedManifestGeneration selected =
      owner
          .load_selected_manifest({.expected_database_id = fixture.database_id,
                                   .expected_wal_id = fixture.wal_id,
                                   .schema_bindings = bindings,
                                   .decode_limits = {},
                                   .part_validation_limits = {}})
          .value();

  RetiredPartSet still_referenced = detail::ManifestStorageTestAccess::make_unpinned_retirement(
      1U, {{.part_id = fixture.part_id, .file_length = fixture.descriptor.file_length}});
  EXPECT_EQ(owner
                .reclaim_retired_parts({.selected_manifest = std::cref(selected),
                                        .retirement = std::cref(still_referenced),
                                        .decode_limits = {}})
                .error()
                .code(),
            common::StatusCode::kCorruption);
  EXPECT_TRUE(owner.is_usable());

  RetiredPartSet retirement = detail::ManifestStorageTestAccess::make_unpinned_retirement(
      1U, {{.part_id = orphan, .file_length = orphan_bytes.size()}});
  EXPECT_EQ(owner
                .reclaim_retired_parts({.selected_manifest = std::cref(selected),
                                        .retirement = std::cref(retirement),
                                        .decode_limits = {}})
                .error()
                .code(),
            common::StatusCode::kIoError);
  EXPECT_TRUE(owner.is_usable());
  EXPECT_TRUE(
      std::filesystem::exists(temporary.path() / kPartsDirectoryName / part_file_name(orphan)));
  EXPECT_EQ(owner.reclamation_metrics().attempts, 2U);
  EXPECT_EQ(owner.reclamation_metrics().failures, 2U);
}

TEST(ManifestStorageTest, PartialUnlinkAndDirectorySyncFailurePoisonTheOwner) {
  for (const bool fail_during_sync : {false, true}) {
    TemporaryDirectory temporary;
    ASSERT_TRUE(temporary.valid());
    establish_layout(temporary.path());
    const PartFixture fixture;
    create_entry(temporary.path(),
                 std::filesystem::path{kManifestDirectoryName} / *manifest_file_name(1U),
                 fixture.manifest(1U).bytes());
    create_entry(temporary.path(),
                 std::filesystem::path{kManifestDirectoryName} / *manifest_file_name(2U),
                 fixture.manifest(2U).bytes());
    create_entry(temporary.path(),
                 std::filesystem::path{kPartsDirectoryName} / part_file_name(fixture.part_id),
                 fixture.encoded.bytes());
    const cseg::PartId first = id<cseg::PartId>(0x10U);
    const cseg::PartId second = id<cseg::PartId>(0x11U);
    const std::array orphan_bytes{std::byte{0x7fU}};
    create_entry(temporary.path(),
                 std::filesystem::path{kPartsDirectoryName} / part_file_name(first), orphan_bytes);
    create_entry(temporary.path(),
                 std::filesystem::path{kPartsDirectoryName} / part_file_name(second), orphan_bytes);
    FailingFsyncSyscalls syscalls{fail_during_sync ? InjectedSyscallFaults{.fail_fsync_call = 1U}
                                                   : InjectedSyscallFaults{.fail_unlink_call = 2U}};
    ManifestStorage owner = detail::ManifestStorageTestAccess::open_existing(
                                {.database_root = temporary.path().string()}, syscalls)
                                .value();
    const auto bindings = fixture.bindings();
    LoadedManifestGeneration selected =
        owner
            .load_selected_manifest({.expected_database_id = fixture.database_id,
                                     .expected_wal_id = fixture.wal_id,
                                     .schema_bindings = bindings,
                                     .decode_limits = {},
                                     .part_validation_limits = {}})
            .value();
    RetiredPartSet retirement = detail::ManifestStorageTestAccess::make_unpinned_retirement(
        1U, {{.part_id = first, .file_length = orphan_bytes.size()},
             {.part_id = second, .file_length = orphan_bytes.size()}});

    const auto reclaimed = owner.reclaim_retired_parts({.selected_manifest = std::cref(selected),
                                                        .retirement = std::cref(retirement),
                                                        .decode_limits = {}});
    ASSERT_FALSE(reclaimed.has_value());
    EXPECT_EQ(reclaimed.error().code(), common::StatusCode::kIoError);
    EXPECT_FALSE(owner.is_usable());
    EXPECT_EQ(owner.poison_status().code(), common::StatusCode::kIoError);
    EXPECT_FALSE(
        std::filesystem::exists(temporary.path() / kPartsDirectoryName / part_file_name(first)));
    EXPECT_EQ(
        std::filesystem::exists(temporary.path() / kPartsDirectoryName / part_file_name(second)),
        fail_during_sync ? false : true);
    EXPECT_EQ(owner.scan_namespace().error().code(), common::StatusCode::kUnavailable);
  }
}

} // namespace
} // namespace chronos::manifest
