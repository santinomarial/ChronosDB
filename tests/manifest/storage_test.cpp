#include "chronos/common/status.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/io/posix_io.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/table_schema.hpp"
#include "cseg/cseg_test_fixture.hpp"
#include "manifest/storage_internal.hpp"

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <filesystem>
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
  std::size_t fail_call_;
  std::size_t fsync_calls_{};
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

struct PartFixture {
  cseg::EncodedCsegPart encoded{cseg::test::make_valid_part(cseg::PageCompression::kZstd)};
  schema::TableId table_id{id<schema::TableId>(2U)};
  schema::TabletId tablet_id{id<schema::TabletId>(3U)};
  schema::SchemaId schema_id{id<schema::SchemaId>(4U)};
  cseg::PartId part_id{id<cseg::PartId>(1U)};
  schema::TableSchema schema_value{make_schema()};
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
  FailingFsyncSyscalls syscalls{2U};
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

} // namespace
} // namespace chronos::manifest
