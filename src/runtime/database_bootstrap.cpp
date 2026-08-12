#include "chronos/runtime/database_bootstrap.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/io/posix_io.hpp"

#include <algorithm>
#include <array>
#include <filesystem>
#include <limits>
#include <memory>
#include <new>
#include <ranges>
#include <string>
#include <utility>
#include <vector>

namespace chronos::runtime {
namespace {

constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'D'}, std::byte{'B'},
                                           std::byte{'R'}, std::byte{'O'}, std::byte{'O'},
                                           std::byte{'T'}, std::byte{'1'}};
constexpr std::uint16_t kMajor = 1U;
constexpr std::uint16_t kMinor = 0U;
constexpr std::size_t kChecksumOffset = kDatabaseBootstrapV1Size - sizeof(std::uint32_t);

[[nodiscard]] common::Status invalid(std::string message) {
  return {common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status corruption(std::string message) {
  return {common::StatusCode::kCorruption, std::move(message)};
}

[[nodiscard]] common::Status unavailable(std::string message) {
  return {common::StatusCode::kUnavailable, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return {common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] common::Status with_context(std::string context, const common::Status& status) {
  context.append(": ");
  context.append(status.message());
  return {status.code(), std::move(context)};
}

[[nodiscard]] common::Status validate_descriptor(const DatabaseBootstrapDescriptor& value) {
  if (value.database_id.is_nil() || value.metadata_group_id.is_nil() ||
      value.database_id == value.metadata_group_id) {
    return invalid("database and metadata-group identities must be distinct and nonzero");
  }
  if (value.local_node_id == 0U || value.mutable_head_rows == 0U ||
      value.maximum_sealed_generations == 0U || value.variable_column_bytes == 0U ||
      value.maximum_retry_entries == 0U || value.wal_segment_target_bytes == 0U ||
      value.raft_segment_target_bytes == 0U) {
    return invalid("database bootstrap operational limits must be nonzero");
  }
  if (value.variable_column_bytes > std::numeric_limits<std::size_t>::max() ||
      value.maximum_retry_entries > std::numeric_limits<std::size_t>::max()) {
    return invalid("database bootstrap limits exceed the process addressable range");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status write_uuid(common::ByteWriter& writer, const common::Uuid& value) {
  return writer.write_exact(value.bytes());
}

[[nodiscard]] common::Result<common::Uuid> read_uuid(common::ByteReader& reader) {
  auto bytes = reader.read_exact(common::Uuid::kSize);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  common::Uuid::Bytes owned{};
  std::ranges::copy(*bytes, owned.begin());
  return common::Uuid{owned};
}

[[nodiscard]] common::Result<DatabaseBootstrapDescriptor>
read_descriptor_file(const io::PosixDirectory& root, const std::string_view name) {
  auto file = root.open_regular_file(name, io::FileOpenMode::kReadOnly);
  if (!file.has_value())
    return common::make_unexpected(
        with_context("open database bootstrap descriptor", file.error()));
  auto size = file->size();
  if (!size.has_value())
    return common::make_unexpected(with_context("read database bootstrap size", size.error()));
  if (*size != kDatabaseBootstrapV1Size)
    return common::make_unexpected(corruption("database bootstrap file has an invalid size"));
  std::array<std::byte, kDatabaseBootstrapV1Size> bytes{};
  auto read = file->read_at(0U, bytes);
  if (!read.has_value())
    return common::make_unexpected(with_context("read database bootstrap", read.error()));
  if (*read != bytes.size())
    return common::make_unexpected(corruption("database bootstrap ended before its exact size"));
  return decode_database_bootstrap_v1(bytes);
}

[[nodiscard]] const io::DirectoryEntry* find_entry(const std::vector<io::DirectoryEntry>& entries,
                                                   const std::string_view name) {
  const auto found = std::ranges::find(entries, name, &io::DirectoryEntry::name);
  return found == entries.end() ? nullptr : &*found;
}

[[nodiscard]] common::Status
validate_directory_entry(const std::vector<io::DirectoryEntry>& entries,
                         const std::string_view name) {
  const io::DirectoryEntry* const entry = find_entry(entries, name);
  if (entry == nullptr || entry->type != io::DirectoryEntryType::kDirectory)
    return corruption(std::string{name} + " is absent or is not a directory");
  return common::Status::ok();
}

[[nodiscard]] common::Status validate_regular_entry(const std::vector<io::DirectoryEntry>& entries,
                                                    const std::string_view name) {
  const io::DirectoryEntry* const entry = find_entry(entries, name);
  if (entry == nullptr || entry->type != io::DirectoryEntryType::kRegularFile)
    return corruption(std::string{name} + " is absent or is not a regular file");
  return common::Status::ok();
}

[[nodiscard]] common::Status
reject_unexpected_creation_entries(const std::vector<io::DirectoryEntry>& entries,
                                   const bool temporary_present) {
  for (const io::DirectoryEntry& entry : entries) {
    const bool expected =
        entry.name == kDatabaseRootLockFileName ||
        (temporary_present && entry.name == kDatabaseBootstrapTemporaryFileName) ||
        entry.name == kDatabaseWalDirectoryName || entry.name == kDatabaseRaftDirectoryName;
    if (!expected)
      return unavailable("database root contains state outside an incomplete bootstrap");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status ensure_directory(io::PosixDirectory& root,
                                              const std::vector<io::DirectoryEntry>& entries,
                                              const std::string_view name,
                                              const std::uint16_t permissions) {
  const io::DirectoryEntry* const entry = find_entry(entries, name);
  if (entry != nullptr) {
    return entry->type == io::DirectoryEntryType::kDirectory
               ? common::Status::ok()
               : corruption(std::string{name} + " exists but is not a directory");
  }
  return root.create_exclusive_directory(name, permissions);
}

} // namespace

common::Result<std::vector<std::byte>>
encode_database_bootstrap_v1(const DatabaseBootstrapDescriptor& descriptor) {
  const common::Status validation = validate_descriptor(descriptor);
  if (!validation.is_ok())
    return common::make_unexpected(validation);
  try {
    std::vector<std::byte> bytes(kDatabaseBootstrapV1Size, std::byte{0U});
    common::ByteWriter writer{bytes};
    for (const common::Status& status :
         {writer.write_exact(kMagic), writer.write_u16_le(kMajor), writer.write_u16_le(kMinor),
          writer.write_u32_le(kDatabaseBootstrapV1Size), write_uuid(writer, descriptor.database_id),
          write_uuid(writer, descriptor.metadata_group_id),
          writer.write_u64_le(descriptor.local_node_id),
          writer.write_u32_le(descriptor.mutable_head_rows),
          writer.write_u32_le(descriptor.maximum_sealed_generations),
          writer.write_u64_le(descriptor.variable_column_bytes),
          writer.write_u64_le(descriptor.maximum_retry_entries),
          writer.write_u64_le(descriptor.wal_segment_target_bytes),
          writer.write_u64_le(descriptor.raft_segment_target_bytes), writer.zero_fill(28U),
          writer.write_u32_le(0U)}) {
      if (!status.is_ok())
        return common::make_unexpected(status);
    }
    if (!writer.full())
      return common::make_unexpected(corruption("database bootstrap encoded size is inconsistent"));
    const std::uint32_t checksum = common::crc32c(common::ByteView{bytes}.first(kChecksumOffset));
    bytes[kChecksumOffset] = static_cast<std::byte>(checksum & 0xffU);
    bytes[kChecksumOffset + 1U] = static_cast<std::byte>((checksum >> 8U) & 0xffU);
    bytes[kChecksumOffset + 2U] = static_cast<std::byte>((checksum >> 16U) & 0xffU);
    bytes[kChecksumOffset + 3U] = static_cast<std::byte>((checksum >> 24U) & 0xffU);
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("database bootstrap encoding allocation failed"));
  }
}

common::Result<DatabaseBootstrapDescriptor>
decode_database_bootstrap_v1(const common::ByteView bytes) {
  if (bytes.size() != kDatabaseBootstrapV1Size)
    return common::make_unexpected(corruption("database bootstrap image has an invalid size"));
  const auto stored_checksum =
      std::to_integer<std::uint32_t>(bytes[kChecksumOffset]) |
      (std::to_integer<std::uint32_t>(bytes[kChecksumOffset + 1U]) << 8U) |
      (std::to_integer<std::uint32_t>(bytes[kChecksumOffset + 2U]) << 16U) |
      (std::to_integer<std::uint32_t>(bytes[kChecksumOffset + 3U]) << 24U);
  if (common::crc32c(bytes.first(kChecksumOffset)) != stored_checksum)
    return common::make_unexpected(corruption("database bootstrap checksum mismatch"));
  common::ByteReader reader{bytes};
  auto magic = reader.read_exact(kMagic.size());
  auto major = reader.read_u16_le();
  auto minor = reader.read_u16_le();
  auto size = reader.read_u32_le();
  auto database_id = read_uuid(reader);
  auto metadata_group_id = read_uuid(reader);
  auto node_id = reader.read_u64_le();
  auto head_rows = reader.read_u32_le();
  auto sealed = reader.read_u32_le();
  auto variable_bytes = reader.read_u64_le();
  auto retry_entries = reader.read_u64_le();
  auto wal_target = reader.read_u64_le();
  auto raft_target = reader.read_u64_le();
  auto reserved = reader.read_exact(28U);
  auto checksum = reader.read_u32_le();
  if (!magic || !major || !minor || !size || !database_id || !metadata_group_id || !node_id ||
      !head_rows || !sealed || !variable_bytes || !retry_entries || !wal_target || !raft_target ||
      !reserved || !checksum || !reader.empty()) {
    return common::make_unexpected(corruption("database bootstrap framing is incomplete"));
  }
  if (!std::ranges::equal(*magic, kMagic))
    return common::make_unexpected(corruption("database bootstrap magic mismatch"));
  if (*major != kMajor || *minor != kMinor || *size != kDatabaseBootstrapV1Size)
    return common::make_unexpected(common::Status{
        common::StatusCode::kNotSupported, "database bootstrap format version is unsupported"});
  if (std::ranges::any_of(*reserved,
                          [](const std::byte value) { return value != std::byte{0U}; })) {
    return common::make_unexpected(corruption("database bootstrap reserved bytes are nonzero"));
  }
  DatabaseBootstrapDescriptor descriptor{.database_id = *database_id,
                                         .metadata_group_id = *metadata_group_id,
                                         .local_node_id = *node_id,
                                         .mutable_head_rows = *head_rows,
                                         .maximum_sealed_generations = *sealed,
                                         .variable_column_bytes = *variable_bytes,
                                         .maximum_retry_entries = *retry_entries,
                                         .wal_segment_target_bytes = *wal_target,
                                         .raft_segment_target_bytes = *raft_target};
  const common::Status validation = validate_descriptor(descriptor);
  if (!validation.is_ok())
    return common::make_unexpected(corruption(validation.message()));
  return descriptor;
}

class DatabaseBootstrap::Impl {
public:
  Impl(std::string configured_root, DatabaseBootstrapDescriptor configured_descriptor,
       io::PosixDirectory configured_directory, io::PosixAdvisoryLock configured_lock) noexcept
      : root(std::move(configured_root)), descriptor(std::move(configured_descriptor)),
        directory(std::move(configured_directory)), lock(std::move(configured_lock)) {}

  std::string root;
  DatabaseBootstrapDescriptor descriptor;
  io::PosixDirectory directory;
  io::PosixAdvisoryLock lock;
};

DatabaseBootstrap::DatabaseBootstrap(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
DatabaseBootstrap::~DatabaseBootstrap() = default;
DatabaseBootstrap::DatabaseBootstrap(DatabaseBootstrap&&) noexcept = default;
DatabaseBootstrap& DatabaseBootstrap::operator=(DatabaseBootstrap&&) noexcept = default;

common::Result<DatabaseBootstrap>
DatabaseBootstrap::open_or_create(const DatabaseBootstrapConfig& config) {
  if (config.database_root.empty() || config.file_permissions == 0U ||
      config.directory_permissions == 0U) {
    return common::make_unexpected(invalid("database bootstrap configuration is invalid"));
  }
  auto root = io::PosixDirectory::open(config.database_root);
  if (!root.has_value())
    return common::make_unexpected(with_context("open database root", root.error()));
  auto lock = root->acquire_exclusive_lock(kDatabaseRootLockFileName, config.file_permissions);
  if (!lock.has_value())
    return common::make_unexpected(with_context("acquire database root lock", lock.error()));
  auto entries = root->list_entries();
  if (!entries.has_value())
    return common::make_unexpected(with_context("list database root", entries.error()));
  const io::DirectoryEntry* const final = find_entry(*entries, kDatabaseBootstrapFileName);
  const io::DirectoryEntry* const temporary =
      find_entry(*entries, kDatabaseBootstrapTemporaryFileName);
  if (final != nullptr && temporary != nullptr)
    return common::make_unexpected(
        corruption("database has both final and temporary bootstrap files"));

  DatabaseBootstrapDescriptor descriptor;
  if (final != nullptr) {
    if (auto status = validate_regular_entry(*entries, kDatabaseBootstrapFileName); !status.is_ok())
      return common::make_unexpected(status);
    if (auto status = validate_directory_entry(*entries, kDatabaseWalDirectoryName);
        !status.is_ok())
      return common::make_unexpected(status);
    if (auto status = validate_directory_entry(*entries, kDatabaseRaftDirectoryName);
        !status.is_ok())
      return common::make_unexpected(status);
    auto decoded = read_descriptor_file(*root, kDatabaseBootstrapFileName);
    if (!decoded.has_value())
      return common::make_unexpected(decoded.error());
    descriptor = std::move(*decoded);
  } else {
    const bool resuming = temporary != nullptr;
    if (auto status = reject_unexpected_creation_entries(*entries, resuming); !status.is_ok())
      return common::make_unexpected(status);
    if (resuming) {
      if (auto status = validate_regular_entry(*entries, kDatabaseBootstrapTemporaryFileName);
          !status.is_ok())
        return common::make_unexpected(status);
      auto decoded = read_descriptor_file(*root, kDatabaseBootstrapTemporaryFileName);
      if (!decoded.has_value())
        return common::make_unexpected(decoded.error());
      descriptor = std::move(*decoded);
    } else {
      auto encoded = encode_database_bootstrap_v1(config.new_database);
      if (!encoded.has_value())
        return common::make_unexpected(encoded.error());
      auto intent = root->create_exclusive_regular_file(kDatabaseBootstrapTemporaryFileName,
                                                        config.file_permissions);
      if (!intent.has_value())
        return common::make_unexpected(
            with_context("create database bootstrap intent", intent.error()));
      auto status = intent->write_all_at(0U, *encoded);
      if (!status.is_ok())
        return common::make_unexpected(with_context("write database bootstrap intent", status));
      status = intent->sync_all();
      if (!status.is_ok())
        return common::make_unexpected(
            with_context("synchronize database bootstrap intent", status));
      status = root->sync();
      if (!status.is_ok())
        return common::make_unexpected(
            with_context("synchronize database bootstrap intent name", status));
      descriptor = config.new_database;
      entries = root->list_entries();
      if (!entries.has_value())
        return common::make_unexpected(with_context("relist database root", entries.error()));
    }

    auto status =
        ensure_directory(*root, *entries, kDatabaseWalDirectoryName, config.directory_permissions);
    if (!status.is_ok())
      return common::make_unexpected(with_context("create database WAL directory", status));
    status =
        ensure_directory(*root, *entries, kDatabaseRaftDirectoryName, config.directory_permissions);
    if (!status.is_ok())
      return common::make_unexpected(with_context("create database Raft directory", status));
    status = root->sync();
    if (!status.is_ok())
      return common::make_unexpected(
          with_context("synchronize database subsystem directories", status));
    status = root->rename_no_replace(
        {.old_name = kDatabaseBootstrapTemporaryFileName, .new_name = kDatabaseBootstrapFileName});
    if (!status.is_ok())
      return common::make_unexpected(with_context("install database bootstrap", status));
    status = root->sync();
    if (!status.is_ok())
      return common::make_unexpected(
          with_context("synchronize installed database bootstrap", status));
  }

  try {
    return DatabaseBootstrap{std::make_unique<Impl>(config.database_root, std::move(descriptor),
                                                    std::move(*root), std::move(*lock))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("database bootstrap owner allocation failed"));
  }
}

const DatabaseBootstrapDescriptor& DatabaseBootstrap::descriptor() const noexcept {
  return impl_->descriptor;
}

const std::string& DatabaseBootstrap::database_root() const noexcept {
  return impl_->root;
}

std::string DatabaseBootstrap::wal_directory_path() const {
  return (std::filesystem::path{impl_->root} / kDatabaseWalDirectoryName).string();
}

std::string DatabaseBootstrap::raft_directory_path() const {
  return (std::filesystem::path{impl_->root} / kDatabaseRaftDirectoryName).string();
}

common::Status DatabaseBootstrap::close() {
  if (impl_ == nullptr)
    return common::Status::ok();
  common::Status result = impl_->lock.close();
  const common::Status directory = impl_->directory.close();
  if (result.is_ok())
    result = directory;
  impl_.reset();
  return result;
}

} // namespace chronos::runtime
