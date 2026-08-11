#include "chronos/cluster/tablet_physical_part_chunk_storage.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/io/posix_io.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <ranges>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

constexpr std::string_view kLockFileName = "LOCK";
constexpr std::string_view kChunkPrefix = "part-chunk-";
constexpr std::string_view kChunkSuffix = ".pchk";
constexpr std::string_view kTemporarySuffix = ".tmp";
constexpr std::string_view kReclaimedFileName = "RECLAIMED";
constexpr std::string_view kReclaimedTemporaryFileName = "RECLAIMED.tmp";
constexpr std::size_t kOffsetDigits = 20U;
constexpr std::size_t kReclaimedMarkerSize = 160U;
constexpr std::size_t kReclaimedMarkerCrcOffset = 152U;
constexpr std::array<std::byte, 8U> kReclaimedMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'R'},
                                                    std::byte{'P'}, std::byte{'R'}, std::byte{'C'},
                                                    std::byte{'L'}, std::byte{0U}};
constexpr std::uint16_t kReclaimedMajor = 1U;
constexpr std::uint16_t kReclaimedMinor = 0U;

[[nodiscard]] common::Status invalid(std::string message) {
  return {common::StatusCode::kInvalidArgument, std::move(message)};
}
[[nodiscard]] common::Status corruption(std::string message) {
  return {common::StatusCode::kCorruption, std::move(message)};
}
[[nodiscard]] common::Status exhausted(std::string message) {
  return {common::StatusCode::kResourceExhausted, std::move(message)};
}
[[nodiscard]] common::Status unavailable(std::string message) {
  return {common::StatusCode::kUnavailable, std::move(message)};
}
[[nodiscard]] common::Status unsupported(std::string message) {
  return {common::StatusCode::kNotSupported, std::move(message)};
}
[[nodiscard]] common::Status with_context(const std::string_view context,
                                          const common::Status& status) {
  std::string message{context};
  message.append(": ");
  message.append(status.message());
  return {status.code(), std::move(message)};
}

void store_u32(std::span<std::byte> bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
}

[[nodiscard]] std::uint32_t load_u32(const common::ByteView bytes, const std::size_t offset) {
  std::uint32_t value{};
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(bytes[offset + index]))
             << (index * 8U);
  }
  return value;
}

[[nodiscard]] bool valid_limits(const TabletPhysicalPartChunkCodecLimits& limits) noexcept {
  return limits.maximum_object_bytes > 0U &&
         limits.maximum_object_bytes <= cseg::format::kMaximumFileLength &&
         limits.maximum_chunk_bytes > 0U &&
         limits.maximum_chunk_bytes <= limits.maximum_object_bytes &&
         limits.maximum_encoded_bytes >=
             kTabletPhysicalPartChunkHeaderSize + 1U + kTabletPhysicalPartChunkTrailerSize &&
         limits.maximum_encoded_bytes <= kMaximumTabletPhysicalPartChunkSize &&
         limits.maximum_chunk_bytes <= limits.maximum_encoded_bytes -
                                           kTabletPhysicalPartChunkHeaderSize -
                                           kTabletPhysicalPartChunkTrailerSize;
}

[[nodiscard]] bool valid_session(const TabletPhysicalPartTransferSession& session,
                                 const TabletPhysicalPartChunkCodecLimits& limits) noexcept {
  return !session.table_id.uuid().is_nil() && !session.tablet_id.uuid().is_nil() &&
         !session.group_id.is_nil() && session.placement_epoch != 0U && session.source_node != 0U &&
         session.target_node != 0U && session.source_node != session.target_node &&
         session.manifest_generation != 0U && !session.part_id.uuid().is_nil() &&
         session.total_bytes != 0U && session.total_bytes <= limits.maximum_object_bytes;
}

[[nodiscard]] std::uint32_t reclaimed_marker_crc(const common::ByteView bytes) {
  std::array<std::byte, kReclaimedMarkerSize> copy{};
  std::ranges::copy(bytes, copy.begin());
  std::fill_n(copy.begin() + static_cast<std::ptrdiff_t>(kReclaimedMarkerCrcOffset),
              sizeof(std::uint32_t), std::byte{0U});
  return common::crc32c(copy);
}

template <typename Identity>
[[nodiscard]] common::Result<Identity> read_identity(common::ByteReader& reader) {
  auto bytes = reader.read_exact(common::Uuid::kSize);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  common::Uuid::Bytes identity{};
  std::ranges::copy(*bytes, identity.begin());
  return Identity::from_bytes(identity);
}

[[nodiscard]] common::Result<common::Uuid> read_uuid(common::ByteReader& reader) {
  auto bytes = reader.read_exact(common::Uuid::kSize);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  common::Uuid::Bytes identity{};
  std::ranges::copy(*bytes, identity.begin());
  return common::Uuid{identity};
}

[[nodiscard]] common::Result<std::array<std::byte, kReclaimedMarkerSize>>
encode_reclaimed_marker(const TabletPhysicalPartTransferSession& session,
                        const TabletPhysicalPartChunkCodecLimits& limits) {
  if (!valid_session(session, limits))
    return common::make_unexpected(invalid("physical receipt reclamation session is invalid"));
  std::array<std::byte, kReclaimedMarkerSize> bytes{};
  common::ByteWriter writer{bytes};
  for (const common::Status& status :
       {writer.write_exact(kReclaimedMagic), writer.write_u16_le(kReclaimedMajor),
        writer.write_u16_le(kReclaimedMinor), writer.write_u32_le(kReclaimedMarkerSize),
        writer.write_exact(session.table_id.bytes()), writer.write_exact(session.tablet_id.bytes()),
        writer.write_exact(session.group_id.bytes()), writer.write_u64_le(session.placement_epoch),
        writer.write_u64_le(session.source_node), writer.write_u64_le(session.target_node),
        writer.write_u64_le(session.manifest_generation),
        writer.write_exact(session.part_id.bytes()), writer.write_u64_le(session.total_bytes),
        writer.write_exact(session.content_sha256.bytes()), writer.write_u32_le(0U),
        writer.zero_fill(4U)}) {
    if (!status.is_ok())
      return common::make_unexpected(status);
  }
  store_u32(bytes, kReclaimedMarkerCrcOffset, reclaimed_marker_crc(bytes));
  return bytes;
}

[[nodiscard]] common::Result<TabletPhysicalPartTransferSession>
decode_reclaimed_marker(const common::ByteView bytes,
                        const TabletPhysicalPartChunkCodecLimits& limits) {
  if (bytes.size() != kReclaimedMarkerSize ||
      !std::ranges::equal(bytes.first(kReclaimedMagic.size()), kReclaimedMagic) ||
      reclaimed_marker_crc(bytes) != load_u32(bytes, kReclaimedMarkerCrcOffset)) {
    return common::make_unexpected(corruption("physical receipt reclamation marker is damaged"));
  }
  common::ByteReader reader{bytes.subspan(kReclaimedMagic.size())};
  auto major = reader.read_u16_le();
  auto minor = reader.read_u16_le();
  auto length = reader.read_u32_le();
  auto table_id = read_identity<schema::TableId>(reader);
  auto tablet_id = read_identity<schema::TabletId>(reader);
  auto group_id = read_uuid(reader);
  auto epoch = reader.read_u64_le();
  auto source = reader.read_u64_le();
  auto target = reader.read_u64_le();
  auto manifest = reader.read_u64_le();
  auto part_id = read_identity<cseg::PartId>(reader);
  auto total_bytes = reader.read_u64_le();
  auto digest = reader.read_exact(ingest::Sha256Digest::kSize);
  auto stored_crc = reader.read_u32_le();
  auto reserved = reader.read_exact(4U);
  if (!major.has_value() || !minor.has_value() || !length.has_value())
    return common::make_unexpected(corruption("physical receipt marker prefix is truncated"));
  if (*major != kReclaimedMajor || *minor != kReclaimedMinor)
    return common::make_unexpected(unsupported("physical receipt marker version is unsupported"));
  if (*length != bytes.size() || !table_id.has_value() || !tablet_id.has_value() ||
      !group_id.has_value() || !epoch.has_value() || !source.has_value() || !target.has_value() ||
      !manifest.has_value() || !part_id.has_value() || !total_bytes.has_value() ||
      !digest.has_value() || !stored_crc.has_value() ||
      *stored_crc != load_u32(bytes, kReclaimedMarkerCrcOffset) || !reserved.has_value() ||
      std::ranges::any_of(*reserved,
                          [](const std::byte value) { return value != std::byte{0U}; })) {
    return common::make_unexpected(corruption("physical receipt reclamation marker is invalid"));
  }
  ingest::Sha256Digest::Bytes digest_bytes{};
  std::ranges::copy(*digest, digest_bytes.begin());
  TabletPhysicalPartTransferSession session{.table_id = *table_id,
                                            .tablet_id = *tablet_id,
                                            .group_id = *group_id,
                                            .placement_epoch = *epoch,
                                            .source_node = *source,
                                            .target_node = *target,
                                            .manifest_generation = *manifest,
                                            .part_id = *part_id,
                                            .total_bytes = *total_bytes,
                                            .content_sha256 = ingest::Sha256Digest{digest_bytes}};
  if (!valid_session(session, limits))
    return common::make_unexpected(corruption("physical receipt reclamation session is invalid"));
  return session;
}

[[nodiscard]] std::string temporary_name(const std::string_view final_name) {
  std::string result{final_name};
  result.append(kTemporarySuffix);
  return result;
}

[[nodiscard]] common::Result<std::uint64_t> parse_name(const std::string_view name,
                                                       const bool temporary) {
  const std::size_t expected = kChunkPrefix.size() + kOffsetDigits + kChunkSuffix.size() +
                               (temporary ? kTemporarySuffix.size() : 0U);
  if (name.size() != expected || !name.starts_with(kChunkPrefix) ||
      !name.substr(kChunkPrefix.size() + kOffsetDigits).starts_with(kChunkSuffix) ||
      (temporary && !name.ends_with(kTemporarySuffix))) {
    return common::make_unexpected(invalid("physical part chunk name is noncanonical"));
  }
  const std::string_view digits = name.substr(kChunkPrefix.size(), kOffsetDigits);
  std::uint64_t offset{};
  const auto parsed = std::from_chars(digits.data(), digits.data() + digits.size(), offset);
  if (parsed.ec != std::errc{} || parsed.ptr != digits.data() + digits.size())
    return common::make_unexpected(invalid("physical part chunk offset name is invalid"));
  auto canonical = tablet_physical_part_chunk_file_name(offset);
  if (!canonical.has_value() || !name.starts_with(*canonical))
    return common::make_unexpected(invalid("physical part chunk name is noncanonical"));
  return offset;
}

struct ChunkFile {
  std::uint64_t offset{};
  std::size_t payload_bytes{};
  std::string file_name;
};

} // namespace

class TabletPhysicalPartChunkStorage::Impl {
public:
  Impl(TabletPhysicalPartChunkStorageConfig config, io::PosixDirectory directory,
       io::PosixAdvisoryLock lock) noexcept
      : config_(std::move(config)), directory_(std::move(directory)), lock_(std::move(lock)) {}

  [[nodiscard]] common::Status fail(common::Status status, const bool poison = false) {
    if (poison && poison_.is_ok())
      poison_ = status;
    return status;
  }

  [[nodiscard]] common::Status check_usable() const {
    return poison_.is_ok()
               ? common::Status::ok()
               : unavailable("physical part chunk storage is poisoned: " + poison_.message());
  }

  [[nodiscard]] common::Status cleanup_temporaries() const {
    auto entries = directory_.list_entries();
    if (!entries.has_value())
      return entries.error();
    bool removed = false;
    for (const io::DirectoryEntry& entry : *entries) {
      if (entry.name == kReclaimedTemporaryFileName) {
        if (entry.type != io::DirectoryEntryType::kRegularFile)
          return corruption("physical receipt reclamation temporary is noncanonical");
        common::Status status = directory_.remove_file(entry.name);
        if (!status.is_ok())
          return status;
        removed = true;
        continue;
      }
      if (!entry.name.starts_with(kChunkPrefix) || !entry.name.ends_with(kTemporarySuffix))
        continue;
      if (!parse_name(entry.name, true).has_value() ||
          entry.type != io::DirectoryEntryType::kRegularFile) {
        return corruption("recognized physical part temporary is noncanonical");
      }
      common::Status status = directory_.remove_file(entry.name);
      if (!status.is_ok())
        return status;
      removed = true;
    }
    return removed ? directory_.sync() : common::Status::ok();
  }

  [[nodiscard]] common::Result<std::optional<TabletPhysicalPartTransferSession>>
  load_reclaimed_marker() const {
    auto file = directory_.open_regular_file(kReclaimedFileName, io::FileOpenMode::kReadOnly);
    if (!file.has_value()) {
      if (file.error().code() == common::StatusCode::kNotFound)
        return std::optional<TabletPhysicalPartTransferSession>{};
      return common::make_unexpected(file.error());
    }
    auto size = file->size();
    if (!size.has_value())
      return common::make_unexpected(size.error());
    if (*size != kReclaimedMarkerSize)
      return common::make_unexpected(
          corruption("physical receipt reclamation marker size changed"));
    std::array<std::byte, kReclaimedMarkerSize> bytes{};
    auto read = file->read_at(0U, bytes);
    if (!read.has_value())
      return common::make_unexpected(read.error());
    if (*read != bytes.size())
      return common::make_unexpected(
          corruption("physical receipt reclamation marker is truncated"));
    auto decoded = decode_reclaimed_marker(bytes, config_.codec_limits);
    if (!decoded.has_value())
      return common::make_unexpected(decoded.error());
    return std::optional<TabletPhysicalPartTransferSession>{std::move(*decoded)};
  }

  [[nodiscard]] common::Result<bool> install_reclaimed_marker() {
    auto existing = load_reclaimed_marker();
    if (!existing.has_value())
      return common::make_unexpected(existing.error());
    if (existing->has_value()) {
      if (**existing != config_.session)
        return common::make_unexpected(
            corruption("physical receipt marker belongs to another session"));
      reclaimed_ = true;
      return true;
    }
    auto encoded = encode_reclaimed_marker(config_.session, config_.codec_limits);
    if (!encoded.has_value())
      return common::make_unexpected(encoded.error());
    common::Status cleanup = directory_.remove_file(kReclaimedTemporaryFileName);
    if (cleanup.is_ok())
      cleanup = directory_.sync();
    else if (cleanup.code() == common::StatusCode::kNotFound)
      cleanup = common::Status::ok();
    if (!cleanup.is_ok())
      return common::make_unexpected(with_context("clean receipt marker temporary", cleanup));
    auto temporary = directory_.create_exclusive_regular_file(kReclaimedTemporaryFileName,
                                                              config_.file_permissions);
    if (!temporary.has_value())
      return common::make_unexpected(temporary.error());
    common::Status operation = temporary->write_all_at(0U, *encoded);
    if (!operation.is_ok())
      return common::make_unexpected(with_context("write receipt marker", operation));
    std::array<std::byte, kReclaimedMarkerSize> readback{};
    auto read = temporary->read_at(0U, readback);
    if (!read.has_value() || *read != readback.size()) {
      return common::make_unexpected(
          read.has_value() ? corruption("physical receipt marker readback is incomplete")
                           : read.error());
    }
    auto decoded = decode_reclaimed_marker(readback, config_.codec_limits);
    if (!decoded.has_value() || *decoded != config_.session) {
      return common::make_unexpected(decoded.has_value()
                                         ? corruption("physical receipt marker readback changed")
                                         : decoded.error());
    }
    operation = temporary->sync_all();
    if (!operation.is_ok())
      return common::make_unexpected(with_context("synchronize receipt marker", operation));
    operation = temporary->close();
    if (!operation.is_ok())
      return common::make_unexpected(with_context("close receipt marker", operation));
    operation = directory_.rename_no_replace({kReclaimedTemporaryFileName, kReclaimedFileName});
    if (!operation.is_ok())
      return common::make_unexpected(with_context("install receipt marker", operation));
    operation = directory_.sync();
    if (!operation.is_ok()) {
      return common::make_unexpected(
          fail(with_context("synchronize receipt marker directory", operation), true));
    }
    reclaimed_ = true;
    return false;
  }

  [[nodiscard]] common::Result<LoadedTabletPhysicalPartChunk>
  load_file(const std::string& file_name, const std::uint64_t expected_offset) const {
    common::Status usable = check_usable();
    if (!usable.is_ok())
      return common::make_unexpected(std::move(usable));
    auto file = directory_.open_regular_file(file_name, io::FileOpenMode::kReadOnly);
    if (!file.has_value())
      return common::make_unexpected(file.error());
    auto size = file->size();
    if (!size.has_value())
      return common::make_unexpected(size.error());
    if (*size > config_.codec_limits.maximum_encoded_bytes ||
        *size > std::numeric_limits<std::size_t>::max()) {
      return common::make_unexpected(exhausted("installed physical part chunk exceeds limit"));
    }
    try {
      std::vector<std::byte> bytes(static_cast<std::size_t>(*size));
      auto read = file->read_at(0U, bytes);
      if (!read.has_value())
        return common::make_unexpected(read.error());
      if (*read != bytes.size())
        return common::make_unexpected(corruption("physical part chunk file is truncated"));
      auto decoded = decode_tablet_physical_part_chunk_v1(bytes, config_.codec_limits);
      if (!decoded.has_value())
        return common::make_unexpected(decoded.error());
      if (decoded->session != config_.session || decoded->offset != expected_offset) {
        return common::make_unexpected(
            corruption("physical part chunk disagrees with owner or name"));
      }
      return LoadedTabletPhysicalPartChunk{file_name, std::move(*decoded), std::move(bytes)};
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("physical part chunk read allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(exhausted("physical part chunk read exceeded limits"));
    }
  }

  [[nodiscard]] common::Status recover_progress() {
    auto entries = directory_.list_entries();
    if (!entries.has_value())
      return entries.error();
    std::vector<std::pair<std::uint64_t, std::string>> candidates;
    for (const io::DirectoryEntry& entry : *entries) {
      if (entry.name == kLockFileName || !entry.name.starts_with(kChunkPrefix))
        continue;
      if (entry.name.ends_with(kTemporarySuffix)) {
        if (!parse_name(entry.name, true).has_value() ||
            entry.type != io::DirectoryEntryType::kRegularFile) {
          return corruption("physical part chunk directory is noncanonical");
        }
        continue;
      }
      auto offset = parse_name(entry.name, false);
      if (!offset.has_value() || entry.type != io::DirectoryEntryType::kRegularFile)
        return corruption("physical part chunk directory is noncanonical");
      if (candidates.size() == config_.maximum_chunks)
        return exhausted("physical part chunk count exceeds configured limit");
      candidates.emplace_back(*offset, entry.name);
    }
    std::ranges::sort(candidates);
    chunks_.clear();
    received_bytes_ = 0U;
    for (const auto& [offset, file_name] : candidates) {
      if (offset != received_bytes_)
        return corruption("physical part chunks are not contiguous from zero");
      auto loaded = load_file(file_name, offset);
      if (!loaded.has_value())
        return loaded.error();
      const std::size_t payload_bytes = loaded->chunk.bytes.size();
      chunks_.push_back({offset, payload_bytes, file_name});
      received_bytes_ += payload_bytes;
    }
    return common::Status::ok();
  }

  TabletPhysicalPartChunkStorageConfig config_;
  io::PosixDirectory directory_;
  io::PosixAdvisoryLock lock_;
  std::vector<ChunkFile> chunks_;
  std::uint64_t received_bytes_{};
  bool reclaimed_{};
  common::Status poison_;
};

common::Result<std::string> tablet_physical_part_chunk_file_name(const std::uint64_t offset) {
  std::array<char, kOffsetDigits> digits{};
  const auto converted = std::to_chars(digits.data(), digits.data() + digits.size(), offset);
  if (converted.ec != std::errc{})
    return common::make_unexpected(invalid("physical part chunk offset is not representable"));
  const std::size_t written = static_cast<std::size_t>(converted.ptr - digits.data());
  std::string result{kChunkPrefix};
  result.append(kOffsetDigits - written, '0');
  result.append(digits.data(), written);
  result.append(kChunkSuffix);
  return result;
}

common::Result<TabletPhysicalPartChunkStorage>
TabletPhysicalPartChunkStorage::open(TabletPhysicalPartChunkStorageConfig config,
                                     const bool create_lock) {
  if (config.directory_path.empty() || !valid_limits(config.codec_limits) ||
      !valid_session(config.session, config.codec_limits) || config.maximum_chunks == 0U ||
      config.maximum_chunks > kMaximumTabletPhysicalPartChunkFiles ||
      config.file_permissions == 0U || (config.file_permissions & ~0777U) != 0U) {
    return common::make_unexpected(invalid("physical part chunk storage config is invalid"));
  }
  auto directory = io::PosixDirectory::open(config.directory_path);
  if (!directory.has_value())
    return common::make_unexpected(directory.error());
  auto lock = create_lock
                  ? directory->acquire_exclusive_lock(kLockFileName, config.file_permissions)
                  : directory->acquire_existing_exclusive_lock(kLockFileName);
  if (!lock.has_value())
    return common::make_unexpected(lock.error());
  auto implementation =
      std::make_unique<Impl>(std::move(config), std::move(*directory), std::move(*lock));
  common::Status status = implementation->cleanup_temporaries();
  if (status.is_ok()) {
    auto marker = implementation->load_reclaimed_marker();
    if (!marker.has_value())
      status = marker.error();
    else if (marker->has_value()) {
      if (**marker != implementation->config_.session)
        status = corruption("physical receipt marker belongs to another session");
      else
        implementation->reclaimed_ = true;
    }
  }
  if (status.is_ok())
    status = implementation->recover_progress();
  if (!status.is_ok())
    return common::make_unexpected(std::move(status));
  return TabletPhysicalPartChunkStorage{std::move(implementation)};
}

TabletPhysicalPartChunkStorage::TabletPhysicalPartChunkStorage(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
TabletPhysicalPartChunkStorage::~TabletPhysicalPartChunkStorage() = default;
TabletPhysicalPartChunkStorage::TabletPhysicalPartChunkStorage(
    TabletPhysicalPartChunkStorage&&) noexcept = default;
TabletPhysicalPartChunkStorage&
TabletPhysicalPartChunkStorage::operator=(TabletPhysicalPartChunkStorage&&) noexcept = default;

common::Result<TabletPhysicalPartChunkStorage>
TabletPhysicalPartChunkStorage::create(TabletPhysicalPartChunkStorageConfig config) {
  try {
    return open(std::move(config), true);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("physical part storage allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("physical part storage exceeded container limits"));
  }
}

common::Result<TabletPhysicalPartChunkStorage>
TabletPhysicalPartChunkStorage::open_existing(TabletPhysicalPartChunkStorageConfig config) {
  try {
    return open(std::move(config), false);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("physical part storage allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("physical part storage exceeded container limits"));
  }
}

common::Result<InstalledTabletPhysicalPartChunk>
TabletPhysicalPartChunkStorage::install(const TabletPhysicalPartChunk& chunk) {
  if (implementation_ == nullptr)
    return common::make_unexpected(invalid("physical part storage was moved from"));
  common::Status usable = implementation_->check_usable();
  if (!usable.is_ok())
    return common::make_unexpected(std::move(usable));
  if (implementation_->reclaimed_)
    return common::make_unexpected(unavailable("physical part receipt was reclaimed"));
  if (chunk.session != implementation_->config_.session)
    return common::make_unexpected(invalid("physical part chunk belongs to another session"));
  auto encoded = encode_tablet_physical_part_chunk_v1(chunk, implementation_->config_.codec_limits);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  auto final_name = tablet_physical_part_chunk_file_name(chunk.offset);
  if (!final_name.has_value())
    return common::make_unexpected(final_name.error());
  auto existing = implementation_->load_file(*final_name, chunk.offset);
  if (existing.has_value()) {
    if (existing->encoded_bytes != *encoded)
      return common::make_unexpected(
          corruption("physical part chunk offset has different durable bytes"));
    return InstalledTabletPhysicalPartChunk{chunk.offset, chunk.bytes.size(), *final_name, true};
  }
  if (existing.error().code() != common::StatusCode::kNotFound)
    return common::make_unexpected(existing.error());
  if (chunk.offset != implementation_->received_bytes_)
    return common::make_unexpected(unavailable("physical part chunk is not the next offset"));
  if (implementation_->chunks_.size() == implementation_->config_.maximum_chunks)
    return common::make_unexpected(exhausted("physical part chunk count limit reached"));

  const std::string temp_name = temporary_name(*final_name);
  common::Status cleanup = implementation_->directory_.remove_file(temp_name);
  if (cleanup.is_ok())
    cleanup = implementation_->directory_.sync();
  else if (cleanup.code() == common::StatusCode::kNotFound)
    cleanup = common::Status::ok();
  if (!cleanup.is_ok())
    return common::make_unexpected(with_context("clean prior physical chunk temporary", cleanup));
  auto temporary = implementation_->directory_.create_exclusive_regular_file(
      temp_name, implementation_->config_.file_permissions);
  if (!temporary.has_value())
    return common::make_unexpected(temporary.error());
  common::Status operation = temporary->write_all_at(0U, *encoded);
  if (!operation.is_ok())
    return common::make_unexpected(with_context("write physical part chunk", operation));
  auto size = temporary->size();
  if (!size.has_value() || *size != encoded->size()) {
    return common::make_unexpected(
        size.has_value() ? corruption("physical chunk temporary size changed") : size.error());
  }
  try {
    std::vector<std::byte> readback(encoded->size());
    auto read = temporary->read_at(0U, readback);
    if (!read.has_value() || *read != readback.size()) {
      return common::make_unexpected(
          read.has_value() ? corruption("physical chunk readback is incomplete") : read.error());
    }
    auto decoded =
        decode_tablet_physical_part_chunk_v1(readback, implementation_->config_.codec_limits);
    if (!decoded.has_value() || *decoded != chunk) {
      return common::make_unexpected(
          decoded.has_value() ? corruption("physical chunk readback changed") : decoded.error());
    }
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("physical chunk readback allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("physical chunk readback exceeded limits"));
  }
  operation = temporary->sync_all();
  if (!operation.is_ok())
    return common::make_unexpected(with_context("synchronize physical chunk", operation));
  operation = temporary->close();
  if (!operation.is_ok())
    return common::make_unexpected(with_context("close physical chunk", operation));
  operation = implementation_->directory_.rename_no_replace({temp_name, *final_name});
  if (!operation.is_ok())
    return common::make_unexpected(with_context("install physical chunk", operation));
  operation = implementation_->directory_.sync();
  if (!operation.is_ok()) {
    return common::make_unexpected(implementation_->fail(
        with_context("synchronize physical chunk directory", operation), true));
  }
  implementation_->chunks_.push_back({chunk.offset, chunk.bytes.size(), *final_name});
  implementation_->received_bytes_ += chunk.bytes.size();
  return InstalledTabletPhysicalPartChunk{chunk.offset, chunk.bytes.size(), *final_name, false};
}

common::Result<LoadedTabletPhysicalPartChunk>
TabletPhysicalPartChunkStorage::load_chunk(const std::uint64_t offset) const {
  if (implementation_ == nullptr)
    return common::make_unexpected(invalid("physical part storage was moved from"));
  if (implementation_->reclaimed_)
    return common::make_unexpected(unavailable("physical part receipt was reclaimed"));
  auto name = tablet_physical_part_chunk_file_name(offset);
  if (!name.has_value())
    return common::make_unexpected(name.error());
  return implementation_->load_file(*name, offset);
}

common::Result<std::uint64_t> TabletPhysicalPartChunkStorage::received_bytes() const {
  if (implementation_ == nullptr)
    return common::make_unexpected(invalid("physical part storage was moved from"));
  common::Status usable = implementation_->check_usable();
  if (!usable.is_ok())
    return common::make_unexpected(std::move(usable));
  return implementation_->received_bytes_;
}

common::Result<CompletedTabletPhysicalPartTransfer>
TabletPhysicalPartChunkStorage::finalize() const {
  if (implementation_ == nullptr)
    return common::make_unexpected(invalid("physical part storage was moved from"));
  common::Status usable = implementation_->check_usable();
  if (!usable.is_ok())
    return common::make_unexpected(std::move(usable));
  if (implementation_->reclaimed_)
    return common::make_unexpected(unavailable("physical part receipt was reclaimed"));
  if (implementation_->received_bytes_ != implementation_->config_.session.total_bytes)
    return common::make_unexpected(unavailable("physical part prefix is incomplete"));
  auto hasher = ingest::Sha256Hasher::create();
  if (!hasher.has_value())
    return common::make_unexpected(hasher.error());
  std::uint64_t expected_offset = 0U;
  for (const ChunkFile& descriptor : implementation_->chunks_) {
    if (descriptor.offset != expected_offset)
      return common::make_unexpected(corruption("physical chunk progress is not contiguous"));
    auto loaded = implementation_->load_file(descriptor.file_name, descriptor.offset);
    if (!loaded.has_value())
      return common::make_unexpected(loaded.error());
    if (loaded->chunk.bytes.size() != descriptor.payload_bytes)
      return common::make_unexpected(corruption("physical chunk progress changed after recovery"));
    common::Status updated = hasher->update(loaded->chunk.bytes);
    if (!updated.is_ok())
      return common::make_unexpected(std::move(updated));
    expected_offset += descriptor.payload_bytes;
  }
  if (expected_offset != implementation_->received_bytes_)
    return common::make_unexpected(corruption("physical chunk prefix boundary is unavailable"));
  auto digest = hasher->finish();
  if (!digest.has_value())
    return common::make_unexpected(digest.error());
  if (*digest != implementation_->config_.session.content_sha256)
    return common::make_unexpected(corruption("physical CSEG content SHA-256 mismatch"));
  return CompletedTabletPhysicalPartTransfer{.session = implementation_->config_.session,
                                             .received_bytes = expected_offset,
                                             .chunk_count = implementation_->chunks_.size(),
                                             .content_sha256 = *digest};
}

common::Result<ReclaimedTabletPhysicalPartReceipt> TabletPhysicalPartChunkStorage::reclaim() {
  if (implementation_ == nullptr)
    return common::make_unexpected(invalid("physical part storage was moved from"));
  common::Status usable = implementation_->check_usable();
  if (!usable.is_ok())
    return common::make_unexpected(std::move(usable));
  const bool marker_already_present = implementation_->reclaimed_;
  if (!marker_already_present) {
    auto complete = finalize();
    if (!complete.has_value())
      return common::make_unexpected(complete.error());
  }
  auto marker = implementation_->install_reclaimed_marker();
  if (!marker.has_value())
    return common::make_unexpected(marker.error());

  ReclaimedTabletPhysicalPartReceipt report{.session = implementation_->config_.session,
                                            .marker_already_present = *marker};
  while (!implementation_->chunks_.empty()) {
    const ChunkFile& chunk = implementation_->chunks_.back();
    auto loaded = implementation_->load_file(chunk.file_name, chunk.offset);
    if (!loaded.has_value())
      return common::make_unexpected(loaded.error());
    common::Status removed = implementation_->directory_.remove_file(chunk.file_name);
    if (!removed.is_ok())
      return common::make_unexpected(with_context("remove reclaimed physical chunk", removed));
    removed = implementation_->directory_.sync();
    if (!removed.is_ok()) {
      return common::make_unexpected(implementation_->fail(
          with_context("synchronize reclaimed physical chunk directory", removed), true));
    }
    ++report.removed_chunks;
    report.removed_payload_bytes += chunk.payload_bytes;
    implementation_->received_bytes_ -= chunk.payload_bytes;
    implementation_->chunks_.pop_back();
  }
  return report;
}

common::Result<TabletPhysicalPartTransferSession>
TabletPhysicalPartChunkStorage::transfer_session() const {
  if (implementation_ == nullptr)
    return common::make_unexpected(invalid("physical part storage was moved from"));
  return implementation_->config_.session;
}

bool TabletPhysicalPartChunkStorage::is_reclaimed() const noexcept {
  return implementation_ != nullptr && implementation_->reclaimed_;
}

bool TabletPhysicalPartChunkStorage::is_usable() const noexcept {
  return implementation_ != nullptr && implementation_->poison_.is_ok();
}

common::Status TabletPhysicalPartChunkStorage::poison_status() const {
  return implementation_ == nullptr ? invalid("physical part storage was moved from")
                                    : implementation_->poison_;
}

} // namespace chronos::cluster
