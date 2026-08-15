#include "chronos/tiering/tiered_pair_commit.hpp"

#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/io/posix_io.hpp"
#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/tiering/tiered_part_loader.hpp"
#include "chronos/tiering/tiered_publication.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::tiering {
namespace {

constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'T'},
                                           std::byte{'R'}, std::byte{'P'}, std::byte{'A'},
                                           std::byte{'I'}, std::byte{'R'}};
constexpr std::uint16_t kMajor = 1U;
constexpr std::uint16_t kMinor = 0U;
constexpr std::uint32_t kHasCold = 1U;
constexpr std::size_t kGenerationDigits = 20U;
constexpr std::string_view kPrefix = "pair-";
constexpr std::string_view kSuffix = ".tpc";
constexpr std::string_view kTemporarySuffix = ".tmp";
constexpr std::size_t kHeaderCrcOffset = 248U;
constexpr std::size_t kFileCrcOffset = 252U;

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
[[nodiscard]] common::Status with_context(const std::string_view context,
                                          const common::Status& status) {
  std::string message{context};
  message.append(": ");
  message.append(status.message());
  return {status.code(), std::move(message)};
}

[[nodiscard]] std::uint16_t load_u16(const common::ByteView bytes,
                                     const std::size_t offset) noexcept {
  return static_cast<std::uint16_t>(std::to_integer<std::uint16_t>(bytes[offset]) |
                                    (std::to_integer<std::uint16_t>(bytes[offset + 1U]) << 8U));
}
[[nodiscard]] std::uint32_t load_u32(const common::ByteView bytes,
                                     const std::size_t offset) noexcept {
  std::uint32_t value{};
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    value |= std::to_integer<std::uint32_t>(bytes[offset + index]) << (index * 8U);
  return value;
}
[[nodiscard]] std::uint64_t load_u64(const common::ByteView bytes,
                                     const std::size_t offset) noexcept {
  std::uint64_t value{};
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    value |= std::to_integer<std::uint64_t>(bytes[offset + index]) << (index * 8U);
  return value;
}

[[nodiscard]] common::Result<manifest::DatabaseId> database_id_at(const common::ByteView bytes) {
  common::Uuid::Bytes id{};
  std::ranges::copy(bytes.subspan(40U, id.size()), id.begin());
  return manifest::DatabaseId::from_bytes(id);
}
[[nodiscard]] common::Uuid uuid_at(const common::ByteView bytes, const std::size_t offset) {
  common::Uuid::Bytes id{};
  std::ranges::copy(bytes.subspan(offset, id.size()), id.begin());
  return common::Uuid{id};
}
[[nodiscard]] ingest::Sha256Digest digest_at(const common::ByteView bytes,
                                             const std::size_t offset) {
  ingest::Sha256Digest::Bytes digest{};
  std::ranges::copy(bytes.subspan(offset, digest.size()), digest.begin());
  return ingest::Sha256Digest{digest};
}

[[nodiscard]] common::Status validate_record(const TieredPairCommitRecord& record) {
  if (record.generation == 0U ||
      record.previous_generation != (record.generation == 1U ? 0U : record.generation - 1U) ||
      record.database_id.uuid().is_nil() || record.object_store_id.is_nil() ||
      record.manifest_generation == 0U || record.manifest_length == 0U) {
    return invalid("tiered pair commit authority is invalid");
  }
  if (record.cold_generation == 0U) {
    if (record.cold_length != 0U ||
        !std::ranges::all_of(record.cold_sha256.bytes(),
                             [](const std::byte value) { return value == std::byte{0}; }))
      return invalid("tiered pair commit absent cold fields are nonzero");
  } else if (record.cold_length == 0U) {
    return invalid("tiered pair commit cold authority is incomplete");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status validate_transition(const TieredPairCommitRecord& predecessor,
                                                 const TieredPairCommitRecord& successor) {
  if (predecessor.generation == std::numeric_limits<std::uint64_t>::max() ||
      successor.generation != predecessor.generation + 1U ||
      successor.previous_generation != predecessor.generation ||
      successor.database_id != predecessor.database_id ||
      successor.object_store_id != predecessor.object_store_id) {
    return invalid("tiered pair commit successor identity is invalid");
  }
  if (successor.manifest_generation < predecessor.manifest_generation ||
      (successor.manifest_generation != predecessor.manifest_generation &&
       (predecessor.manifest_generation == std::numeric_limits<std::uint64_t>::max() ||
        successor.manifest_generation != predecessor.manifest_generation + 1U))) {
    return invalid("tiered pair commit Manifest generation transition is invalid");
  }
  if (successor.manifest_generation == predecessor.manifest_generation &&
      (successor.manifest_length != predecessor.manifest_length ||
       successor.manifest_sha256 != predecessor.manifest_sha256)) {
    return invalid("tiered pair commit changes equal Manifest generation bytes");
  }
  if (predecessor.cold_generation == 0U) {
    if (successor.cold_generation > 1U)
      return invalid("tiered pair commit skips first cold generation");
  } else if (successor.cold_generation < predecessor.cold_generation ||
             (successor.cold_generation != predecessor.cold_generation &&
              (predecessor.cold_generation == std::numeric_limits<std::uint64_t>::max() ||
               successor.cold_generation != predecessor.cold_generation + 1U))) {
    return invalid("tiered pair commit cold generation transition is invalid");
  }
  if (successor.cold_generation == predecessor.cold_generation &&
      (successor.cold_length != predecessor.cold_length ||
       successor.cold_sha256 != predecessor.cold_sha256)) {
    return invalid("tiered pair commit changes equal cold generation bytes");
  }
  return common::Status::ok();
}

class CommittedColdMissingPartValidator final : public manifest::TemporalMissingPartValidator {
public:
  CommittedColdMissingPartValidator(const LoadedColdLocationManifest* cold_manifest,
                                    const ObjectStore* remote_store) noexcept
      : cold_manifest_(cold_manifest), remote_store_(remote_store) {}

  common::Status
  validate_missing_part(const manifest::TemporalPartDescriptor& descriptor,
                        const manifest::TemporalTabletDescriptor& owner,
                        const schema::TableSchema& schema,
                        const manifest::TemporalPartValidationLimits limits) const override {
    if (cold_manifest_ == nullptr)
      return corruption("committed pair has no cold authority for missing local CSEG");
    const auto locations = cold_manifest_->manifest().locations();
    const auto found = std::ranges::lower_bound(locations, descriptor.part_id, {},
                                                &ColdPartLocationDescriptor::part_id);
    if (found == locations.end() || found->part_id != descriptor.part_id)
      return corruption("committed cold authority has no route for missing local CSEG");
    if (remote_store_ == nullptr)
      return unavailable("tiered pair recovery requires the committed object store");
    auto image = load_validated_remote_temporal_part_image(*remote_store_, *found, descriptor,
                                                           owner, schema, limits);
    return image.has_value() ? common::Status::ok() : image.error();
  }

private:
  const LoadedColdLocationManifest* cold_manifest_{};
  const ObjectStore* remote_store_{};
};

[[nodiscard]] bool same_pair(const TieredPairCommitRecord& left,
                             const TieredPairCommitRecord& right) noexcept {
  return left.database_id == right.database_id && left.object_store_id == right.object_store_id &&
         left.manifest_generation == right.manifest_generation &&
         left.cold_generation == right.cold_generation &&
         left.manifest_length == right.manifest_length && left.cold_length == right.cold_length &&
         left.manifest_sha256 == right.manifest_sha256 && left.cold_sha256 == right.cold_sha256;
}

[[nodiscard]] common::Result<std::uint64_t> parse_name(const std::string_view name,
                                                       const bool temporary) {
  const std::size_t expected = kPrefix.size() + kGenerationDigits + kSuffix.size() +
                               (temporary ? kTemporarySuffix.size() : 0U);
  if (name.size() != expected || !name.starts_with(kPrefix) ||
      !name.substr(kPrefix.size() + kGenerationDigits).starts_with(kSuffix) ||
      (temporary && !name.ends_with(kTemporarySuffix))) {
    return common::make_unexpected(corruption("tiered pair commit filename is noncanonical"));
  }
  const std::string_view digits = name.substr(kPrefix.size(), kGenerationDigits);
  std::uint64_t generation{};
  const auto parsed = std::from_chars(digits.data(), digits.data() + digits.size(), generation);
  if (generation == 0U || parsed.ec != std::errc{} || parsed.ptr != digits.data() + digits.size()) {
    return common::make_unexpected(corruption("tiered pair commit filename generation is invalid"));
  }
  auto canonical = tiered_pair_commit_file_name(generation);
  if (!canonical.has_value() || !name.starts_with(*canonical))
    return common::make_unexpected(corruption("tiered pair commit filename is noncanonical"));
  return generation;
}

[[nodiscard]] std::string temporary_name(const std::string_view final_name) {
  std::string result{final_name};
  result.append(kTemporarySuffix);
  return result;
}

} // namespace

common::Result<std::vector<std::byte>>
encode_tiered_pair_commit_v1(const TieredPairCommitRecord& record) {
  common::Status validation = validate_record(record);
  if (!validation.is_ok())
    return common::make_unexpected(std::move(validation));
  try {
    std::vector<std::byte> bytes(kTieredPairCommitV1Size, std::byte{0});
    common::ByteWriter writer{bytes};
    common::Status status = writer.write_exact(kMagic);
    if (status.is_ok())
      status = writer.write_u16_le(kMajor);
    if (status.is_ok())
      status = writer.write_u16_le(kMinor);
    if (status.is_ok())
      status = writer.write_u32_le(kTieredPairCommitV1Size);
    if (status.is_ok())
      status = writer.write_u32_le(record.cold_generation == 0U ? 0U : kHasCold);
    if (status.is_ok())
      status = writer.write_u32_le(0U);
    if (status.is_ok())
      status = writer.write_u64_le(record.generation);
    if (status.is_ok())
      status = writer.write_u64_le(record.previous_generation);
    if (status.is_ok())
      status = writer.write_exact(record.database_id.bytes());
    if (status.is_ok())
      status = writer.write_exact(record.object_store_id.bytes());
    if (status.is_ok())
      status = writer.write_u64_le(record.manifest_generation);
    if (status.is_ok())
      status = writer.write_u64_le(record.cold_generation);
    if (status.is_ok())
      status = writer.write_u64_le(record.manifest_length);
    if (status.is_ok())
      status = writer.write_u64_le(record.cold_length);
    if (status.is_ok())
      status = writer.write_exact(record.manifest_sha256.bytes());
    if (status.is_ok())
      status = writer.write_exact(record.cold_sha256.bytes());
    if (status.is_ok())
      status = writer.zero_fill(kHeaderCrcOffset - writer.offset());
    if (!status.is_ok() || writer.offset() != kHeaderCrcOffset)
      return common::make_unexpected(corruption("tiered pair commit encoding layout failed"));
    common::ByteWriter checksums{common::MutableByteView{bytes}.subspan(kHeaderCrcOffset)};
    status =
        checksums.write_u32_le(common::crc32c(common::ByteView{bytes}.first(kHeaderCrcOffset)));
    if (status.is_ok())
      status =
          checksums.write_u32_le(common::crc32c(common::ByteView{bytes}.first(kFileCrcOffset)));
    if (!status.is_ok() || !checksums.full())
      return common::make_unexpected(corruption("tiered pair commit checksum layout failed"));
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("tiered pair commit allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("tiered pair commit exceeds container limits"));
  }
}

common::Result<TieredPairCommitRecord>
decode_tiered_pair_commit_v1_exact(const common::ByteView bytes) {
  if (bytes.size() != kTieredPairCommitV1Size)
    return common::make_unexpected(corruption("tiered pair commit length is invalid"));
  if (!std::ranges::equal(bytes.first(kMagic.size()), kMagic) ||
      common::crc32c(bytes.first(kHeaderCrcOffset)) != load_u32(bytes, kHeaderCrcOffset) ||
      common::crc32c(bytes.first(kFileCrcOffset)) != load_u32(bytes, kFileCrcOffset)) {
    return common::make_unexpected(corruption("tiered pair commit integrity is invalid"));
  }
  if (load_u16(bytes, 8U) == 0U)
    return common::make_unexpected(corruption("tiered pair commit format major is zero"));
  if (load_u16(bytes, 8U) != kMajor || load_u16(bytes, 10U) != kMinor)
    return common::make_unexpected(common::Status{common::StatusCode::kNotSupported,
                                                  "tiered pair commit version is unsupported"});
  const std::uint32_t flags = load_u32(bytes, 16U);
  if ((flags & ~kHasCold) != 0U)
    return common::make_unexpected(common::Status{common::StatusCode::kNotSupported,
                                                  "tiered pair commit flags are unsupported"});
  if (load_u32(bytes, 12U) != kTieredPairCommitV1Size || load_u32(bytes, 20U) != 0U ||
      !std::ranges::all_of(bytes.subspan(168U, 80U),
                           [](const std::byte value) { return value == std::byte{0}; })) {
    return common::make_unexpected(corruption("tiered pair commit fixed fields are invalid"));
  }
  auto database_id = database_id_at(bytes);
  if (!database_id.has_value())
    return common::make_unexpected(corruption("tiered pair commit database is nil"));
  TieredPairCommitRecord record{.generation = load_u64(bytes, 24U),
                                .previous_generation = load_u64(bytes, 32U),
                                .database_id = *database_id,
                                .object_store_id = uuid_at(bytes, 56U),
                                .manifest_generation = load_u64(bytes, 72U),
                                .cold_generation = load_u64(bytes, 80U),
                                .manifest_length = load_u64(bytes, 88U),
                                .cold_length = load_u64(bytes, 96U),
                                .manifest_sha256 = digest_at(bytes, 104U),
                                .cold_sha256 = digest_at(bytes, 136U)};
  common::Status validation = validate_record(record);
  if (!validation.is_ok() || ((flags & kHasCold) != 0U) != (record.cold_generation != 0U))
    return common::make_unexpected(corruption("tiered pair commit authority is invalid"));
  return record;
}

common::Result<std::string> tiered_pair_commit_file_name(const std::uint64_t generation) {
  if (generation == 0U)
    return common::make_unexpected(invalid("tiered pair commit generation must be nonzero"));
  std::array<char, kGenerationDigits> digits{};
  const auto converted = std::to_chars(digits.data(), digits.data() + digits.size(), generation);
  if (converted.ec != std::errc{})
    return common::make_unexpected(invalid("tiered pair commit generation is not representable"));
  const std::size_t written = static_cast<std::size_t>(converted.ptr - digits.data());
  std::string result{kPrefix};
  result.append(kGenerationDigits - written, '0');
  result.append(digits.data(), written);
  result.append(kSuffix);
  return result;
}

class TieredPairCommitStorage::Impl {
public:
  Impl(TieredPairCommitStorageConfig config, io::PosixDirectory directory,
       io::PosixAdvisoryLock lock) noexcept
      : config_(std::move(config)), directory_(std::move(directory)), lock_(std::move(lock)) {}

  [[nodiscard]] common::Status usable() const {
    return poison_.is_ok()
               ? common::Status::ok()
               : unavailable("tiered pair commit storage is poisoned: " + poison_.message());
  }

  [[nodiscard]] common::Result<std::vector<std::uint64_t>> scan() const {
    auto entries = directory_.list_entries();
    if (!entries.has_value())
      return common::make_unexpected(entries.error());
    try {
      std::vector<std::uint64_t> generations;
      for (const io::DirectoryEntry& entry : *entries) {
        if (entry.name == kTieredPairCommitLockFileName)
          continue;
        if (entry.name.ends_with(kTemporarySuffix))
          return common::make_unexpected(
              corruption("tiered pair temporary remains after recovery"));
        auto parsed = parse_name(entry.name, false);
        if (!parsed.has_value() || entry.type != io::DirectoryEntryType::kRegularFile)
          return common::make_unexpected(corruption("tiered pair directory is noncanonical"));
        generations.push_back(*parsed);
      }
      std::ranges::sort(generations);
      for (std::size_t index = 0U; index < generations.size(); ++index) {
        if (generations[index] != static_cast<std::uint64_t>(index) + 1U)
          return common::make_unexpected(corruption("tiered pair commit chain has a gap"));
      }
      return generations;
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("tiered pair namespace allocation failed"));
    }
  }

  [[nodiscard]] common::Status cleanup() const {
    auto entries = directory_.list_entries();
    if (!entries.has_value())
      return entries.error();
    bool removed = false;
    for (const io::DirectoryEntry& entry : *entries) {
      if (!entry.name.ends_with(kTemporarySuffix))
        continue;
      if (!parse_name(entry.name, true).has_value() ||
          entry.type != io::DirectoryEntryType::kRegularFile)
        return corruption("recognized tiered pair temporary is noncanonical");
      common::Status status = directory_.remove_file(entry.name);
      if (!status.is_ok())
        return status;
      removed = true;
    }
    return removed ? directory_.sync() : common::Status::ok();
  }

  [[nodiscard]] common::Result<std::pair<TieredPairCommitRecord, std::vector<std::byte>>>
  load(const std::uint64_t generation) const {
    auto name = tiered_pair_commit_file_name(generation);
    if (!name.has_value())
      return common::make_unexpected(name.error());
    auto file = directory_.open_regular_file(*name, io::FileOpenMode::kReadOnly);
    if (!file.has_value())
      return common::make_unexpected(file.error());
    auto size = file->size();
    if (!size.has_value())
      return common::make_unexpected(size.error());
    if (*size != kTieredPairCommitV1Size)
      return common::make_unexpected(corruption("installed tiered pair commit size is invalid"));
    std::vector<std::byte> bytes(kTieredPairCommitV1Size);
    auto read = file->read_at(0U, bytes);
    if (!read.has_value())
      return common::make_unexpected(read.error());
    if (*read != bytes.size())
      return common::make_unexpected(corruption("installed tiered pair commit is truncated"));
    auto record = decode_tiered_pair_commit_v1_exact(bytes);
    if (!record.has_value())
      return common::make_unexpected(record.error());
    if (record->generation != generation || record->database_id != config_.expected_database_id ||
        record->object_store_id != config_.expected_object_store_id)
      return common::make_unexpected(corruption("tiered pair commit disagrees with owner or name"));
    return std::pair<TieredPairCommitRecord, std::vector<std::byte>>{*record, std::move(bytes)};
  }

  TieredPairCommitStorageConfig config_;
  io::PosixDirectory directory_;
  io::PosixAdvisoryLock lock_;
  common::Status poison_;
};

common::Result<TieredPairCommitStorage>
TieredPairCommitStorage::open(TieredPairCommitStorageConfig config, const bool create_lock) {
  if (config.directory_path.empty() || config.expected_database_id.uuid().is_nil() ||
      config.expected_object_store_id.is_nil() || config.file_permissions == 0U ||
      (config.file_permissions & ~0777U) != 0U) {
    return common::make_unexpected(invalid("tiered pair commit storage config is invalid"));
  }
  auto directory = io::PosixDirectory::open(config.directory_path);
  if (!directory.has_value())
    return common::make_unexpected(directory.error());
  auto lock = create_lock
                  ? directory->acquire_exclusive_lock(kTieredPairCommitLockFileName,
                                                      config.file_permissions)
                  : directory->acquire_existing_exclusive_lock(kTieredPairCommitLockFileName);
  if (!lock.has_value())
    return common::make_unexpected(lock.error());
  try {
    auto impl = std::make_unique<Impl>(std::move(config), std::move(*directory), std::move(*lock));
    common::Status cleanup = impl->cleanup();
    if (!cleanup.is_ok())
      return common::make_unexpected(cleanup);
    auto generations = impl->scan();
    if (!generations.has_value())
      return common::make_unexpected(generations.error());
    return TieredPairCommitStorage{std::move(impl)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("tiered pair commit storage allocation failed"));
  }
}

TieredPairCommitStorage::TieredPairCommitStorage(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
TieredPairCommitStorage::~TieredPairCommitStorage() = default;
TieredPairCommitStorage::TieredPairCommitStorage(TieredPairCommitStorage&&) noexcept = default;
TieredPairCommitStorage&
TieredPairCommitStorage::operator=(TieredPairCommitStorage&&) noexcept = default;

common::Result<TieredPairCommitStorage>
TieredPairCommitStorage::create(TieredPairCommitStorageConfig config) {
  return open(std::move(config), true);
}
common::Result<TieredPairCommitStorage>
TieredPairCommitStorage::open_existing(TieredPairCommitStorageConfig config) {
  return open(std::move(config), false);
}

common::Result<InstalledTieredPairCommit> TieredPairCommitStorage::commit(
    const manifest::TemporalDatabaseStorageSnapshot& manifest_snapshot,
    const std::shared_ptr<const LoadedColdLocationManifest>& cold_manifest) {
  if (impl_ == nullptr)
    return common::make_unexpected(invalid("tiered pair commit storage was moved from"));
  common::Status usable = impl_->usable();
  if (!usable.is_ok())
    return common::make_unexpected(std::move(usable));
  try {
    auto manifest_decoded =
        manifest::decode_manifest_v2_temporal_exact(manifest_snapshot.manifest_bytes());
    if (!manifest_decoded.has_value() ||
        manifest_decoded->database_id() != impl_->config_.expected_database_id ||
        manifest_decoded->generation() != manifest_snapshot.generation()) {
      return common::make_unexpected(invalid("tiered pair Manifest owner is invalid"));
    }
    if (cold_manifest != nullptr) {
      auto cold_decoded = decode_cold_location_manifest_v1_exact(cold_manifest->encoded_bytes());
      if (!cold_decoded.has_value() ||
          cold_decoded->object_store_id() != impl_->config_.expected_object_store_id ||
          !validate_cold_location_manifest_binding(*cold_decoded, *manifest_decoded).is_ok()) {
        return common::make_unexpected(invalid("tiered pair cold owner is incompatible"));
      }
    }
    auto manifest_digest = ingest::sha256(manifest_snapshot.manifest_bytes());
    if (!manifest_digest.has_value())
      return common::make_unexpected(manifest_digest.error());
    ingest::Sha256Digest cold_digest{ingest::Sha256Digest::Bytes{}};
    if (cold_manifest != nullptr) {
      auto digest = ingest::sha256(cold_manifest->encoded_bytes());
      if (!digest.has_value())
        return common::make_unexpected(digest.error());
      cold_digest = *digest;
    }
    auto generations = impl_->scan();
    if (!generations.has_value())
      return common::make_unexpected(generations.error());
    const bool generation_exhausted =
        !generations->empty() && generations->back() == std::numeric_limits<std::uint64_t>::max();
    const std::uint64_t next_generation =
        generations->empty()
            ? 1U
            : (generation_exhausted ? generations->back() : generations->back() + 1U);
    TieredPairCommitRecord record{
        .generation = next_generation,
        .previous_generation = next_generation == 1U ? 0U : next_generation - 1U,
        .database_id = manifest_snapshot.database_id(),
        .object_store_id = impl_->config_.expected_object_store_id,
        .manifest_generation = manifest_snapshot.generation(),
        .cold_generation = cold_manifest == nullptr ? 0U : cold_manifest->manifest().generation(),
        .manifest_length = manifest_snapshot.manifest_bytes().size(),
        .cold_length = cold_manifest == nullptr ? 0U : cold_manifest->encoded_bytes().size(),
        .manifest_sha256 = *manifest_digest,
        .cold_sha256 = cold_digest};
    if (!generations->empty()) {
      auto predecessor = impl_->load(generations->back());
      if (!predecessor.has_value())
        return common::make_unexpected(predecessor.error());
      if (same_pair(predecessor->first, record)) {
        auto name = tiered_pair_commit_file_name(predecessor->first.generation);
        return InstalledTieredPairCommit{*name, predecessor->first, true};
      }
      if (generation_exhausted) {
        return common::make_unexpected(
            exhausted("tiered pair commit generation space is exhausted"));
      }
      common::Status transition = validate_transition(predecessor->first, record);
      if (!transition.is_ok())
        return common::make_unexpected(std::move(transition));
    }
    auto encoded = encode_tiered_pair_commit_v1(record);
    if (!encoded.has_value())
      return common::make_unexpected(encoded.error());
    auto final_name = tiered_pair_commit_file_name(record.generation);
    if (!final_name.has_value())
      return common::make_unexpected(final_name.error());
    const std::string temp_name = temporary_name(*final_name);
    common::Status operation = impl_->directory_.remove_file(temp_name);
    if (operation.is_ok())
      operation = impl_->directory_.sync();
    else if (operation.code() == common::StatusCode::kNotFound)
      operation = common::Status::ok();
    if (!operation.is_ok())
      return common::make_unexpected(operation);
    auto temporary =
        impl_->directory_.create_exclusive_regular_file(temp_name, impl_->config_.file_permissions);
    if (!temporary.has_value())
      return common::make_unexpected(temporary.error());
    operation = temporary->write_all_at(0U, *encoded);
    if (!operation.is_ok())
      return common::make_unexpected(operation);
    auto temporary_size = temporary->size();
    if (!temporary_size.has_value())
      return common::make_unexpected(temporary_size.error());
    if (*temporary_size != encoded->size())
      return common::make_unexpected(corruption("tiered pair commit temporary size changed"));
    std::vector<std::byte> readback(encoded->size());
    auto read = temporary->read_at(0U, readback);
    if (!read.has_value() || *read != readback.size() || readback != *encoded ||
        !decode_tiered_pair_commit_v1_exact(readback).has_value()) {
      return common::make_unexpected(corruption("tiered pair commit readback is invalid"));
    }
    operation = temporary->sync_all();
    if (operation.is_ok())
      operation = temporary->close();
    if (operation.is_ok())
      operation =
          impl_->directory_.rename_no_replace({.old_name = temp_name, .new_name = *final_name});
    if (!operation.is_ok())
      return common::make_unexpected(operation);
    operation = impl_->directory_.sync();
    if (!operation.is_ok()) {
      impl_->poison_ = with_context("synchronize tiered pair commit directory", operation);
      return common::make_unexpected(impl_->poison_);
    }
    return InstalledTieredPairCommit{*final_name, record, false};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("tiered pair commit installation allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("tiered pair commit installation exceeded limits"));
  }
}

common::Result<std::optional<RecoveredTieredPair>>
TieredPairCommitStorage::recover(manifest::ManifestStorage& manifest_storage,
                                 const ColdLocationManifestStorage& cold_storage,
                                 const TieredPairRecoveryRequest& request) const {
  if (impl_ == nullptr)
    return common::make_unexpected(invalid("tiered pair commit storage was moved from"));
  common::Status usable = impl_->usable();
  if (!usable.is_ok())
    return common::make_unexpected(std::move(usable));
  if (request.manifest_request.missing_part_validator != nullptr) {
    return common::make_unexpected(
        invalid("tiered pair recovery owns the missing-part validation authority"));
  }
  try {
    auto generations = impl_->scan();
    if (!generations.has_value())
      return common::make_unexpected(generations.error());
    if (generations->empty())
      return std::optional<RecoveredTieredPair>{};
    auto loaded_record = impl_->load(generations->back());
    if (!loaded_record.has_value())
      return common::make_unexpected(loaded_record.error());
    const TieredPairCommitRecord& record = loaded_record->first;
    auto metadata = manifest_storage.load_temporal_manifest_metadata(record.manifest_generation,
                                                                     request.manifest_request);
    if (!metadata.has_value())
      return common::make_unexpected(metadata.error());
    if (metadata->encoded_bytes().size() != record.manifest_length)
      return common::make_unexpected(corruption("committed Manifest v2 length changed"));
    auto manifest_digest = ingest::sha256(metadata->encoded_bytes());
    if (!manifest_digest.has_value() || *manifest_digest != record.manifest_sha256)
      return common::make_unexpected(corruption("committed Manifest v2 digest changed"));
    auto decoded_manifest = manifest::decode_manifest_v2_temporal_exact(
        metadata->encoded_bytes(), request.manifest_request.decode_limits);
    if (!decoded_manifest.has_value())
      return common::make_unexpected(decoded_manifest.error().status());
    std::shared_ptr<const LoadedColdLocationManifest> cold_owner;
    if (record.cold_generation != 0U) {
      auto loaded_cold = cold_storage.load_generation(record.cold_generation, *decoded_manifest);
      if (!loaded_cold.has_value())
        return common::make_unexpected(loaded_cold.error());
      if (loaded_cold->encoded_bytes().size() != record.cold_length)
        return common::make_unexpected(corruption("committed cold manifest length changed"));
      auto cold_digest = ingest::sha256(loaded_cold->encoded_bytes());
      if (!cold_digest.has_value() || *cold_digest != record.cold_sha256)
        return common::make_unexpected(corruption("committed cold manifest digest changed"));
      cold_owner = std::make_shared<const LoadedColdLocationManifest>(std::move(*loaded_cold));
    }
    const CommittedColdMissingPartValidator missing_validator{cold_owner.get(),
                                                              request.remote_store};
    manifest::TemporalManifestLoadRequest full_request = request.manifest_request;
    full_request.missing_part_validator = &missing_validator;
    auto loaded_manifest = manifest_storage.load_temporal_manifest_generation(
        record.manifest_generation, full_request);
    if (!loaded_manifest.has_value())
      return common::make_unexpected(loaded_manifest.error());
    if (loaded_manifest->encoded_bytes().size() != record.manifest_length)
      return common::make_unexpected(
          corruption("committed Manifest v2 length changed during load"));
    auto loaded_digest = ingest::sha256(loaded_manifest->encoded_bytes());
    if (!loaded_digest.has_value() || *loaded_digest != record.manifest_sha256)
      return common::make_unexpected(
          corruption("committed Manifest v2 digest changed during load"));
    auto manifest_owner = std::make_shared<const manifest::LoadedTemporalManifestGeneration>(
        std::move(*loaded_manifest));
    auto manifest_publisher = manifest::TemporalDatabaseStoragePublisher::create(
        manifest_owner, request.manifest_request.schema_bindings,
        request.manifest_request.decode_limits);
    if (!manifest_publisher.has_value())
      return common::make_unexpected(manifest_publisher.error());
    auto manifest_snapshot = manifest_publisher->snapshot();
    if (!manifest_snapshot.has_value())
      return common::make_unexpected(manifest_snapshot.error());
    return std::optional<RecoveredTieredPair>{
        RecoveredTieredPair{record, std::move(*manifest_snapshot), std::move(cold_owner)}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("tiered pair recovery allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("tiered pair recovery exceeded limits"));
  }
}

common::Result<std::optional<TieredPairCommitRecord>>
TieredPairCommitStorage::load_selected_record() const {
  if (impl_ == nullptr)
    return common::make_unexpected(invalid("tiered pair commit storage was moved from"));
  common::Status usable = impl_->usable();
  if (!usable.is_ok())
    return common::make_unexpected(std::move(usable));
  auto generations = impl_->scan();
  if (!generations.has_value())
    return common::make_unexpected(generations.error());
  if (generations->empty())
    return std::optional<TieredPairCommitRecord>{};
  auto loaded = impl_->load(generations->back());
  if (!loaded.has_value())
    return common::make_unexpected(loaded.error());
  return std::optional<TieredPairCommitRecord>{loaded->first};
}

bool TieredPairCommitStorage::is_usable() const noexcept {
  return impl_ != nullptr && impl_->poison_.is_ok();
}
common::Status TieredPairCommitStorage::poison_status() const {
  return impl_ == nullptr ? invalid("tiered pair commit storage was moved from") : impl_->poison_;
}

} // namespace chronos::tiering
