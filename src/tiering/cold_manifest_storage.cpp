#include "chronos/tiering/cold_manifest_storage.hpp"

#include "chronos/io/posix_io.hpp"
#include "io/posix_syscalls.hpp"

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
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::tiering {
namespace {

constexpr std::string_view kGenerationPrefix = "generation-";
constexpr std::string_view kGenerationSuffix = ".clm";
constexpr std::string_view kTemporarySuffix = ".tmp";
constexpr std::size_t kGenerationDigits = 20U;

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

[[nodiscard]] bool valid_limits(const ColdLocationManifestDecodeLimits limits) noexcept {
  return limits.maximum_file_length >=
             cold_manifest_format::kHeaderLength + cold_manifest_format::kTrailerLength &&
         limits.maximum_file_length <= cold_manifest_format::kMaximumFileLength &&
         limits.maximum_locations <= cold_manifest_format::kMaximumLocationCount &&
         limits.maximum_key_bytes <= cold_manifest_format::kMaximumKeyBytes;
}

[[nodiscard]] std::string temporary_name(const std::string_view final_name) {
  std::string result{final_name};
  result.append(kTemporarySuffix);
  return result;
}

[[nodiscard]] common::Result<std::uint64_t> parse_generation_name(const std::string_view name,
                                                                  const bool temporary) {
  const std::size_t expected = kGenerationPrefix.size() + kGenerationDigits +
                               kGenerationSuffix.size() +
                               (temporary ? kTemporarySuffix.size() : 0U);
  if (name.size() != expected || !name.starts_with(kGenerationPrefix) ||
      !name.substr(kGenerationPrefix.size() + kGenerationDigits).starts_with(kGenerationSuffix) ||
      (temporary && !name.ends_with(kTemporarySuffix))) {
    return common::make_unexpected(corruption("cold manifest filename is noncanonical"));
  }
  const std::string_view digits = name.substr(kGenerationPrefix.size(), kGenerationDigits);
  std::uint64_t generation{};
  const auto parsed = std::from_chars(digits.data(), digits.data() + digits.size(), generation);
  if (generation == 0U || parsed.ec != std::errc{} || parsed.ptr != digits.data() + digits.size()) {
    return common::make_unexpected(corruption("cold manifest filename generation is invalid"));
  }
  auto canonical = cold_location_manifest_file_name(generation);
  if (!canonical.has_value() || !name.starts_with(*canonical))
    return common::make_unexpected(corruption("cold manifest filename is noncanonical"));
  return generation;
}

[[nodiscard]] common::Status validate_owner(const DecodedColdLocationManifest& decoded,
                                            const ColdLocationManifestStorageConfig& config) {
  if (decoded.database_id() != config.expected_database_id ||
      decoded.object_store_id() != config.expected_object_store_id) {
    return corruption("cold manifest belongs to another database or object store");
  }
  return common::Status::ok();
}

void saturating_add(std::uint64_t& target, const std::uint64_t increment) noexcept {
  const std::uint64_t maximum = std::numeric_limits<std::uint64_t>::max();
  target = target > maximum - increment ? maximum : target + increment;
}

} // namespace

class ColdLocationManifestStorage::Impl {
public:
  Impl(ColdLocationManifestStorageConfig config, io::PosixDirectory directory,
       io::PosixAdvisoryLock lock) noexcept
      : config_(std::move(config)), directory_(std::move(directory)), lock_(std::move(lock)) {}

  [[nodiscard]] common::Status check_usable() const {
    return poison_.is_ok() ? common::Status::ok()
                           : unavailable("cold manifest storage is poisoned: " + poison_.message());
  }

  [[nodiscard]] common::Status cleanup_temporaries() {
    auto entries = directory_.list_entries();
    if (!entries.has_value())
      return entries.error();
    bool removed = false;
    for (const io::DirectoryEntry& entry : *entries) {
      if (!entry.name.ends_with(kTemporarySuffix))
        continue;
      if (!parse_generation_name(entry.name, true).has_value() ||
          entry.type != io::DirectoryEntryType::kRegularFile) {
        return corruption("recognized cold manifest temporary is noncanonical");
      }
      common::Status status = directory_.remove_file(entry.name);
      if (!status.is_ok())
        return status;
      removed = true;
    }
    return removed ? directory_.sync() : common::Status::ok();
  }

  [[nodiscard]] common::Result<std::vector<std::uint64_t>> scan_generations() const {
    auto entries = directory_.list_entries();
    if (!entries.has_value())
      return common::make_unexpected(entries.error());
    std::vector<std::uint64_t> generations;
    try {
      for (const io::DirectoryEntry& entry : *entries) {
        if (entry.name == kColdLocationManifestLockFileName)
          continue;
        if (entry.name.ends_with(kTemporarySuffix)) {
          return common::make_unexpected(
              corruption("cold manifest temporary remains after ownership recovery"));
        }
        auto generation = parse_generation_name(entry.name, false);
        if (!generation.has_value() || entry.type != io::DirectoryEntryType::kRegularFile)
          return common::make_unexpected(corruption("cold manifest directory is noncanonical"));
        generations.push_back(*generation);
      }
      std::ranges::sort(generations);
      for (std::size_t index = 0U; index < generations.size(); ++index) {
        if (generations[index] != static_cast<std::uint64_t>(index) + 1U)
          return common::make_unexpected(corruption("cold manifest generation chain has a gap"));
      }
      return generations;
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("cold manifest namespace allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(exhausted("cold manifest namespace exceeds limits"));
    }
  }

  [[nodiscard]] common::Result<LoadedColdLocationManifest>
  load_generation(const std::uint64_t generation,
                  const manifest::DecodedTemporalManifestView* const base_manifest) const {
    common::Status usable = check_usable();
    if (!usable.is_ok())
      return common::make_unexpected(std::move(usable));
    auto name = cold_location_manifest_file_name(generation);
    if (!name.has_value())
      return common::make_unexpected(name.error());
    auto file = directory_.open_regular_file(*name, io::FileOpenMode::kReadOnly);
    if (!file.has_value())
      return common::make_unexpected(file.error());
    auto length = file->size();
    if (!length.has_value())
      return common::make_unexpected(length.error());
    if (*length > config_.decode_limits.maximum_file_length ||
        *length > std::numeric_limits<std::size_t>::max()) {
      return common::make_unexpected(exhausted("installed cold manifest exceeds decode limit"));
    }
    try {
      std::vector<std::byte> bytes(static_cast<std::size_t>(*length));
      auto read = file->read_at(0U, bytes);
      if (!read.has_value())
        return common::make_unexpected(read.error());
      if (*read != bytes.size())
        return common::make_unexpected(corruption("installed cold manifest is truncated"));
      auto decoded = decode_cold_location_manifest_v1_exact(bytes, config_.decode_limits);
      if (!decoded.has_value())
        return common::make_unexpected(decoded.error().status());
      if (decoded->generation() != generation)
        return common::make_unexpected(
            corruption("cold manifest final name disagrees with encoded generation"));
      common::Status validation = validate_owner(*decoded, config_);
      if (validation.is_ok() && base_manifest != nullptr)
        validation = validate_cold_location_manifest_binding(*decoded, *base_manifest);
      if (!validation.is_ok())
        return common::make_unexpected(std::move(validation));
      return LoadedColdLocationManifest{*name, std::move(*decoded), std::move(bytes)};
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("cold manifest read allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(exhausted("cold manifest read exceeds limits"));
    }
  }

  [[nodiscard]] common::Result<InstalledColdLocationManifest> fail(common::Status status) {
    saturating_add(metrics_.install_failures, 1U);
    return common::make_unexpected(std::move(status));
  }

  ColdLocationManifestStorageConfig config_;
  io::PosixDirectory directory_;
  io::PosixAdvisoryLock lock_;
  common::Status poison_;
  ColdLocationManifestStorageMetrics metrics_;
};

common::Result<std::string> cold_location_manifest_file_name(const std::uint64_t generation) {
  if (generation == 0U)
    return common::make_unexpected(invalid("cold manifest generation must be nonzero"));
  std::array<char, kGenerationDigits> digits{};
  const auto converted = std::to_chars(digits.data(), digits.data() + digits.size(), generation);
  if (converted.ec != std::errc{})
    return common::make_unexpected(invalid("cold manifest generation is not representable"));
  const std::size_t written = static_cast<std::size_t>(converted.ptr - digits.data());
  std::string result{kGenerationPrefix};
  result.append(kGenerationDigits - written, '0');
  result.append(digits.data(), written);
  result.append(kGenerationSuffix);
  return result;
}

common::Result<ColdLocationManifestStorage>
ColdLocationManifestStorage::open(ColdLocationManifestStorageConfig config, const bool create_lock,
                                  io::detail::PosixSyscalls& syscalls) {
  if (config.directory_path.empty() || config.expected_database_id.uuid().is_nil() ||
      config.expected_object_store_id.is_nil() || !valid_limits(config.decode_limits) ||
      config.file_permissions == 0U || (config.file_permissions & ~0777U) != 0U) {
    return common::make_unexpected(invalid("cold manifest storage config is invalid"));
  }
  auto directory = io::detail::PosixHandleFactory::open_directory(config.directory_path, syscalls);
  if (!directory.has_value())
    return common::make_unexpected(with_context("open cold manifest directory", directory.error()));
  auto lock = create_lock
                  ? directory->acquire_exclusive_lock(kColdLocationManifestLockFileName,
                                                      config.file_permissions)
                  : directory->acquire_existing_exclusive_lock(kColdLocationManifestLockFileName);
  if (!lock.has_value())
    return common::make_unexpected(with_context("acquire cold manifest lock", lock.error()));
  try {
    auto impl = std::make_unique<Impl>(std::move(config), std::move(*directory), std::move(*lock));
    common::Status cleanup = impl->cleanup_temporaries();
    if (!cleanup.is_ok())
      return common::make_unexpected(with_context("recover cold manifest temporaries", cleanup));
    auto generations = impl->scan_generations();
    if (!generations.has_value())
      return common::make_unexpected(generations.error());
    return ColdLocationManifestStorage{std::move(impl)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("cold manifest storage allocation failed"));
  }
}

ColdLocationManifestStorage::ColdLocationManifestStorage(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
ColdLocationManifestStorage::~ColdLocationManifestStorage() = default;
ColdLocationManifestStorage::ColdLocationManifestStorage(ColdLocationManifestStorage&&) noexcept =
    default;
ColdLocationManifestStorage&
ColdLocationManifestStorage::operator=(ColdLocationManifestStorage&&) noexcept = default;

common::Result<ColdLocationManifestStorage>
ColdLocationManifestStorage::create(ColdLocationManifestStorageConfig config) {
  return open(std::move(config), true, io::detail::system_posix_syscalls());
}

common::Result<ColdLocationManifestStorage>
ColdLocationManifestStorage::open_existing(ColdLocationManifestStorageConfig config) {
  return open(std::move(config), false, io::detail::system_posix_syscalls());
}

common::Result<InstalledColdLocationManifest> ColdLocationManifestStorage::install(
    const std::reference_wrapper<const EncodedColdLocationManifest> encoded_reference,
    const manifest::DecodedTemporalManifestView& base_manifest) {
  if (impl_ == nullptr)
    return common::make_unexpected(invalid("cold manifest storage was moved from"));
  Impl& impl = *impl_;
  saturating_add(impl.metrics_.install_attempts, 1U);
  common::Status usable = impl.check_usable();
  if (!usable.is_ok())
    return impl.fail(std::move(usable));
  const EncodedColdLocationManifest& encoded = encoded_reference.get();
  auto candidate =
      decode_cold_location_manifest_v1_exact(encoded.bytes(), impl.config_.decode_limits);
  if (!candidate.has_value())
    return impl.fail(with_context("decode cold manifest candidate", candidate.error().status()));
  common::Status validation = validate_owner(*candidate, impl.config_);
  if (validation.is_ok())
    validation = validate_cold_location_manifest_binding(*candidate, base_manifest);
  if (!validation.is_ok())
    return impl.fail(with_context("validate cold manifest candidate authority", validation));

  auto generations = impl.scan_generations();
  if (!generations.has_value())
    return impl.fail(generations.error());
  if (!generations->empty() && candidate->generation() <= generations->back()) {
    auto existing = impl.load_generation(candidate->generation(), &base_manifest);
    if (!existing.has_value())
      return impl.fail(existing.error());
    if (!std::ranges::equal(existing->encoded_bytes, encoded.bytes()))
      return impl.fail(corruption("cold manifest generation already has different durable bytes"));
    return InstalledColdLocationManifest{.file_name = existing->file_name,
                                         .generation = candidate->generation(),
                                         .base_manifest_generation =
                                             candidate->base_manifest_generation(),
                                         .location_count = candidate->locations().size(),
                                         .already_present = true};
  }
  if (!generations->empty() && generations->back() == std::numeric_limits<std::uint64_t>::max()) {
    return impl.fail(exhausted("cold manifest generation space is exhausted"));
  }
  const std::uint64_t expected_generation = generations->empty() ? 1U : generations->back() + 1U;
  if (candidate->generation() != expected_generation)
    return impl.fail(invalid("cold manifest candidate is not the exact next generation"));
  if (!generations->empty()) {
    auto predecessor = impl.load_generation(generations->back(), nullptr);
    if (!predecessor.has_value())
      return impl.fail(with_context("load cold manifest predecessor", predecessor.error()));
    validation = validate_cold_location_manifest_transition(predecessor->manifest, *candidate);
    if (!validation.is_ok())
      return impl.fail(with_context("validate cold manifest successor", validation));
  }

  auto final_name = cold_location_manifest_file_name(candidate->generation());
  if (!final_name.has_value())
    return impl.fail(final_name.error());
  const std::string temp_name = temporary_name(*final_name);
  common::Status operation = impl.directory_.remove_file(temp_name);
  if (operation.is_ok()) {
    operation = impl.directory_.sync();
  } else if (operation.code() == common::StatusCode::kNotFound) {
    operation = common::Status::ok();
  }
  if (!operation.is_ok())
    return impl.fail(with_context("clean prior cold manifest temporary", operation));
  auto temporary =
      impl.directory_.create_exclusive_regular_file(temp_name, impl.config_.file_permissions);
  if (!temporary.has_value())
    return impl.fail(with_context("create cold manifest temporary", temporary.error()));
  operation = temporary->write_all_at(0U, encoded.bytes());
  if (!operation.is_ok())
    return impl.fail(with_context("write cold manifest temporary", operation));
  auto length = temporary->size();
  if (!length.has_value())
    return impl.fail(with_context("read cold manifest temporary size", length.error()));
  if (*length != encoded.size())
    return impl.fail(corruption("cold manifest temporary size changed after complete write"));
  try {
    std::vector<std::byte> readback(encoded.size());
    auto read = temporary->read_at(0U, readback);
    if (!read.has_value())
      return impl.fail(with_context("read cold manifest temporary", read.error()));
    if (*read != readback.size())
      return impl.fail(corruption("cold manifest temporary readback is incomplete"));
    auto decoded = decode_cold_location_manifest_v1_exact(readback, impl.config_.decode_limits);
    if (!decoded.has_value() || !std::ranges::equal(readback, encoded.bytes())) {
      return impl.fail(decoded.has_value() ? corruption("cold manifest temporary readback changed")
                                           : with_context("decode cold manifest temporary",
                                                          decoded.error().status()));
    }
    validation = validate_owner(*decoded, impl.config_);
    if (validation.is_ok())
      validation = validate_cold_location_manifest_binding(*decoded, base_manifest);
    if (!validation.is_ok())
      return impl.fail(with_context("validate cold manifest temporary authority", validation));
  } catch (const std::bad_alloc&) {
    return impl.fail(exhausted("cold manifest readback allocation failed"));
  } catch (const std::length_error&) {
    return impl.fail(exhausted("cold manifest readback exceeds limits"));
  }
  operation = temporary->sync_all();
  if (!operation.is_ok())
    return impl.fail(with_context("synchronize cold manifest temporary", operation));
  saturating_add(impl.metrics_.file_syncs, 1U);
  operation = temporary->close();
  if (!operation.is_ok())
    return impl.fail(with_context("close cold manifest temporary", operation));
  operation = impl.directory_.rename_no_replace({.old_name = temp_name, .new_name = *final_name});
  if (!operation.is_ok())
    return impl.fail(with_context("install cold manifest final name", operation));
  operation = impl.directory_.sync();
  if (!operation.is_ok()) {
    impl.poison_ = with_context("synchronize cold manifest directory after install", operation);
    return impl.fail(impl.poison_);
  }
  saturating_add(impl.metrics_.directory_syncs, 1U);
  saturating_add(impl.metrics_.installed_generations, 1U);
  saturating_add(impl.metrics_.installed_bytes, static_cast<std::uint64_t>(encoded.size()));
  return InstalledColdLocationManifest{.file_name = *final_name,
                                       .generation = candidate->generation(),
                                       .base_manifest_generation =
                                           candidate->base_manifest_generation(),
                                       .location_count = candidate->locations().size(),
                                       .already_present = false};
}

common::Result<std::optional<LoadedColdLocationManifest>>
ColdLocationManifestStorage::load_selected(
    const manifest::DecodedTemporalManifestView& base_manifest) const {
  if (impl_ == nullptr)
    return common::make_unexpected(invalid("cold manifest storage was moved from"));
  common::Status usable = impl_->check_usable();
  if (!usable.is_ok())
    return common::make_unexpected(std::move(usable));
  auto generations = impl_->scan_generations();
  if (!generations.has_value())
    return common::make_unexpected(generations.error());
  if (generations->empty())
    return std::optional<LoadedColdLocationManifest>{};
  auto loaded = impl_->load_generation(generations->back(), &base_manifest);
  if (!loaded.has_value())
    return common::make_unexpected(loaded.error());
  return std::optional<LoadedColdLocationManifest>{std::move(*loaded)};
}

bool ColdLocationManifestStorage::is_usable() const noexcept {
  return impl_ != nullptr && impl_->poison_.is_ok();
}

common::Status ColdLocationManifestStorage::poison_status() const {
  return impl_ == nullptr ? invalid("cold manifest storage was moved from") : impl_->poison_;
}

ColdLocationManifestStorageMetrics ColdLocationManifestStorage::metrics() const noexcept {
  return impl_ == nullptr ? ColdLocationManifestStorageMetrics{} : impl_->metrics_;
}

} // namespace chronos::tiering
