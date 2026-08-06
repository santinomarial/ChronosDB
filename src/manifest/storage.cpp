#include "chronos/manifest/storage.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/io/posix_io.hpp"
#include "chronos/manifest/naming.hpp"
#include "io/posix_syscalls.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::manifest {
namespace {

[[nodiscard]] common::Status invalid(const std::string_view message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::string{message}};
}

[[nodiscard]] common::Status corruption(const std::string_view message) {
  return common::Status{common::StatusCode::kCorruption, std::string{message}};
}

[[nodiscard]] common::Status with_context(const std::string_view context,
                                          const common::Status& cause) {
  std::string message{context};
  message.append(": ");
  message.append(cause.message());
  return common::Status{cause.code(), std::move(message)};
}

struct FileReadRequest {
  std::string_view name;
  std::uint64_t maximum_length{};
  std::optional<std::uint64_t> exact_length;
  std::string_view description;
};

[[nodiscard]] common::Result<std::vector<std::byte>>
read_final_file(const io::PosixDirectory& directory, const FileReadRequest& request) {
  common::Result<io::PosixFile> file =
      directory.open_regular_file(request.name, io::FileOpenMode::kReadOnly);
  if (!file.has_value()) {
    if (file.error().code() == common::StatusCode::kNotFound) {
      return common::make_unexpected(
          corruption(std::string{request.description}.append(" is missing")));
    }
    return common::make_unexpected(
        with_context(std::string{"open "}.append(request.description), file.error()));
  }
  const common::Result<std::uint64_t> size = file->size();
  if (!size.has_value()) {
    return common::make_unexpected(with_context(
        std::string{"read "}.append(request.description).append(" size"), size.error()));
  }
  if (request.exact_length.has_value() && *size != *request.exact_length) {
    return common::make_unexpected(
        corruption(std::string{request.description}.append(" has an unexpected length")));
  }
  if (*size > request.maximum_length ||
      *size > static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kResourceExhausted,
        std::string{request.description}.append(" exceeds the configured read limit")});
  }
  std::vector<std::byte> image;
  try {
    image.resize(static_cast<std::size_t>(*size));
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kResourceExhausted,
        std::string{"Cannot allocate "}.append(request.description).append(" readback")});
  }
  const common::Result<std::size_t> read = file->read_at(0U, image);
  if (!read.has_value()) {
    return common::make_unexpected(
        with_context(std::string{"read "}.append(request.description), read.error()));
  }
  if (*read != image.size()) {
    return common::make_unexpected(
        corruption(std::string{request.description}.append(" ended before its exact file size")));
  }
  common::Status close = file->close();
  if (!close.is_ok()) {
    return common::make_unexpected(
        with_context(std::string{"close "}.append(request.description), close));
  }
  return image;
}

[[nodiscard]] common::Status manifest_decode_failure(const ManifestDecodeError& error,
                                                     const std::string_view description) {
  if (error.kind() == ManifestDecodeErrorKind::kIncomplete) {
    return corruption(std::string{description}.append(" is incomplete"));
  }
  return with_context(std::string{"decode "}.append(description), error.status());
}

[[nodiscard]] const TabletSchemaBinding*
find_binding(const std::span<const TabletSchemaBinding> bindings,
             const schema::TabletId& tablet_id) noexcept {
  const auto found =
      std::ranges::lower_bound(bindings, tablet_id, {}, [](const TabletSchemaBinding& binding) {
        return binding.tablet_id;
      });
  return found != bindings.end() && found->tablet_id == tablet_id ? &*found : nullptr;
}

void saturating_add(std::uint64_t& target, const std::uint64_t value) noexcept {
  const std::uint64_t maximum = std::numeric_limits<std::uint64_t>::max();
  target = target > maximum - value ? maximum : target + value;
}

[[nodiscard]] common::Status validate_config(const ManifestStorageConfig& config) {
  if (config.database_root.empty() || config.database_root.find('\0') != std::string::npos) {
    return invalid("Manifest storage database root must be a nonempty path without NUL");
  }
  if ((config.file_permissions & static_cast<std::uint16_t>(~0777U)) != 0U) {
    return invalid("Manifest storage file permissions contain bits outside 0777");
  }
  return common::Status::ok();
}

} // namespace

class LoadedManifestGeneration::Impl {
public:
  Impl(std::vector<std::byte> encoded_bytes, DecodedManifestView decoded,
       std::vector<cseg::PartId> orphan_parts, std::vector<std::string> temporary_parts,
       std::vector<std::string> temporary_manifests) noexcept
      : encoded_bytes_(std::move(encoded_bytes)), decoded_(std::move(decoded)),
        orphan_parts_(std::move(orphan_parts)), temporary_parts_(std::move(temporary_parts)),
        temporary_manifests_(std::move(temporary_manifests)) {}

  // The decoded view borrows this allocation. Vector move transfers its element references, and
  // member order ensures the view is destroyed before the owning bytes.
  std::vector<std::byte> encoded_bytes_;
  DecodedManifestView decoded_;
  std::vector<cseg::PartId> orphan_parts_;
  std::vector<std::string> temporary_parts_;
  std::vector<std::string> temporary_manifests_;
};

LoadedManifestGeneration::LoadedManifestGeneration(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}

LoadedManifestGeneration::~LoadedManifestGeneration() = default;
LoadedManifestGeneration::LoadedManifestGeneration(LoadedManifestGeneration&&) noexcept = default;
LoadedManifestGeneration&
LoadedManifestGeneration::operator=(LoadedManifestGeneration&&) noexcept = default;

std::uint64_t LoadedManifestGeneration::generation() const noexcept {
  return implementation_->decoded_.generation();
}

std::uint64_t LoadedManifestGeneration::previous_generation() const noexcept {
  return implementation_->decoded_.previous_generation();
}

const DatabaseId& LoadedManifestGeneration::database_id() const noexcept {
  return implementation_->decoded_.database_id();
}

const wal::WalId& LoadedManifestGeneration::wal_id() const noexcept {
  return implementation_->decoded_.wal_id();
}

const WalCheckpoint& LoadedManifestGeneration::reclaim_checkpoint() const noexcept {
  return implementation_->decoded_.reclaim_checkpoint();
}

std::span<const TabletDescriptor> LoadedManifestGeneration::tablets() const noexcept {
  return implementation_->decoded_.tablets();
}

std::span<const PartDescriptor> LoadedManifestGeneration::parts() const noexcept {
  return implementation_->decoded_.parts();
}

std::span<const RetryDescriptor> LoadedManifestGeneration::retries() const noexcept {
  return implementation_->decoded_.retries();
}

common::ByteView LoadedManifestGeneration::encoded_bytes() const noexcept {
  return implementation_->encoded_bytes_;
}

std::span<const cseg::PartId> LoadedManifestGeneration::orphan_parts() const noexcept {
  return implementation_->orphan_parts_;
}

std::span<const std::string> LoadedManifestGeneration::temporary_parts() const noexcept {
  return implementation_->temporary_parts_;
}

std::span<const std::string> LoadedManifestGeneration::temporary_manifests() const noexcept {
  return implementation_->temporary_manifests_;
}

class ManifestStorage::Impl {
public:
  Impl(io::PosixDirectory root, io::PosixDirectory parts, io::PosixDirectory manifests,
       io::PosixAdvisoryLock lock, const std::uint16_t file_permissions) noexcept
      : root_(std::move(root)), parts_(std::move(parts)), manifests_(std::move(manifests)),
        lock_(std::move(lock)), file_permissions_(file_permissions) {}

  [[nodiscard]] common::Result<InstalledPart> fail(common::Status failure) {
    ++metrics_.failures;
    return common::make_unexpected(std::move(failure));
  }

  [[nodiscard]] common::Result<InstalledManifest> fail_manifest(common::Status failure) {
    ++manifest_metrics_.failures;
    return common::make_unexpected(std::move(failure));
  }

  io::PosixDirectory root_;
  io::PosixDirectory parts_;
  io::PosixDirectory manifests_;
  io::PosixAdvisoryLock lock_;
  std::uint16_t file_permissions_;
  bool poisoned_{false};
  common::Status poison_status_;
  PartInstallationMetrics metrics_;
  ManifestInstallationMetrics manifest_metrics_;
};

ManifestStorage::ManifestStorage(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}

ManifestStorage::~ManifestStorage() = default;
ManifestStorage::ManifestStorage(ManifestStorage&&) noexcept = default;
ManifestStorage& ManifestStorage::operator=(ManifestStorage&&) noexcept = default;

common::Result<ManifestStorage>
ManifestStorage::open_existing(const ManifestStorageConfig& config) {
  return open_existing_with(config, io::detail::system_posix_syscalls());
}

common::Result<ManifestStorage>
ManifestStorage::open_existing_with(const ManifestStorageConfig& config,
                                    io::detail::PosixSyscalls& syscalls) {
  common::Status config_status = validate_config(config);
  if (!config_status.is_ok()) {
    return common::make_unexpected(std::move(config_status));
  }
  common::Result<io::PosixDirectory> root =
      io::detail::PosixHandleFactory::open_directory(config.database_root, syscalls);
  if (!root.has_value()) {
    return common::make_unexpected(with_context("open manifest database root", root.error()));
  }
  common::Result<io::PosixDirectory> parts = root->open_directory(kPartsDirectoryName);
  if (!parts.has_value()) {
    return common::make_unexpected(with_context("open manifest parts directory", parts.error()));
  }
  common::Result<io::PosixDirectory> manifests = root->open_directory(kManifestDirectoryName);
  if (!manifests.has_value()) {
    return common::make_unexpected(
        with_context("open manifest generation directory", manifests.error()));
  }
  common::Result<io::PosixAdvisoryLock> lock =
      manifests->acquire_existing_exclusive_lock(kManifestLockFileName);
  if (!lock.has_value()) {
    return common::make_unexpected(with_context("acquire manifest writer lock", lock.error()));
  }
  try {
    return ManifestStorage{std::make_unique<Impl>(std::move(*root), std::move(*parts),
                                                  std::move(*manifests), std::move(*lock),
                                                  config.file_permissions)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "Cannot allocate Manifest storage owner"});
  }
}

common::Result<InstalledPart> ManifestStorage::install_part(const PartInstallRequest& request) {
  if (!implementation_) {
    return common::make_unexpected(invalid("Manifest storage owner was moved from"));
  }
  Impl& implementation = *implementation_;
  ++implementation.metrics_.attempts;
  if (implementation.poisoned_) {
    std::string message{"Manifest storage owner is poisoned: "};
    message.append(implementation.poison_status_.message());
    return implementation.fail(
        common::Status{common::StatusCode::kUnavailable, std::move(message)});
  }
  if (request.nonce.is_nil()) {
    return implementation.fail(invalid("Part installation nonce must be nonzero"));
  }

  const cseg::EncodedCsegPart& encoded = request.encoded_part.get();
  const std::string final_name = part_file_name(request.descriptor.part_id);
  const std::string temporary_name =
      temporary_part_file_name(request.descriptor.part_id, request.nonce);
  common::Status validation = validate_manifest_v1_part_image(
      request.descriptor, request.wal_id, request.schema.get(),
      {.file_name = final_name, .bytes = encoded.bytes()}, request.validation_limits);
  if (!validation.is_ok()) {
    return implementation.fail(with_context("prevalidate CSEG part installation", validation));
  }

  common::Result<io::PosixFile> temporary = implementation.parts_.create_exclusive_regular_file(
      temporary_name, implementation.file_permissions_);
  if (!temporary.has_value()) {
    return implementation.fail(with_context("create temporary CSEG part", temporary.error()));
  }
  common::Status operation = temporary->write_all_at(0U, encoded.bytes());
  if (!operation.is_ok()) {
    return implementation.fail(with_context("write temporary CSEG part", operation));
  }
  const common::Result<std::uint64_t> size = temporary->size();
  if (!size.has_value()) {
    return implementation.fail(with_context("read temporary CSEG part size", size.error()));
  }
  if (*size != request.descriptor.file_length) {
    return implementation.fail(common::Status{
        common::StatusCode::kIoError, "Temporary CSEG part size changed after complete write"});
  }

  std::vector<std::byte> readback;
  try {
    readback.resize(encoded.size());
  } catch (const std::bad_alloc&) {
    return implementation.fail(common::Status{common::StatusCode::kResourceExhausted,
                                              "Cannot allocate CSEG installation readback"});
  }
  const common::Result<std::size_t> read = temporary->read_at(0U, readback);
  if (!read.has_value()) {
    return implementation.fail(with_context("read back temporary CSEG part", read.error()));
  }
  if (*read != readback.size()) {
    return implementation.fail(common::Status{
        common::StatusCode::kIoError, "Temporary CSEG part readback ended before exact file size"});
  }
  validation = validate_manifest_v1_part_image(
      request.descriptor, request.wal_id, request.schema.get(),
      {.file_name = final_name, .bytes = readback}, request.validation_limits);
  if (!validation.is_ok()) {
    return implementation.fail(with_context("validate temporary CSEG part readback", validation));
  }

  operation = temporary->sync_all();
  if (!operation.is_ok()) {
    return implementation.fail(with_context("synchronize temporary CSEG part", operation));
  }
  ++implementation.metrics_.file_syncs;
  operation = temporary->close();
  if (!operation.is_ok()) {
    return implementation.fail(with_context("close synchronized temporary CSEG part", operation));
  }
  operation =
      implementation.parts_.rename_no_replace({.old_name = temporary_name, .new_name = final_name});
  if (!operation.is_ok()) {
    return implementation.fail(with_context("install CSEG part final name", operation));
  }
  operation = implementation.parts_.sync();
  if (!operation.is_ok()) {
    implementation.poisoned_ = true;
    implementation.poison_status_ =
        with_context("synchronize parts directory after CSEG install", operation);
    return implementation.fail(implementation.poison_status_);
  }
  ++implementation.metrics_.directory_syncs;
  ++implementation.metrics_.installed_parts;
  const std::uint64_t maximum = std::numeric_limits<std::uint64_t>::max();
  implementation.metrics_.installed_bytes =
      implementation.metrics_.installed_bytes > maximum - request.descriptor.file_length
          ? maximum
          : implementation.metrics_.installed_bytes + request.descriptor.file_length;
  return InstalledPart{.file_name = final_name, .descriptor = request.descriptor};
}

common::Result<InstalledManifest>
ManifestStorage::install_manifest(const ManifestInstallRequest& request) {
  if (!implementation_) {
    return common::make_unexpected(invalid("Manifest storage owner was moved from"));
  }
  Impl& implementation = *implementation_;
  ++implementation.manifest_metrics_.attempts;
  if (implementation.poisoned_) {
    std::string message{"Manifest storage owner is poisoned: "};
    message.append(implementation.poison_status_.message());
    return implementation.fail_manifest(
        common::Status{common::StatusCode::kUnavailable, std::move(message)});
  }
  if (request.nonce.is_nil()) {
    return implementation.fail_manifest(invalid("Manifest installation nonce must be nonzero"));
  }

  const EncodedManifest& encoded = request.encoded_manifest.get();
  ManifestDecodeResult candidate = decode_manifest_v1_exact(encoded.bytes(), request.decode_limits);
  if (!candidate.has_value()) {
    return implementation.fail_manifest(
        manifest_decode_failure(candidate.error(), "candidate Manifest generation"));
  }
  common::Result<ManifestNamespaceSnapshot> snapshot = scan_namespace();
  if (!snapshot.has_value()) {
    return implementation.fail_manifest(snapshot.error());
  }
  const std::uint64_t predecessor_generation = snapshot->generations.back();
  const common::Result<std::string> predecessor_name = manifest_file_name(predecessor_generation);
  if (!predecessor_name.has_value()) {
    return implementation.fail_manifest(common::Status{
        common::StatusCode::kInternal, "Selected Manifest generation cannot be formatted"});
  }
  common::Result<std::vector<std::byte>> predecessor_bytes = read_final_file(
      implementation.manifests_, {.name = *predecessor_name,
                                  .maximum_length = request.decode_limits.max_file_length,
                                  .exact_length = std::nullopt,
                                  .description = "selected final Manifest generation"});
  if (!predecessor_bytes.has_value()) {
    return implementation.fail_manifest(predecessor_bytes.error());
  }
  ManifestDecodeResult predecessor =
      decode_manifest_v1_exact(*predecessor_bytes, request.decode_limits);
  if (!predecessor.has_value()) {
    return implementation.fail_manifest(
        manifest_decode_failure(predecessor.error(), "selected final Manifest generation"));
  }
  if (predecessor->generation() != predecessor_generation) {
    return implementation.fail_manifest(
        corruption("Selected final Manifest filename disagrees with its encoded generation"));
  }

  common::Status validation =
      validate_manifest_v1_transition(*predecessor, *candidate, request.schema_bindings);
  if (!validation.is_ok()) {
    return implementation.fail_manifest(
        with_context("validate Manifest generation transition", validation));
  }
  for (const PartDescriptor& descriptor : candidate->parts()) {
    if (!std::ranges::binary_search(snapshot->final_parts, descriptor.part_id)) {
      return implementation.fail_manifest(
          corruption("Manifest candidate references a missing final CSEG part"));
    }
    const TabletSchemaBinding* binding =
        find_binding(request.schema_bindings, descriptor.tablet_id);
    if (binding == nullptr) {
      return implementation.fail_manifest(common::Status{
          common::StatusCode::kInternal, "Validated Manifest schema binding became inaccessible"});
    }
    const std::shared_ptr<const schema::TableSchema> schema_value =
        binding->lineage.get().find(descriptor.schema_id);
    if (!schema_value) {
      return implementation.fail_manifest(common::Status{
          common::StatusCode::kInternal, "Validated Manifest part schema became inaccessible"});
    }
    const std::string file_name = part_file_name(descriptor.part_id);
    common::Result<std::vector<std::byte>> part_bytes =
        read_final_file(implementation.parts_,
                        {.name = file_name,
                         .maximum_length = request.part_validation_limits.decode.max_file_length,
                         .exact_length = descriptor.file_length,
                         .description = "referenced final CSEG part"});
    if (!part_bytes.has_value()) {
      return implementation.fail_manifest(part_bytes.error());
    }
    validation = validate_manifest_v1_part_image(descriptor, candidate->wal_id(), *schema_value,
                                                 {.file_name = file_name, .bytes = *part_bytes},
                                                 request.part_validation_limits);
    if (!validation.is_ok()) {
      return implementation.fail_manifest(
          with_context("validate referenced final CSEG part", validation));
    }
    saturating_add(implementation.manifest_metrics_.referenced_parts_validated, 1U);
  }

  const common::Result<std::string> final_name = manifest_file_name(candidate->generation());
  const common::Result<std::string> temporary_name =
      temporary_manifest_file_name(candidate->generation(), request.nonce);
  if (!final_name.has_value() || !temporary_name.has_value()) {
    return implementation.fail_manifest(common::Status{
        common::StatusCode::kInternal, "Validated Manifest generation names cannot be formatted"});
  }
  common::Result<io::PosixFile> temporary = implementation.manifests_.create_exclusive_regular_file(
      *temporary_name, implementation.file_permissions_);
  if (!temporary.has_value()) {
    return implementation.fail_manifest(
        with_context("create temporary Manifest generation", temporary.error()));
  }
  common::Status operation = temporary->write_all_at(0U, encoded.bytes());
  if (!operation.is_ok()) {
    return implementation.fail_manifest(
        with_context("write temporary Manifest generation", operation));
  }
  const common::Result<std::uint64_t> temporary_size = temporary->size();
  if (!temporary_size.has_value()) {
    return implementation.fail_manifest(
        with_context("read temporary Manifest generation size", temporary_size.error()));
  }
  if (*temporary_size != encoded.size()) {
    return implementation.fail_manifest(
        common::Status{common::StatusCode::kIoError,
                       "Temporary Manifest generation size changed after complete write"});
  }
  std::vector<std::byte> readback;
  try {
    readback.resize(encoded.size());
  } catch (const std::bad_alloc&) {
    return implementation.fail_manifest(
        common::Status{common::StatusCode::kResourceExhausted,
                       "Cannot allocate Manifest generation installation readback"});
  }
  const common::Result<std::size_t> read = temporary->read_at(0U, readback);
  if (!read.has_value()) {
    return implementation.fail_manifest(
        with_context("read back temporary Manifest generation", read.error()));
  }
  if (*read != readback.size()) {
    return implementation.fail_manifest(
        common::Status{common::StatusCode::kIoError,
                       "Temporary Manifest generation readback ended before exact file size"});
  }
  ManifestDecodeResult decoded_readback = decode_manifest_v1_exact(readback, request.decode_limits);
  if (!decoded_readback.has_value()) {
    return implementation.fail_manifest(
        manifest_decode_failure(decoded_readback.error(), "temporary Manifest generation"));
  }
  if (!std::ranges::equal(readback, encoded.bytes())) {
    return implementation.fail_manifest(
        corruption("Temporary Manifest generation readback differs from candidate bytes"));
  }

  operation = temporary->sync_all();
  if (!operation.is_ok()) {
    return implementation.fail_manifest(
        with_context("synchronize temporary Manifest generation", operation));
  }
  ++implementation.manifest_metrics_.file_syncs;
  operation = temporary->close();
  if (!operation.is_ok()) {
    return implementation.fail_manifest(
        with_context("close synchronized temporary Manifest generation", operation));
  }
  operation = implementation.manifests_.rename_no_replace(
      {.old_name = *temporary_name, .new_name = *final_name});
  if (!operation.is_ok()) {
    return implementation.fail_manifest(
        with_context("install Manifest generation final name", operation));
  }
  operation = implementation.manifests_.sync();
  if (!operation.is_ok()) {
    implementation.poisoned_ = true;
    implementation.poison_status_ =
        with_context("synchronize Manifest directory after generation install", operation);
    return implementation.fail_manifest(implementation.poison_status_);
  }
  ++implementation.manifest_metrics_.directory_syncs;
  ++implementation.manifest_metrics_.installed_generations;
  saturating_add(implementation.manifest_metrics_.installed_bytes,
                 static_cast<std::uint64_t>(encoded.size()));
  return InstalledManifest{
      .file_name = *final_name,
      .generation = candidate->generation(),
      .reclaim_checkpoint = candidate->reclaim_checkpoint(),
      .tablet_count = static_cast<std::uint64_t>(candidate->tablets().size()),
      .part_count = static_cast<std::uint64_t>(candidate->parts().size()),
      .retry_count = static_cast<std::uint64_t>(candidate->retries().size()),
  };
}

common::Result<ManifestNamespaceSnapshot> ManifestStorage::scan_namespace() const {
  if (!implementation_) {
    return common::make_unexpected(invalid("Manifest storage owner was moved from"));
  }
  if (implementation_->poisoned_) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kUnavailable, "Manifest storage owner is poisoned"});
  }
  const common::Result<std::vector<io::DirectoryEntry>> part_entries =
      implementation_->parts_.list_entries();
  if (!part_entries.has_value()) {
    return common::make_unexpected(
        with_context("list Manifest parts directory", part_entries.error()));
  }
  const common::Result<std::vector<io::DirectoryEntry>> manifest_entries =
      implementation_->manifests_.list_entries();
  if (!manifest_entries.has_value()) {
    return common::make_unexpected(
        with_context("list Manifest generation directory", manifest_entries.error()));
  }

  ManifestNamespaceSnapshot snapshot;
  try {
    for (const io::DirectoryEntry& entry : *part_entries) {
      if (entry.type != io::DirectoryEntryType::kRegularFile) {
        return common::make_unexpected(
            corruption("Manifest parts directory contains a non-regular entry"));
      }
      const common::Result<cseg::PartId> final = parse_part_file_name(entry.name);
      if (final.has_value()) {
        snapshot.final_parts.push_back(*final);
        continue;
      }
      if (parse_temporary_part_file_name(entry.name).has_value()) {
        snapshot.temporary_parts.push_back(entry.name);
        continue;
      }
      return common::make_unexpected(
          corruption("Manifest parts directory contains an unrelated or malformed entry"));
    }

    bool saw_lock = false;
    for (const io::DirectoryEntry& entry : *manifest_entries) {
      if (entry.name == kManifestLockFileName) {
        if (entry.type != io::DirectoryEntryType::kRegularFile) {
          return common::make_unexpected(corruption("Manifest LOCK is not a regular file"));
        }
        saw_lock = true;
        continue;
      }
      if (entry.type != io::DirectoryEntryType::kRegularFile) {
        return common::make_unexpected(
            corruption("Manifest generation directory contains a non-regular entry"));
      }
      const common::Result<std::uint64_t> generation = parse_manifest_file_name(entry.name);
      if (generation.has_value()) {
        snapshot.generations.push_back(*generation);
        continue;
      }
      if (parse_temporary_manifest_file_name(entry.name).has_value()) {
        snapshot.temporary_manifests.push_back(entry.name);
        continue;
      }
      return common::make_unexpected(
          corruption("Manifest generation directory contains an unrelated or malformed entry"));
    }
    if (!saw_lock) {
      return common::make_unexpected(corruption("Manifest generation directory is missing LOCK"));
    }
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "Cannot allocate Manifest namespace snapshot"});
  }

  if (snapshot.generations.empty() || snapshot.generations.front() != 1U) {
    return common::make_unexpected(
        corruption("Manifest final generations must begin at generation one"));
  }
  for (std::size_t index = 1U; index < snapshot.generations.size(); ++index) {
    const std::optional<std::uint64_t> expected =
        common::checked_add(snapshot.generations[index - 1U], std::uint64_t{1U});
    if (!expected.has_value() || snapshot.generations[index] != *expected) {
      return common::make_unexpected(corruption("Manifest final generations are not consecutive"));
    }
  }
  return snapshot;
}

common::Result<TemporaryCleanupReport> ManifestStorage::cleanup_temporaries() {
  common::Result<ManifestNamespaceSnapshot> snapshot = scan_namespace();
  if (!snapshot.has_value()) {
    return common::make_unexpected(snapshot.error());
  }
  TemporaryCleanupReport report;
  for (const std::string& name : snapshot->temporary_parts) {
    common::Status removal = implementation_->parts_.remove_file(name);
    if (!removal.is_ok()) {
      return common::make_unexpected(with_context("remove temporary CSEG part", removal));
    }
    ++report.removed_parts;
  }
  if (report.removed_parts != 0U) {
    common::Status sync = implementation_->parts_.sync();
    if (!sync.is_ok()) {
      implementation_->poisoned_ = true;
      implementation_->poison_status_ =
          with_context("synchronize parts directory after temporary cleanup", sync);
      return common::make_unexpected(implementation_->poison_status_);
    }
    ++report.directory_syncs;
  }
  for (const std::string& name : snapshot->temporary_manifests) {
    common::Status removal = implementation_->manifests_.remove_file(name);
    if (!removal.is_ok()) {
      return common::make_unexpected(with_context("remove temporary Manifest generation", removal));
    }
    ++report.removed_manifests;
  }
  if (report.removed_manifests != 0U) {
    common::Status sync = implementation_->manifests_.sync();
    if (!sync.is_ok()) {
      implementation_->poisoned_ = true;
      implementation_->poison_status_ =
          with_context("synchronize Manifest directory after temporary cleanup", sync);
      return common::make_unexpected(implementation_->poison_status_);
    }
    ++report.directory_syncs;
  }
  return report;
}

common::Result<LoadedManifestGeneration>
ManifestStorage::load_selected_manifest(const ManifestLoadRequest& request) const {
  common::Result<ManifestNamespaceSnapshot> snapshot = scan_namespace();
  if (!snapshot.has_value()) {
    return common::make_unexpected(snapshot.error());
  }
  const std::uint64_t selected_generation = snapshot->generations.back();
  const common::Result<std::string> selected_name = manifest_file_name(selected_generation);
  if (!selected_name.has_value()) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInternal, "Selected Manifest generation cannot be formatted"});
  }
  common::Result<std::vector<std::byte>> encoded = read_final_file(
      implementation_->manifests_, {.name = *selected_name,
                                    .maximum_length = request.decode_limits.max_file_length,
                                    .exact_length = std::nullopt,
                                    .description = "selected final Manifest generation"});
  if (!encoded.has_value()) {
    return common::make_unexpected(encoded.error());
  }
  ManifestDecodeResult decoded = decode_manifest_v1_exact(*encoded, request.decode_limits);
  if (!decoded.has_value()) {
    return common::make_unexpected(
        manifest_decode_failure(decoded.error(), "selected final Manifest generation"));
  }
  if (decoded->generation() != selected_generation) {
    return common::make_unexpected(
        corruption("Selected final Manifest filename disagrees with its encoded generation"));
  }
  if (decoded->database_id() != request.expected_database_id ||
      decoded->wal_id() != request.expected_wal_id) {
    return common::make_unexpected(
        invalid("Selected Manifest database or WAL identity does not match recovery context"));
  }
  common::Status validation =
      validate_manifest_v1_schema_binding(*decoded, request.schema_bindings);
  if (!validation.is_ok()) {
    return common::make_unexpected(
        with_context("bind selected Manifest to retained catalog", validation));
  }

  for (const PartDescriptor& descriptor : decoded->parts()) {
    if (!std::ranges::binary_search(snapshot->final_parts, descriptor.part_id)) {
      return common::make_unexpected(
          corruption("Selected Manifest references a missing final CSEG part"));
    }
    const TabletSchemaBinding* binding =
        find_binding(request.schema_bindings, descriptor.tablet_id);
    if (binding == nullptr) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kInternal, "Validated Manifest schema binding became inaccessible"});
    }
    const std::shared_ptr<const schema::TableSchema> schema_value =
        binding->lineage.get().find(descriptor.schema_id);
    if (!schema_value) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kInternal, "Validated Manifest part schema became inaccessible"});
    }
    const std::string file_name = part_file_name(descriptor.part_id);
    common::Result<std::vector<std::byte>> part_bytes =
        read_final_file(implementation_->parts_,
                        {.name = file_name,
                         .maximum_length = request.part_validation_limits.decode.max_file_length,
                         .exact_length = descriptor.file_length,
                         .description = "referenced final CSEG part"});
    if (!part_bytes.has_value()) {
      return common::make_unexpected(part_bytes.error());
    }
    validation = validate_manifest_v1_part_image(descriptor, decoded->wal_id(), *schema_value,
                                                 {.file_name = file_name, .bytes = *part_bytes},
                                                 request.part_validation_limits);
    if (!validation.is_ok()) {
      return common::make_unexpected(
          with_context("validate referenced final CSEG part", validation));
    }
  }

  try {
    std::vector<cseg::PartId> referenced_parts;
    referenced_parts.reserve(decoded->parts().size());
    for (const PartDescriptor& descriptor : decoded->parts()) {
      referenced_parts.push_back(descriptor.part_id);
    }
    std::ranges::sort(referenced_parts);
    std::vector<cseg::PartId> orphan_parts;
    orphan_parts.reserve(snapshot->final_parts.size());
    for (const cseg::PartId& part_id : snapshot->final_parts) {
      if (!std::ranges::binary_search(referenced_parts, part_id)) {
        orphan_parts.push_back(part_id);
      }
    }
    return LoadedManifestGeneration{std::make_unique<LoadedManifestGeneration::Impl>(
        std::move(*encoded), std::move(*decoded), std::move(orphan_parts),
        std::move(snapshot->temporary_parts), std::move(snapshot->temporary_manifests))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "Cannot allocate loaded Manifest generation"});
  }
}

bool ManifestStorage::is_usable() const noexcept {
  return implementation_ && !implementation_->poisoned_;
}

common::Status ManifestStorage::poison_status() const {
  return implementation_ ? implementation_->poison_status_
                         : invalid("Manifest storage owner was moved from or is unavailable");
}

PartInstallationMetrics ManifestStorage::metrics() const noexcept {
  return implementation_ ? implementation_->metrics_ : PartInstallationMetrics{};
}

ManifestInstallationMetrics ManifestStorage::manifest_metrics() const noexcept {
  return implementation_ ? implementation_->manifest_metrics_ : ManifestInstallationMetrics{};
}

} // namespace chronos::manifest
