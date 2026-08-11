#include "chronos/manifest/storage.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/ingest/sha256.hpp"
#include "chronos/io/posix_io.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/publication.hpp"
#include "chronos/manifest/raft_tablet_physical_snapshot.hpp"
#include "io/posix_syscalls.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::manifest {
namespace {

inline constexpr std::size_t kConservativeAllocationOverheadBytes = 64U;

[[nodiscard]] std::size_t saturating_size_add(const std::size_t left,
                                              const std::size_t right) noexcept {
  return common::checked_add(left, right).value_or(std::numeric_limits<std::size_t>::max());
}

template <typename Value>
[[nodiscard]] std::size_t retained_vector_bytes(const std::vector<Value>& values) noexcept {
  return common::checked_multiply(values.capacity(), sizeof(Value))
      .value_or(std::numeric_limits<std::size_t>::max());
}

[[nodiscard]] std::size_t retained_strings_bytes(const std::vector<std::string>& values) noexcept {
  std::size_t total = retained_vector_bytes(values);
  for (const std::string& value : values) {
    total = saturating_size_add(total, value.capacity());
    total = saturating_size_add(total, kConservativeAllocationOverheadBytes);
  }
  return total;
}

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

[[nodiscard]] const PartDescriptor* find_part(const DecodedManifestView& manifest,
                                              const cseg::PartId& part_id) noexcept {
  const auto found = std::ranges::find(manifest.parts(), part_id, &PartDescriptor::part_id);
  return found == manifest.parts().end() ? nullptr : &*found;
}

[[nodiscard]] const TemporalTabletDescriptor*
find_temporal_tablet(const std::span<const TemporalTabletDescriptor> tablets,
                     const schema::TabletId& tablet_id) noexcept {
  const auto found =
      std::ranges::lower_bound(tablets, tablet_id, {}, [](const TemporalTabletDescriptor& tablet) {
        return tablet.tablet_id;
      });
  return found != tablets.end() && found->tablet_id == tablet_id ? &*found : nullptr;
}

struct OwnedCompactionImage {
  cseg::PartId part_id;
  std::vector<std::byte> bytes;
};

[[nodiscard]] common::Status validate_on_disk_compaction_equivalence(
    const io::PosixDirectory& parts_directory,
    const DecodedManifestView& predecessor, // NOLINT(bugprone-easily-swappable-parameters)
    const DecodedManifestView& candidate,
    const std::span<const TabletSchemaBinding> schema_bindings,
    const ManifestCompactionReplacement& replacement, const CompactionEquivalenceLimits limits) {
  const PartDescriptor* first_input = find_part(predecessor, replacement.input_part_ids.front());
  const TabletSchemaBinding* binding = nullptr;
  if (first_input != nullptr) {
    binding = find_binding(schema_bindings, first_input->tablet_id);
  }
  const std::shared_ptr<const schema::TableSchema> schema_value =
      binding == nullptr || first_input == nullptr
          ? nullptr
          : binding->lineage.get().find(first_input->schema_id);
  if (first_input == nullptr || schema_value == nullptr) {
    return corruption("Validated compaction input schema became inaccessible");
  }

  try {
    std::vector<OwnedCompactionImage> input_owners;
    std::vector<OwnedCompactionImage> output_owners;
    input_owners.reserve(replacement.input_part_ids.size());
    output_owners.reserve(replacement.output_part_ids.size());
    for (const cseg::PartId& part_id : replacement.input_part_ids) {
      const PartDescriptor* descriptor = find_part(predecessor, part_id);
      if (descriptor == nullptr) {
        return corruption("Validated compaction input descriptor became inaccessible");
      }
      common::Result<std::vector<std::byte>> bytes =
          read_final_file(parts_directory, {.name = part_file_name(part_id),
                                            .maximum_length = limits.decode.max_file_length,
                                            .exact_length = descriptor->file_length,
                                            .description = "compaction input CSEG part"});
      if (!bytes.has_value()) {
        return bytes.error();
      }
      input_owners.push_back({.part_id = part_id, .bytes = std::move(*bytes)});
    }
    for (const cseg::PartId& part_id : replacement.output_part_ids) {
      const PartDescriptor* descriptor = find_part(candidate, part_id);
      if (descriptor == nullptr) {
        return corruption("Validated compaction output descriptor became inaccessible");
      }
      common::Result<std::vector<std::byte>> bytes =
          read_final_file(parts_directory, {.name = part_file_name(part_id),
                                            .maximum_length = limits.decode.max_file_length,
                                            .exact_length = descriptor->file_length,
                                            .description = "compaction output CSEG part"});
      if (!bytes.has_value()) {
        return bytes.error();
      }
      output_owners.push_back({.part_id = part_id, .bytes = std::move(*bytes)});
    }
    std::vector<CompactionPartImage> inputs;
    std::vector<CompactionPartImage> outputs;
    inputs.reserve(input_owners.size());
    outputs.reserve(output_owners.size());
    for (const OwnedCompactionImage& owner : input_owners) {
      inputs.push_back({.part_id = owner.part_id, .bytes = owner.bytes});
    }
    for (const OwnedCompactionImage& owner : output_owners) {
      outputs.push_back({.part_id = owner.part_id, .bytes = owner.bytes});
    }
    return validate_append_only_cseg_v1_equivalence(
        inputs, outputs, *schema_value, replacement.tablet_id, predecessor.wal_id(), limits);
  } catch (const std::bad_alloc&) {
    return common::Status{common::StatusCode::kResourceExhausted,
                          "Cannot allocate on-disk compaction equivalence state"};
  } catch (const std::length_error&) {
    return common::Status{common::StatusCode::kResourceExhausted,
                          "On-disk compaction equivalence state exceeds container limits"};
  }
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

class LoadedTemporalManifestGeneration::Impl {
public:
  Impl(std::vector<std::byte> encoded_bytes, DecodedTemporalManifestView decoded,
       std::vector<cseg::PartId> orphan_parts, std::vector<std::string> temporary_parts,
       std::vector<std::string> temporary_manifests) noexcept
      : encoded_bytes_(std::move(encoded_bytes)), decoded_(std::move(decoded)),
        orphan_parts_(std::move(orphan_parts)), temporary_parts_(std::move(temporary_parts)),
        temporary_manifests_(std::move(temporary_manifests)) {}

  // The decoded view borrows this allocation. Member order destroys the view before its bytes.
  std::vector<std::byte> encoded_bytes_;
  DecodedTemporalManifestView decoded_;
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

std::size_t LoadedManifestGeneration::retained_buffer_bytes() const noexcept {
  std::size_t total = sizeof(LoadedManifestGeneration) + sizeof(Impl);
  total = saturating_size_add(total, retained_vector_bytes(implementation_->encoded_bytes_));
  total = saturating_size_add(total, implementation_->decoded_.retained_buffer_bytes());
  total = saturating_size_add(total, retained_vector_bytes(implementation_->orphan_parts_));
  total = saturating_size_add(total, retained_strings_bytes(implementation_->temporary_parts_));
  total = saturating_size_add(total, retained_strings_bytes(implementation_->temporary_manifests_));
  constexpr std::size_t owner_allocation_count = 9U;
  return saturating_size_add(total, owner_allocation_count * kConservativeAllocationOverheadBytes);
}

LoadedTemporalManifestGeneration::LoadedTemporalManifestGeneration(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}

LoadedTemporalManifestGeneration::~LoadedTemporalManifestGeneration() = default;
LoadedTemporalManifestGeneration::LoadedTemporalManifestGeneration(
    LoadedTemporalManifestGeneration&&) noexcept = default;
LoadedTemporalManifestGeneration&
LoadedTemporalManifestGeneration::operator=(LoadedTemporalManifestGeneration&&) noexcept = default;

std::uint64_t LoadedTemporalManifestGeneration::generation() const noexcept {
  return implementation_->decoded_.generation();
}

std::uint64_t LoadedTemporalManifestGeneration::previous_generation() const noexcept {
  return implementation_->decoded_.previous_generation();
}

const DatabaseId& LoadedTemporalManifestGeneration::database_id() const noexcept {
  return implementation_->decoded_.database_id();
}

const std::optional<TemporalWalReclaimCheckpoint>&
LoadedTemporalManifestGeneration::wal_reclaim_checkpoint() const noexcept {
  return implementation_->decoded_.wal_reclaim_checkpoint();
}

std::span<const TemporalTabletDescriptor>
LoadedTemporalManifestGeneration::tablets() const noexcept {
  return implementation_->decoded_.tablets();
}

std::span<const TemporalPartDescriptor> LoadedTemporalManifestGeneration::parts() const noexcept {
  return implementation_->decoded_.parts();
}

std::span<const TemporalRetryDescriptor>
LoadedTemporalManifestGeneration::retries() const noexcept {
  return implementation_->decoded_.retries();
}

common::ByteView LoadedTemporalManifestGeneration::encoded_bytes() const noexcept {
  return implementation_->encoded_bytes_;
}

std::span<const cseg::PartId> LoadedTemporalManifestGeneration::orphan_parts() const noexcept {
  return implementation_->orphan_parts_;
}

std::span<const std::string> LoadedTemporalManifestGeneration::temporary_parts() const noexcept {
  return implementation_->temporary_parts_;
}

std::span<const std::string>
LoadedTemporalManifestGeneration::temporary_manifests() const noexcept {
  return implementation_->temporary_manifests_;
}

std::size_t LoadedTemporalManifestGeneration::retained_buffer_bytes() const noexcept {
  std::size_t total = sizeof(LoadedTemporalManifestGeneration) + sizeof(Impl);
  total = saturating_size_add(total, retained_vector_bytes(implementation_->encoded_bytes_));
  total = saturating_size_add(total, implementation_->decoded_.retained_buffer_bytes());
  total = saturating_size_add(total, retained_vector_bytes(implementation_->orphan_parts_));
  total = saturating_size_add(total, retained_strings_bytes(implementation_->temporary_parts_));
  total = saturating_size_add(total, retained_strings_bytes(implementation_->temporary_manifests_));
  constexpr std::size_t owner_allocation_count = 9U;
  return saturating_size_add(total, owner_allocation_count * kConservativeAllocationOverheadBytes);
}

LoadedTemporalManifestMetadata::LoadedTemporalManifestMetadata(
    const std::uint64_t generation, std::vector<std::byte> encoded_bytes) noexcept
    : generation_(generation), encoded_bytes_(std::move(encoded_bytes)) {}

std::uint64_t LoadedTemporalManifestMetadata::generation() const noexcept {
  return generation_;
}

common::ByteView LoadedTemporalManifestMetadata::encoded_bytes() const noexcept {
  return encoded_bytes_;
}

LoadedPartImage::LoadedPartImage(PartDescriptor descriptor, std::vector<std::byte> bytes) noexcept
    : descriptor_(descriptor), bytes_(std::move(bytes)) {}

const PartDescriptor& LoadedPartImage::descriptor() const noexcept {
  return descriptor_;
}

common::ByteView LoadedPartImage::bytes() const noexcept {
  return bytes_;
}

LoadedTemporalPartImage::LoadedTemporalPartImage(
    std::shared_ptr<const LoadedTemporalManifestGeneration> generation,
    const TemporalPartDescriptor descriptor, std::vector<std::byte> bytes) noexcept
    : generation_(std::move(generation)), descriptor_(descriptor), bytes_(std::move(bytes)) {}

std::uint64_t LoadedTemporalPartImage::generation() const noexcept {
  return generation_->generation();
}

const TemporalPartDescriptor& LoadedTemporalPartImage::descriptor() const noexcept {
  return descriptor_;
}

common::ByteView LoadedTemporalPartImage::bytes() const noexcept {
  return bytes_;
}

std::size_t LoadedTemporalPartImage::retained_buffer_bytes() const noexcept {
  std::size_t total = sizeof(LoadedTemporalPartImage);
  total = saturating_size_add(total, bytes_.capacity());
  total = saturating_size_add(total, generation_->retained_buffer_bytes());
  return saturating_size_add(total, 2U * kConservativeAllocationOverheadBytes);
}

SnapshotPartImage::SnapshotPartImage(const DatabaseId database_id, const wal::WalId wal_id,
                                     const std::uint64_t snapshot_generation,
                                     const PartDescriptor descriptor, std::vector<std::byte> bytes,
                                     DatabaseStorageRetentionToken retention,
                                     const std::size_t snapshot_retained_buffer_bytes) noexcept
    : database_id_(database_id), wal_id_(wal_id), snapshot_generation_(snapshot_generation),
      descriptor_(descriptor), bytes_(std::move(bytes)), retention_(std::move(retention)),
      snapshot_retained_buffer_bytes_(snapshot_retained_buffer_bytes) {}

const DatabaseId& SnapshotPartImage::database_id() const noexcept {
  return database_id_;
}

const wal::WalId& SnapshotPartImage::wal_id() const noexcept {
  return wal_id_;
}

std::uint64_t SnapshotPartImage::snapshot_generation() const noexcept {
  return snapshot_generation_;
}

const PartDescriptor& SnapshotPartImage::descriptor() const noexcept {
  return descriptor_;
}

common::ByteView SnapshotPartImage::bytes() const noexcept {
  return bytes_;
}

std::size_t SnapshotPartImage::publication_retained_buffer_bytes() const noexcept {
  return snapshot_retained_buffer_bytes_;
}

std::size_t SnapshotPartImage::owned_retained_buffer_bytes() const noexcept {
  std::size_t total = bytes_.capacity();
  total = saturating_size_add(total, sizeof(SnapshotPartImage));
  return saturating_size_add(total, 128U);
}

std::size_t SnapshotPartImage::retained_buffer_bytes() const noexcept {
  return saturating_size_add(publication_retained_buffer_bytes(), owned_retained_buffer_bytes());
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

  [[nodiscard]] common::Result<InstalledTemporalPart> fail_temporal_part(common::Status failure) {
    ++metrics_.failures;
    return common::make_unexpected(std::move(failure));
  }

  [[nodiscard]] common::Result<InstalledTemporalManifest>
  fail_temporal_manifest(common::Status failure) {
    ++manifest_metrics_.failures;
    return common::make_unexpected(std::move(failure));
  }

  [[nodiscard]] common::Result<PartReclamationReport> fail_reclamation(common::Status failure) {
    ++reclamation_metrics_.failures;
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
  PartReclamationMetrics reclamation_metrics_;
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

common::Result<InstalledTemporalPart>
ManifestStorage::install_temporal_part(const TemporalPartInstallRequest& request) {
  if (!implementation_) {
    return common::make_unexpected(invalid("Manifest storage owner was moved from"));
  }
  Impl& implementation = *implementation_;
  ++implementation.metrics_.attempts;
  if (implementation.poisoned_) {
    std::string message{"Manifest storage owner is poisoned: "};
    message.append(implementation.poison_status_.message());
    return implementation.fail_temporal_part(
        common::Status{common::StatusCode::kUnavailable, std::move(message)});
  }
  if (request.nonce.is_nil()) {
    return implementation.fail_temporal_part(
        invalid("Temporal part installation nonce must be nonzero"));
  }

  const cseg::EncodedCsegPart& encoded = request.encoded_part.get();
  const std::string final_name = part_file_name(request.descriptor.part_id);
  const std::string temporary_name =
      temporary_part_file_name(request.descriptor.part_id, request.nonce);
  common::Status validation =
      validate_manifest_v2_temporal_part_image(request.descriptor, request.owner, encoded.bytes(),
                                               request.schema.get(), request.validation_limits);
  if (!validation.is_ok()) {
    return implementation.fail_temporal_part(
        with_context("prevalidate temporal CSEG part installation", validation));
  }

  // A crash may leave a fully durable final part before its successor Manifest is published.
  // Exact bytes are an idempotent retry; a same-identity difference is corruption.
  common::Result<io::PosixFile> existing =
      implementation.parts_.open_regular_file(final_name, io::FileOpenMode::kReadOnly);
  if (existing.has_value()) {
    const common::Result<std::uint64_t> existing_size = existing->size();
    if (!existing_size.has_value()) {
      return implementation.fail_temporal_part(
          with_context("read existing temporal CSEG size", existing_size.error()));
    }
    if (*existing_size != encoded.size()) {
      return implementation.fail_temporal_part(
          corruption("Existing temporal CSEG identity has a different size"));
    }
    std::vector<std::byte> existing_bytes;
    try {
      existing_bytes.resize(encoded.size());
    } catch (const std::bad_alloc&) {
      return implementation.fail_temporal_part(
          common::Status{common::StatusCode::kResourceExhausted,
                         "Cannot allocate existing temporal CSEG verification"});
    } catch (const std::length_error&) {
      return implementation.fail_temporal_part(
          common::Status{common::StatusCode::kResourceExhausted,
                         "Existing temporal CSEG verification exceeds container limits"});
    }
    const common::Result<std::size_t> existing_read = existing->read_at(0U, existing_bytes);
    if (!existing_read.has_value()) {
      return implementation.fail_temporal_part(
          with_context("read existing temporal CSEG", existing_read.error()));
    }
    if (*existing_read != existing_bytes.size() ||
        !std::ranges::equal(existing_bytes, encoded.bytes())) {
      return implementation.fail_temporal_part(
          corruption("Existing temporal CSEG identity has different durable bytes"));
    }
    validation =
        validate_manifest_v2_temporal_part_image(request.descriptor, request.owner, existing_bytes,
                                                 request.schema.get(), request.validation_limits);
    if (!validation.is_ok()) {
      return implementation.fail_temporal_part(
          with_context("validate existing temporal CSEG", validation));
    }
    return InstalledTemporalPart{.file_name = final_name, .descriptor = request.descriptor};
  }
  if (existing.error().code() != common::StatusCode::kNotFound) {
    return implementation.fail_temporal_part(
        with_context("open existing temporal CSEG", existing.error()));
  }

  common::Result<io::PosixFile> temporary = implementation.parts_.create_exclusive_regular_file(
      temporary_name, implementation.file_permissions_);
  if (!temporary.has_value()) {
    return implementation.fail_temporal_part(
        with_context("create temporary temporal CSEG part", temporary.error()));
  }
  common::Status operation = temporary->write_all_at(0U, encoded.bytes());
  if (!operation.is_ok()) {
    return implementation.fail_temporal_part(
        with_context("write temporary temporal CSEG part", operation));
  }
  const common::Result<std::uint64_t> size = temporary->size();
  if (!size.has_value()) {
    return implementation.fail_temporal_part(
        with_context("read temporary temporal CSEG part size", size.error()));
  }
  if (*size != request.descriptor.file_length) {
    return implementation.fail_temporal_part(
        common::Status{common::StatusCode::kIoError,
                       "Temporary temporal CSEG part size changed after complete write"});
  }

  std::vector<std::byte> readback;
  try {
    readback.resize(encoded.size());
  } catch (const std::bad_alloc&) {
    return implementation.fail_temporal_part(
        common::Status{common::StatusCode::kResourceExhausted,
                       "Cannot allocate temporal CSEG installation readback"});
  }
  const common::Result<std::size_t> read = temporary->read_at(0U, readback);
  if (!read.has_value()) {
    return implementation.fail_temporal_part(
        with_context("read back temporary temporal CSEG part", read.error()));
  }
  if (*read != readback.size()) {
    return implementation.fail_temporal_part(
        common::Status{common::StatusCode::kIoError,
                       "Temporary temporal CSEG part readback ended before exact file size"});
  }
  validation = validate_manifest_v2_temporal_part_image(
      request.descriptor, request.owner, readback, request.schema.get(), request.validation_limits);
  if (!validation.is_ok()) {
    return implementation.fail_temporal_part(
        with_context("validate temporary temporal CSEG part readback", validation));
  }

  operation = temporary->sync_all();
  if (!operation.is_ok()) {
    return implementation.fail_temporal_part(
        with_context("synchronize temporary temporal CSEG part", operation));
  }
  ++implementation.metrics_.file_syncs;
  operation = temporary->close();
  if (!operation.is_ok()) {
    return implementation.fail_temporal_part(
        with_context("close synchronized temporary temporal CSEG part", operation));
  }
  operation =
      implementation.parts_.rename_no_replace({.old_name = temporary_name, .new_name = final_name});
  if (!operation.is_ok()) {
    return implementation.fail_temporal_part(
        with_context("install temporal CSEG part final name", operation));
  }
  operation = implementation.parts_.sync();
  if (!operation.is_ok()) {
    implementation.poisoned_ = true;
    implementation.poison_status_ =
        with_context("synchronize parts directory after temporal CSEG install", operation);
    return implementation.fail_temporal_part(implementation.poison_status_);
  }
  ++implementation.metrics_.directory_syncs;
  ++implementation.metrics_.installed_parts;
  saturating_add(implementation.metrics_.installed_bytes, request.descriptor.file_length);
  return InstalledTemporalPart{.file_name = final_name, .descriptor = request.descriptor};
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
      request.compaction_replacement == nullptr
          ? validate_manifest_v1_transition(*predecessor, *candidate, request.schema_bindings)
          : validate_manifest_v1_compaction_transition(
                *predecessor, *candidate, request.schema_bindings, *request.compaction_replacement);
  if (!validation.is_ok()) {
    return implementation.fail_manifest(
        with_context("validate Manifest generation transition", validation));
  }
  if (request.compaction_replacement != nullptr) {
    validation = validate_on_disk_compaction_equivalence(
        implementation.parts_, *predecessor, *candidate, request.schema_bindings,
        *request.compaction_replacement, request.compaction_equivalence_limits);
    if (!validation.is_ok()) {
      return implementation.fail_manifest(
          with_context("prove on-disk CSEG compaction equivalence", validation));
    }
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

common::Result<InstalledTemporalManifest>
ManifestStorage::install_temporal_manifest(const TemporalManifestInstallRequest& request) {
  if (!implementation_) {
    return common::make_unexpected(invalid("Manifest storage owner was moved from"));
  }
  Impl& implementation = *implementation_;
  ++implementation.manifest_metrics_.attempts;
  if (implementation.poisoned_) {
    std::string message{"Manifest storage owner is poisoned: "};
    message.append(implementation.poison_status_.message());
    return implementation.fail_temporal_manifest(
        common::Status{common::StatusCode::kUnavailable, std::move(message)});
  }
  if (request.nonce.is_nil()) {
    return implementation.fail_temporal_manifest(
        invalid("Temporal Manifest installation nonce must be nonzero"));
  }

  const EncodedTemporalManifest& encoded = request.encoded_manifest.get();
  TemporalManifestDecodeResult candidate =
      decode_manifest_v2_temporal_exact(encoded.bytes(), request.decode_limits);
  if (!candidate.has_value()) {
    return implementation.fail_temporal_manifest(
        manifest_decode_failure(candidate.error(), "candidate Manifest v2 generation"));
  }
  common::Result<ManifestNamespaceSnapshot> snapshot = scan_namespace();
  if (!snapshot.has_value()) {
    return implementation.fail_temporal_manifest(snapshot.error());
  }
  const std::uint64_t predecessor_generation = snapshot->generations.back();
  const common::Result<std::string> predecessor_name = manifest_file_name(predecessor_generation);
  if (!predecessor_name.has_value()) {
    return implementation.fail_temporal_manifest(common::Status{
        common::StatusCode::kInternal, "Selected Manifest v2 generation cannot be formatted"});
  }
  common::Result<std::vector<std::byte>> predecessor_bytes = read_final_file(
      implementation.manifests_, {.name = *predecessor_name,
                                  .maximum_length = request.decode_limits.max_file_length,
                                  .exact_length = std::nullopt,
                                  .description = "selected final Manifest v2 generation"});
  if (!predecessor_bytes.has_value()) {
    return implementation.fail_temporal_manifest(predecessor_bytes.error());
  }
  TemporalManifestDecodeResult predecessor =
      decode_manifest_v2_temporal_exact(*predecessor_bytes, request.decode_limits);
  if (!predecessor.has_value()) {
    return implementation.fail_temporal_manifest(
        manifest_decode_failure(predecessor.error(), "selected final Manifest v2 generation"));
  }
  if (predecessor->generation() != predecessor_generation) {
    return implementation.fail_temporal_manifest(
        corruption("Selected final Manifest v2 filename disagrees with its encoded generation"));
  }

  common::Status validation;
  if (request.source_retirement == nullptr) {
    validation =
        validate_manifest_v2_temporal_transition(*predecessor, *candidate, request.schema_bindings);
  } else {
    common::Result<BuiltRaftTabletSourceRetirementManifest> rebuilt =
        build_raft_tablet_source_retirement_manifest(*predecessor, *request.source_retirement);
    if (!rebuilt.has_value()) {
      return implementation.fail_temporal_manifest(with_context(
          "rebuild authorized source-retirement Manifest v2 generation", rebuilt.error()));
    }
    if (!std::ranges::equal(rebuilt->manifest.bytes(), encoded.bytes())) {
      return implementation.fail_temporal_manifest(invalid(
          "Source-retirement Manifest v2 candidate differs from the authorized exact successor"));
    }
    validation = validate_manifest_v2_temporal_schema_binding(*candidate, request.schema_bindings);
  }
  if (!validation.is_ok()) {
    return implementation.fail_temporal_manifest(
        with_context("validate Manifest v2 generation transition", validation));
  }
  for (const TemporalPartDescriptor& descriptor : candidate->parts()) {
    if (!std::ranges::binary_search(snapshot->final_parts, descriptor.part_id)) {
      return implementation.fail_temporal_manifest(
          corruption("Manifest v2 candidate references a missing final CSEG part"));
    }
    const TemporalTabletDescriptor* owner =
        find_temporal_tablet(candidate->tablets(), descriptor.tablet_id);
    const TabletSchemaBinding* binding =
        find_binding(request.schema_bindings, descriptor.tablet_id);
    if (owner == nullptr || binding == nullptr) {
      return implementation.fail_temporal_manifest(
          common::Status{common::StatusCode::kInternal,
                         "Validated Manifest v2 tablet or schema binding became inaccessible"});
    }
    const std::shared_ptr<const schema::TableSchema> schema_value =
        binding->lineage.get().find(descriptor.schema_id);
    if (!schema_value) {
      return implementation.fail_temporal_manifest(common::Status{
          common::StatusCode::kInternal, "Validated Manifest v2 part schema became inaccessible"});
    }
    const std::string file_name = part_file_name(descriptor.part_id);
    common::Result<std::vector<std::byte>> part_bytes =
        read_final_file(implementation.parts_,
                        {.name = file_name,
                         .maximum_length = request.part_validation_limits.decode.max_file_length,
                         .exact_length = descriptor.file_length,
                         .description = "referenced final temporal CSEG part"});
    if (!part_bytes.has_value()) {
      return implementation.fail_temporal_manifest(part_bytes.error());
    }
    validation = validate_manifest_v2_temporal_part_image(
        descriptor, *owner, *part_bytes, *schema_value, request.part_validation_limits);
    if (!validation.is_ok()) {
      return implementation.fail_temporal_manifest(
          with_context("validate referenced final temporal CSEG part", validation));
    }
    saturating_add(implementation.manifest_metrics_.referenced_parts_validated, 1U);
  }

  const common::Result<std::string> final_name = manifest_file_name(candidate->generation());
  const common::Result<std::string> temporary_name =
      temporary_manifest_file_name(candidate->generation(), request.nonce);
  if (!final_name.has_value() || !temporary_name.has_value()) {
    return implementation.fail_temporal_manifest(
        common::Status{common::StatusCode::kInternal,
                       "Validated Manifest v2 generation names cannot be formatted"});
  }
  common::Result<io::PosixFile> temporary = implementation.manifests_.create_exclusive_regular_file(
      *temporary_name, implementation.file_permissions_);
  if (!temporary.has_value()) {
    return implementation.fail_temporal_manifest(
        with_context("create temporary Manifest v2 generation", temporary.error()));
  }
  common::Status operation = temporary->write_all_at(0U, encoded.bytes());
  if (!operation.is_ok()) {
    return implementation.fail_temporal_manifest(
        with_context("write temporary Manifest v2 generation", operation));
  }
  const common::Result<std::uint64_t> temporary_size = temporary->size();
  if (!temporary_size.has_value()) {
    return implementation.fail_temporal_manifest(
        with_context("read temporary Manifest v2 generation size", temporary_size.error()));
  }
  if (*temporary_size != encoded.size()) {
    return implementation.fail_temporal_manifest(
        common::Status{common::StatusCode::kIoError,
                       "Temporary Manifest v2 generation size changed after complete write"});
  }
  std::vector<std::byte> readback;
  try {
    readback.resize(encoded.size());
  } catch (const std::bad_alloc&) {
    return implementation.fail_temporal_manifest(
        common::Status{common::StatusCode::kResourceExhausted,
                       "Cannot allocate Manifest v2 generation installation readback"});
  }
  const common::Result<std::size_t> read = temporary->read_at(0U, readback);
  if (!read.has_value()) {
    return implementation.fail_temporal_manifest(
        with_context("read back temporary Manifest v2 generation", read.error()));
  }
  if (*read != readback.size()) {
    return implementation.fail_temporal_manifest(
        common::Status{common::StatusCode::kIoError,
                       "Temporary Manifest v2 generation readback ended before exact file size"});
  }
  TemporalManifestDecodeResult decoded_readback =
      decode_manifest_v2_temporal_exact(readback, request.decode_limits);
  if (!decoded_readback.has_value()) {
    return implementation.fail_temporal_manifest(
        manifest_decode_failure(decoded_readback.error(), "temporary Manifest v2 generation"));
  }
  if (!std::ranges::equal(readback, encoded.bytes())) {
    return implementation.fail_temporal_manifest(
        corruption("Temporary Manifest v2 generation readback differs from candidate bytes"));
  }

  operation = temporary->sync_all();
  if (!operation.is_ok()) {
    return implementation.fail_temporal_manifest(
        with_context("synchronize temporary Manifest v2 generation", operation));
  }
  ++implementation.manifest_metrics_.file_syncs;
  operation = temporary->close();
  if (!operation.is_ok()) {
    return implementation.fail_temporal_manifest(
        with_context("close synchronized temporary Manifest v2 generation", operation));
  }
  operation = implementation.manifests_.rename_no_replace(
      {.old_name = *temporary_name, .new_name = *final_name});
  if (!operation.is_ok()) {
    return implementation.fail_temporal_manifest(
        with_context("install Manifest v2 generation final name", operation));
  }
  operation = implementation.manifests_.sync();
  if (!operation.is_ok()) {
    implementation.poisoned_ = true;
    implementation.poison_status_ =
        with_context("synchronize Manifest directory after v2 generation install", operation);
    return implementation.fail_temporal_manifest(implementation.poison_status_);
  }
  ++implementation.manifest_metrics_.directory_syncs;
  ++implementation.manifest_metrics_.installed_generations;
  saturating_add(implementation.manifest_metrics_.installed_bytes,
                 static_cast<std::uint64_t>(encoded.size()));
  return InstalledTemporalManifest{
      .file_name = *final_name,
      .generation = candidate->generation(),
      .wal_reclaim_checkpoint = candidate->wal_reclaim_checkpoint(),
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

common::Result<PartReclamationReport>
ManifestStorage::reclaim_retired_parts(const PartReclamationRequest& request) {
  if (!implementation_) {
    return common::make_unexpected(invalid("Manifest storage owner was moved from"));
  }
  Impl& implementation = *implementation_;
  ++implementation.reclamation_metrics_.attempts;
  if (implementation.poisoned_) {
    return implementation.fail_reclamation(
        common::Status{common::StatusCode::kUnavailable, "Manifest storage owner is poisoned"});
  }

  const RetiredPartSet& retirement = request.retirement.get();
  PartReclamationReport report{
      .outcome = PartReclamationOutcome::kPending,
      .predecessor_generation = retirement.predecessor_generation(),
      .candidate_parts = static_cast<std::uint64_t>(retirement.parts().size()),
  };
  if (retirement.parts().empty()) {
    return implementation.fail_reclamation(invalid("Part retirement set is empty"));
  }
  if (retirement.is_pinned()) {
    saturating_add(implementation.reclamation_metrics_.pending, 1U);
    return report;
  }

  const LoadedManifestGeneration& selected = request.selected_manifest.get();
  if (retirement.predecessor_generation() >= selected.generation()) {
    return implementation.fail_reclamation(
        invalid("Part retirement predecessor is not older than the selected Manifest"));
  }
  for (std::size_t index = 0U; index < retirement.parts().size(); ++index) {
    const RetiredPartFile& candidate = retirement.parts()[index];
    if (candidate.file_length == 0U ||
        (index != 0U && !(retirement.parts()[index - 1U].part_id < candidate.part_id))) {
      return implementation.fail_reclamation(
          invalid("Part retirement files are not nonempty and strictly sorted"));
    }
  }

  common::Result<ManifestNamespaceSnapshot> snapshot = scan_namespace();
  if (!snapshot.has_value()) {
    return implementation.fail_reclamation(snapshot.error());
  }
  if (snapshot->generations.back() != selected.generation()) {
    return implementation.fail_reclamation(
        invalid("Part reclamation Manifest owner is no longer selected"));
  }
  const common::Result<std::string> selected_name = manifest_file_name(selected.generation());
  if (!selected_name.has_value()) {
    return implementation.fail_reclamation(common::Status{
        common::StatusCode::kInternal, "Selected Manifest generation cannot be formatted"});
  }
  common::Result<std::vector<std::byte>> current_bytes =
      read_final_file(implementation.manifests_,
                      {.name = *selected_name,
                       .maximum_length = request.decode_limits.max_file_length,
                       .exact_length = static_cast<std::uint64_t>(selected.encoded_bytes().size()),
                       .description = "current final Manifest generation"});
  if (!current_bytes.has_value()) {
    return implementation.fail_reclamation(current_bytes.error());
  }
  if (!std::ranges::equal(*current_bytes, selected.encoded_bytes())) {
    return implementation.fail_reclamation(
        corruption("Current final Manifest bytes disagree with the supplied owner"));
  }

  for (const RetiredPartFile& candidate : retirement.parts()) {
    if (std::ranges::find(selected.parts(), candidate.part_id, &PartDescriptor::part_id) !=
        selected.parts().end()) {
      return implementation.fail_reclamation(
          corruption("Current Manifest still references a retired part candidate"));
    }
  }

  bool unlinked = false;
  for (const RetiredPartFile& candidate : retirement.parts()) {
    const bool present =
        std::ranges::find(snapshot->final_parts, candidate.part_id) != snapshot->final_parts.end();
    if (!present) {
      ++report.already_absent_parts;
      continue;
    }
    const common::Status removal =
        implementation.parts_.remove_file(part_file_name(candidate.part_id));
    if (!removal.is_ok()) {
      if (unlinked) {
        implementation.poisoned_ = true;
        implementation.poison_status_ =
            with_context("remove retired CSEG part after a prior unlink", removal);
        return implementation.fail_reclamation(implementation.poison_status_);
      }
      return implementation.fail_reclamation(with_context("remove retired CSEG part", removal));
    }
    unlinked = true;
    ++report.removed_parts;
    saturating_add(report.removed_bytes, candidate.file_length);
  }
  if (unlinked) {
    const common::Status sync = implementation.parts_.sync();
    if (!sync.is_ok()) {
      implementation.poisoned_ = true;
      implementation.poison_status_ =
          with_context("synchronize parts directory after reclamation", sync);
      return implementation.fail_reclamation(implementation.poison_status_);
    }
    report.directory_syncs = 1U;
  }
  report.outcome = PartReclamationOutcome::kReclaimed;
  saturating_add(implementation.reclamation_metrics_.reclaimed_parts, report.removed_parts);
  saturating_add(implementation.reclamation_metrics_.reclaimed_bytes, report.removed_bytes);
  saturating_add(implementation.reclamation_metrics_.already_absent_parts,
                 report.already_absent_parts);
  saturating_add(implementation.reclamation_metrics_.directory_syncs, report.directory_syncs);
  return report;
}

common::Result<PartReclamationReport>
ManifestStorage::reclaim_retired_temporal_parts(const TemporalPartReclamationRequest& request) {
  if (!implementation_) {
    return common::make_unexpected(invalid("Manifest storage owner was moved from"));
  }
  Impl& implementation = *implementation_;
  ++implementation.reclamation_metrics_.attempts;
  if (implementation.poisoned_) {
    return implementation.fail_reclamation(
        common::Status{common::StatusCode::kUnavailable, "Manifest storage owner is poisoned"});
  }

  const TemporalRetiredPartSet& retirement = request.retirement.get();
  PartReclamationReport report{
      .outcome = PartReclamationOutcome::kPending,
      .predecessor_generation = retirement.predecessor_generation(),
      .candidate_parts = static_cast<std::uint64_t>(retirement.parts().size()),
  };
  if (retirement.is_pinned()) {
    saturating_add(implementation.reclamation_metrics_.pending, 1U);
    return report;
  }

  const LoadedTemporalManifestGeneration& selected = request.selected_manifest.get();
  if (retirement.predecessor_generation() >= selected.generation()) {
    return implementation.fail_reclamation(
        invalid("Temporal part retirement predecessor is not older than the selected Manifest"));
  }
  for (std::size_t index = 0U; index < retirement.parts().size(); ++index) {
    const TemporalPartDescriptor& candidate = retirement.parts()[index];
    if (candidate.file_length == 0U ||
        (index != 0U && !(retirement.parts()[index - 1U].part_id < candidate.part_id))) {
      return implementation.fail_reclamation(
          invalid("Temporal part retirement descriptors are not nonempty and strictly sorted"));
    }
  }

  common::Result<ManifestNamespaceSnapshot> snapshot = scan_namespace();
  if (!snapshot.has_value()) {
    return implementation.fail_reclamation(snapshot.error());
  }
  if (snapshot->generations.back() != selected.generation()) {
    return implementation.fail_reclamation(
        invalid("Temporal part reclamation Manifest owner is no longer selected"));
  }
  const common::Result<std::string> selected_name = manifest_file_name(selected.generation());
  if (!selected_name.has_value()) {
    return implementation.fail_reclamation(common::Status{
        common::StatusCode::kInternal, "Selected Manifest v2 generation cannot be formatted"});
  }
  common::Result<std::vector<std::byte>> current_bytes =
      read_final_file(implementation.manifests_,
                      {.name = *selected_name,
                       .maximum_length = request.decode_limits.max_file_length,
                       .exact_length = static_cast<std::uint64_t>(selected.encoded_bytes().size()),
                       .description = "current final Manifest v2 generation"});
  if (!current_bytes.has_value()) {
    return implementation.fail_reclamation(current_bytes.error());
  }
  if (!std::ranges::equal(*current_bytes, selected.encoded_bytes())) {
    return implementation.fail_reclamation(
        corruption("Current final Manifest v2 bytes disagree with the supplied owner"));
  }
  for (const TemporalPartDescriptor& candidate : retirement.parts()) {
    if (std::ranges::find(selected.parts(), candidate.part_id, &TemporalPartDescriptor::part_id) !=
        selected.parts().end()) {
      return implementation.fail_reclamation(
          corruption("Current Manifest v2 still references a retired temporal part candidate"));
    }
  }

  // Validate every present candidate before the first unlink so corruption never turns into a
  // partially completed reclamation attempt.
  for (const TemporalPartDescriptor& candidate : retirement.parts()) {
    if (!std::ranges::binary_search(snapshot->final_parts, candidate.part_id))
      continue;
    common::Result<std::vector<std::byte>> bytes =
        read_final_file(implementation.parts_,
                        {.name = part_file_name(candidate.part_id),
                         .maximum_length = request.part_validation_limits.decode.max_file_length,
                         .exact_length = candidate.file_length,
                         .description = "retired temporal CSEG part"});
    if (!bytes.has_value())
      return implementation.fail_reclamation(bytes.error());
    common::Result<ingest::Sha256Digest> digest = ingest::sha256(*bytes);
    if (!digest.has_value())
      return implementation.fail_reclamation(digest.error());
    if (*digest != candidate.content_sha256) {
      return implementation.fail_reclamation(
          corruption("Retired temporal CSEG bytes disagree with their published descriptor"));
    }
  }

  bool unlinked = false;
  for (const TemporalPartDescriptor& candidate : retirement.parts()) {
    if (!std::ranges::binary_search(snapshot->final_parts, candidate.part_id)) {
      ++report.already_absent_parts;
      continue;
    }
    const common::Status removal =
        implementation.parts_.remove_file(part_file_name(candidate.part_id));
    if (!removal.is_ok()) {
      if (unlinked) {
        implementation.poisoned_ = true;
        implementation.poison_status_ =
            with_context("remove retired temporal CSEG part after a prior unlink", removal);
        return implementation.fail_reclamation(implementation.poison_status_);
      }
      return implementation.fail_reclamation(
          with_context("remove retired temporal CSEG part", removal));
    }
    unlinked = true;
    ++report.removed_parts;
    saturating_add(report.removed_bytes, candidate.file_length);
  }
  if (unlinked) {
    const common::Status sync = implementation.parts_.sync();
    if (!sync.is_ok()) {
      implementation.poisoned_ = true;
      implementation.poison_status_ =
          with_context("synchronize parts directory after temporal reclamation", sync);
      return implementation.fail_reclamation(implementation.poison_status_);
    }
    report.directory_syncs = 1U;
  }
  report.outcome = PartReclamationOutcome::kReclaimed;
  saturating_add(implementation.reclamation_metrics_.reclaimed_parts, report.removed_parts);
  saturating_add(implementation.reclamation_metrics_.reclaimed_bytes, report.removed_bytes);
  saturating_add(implementation.reclamation_metrics_.already_absent_parts,
                 report.already_absent_parts);
  saturating_add(implementation.reclamation_metrics_.directory_syncs, report.directory_syncs);
  return report;
}

common::Result<TemporalRetiredPartSet> ManifestStorage::recover_temporal_source_retirement(
    const TemporalSourceRetirementRecoveryRequest& request) const {
  if (!implementation_) {
    return common::make_unexpected(invalid("Manifest storage owner was moved from"));
  }
  if (implementation_->poisoned_) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kUnavailable, "Manifest storage owner is poisoned"});
  }
  common::Result<ManifestNamespaceSnapshot> snapshot = scan_namespace();
  if (!snapshot.has_value())
    return common::make_unexpected(snapshot.error());
  const LoadedTemporalManifestGeneration& selected = request.selected_manifest.get();
  if (snapshot->generations.back() != selected.generation()) {
    return common::make_unexpected(
        invalid("Recovered temporal retirement Manifest owner is no longer selected"));
  }
  const auto read_generation =
      [&](const std::uint64_t generation) -> common::Result<std::vector<std::byte>> {
    const common::Result<std::string> name = manifest_file_name(generation);
    if (!name.has_value()) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kInternal, "Temporal retirement generation cannot be formatted"});
    }
    return read_final_file(implementation_->manifests_,
                           {.name = *name,
                            .maximum_length = request.decode_limits.max_file_length,
                            .exact_length = std::nullopt,
                            .description = "temporal retirement history generation"});
  };

  {
    common::Result<std::vector<std::byte>> selected_bytes = read_generation(selected.generation());
    if (!selected_bytes.has_value())
      return common::make_unexpected(selected_bytes.error());
    if (!std::ranges::equal(*selected_bytes, selected.encoded_bytes())) {
      return common::make_unexpected(
          corruption("Selected Manifest v2 bytes changed before retirement recovery"));
    }
  }

  std::optional<BuiltRaftTabletSourceRetirementManifest> recovered;
  common::Result<std::vector<std::byte>> current = read_generation(snapshot->generations.front());
  if (!current.has_value())
    return common::make_unexpected(current.error());
  for (std::size_t index = 0U; index + 1U < snapshot->generations.size(); ++index) {
    common::Result<std::vector<std::byte>> next =
        read_generation(snapshot->generations[index + 1U]);
    if (!next.has_value())
      return common::make_unexpected(next.error());
    TemporalManifestDecodeResult predecessor =
        decode_manifest_v2_temporal_exact(*current, request.decode_limits);
    if (!predecessor.has_value()) {
      if (predecessor.error().kind() == ManifestDecodeErrorKind::kUnsupported) {
        current = std::move(next);
        continue;
      }
      return common::make_unexpected(
          manifest_decode_failure(predecessor.error(), "temporal retirement predecessor"));
    }
    if (predecessor->generation() != snapshot->generations[index]) {
      return common::make_unexpected(
          corruption("Temporal retirement history filename disagrees with its encoded generation"));
    }
    common::Result<BuiltRaftTabletSourceRetirementManifest> candidate =
        build_raft_tablet_source_retirement_manifest(*predecessor, request.source_retirement.get());
    if (candidate.has_value() && std::ranges::equal(candidate->manifest.bytes(), *next)) {
      if (recovered.has_value()) {
        return common::make_unexpected(
            corruption("Multiple durable transitions match one temporal source retirement"));
      }
      recovered.emplace(std::move(*candidate));
    } else if (!candidate.has_value() &&
               candidate.error().code() != common::StatusCode::kInvalidArgument) {
      return common::make_unexpected(candidate.error());
    }
    current = std::move(next);
  }
  if (!recovered.has_value()) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kNotFound,
        "No durable Manifest v2 transition matches the temporal source-retirement authority"});
  }
  if (std::ranges::find(selected.tablets(), request.source_retirement.get().tablet_id,
                        &TemporalTabletDescriptor::tablet_id) != selected.tablets().end()) {
    return common::make_unexpected(
        corruption("Selected Manifest v2 reintroduced the retired source tablet"));
  }
  for (const TemporalPartDescriptor& retired : recovered->retired_parts) {
    if (std::ranges::find(selected.parts(), retired.part_id, &TemporalPartDescriptor::part_id) !=
        selected.parts().end()) {
      return common::make_unexpected(
          corruption("Selected Manifest v2 reintroduced a retired source part"));
    }
  }
  return TemporalRetiredPartSet{
      recovered->predecessor_generation, std::move(recovered->retired_parts), {}};
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

common::Result<LoadedTemporalManifestGeneration>
ManifestStorage::load_selected_temporal_manifest(const TemporalManifestLoadRequest& request) const {
  common::Result<ManifestNamespaceSnapshot> snapshot = scan_namespace();
  if (!snapshot.has_value()) {
    return common::make_unexpected(snapshot.error());
  }
  return load_temporal_manifest_generation(snapshot->generations.back(), request);
}

common::Result<LoadedTemporalManifestMetadata>
ManifestStorage::load_temporal_manifest_metadata(const std::uint64_t selected_generation,
                                                 const TemporalManifestLoadRequest& request) const {
  common::Result<ManifestNamespaceSnapshot> snapshot = scan_namespace();
  if (!snapshot.has_value())
    return common::make_unexpected(snapshot.error());
  if (!std::ranges::binary_search(snapshot->generations, selected_generation)) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kNotFound, "Requested Manifest v2 generation is not installed"});
  }
  const common::Result<std::string> selected_name = manifest_file_name(selected_generation);
  if (!selected_name.has_value()) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInternal, "Selected Manifest v2 generation cannot be formatted"});
  }
  common::Result<std::vector<std::byte>> encoded = read_final_file(
      implementation_->manifests_, {.name = *selected_name,
                                    .maximum_length = request.decode_limits.max_file_length,
                                    .exact_length = std::nullopt,
                                    .description = "selected final Manifest v2 metadata"});
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  TemporalManifestDecodeResult decoded =
      decode_manifest_v2_temporal_exact(*encoded, request.decode_limits);
  if (!decoded.has_value()) {
    return common::make_unexpected(
        manifest_decode_failure(decoded.error(), "selected final Manifest v2 metadata"));
  }
  if (decoded->generation() != selected_generation) {
    return common::make_unexpected(
        corruption("Selected final Manifest v2 filename disagrees with its encoded generation"));
  }
  if (decoded->database_id() != request.expected_database_id) {
    return common::make_unexpected(
        invalid("Selected Manifest v2 database identity does not match recovery context"));
  }
  common::Status validation =
      validate_manifest_v2_temporal_schema_binding(*decoded, request.schema_bindings);
  if (!validation.is_ok()) {
    return common::make_unexpected(
        with_context("bind selected Manifest v2 to retained catalog", validation));
  }
  validation = validate_manifest_v2_temporal_source_binding(*decoded, request.source_bindings);
  if (!validation.is_ok()) {
    return common::make_unexpected(
        with_context("bind selected Manifest v2 to configured sources", validation));
  }
  return LoadedTemporalManifestMetadata{selected_generation, std::move(*encoded)};
}

common::Result<LoadedTemporalManifestGeneration> ManifestStorage::load_temporal_manifest_generation(
    const std::uint64_t selected_generation, const TemporalManifestLoadRequest& request) const {
  auto metadata = load_temporal_manifest_metadata(selected_generation, request);
  if (!metadata.has_value())
    return common::make_unexpected(metadata.error());
  common::Result<ManifestNamespaceSnapshot> snapshot = scan_namespace();
  if (!snapshot.has_value())
    return common::make_unexpected(snapshot.error());
  if (!std::ranges::binary_search(snapshot->generations, selected_generation)) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kNotFound, "Requested Manifest v2 generation is not installed"});
  }
  std::vector<std::byte> encoded = std::move(metadata->encoded_bytes_);
  TemporalManifestDecodeResult decoded =
      decode_manifest_v2_temporal_exact(encoded, request.decode_limits);
  if (!decoded.has_value()) {
    return common::make_unexpected(
        manifest_decode_failure(decoded.error(), "selected final Manifest v2 generation"));
  }

  for (const TemporalPartDescriptor& descriptor : decoded->parts()) {
    if (!std::ranges::binary_search(snapshot->final_parts, descriptor.part_id)) {
      if (request.missing_part_validator == nullptr)
        return common::make_unexpected(
            corruption("Selected Manifest v2 references a missing final CSEG part"));
      const TemporalTabletDescriptor* owner =
          find_temporal_tablet(decoded->tablets(), descriptor.tablet_id);
      const TabletSchemaBinding* binding =
          find_binding(request.schema_bindings, descriptor.tablet_id);
      const std::shared_ptr<const schema::TableSchema> schema_value =
          binding == nullptr ? nullptr : binding->lineage.get().find(descriptor.schema_id);
      if (owner == nullptr || schema_value == nullptr) {
        return common::make_unexpected(
            common::Status{common::StatusCode::kInternal,
                           "Validated missing Manifest v2 part binding became inaccessible"});
      }
      common::Status remote_validation = request.missing_part_validator->validate_missing_part(
          descriptor, *owner, *schema_value, request.part_validation_limits);
      if (!remote_validation.is_ok()) {
        return common::make_unexpected(
            with_context("validate missing referenced temporal CSEG part", remote_validation));
      }
      continue;
    }
    const TemporalTabletDescriptor* owner =
        find_temporal_tablet(decoded->tablets(), descriptor.tablet_id);
    const TabletSchemaBinding* binding =
        find_binding(request.schema_bindings, descriptor.tablet_id);
    if (owner == nullptr || binding == nullptr) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kInternal,
                         "Validated Manifest v2 tablet or schema binding became inaccessible"});
    }
    const std::shared_ptr<const schema::TableSchema> schema_value =
        binding->lineage.get().find(descriptor.schema_id);
    if (!schema_value) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kInternal, "Validated Manifest v2 part schema became inaccessible"});
    }
    const std::string file_name = part_file_name(descriptor.part_id);
    common::Result<std::vector<std::byte>> part_bytes =
        read_final_file(implementation_->parts_,
                        {.name = file_name,
                         .maximum_length = request.part_validation_limits.decode.max_file_length,
                         .exact_length = descriptor.file_length,
                         .description = "referenced final temporal CSEG part"});
    if (!part_bytes.has_value()) {
      return common::make_unexpected(part_bytes.error());
    }
    common::Status validation = validate_manifest_v2_temporal_part_image(
        descriptor, *owner, *part_bytes, *schema_value, request.part_validation_limits);
    if (!validation.is_ok()) {
      return common::make_unexpected(
          with_context("validate referenced final temporal CSEG part", validation));
    }
  }

  try {
    std::vector<cseg::PartId> referenced_parts;
    referenced_parts.reserve(decoded->parts().size());
    for (const TemporalPartDescriptor& descriptor : decoded->parts()) {
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
    return LoadedTemporalManifestGeneration{
        std::make_unique<LoadedTemporalManifestGeneration::Impl>(
            std::move(encoded), std::move(*decoded), std::move(orphan_parts),
            std::move(snapshot->temporary_parts), std::move(snapshot->temporary_manifests))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "Cannot allocate loaded Manifest v2 generation"});
  } catch (const std::length_error&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "Loaded Manifest v2 generation exceeds container limits"});
  }
}

common::Result<std::vector<LoadedTemporalPartImage>> ManifestStorage::load_temporal_part_images(
    std::shared_ptr<const LoadedTemporalManifestGeneration> selected,
    const std::span<const cseg::PartId> part_ids,
    const std::span<const TabletSchemaBinding> schema_bindings,
    const TemporalPartValidationLimits limits) const {
  if (selected == nullptr || part_ids.empty()) {
    return common::make_unexpected(
        invalid("Temporal part-image request requires a generation and part identities"));
  }
  for (std::size_t index = 1U; index < part_ids.size(); ++index) {
    if (!(part_ids[index - 1U] < part_ids[index])) {
      return common::make_unexpected(
          invalid("Temporal part-image identities are not strictly sorted"));
    }
  }
  common::Result<ManifestNamespaceSnapshot> snapshot = scan_namespace();
  if (!snapshot.has_value()) {
    return common::make_unexpected(snapshot.error());
  }
  if (!std::ranges::binary_search(snapshot->generations, selected->generation())) {
    return common::make_unexpected(
        invalid("Temporal part-image generation is absent from the Manifest namespace"));
  }

  try {
    std::vector<LoadedTemporalPartImage> images;
    images.reserve(part_ids.size());
    for (const cseg::PartId& part_id : part_ids) {
      const auto descriptor =
          std::ranges::find(selected->parts(), part_id, &TemporalPartDescriptor::part_id);
      if (descriptor == selected->parts().end()) {
        return common::make_unexpected(
            invalid("Temporal part-image identity is not referenced by its generation"));
      }
      const TemporalTabletDescriptor* owner =
          find_temporal_tablet(selected->tablets(), descriptor->tablet_id);
      const TabletSchemaBinding* binding = find_binding(schema_bindings, descriptor->tablet_id);
      const std::shared_ptr<const schema::TableSchema> schema_value =
          binding == nullptr ? nullptr : binding->lineage.get().find(descriptor->schema_id);
      if (owner == nullptr || schema_value == nullptr) {
        return common::make_unexpected(
            invalid("Temporal part image has no exact tablet/schema binding"));
      }
      const std::string file_name = part_file_name(part_id);
      common::Result<std::vector<std::byte>> bytes = read_final_file(
          implementation_->parts_, {.name = file_name,
                                    .maximum_length = limits.decode.max_file_length,
                                    .exact_length = descriptor->file_length,
                                    .description = "generation-pinned temporal CSEG part image"});
      if (!bytes.has_value()) {
        return common::make_unexpected(bytes.error());
      }
      common::Status validation = validate_manifest_v2_temporal_part_image(
          *descriptor, *owner, *bytes, *schema_value, limits);
      if (!validation.is_ok()) {
        return common::make_unexpected(std::move(validation));
      }
      images.push_back(LoadedTemporalPartImage{selected, *descriptor, std::move(*bytes)});
    }
    return images;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "Cannot allocate temporal part images"});
  } catch (const std::length_error&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "Temporal part images exceed container limits"});
  }
}

common::Result<std::vector<LoadedPartImage>> ManifestStorage::load_selected_part_images(
    const LoadedManifestGeneration& selected, const std::span<const cseg::PartId> part_ids,
    const std::span<const TabletSchemaBinding> schema_bindings,
    const ReferencedPartValidationLimits limits) const {
  if (part_ids.empty()) {
    return common::make_unexpected(invalid("Selected part-image request is empty"));
  }
  for (std::size_t index = 1U; index < part_ids.size(); ++index) {
    if (!(part_ids[index - 1U] < part_ids[index])) {
      return common::make_unexpected(
          invalid("Selected part-image identities are not strictly sorted"));
    }
  }
  common::Result<ManifestNamespaceSnapshot> snapshot = scan_namespace();
  if (!snapshot.has_value()) {
    return common::make_unexpected(snapshot.error());
  }
  if (snapshot->generations.back() != selected.generation()) {
    return common::make_unexpected(
        invalid("Part-image generation is no longer the selected Manifest"));
  }
  try {
    std::vector<LoadedPartImage> images;
    images.reserve(part_ids.size());
    for (const cseg::PartId& part_id : part_ids) {
      const auto descriptor =
          std::ranges::find(selected.parts(), part_id, &PartDescriptor::part_id);
      if (descriptor == selected.parts().end()) {
        return common::make_unexpected(
            invalid("Selected part-image identity is not referenced by its Manifest"));
      }
      const TabletSchemaBinding* binding = find_binding(schema_bindings, descriptor->tablet_id);
      const std::shared_ptr<const schema::TableSchema> schema_value =
          binding == nullptr ? nullptr : binding->lineage.get().find(descriptor->schema_id);
      if (schema_value == nullptr) {
        return common::make_unexpected(invalid("Selected part image has no exact schema binding"));
      }
      const std::string file_name = part_file_name(part_id);
      common::Result<std::vector<std::byte>> bytes = read_final_file(
          implementation_->parts_, {.name = file_name,
                                    .maximum_length = limits.decode.max_file_length,
                                    .exact_length = descriptor->file_length,
                                    .description = "selected final CSEG part image"});
      if (!bytes.has_value()) {
        return common::make_unexpected(bytes.error());
      }
      common::Status validation =
          validate_manifest_v1_part_image(*descriptor, selected.wal_id(), *schema_value,
                                          {.file_name = file_name, .bytes = *bytes}, limits);
      if (!validation.is_ok()) {
        return common::make_unexpected(std::move(validation));
      }
      images.push_back(LoadedPartImage{*descriptor, std::move(*bytes)});
    }
    return images;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "Cannot allocate selected part images"});
  } catch (const std::length_error&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "Selected part images exceed container limits"});
  }
}

common::Result<std::vector<SnapshotPartImage>> ManifestStorage::load_snapshot_part_images(
    const DatabaseStorageSnapshot& database_snapshot, const std::span<const cseg::PartId> part_ids,
    const std::span<const TabletSchemaBinding> schema_bindings,
    const ReferencedPartValidationLimits limits) const {
  if (part_ids.empty()) {
    return common::make_unexpected(invalid("Snapshot part-image request is empty"));
  }
  for (std::size_t index = 1U; index < part_ids.size(); ++index) {
    if (!(part_ids[index - 1U] < part_ids[index])) {
      return common::make_unexpected(
          invalid("Snapshot part-image identities are not strictly sorted"));
    }
  }
  common::Result<ManifestNamespaceSnapshot> namespace_snapshot = scan_namespace();
  if (!namespace_snapshot.has_value()) {
    return common::make_unexpected(namespace_snapshot.error());
  }

  try {
    std::vector<SnapshotPartImage> images;
    images.reserve(part_ids.size());
    for (const cseg::PartId& part_id : part_ids) {
      const auto descriptor =
          std::ranges::find(database_snapshot.parts(), part_id, &PartDescriptor::part_id);
      if (descriptor == database_snapshot.parts().end()) {
        return common::make_unexpected(
            invalid("Snapshot part-image identity is not selected by its database epoch"));
      }
      if (!std::ranges::binary_search(namespace_snapshot->final_parts, part_id)) {
        return common::make_unexpected(
            corruption("Snapshot-selected CSEG part is missing from the locked namespace"));
      }
      const TabletSchemaBinding* binding = find_binding(schema_bindings, descriptor->tablet_id);
      const std::shared_ptr<const schema::TableSchema> schema_value =
          binding == nullptr ? nullptr : binding->lineage.get().find(descriptor->schema_id);
      if (schema_value == nullptr) {
        return common::make_unexpected(invalid("Snapshot part image has no exact schema binding"));
      }
      DatabaseStorageRetentionToken retention = database_snapshot.retention_token();
      const std::string file_name = part_file_name(part_id);
      common::Result<std::vector<std::byte>> bytes = read_final_file(
          implementation_->parts_, {.name = file_name,
                                    .maximum_length = limits.decode.max_file_length,
                                    .exact_length = descriptor->file_length,
                                    .description = "snapshot-selected final CSEG part image"});
      if (!bytes.has_value()) {
        return common::make_unexpected(bytes.error());
      }
      common::Status validation =
          validate_manifest_v1_part_image(*descriptor, database_snapshot.wal_id(), *schema_value,
                                          {.file_name = file_name, .bytes = *bytes}, limits);
      if (!validation.is_ok()) {
        return common::make_unexpected(std::move(validation));
      }
      images.push_back(SnapshotPartImage{database_snapshot.database_id(),
                                         database_snapshot.wal_id(), database_snapshot.generation(),
                                         *descriptor, std::move(*bytes), std::move(retention),
                                         database_snapshot.retained_buffer_bytes()});
    }
    return images;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "Cannot allocate snapshot part images"});
  } catch (const std::length_error&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "Snapshot part images exceed container limits"});
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

PartReclamationMetrics ManifestStorage::reclamation_metrics() const noexcept {
  return implementation_ ? implementation_->reclamation_metrics_ : PartReclamationMetrics{};
}

} // namespace chronos::manifest
