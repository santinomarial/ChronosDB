#include "chronos/manifest/storage.hpp"

#include "chronos/io/posix_io.hpp"
#include "chronos/manifest/naming.hpp"
#include "io/posix_syscalls.hpp"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::manifest {
namespace {

[[nodiscard]] common::Status invalid(const std::string_view message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::string{message}};
}

[[nodiscard]] common::Status with_context(const std::string_view context,
                                          const common::Status& cause) {
  std::string message{context};
  message.append(": ");
  message.append(cause.message());
  return common::Status{cause.code(), std::move(message)};
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

  io::PosixDirectory root_;
  io::PosixDirectory parts_;
  io::PosixDirectory manifests_;
  io::PosixAdvisoryLock lock_;
  std::uint16_t file_permissions_;
  bool poisoned_{false};
  common::Status poison_status_;
  PartInstallationMetrics metrics_;
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

} // namespace chronos::manifest
