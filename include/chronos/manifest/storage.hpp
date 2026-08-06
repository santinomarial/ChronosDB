#ifndef CHRONOS_MANIFEST_STORAGE_HPP_
#define CHRONOS_MANIFEST_STORAGE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "chronos/manifest/part_validation.hpp"
#include "chronos/manifest/types.hpp"
#include "chronos/schema/table_schema.hpp"
#include "chronos/wal/types.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

namespace chronos::io::detail {
class PosixSyscalls;
}

namespace chronos::manifest {

namespace detail {
class ManifestStorageTestAccess;
}

inline constexpr std::string_view kPartsDirectoryName = "parts";
inline constexpr std::string_view kManifestDirectoryName = "manifest";
inline constexpr std::string_view kManifestLockFileName = "LOCK";

struct ManifestStorageConfig {
  std::string database_root;
  std::uint16_t file_permissions{0600U};
};

struct PartInstallRequest {
  std::reference_wrapper<const cseg::EncodedCsegPart> encoded_part;
  PartDescriptor descriptor;
  wal::WalId wal_id;
  std::reference_wrapper<const schema::TableSchema> schema;
  common::Uuid nonce;
  ReferencedPartValidationLimits validation_limits;
};

struct InstalledPart {
  std::string file_name;
  PartDescriptor descriptor;
};

struct PartInstallationMetrics {
  std::uint64_t attempts{};
  std::uint64_t failures{};
  std::uint64_t installed_parts{};
  std::uint64_t installed_bytes{};
  std::uint64_t file_syncs{};
  std::uint64_t directory_syncs{};

  friend bool operator==(const PartInstallationMetrics&, const PartInstallationMetrics&) = default;
};

// A move-only, single-threaded owner of the existing database root, parts directory, manifest
// directory, and manifest writer lock. open_existing() never creates missing directories or LOCK.
// The deployment must prevent out-of-band mutation while the lock is held.
class ManifestStorage {
public:
  ManifestStorage() = delete;
  ~ManifestStorage();

  ManifestStorage(const ManifestStorage&) = delete;
  ManifestStorage& operator=(const ManifestStorage&) = delete;
  ManifestStorage(ManifestStorage&&) noexcept;
  ManifestStorage& operator=(ManifestStorage&&) noexcept;

  [[nodiscard]] static common::Result<ManifestStorage>
  open_existing(const ManifestStorageConfig& config);

  // Implements the complete per-part installation ordering. The input and exact readback are both
  // validated before file sync. A final name is never replaced. Failure after rename but before
  // directory sync poisons this owner; restart/recovery must resolve the durable namespace.
  [[nodiscard]] common::Result<InstalledPart> install_part(const PartInstallRequest& request);

  [[nodiscard]] bool is_usable() const noexcept;
  [[nodiscard]] common::Status poison_status() const;
  [[nodiscard]] PartInstallationMetrics metrics() const noexcept;

private:
  class Impl;
  explicit ManifestStorage(std::unique_ptr<Impl> implementation) noexcept;
  [[nodiscard]] static common::Result<ManifestStorage>
  open_existing_with(const ManifestStorageConfig& config, io::detail::PosixSyscalls& syscalls);

  std::unique_ptr<Impl> implementation_;

  friend class detail::ManifestStorageTestAccess;
};

} // namespace chronos::manifest

#endif // CHRONOS_MANIFEST_STORAGE_HPP_
