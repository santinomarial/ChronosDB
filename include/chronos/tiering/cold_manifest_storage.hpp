#ifndef CHRONOS_TIERING_COLD_MANIFEST_STORAGE_HPP_
#define CHRONOS_TIERING_COLD_MANIFEST_STORAGE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/manifest/types.hpp"
#include "chronos/tiering/cold_manifest.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace chronos::io::detail {
class PosixSyscalls;
}

namespace chronos::tiering {

namespace detail {
class ColdLocationManifestStorageTestAccess;
}

inline constexpr std::string_view kColdLocationManifestLockFileName = "LOCK";

struct ColdLocationManifestStorageConfig {
  std::string directory_path;
  manifest::DatabaseId expected_database_id;
  common::Uuid expected_object_store_id;
  ColdLocationManifestDecodeLimits decode_limits{};
  std::uint16_t file_permissions{0600U};
};

struct InstalledColdLocationManifest {
  std::string file_name;
  std::uint64_t generation{};
  std::uint64_t base_manifest_generation{};
  std::uint64_t location_count{};
  bool already_present{false};
};

class LoadedColdLocationManifest {
public:
  LoadedColdLocationManifest() = delete;
  LoadedColdLocationManifest(const LoadedColdLocationManifest&) = delete;
  LoadedColdLocationManifest& operator=(const LoadedColdLocationManifest&) = delete;
  LoadedColdLocationManifest(LoadedColdLocationManifest&&) noexcept = default;
  LoadedColdLocationManifest& operator=(LoadedColdLocationManifest&&) noexcept = default;

  [[nodiscard]] const std::string& file_name() const noexcept;
  [[nodiscard]] const DecodedColdLocationManifest& manifest() const noexcept;
  [[nodiscard]] common::ByteView encoded_bytes() const noexcept;

private:
  LoadedColdLocationManifest(std::string file_name, DecodedColdLocationManifest manifest,
                             std::vector<std::byte> encoded_bytes) noexcept;

  std::string file_name_;
  DecodedColdLocationManifest manifest_;
  std::vector<std::byte> encoded_bytes_;

  friend class ColdLocationManifestStorage;
};

struct ColdLocationManifestStorageMetrics {
  std::uint64_t install_attempts{};
  std::uint64_t install_failures{};
  std::uint64_t installed_generations{};
  std::uint64_t installed_bytes{};
  std::uint64_t file_syncs{};
  std::uint64_t directory_syncs{};

  friend bool operator==(const ColdLocationManifestStorageMetrics&,
                         const ColdLocationManifestStorageMetrics&) = default;
};

[[nodiscard]] common::Result<std::string>
cold_location_manifest_file_name(std::uint64_t generation);

// Single-threaded owner of one existing cold-authority directory. create() creates only LOCK;
// neither entry point creates the directory. Ownership acquisition removes only recognized
// interrupted temporaries and synchronizes that cleanup. Final generations remain immutable.
class ColdLocationManifestStorage {
public:
  ColdLocationManifestStorage() = delete;
  ~ColdLocationManifestStorage();
  ColdLocationManifestStorage(const ColdLocationManifestStorage&) = delete;
  ColdLocationManifestStorage& operator=(const ColdLocationManifestStorage&) = delete;
  ColdLocationManifestStorage(ColdLocationManifestStorage&&) noexcept;
  ColdLocationManifestStorage& operator=(ColdLocationManifestStorage&&) noexcept;

  [[nodiscard]] static common::Result<ColdLocationManifestStorage>
  create(ColdLocationManifestStorageConfig config);
  [[nodiscard]] static common::Result<ColdLocationManifestStorage>
  open_existing(ColdLocationManifestStorageConfig config);

  // Installs only generation 1 or the exact successor to the highest final generation. Routes are
  // add-only at one base generation; a newer base may drop only routes for no-longer-logical parts.
  // Candidate and readback must bind to base_manifest. The temporary is completely written,
  // exact-read, synchronized, closed, no-replace renamed, then followed by directory sync.
  [[nodiscard]] common::Result<InstalledColdLocationManifest>
  install(std::reference_wrapper<const EncodedColdLocationManifest> encoded,
          const manifest::DecodedTemporalManifestView& base_manifest);

  // Selects the highest consecutive final name, exact-decodes it without fallback, and binds it to
  // base_manifest. An empty directory has no selected authority and returns nullopt.
  [[nodiscard]] common::Result<std::optional<LoadedColdLocationManifest>>
  load_selected(const manifest::DecodedTemporalManifestView& base_manifest) const;

  // Higher-level pair-commit recovery may select an older exact cold generation while newer
  // uncommitted finals remain forensic orphans. The directory generation chain is still strict.
  [[nodiscard]] common::Result<LoadedColdLocationManifest>
  load_generation(std::uint64_t generation,
                  const manifest::DecodedTemporalManifestView& base_manifest) const;

  // Reads and exact-decodes one installed generation and validates the configured database/store
  // ownership without accepting it as publishable authority. Restart garbage discovery binds this
  // metadata to its exact historical Manifest before using any route.
  [[nodiscard]] common::Result<DecodedColdLocationManifest>
  load_generation_metadata(std::uint64_t generation) const;

  [[nodiscard]] bool is_usable() const noexcept;
  [[nodiscard]] common::Status poison_status() const;
  [[nodiscard]] ColdLocationManifestStorageMetrics metrics() const noexcept;

private:
  class Impl;
  explicit ColdLocationManifestStorage(std::unique_ptr<Impl> impl) noexcept;
  [[nodiscard]] static common::Result<ColdLocationManifestStorage>
  open(ColdLocationManifestStorageConfig config, bool create_lock,
       io::detail::PosixSyscalls& syscalls);

  std::unique_ptr<Impl> impl_;

  friend class detail::ColdLocationManifestStorageTestAccess;
};

} // namespace chronos::tiering

#endif // CHRONOS_TIERING_COLD_MANIFEST_STORAGE_HPP_
