#ifndef CHRONOS_MANIFEST_STORAGE_INTERNAL_HPP_
#define CHRONOS_MANIFEST_STORAGE_INTERNAL_HPP_

#include "chronos/manifest/storage.hpp"
#include "io/posix_syscalls.hpp"

#include <utility>

namespace chronos::manifest::detail {

class ManifestStorageTestAccess {
public:
  [[nodiscard]] static common::Result<ManifestStorage>
  open_existing(const ManifestStorageConfig& config, io::detail::PosixSyscalls& syscalls) {
    return ManifestStorage::open_existing_with(config, syscalls);
  }

  [[nodiscard]] static RetiredPartSet
  make_unpinned_retirement(const std::uint64_t predecessor_generation,
                           std::vector<RetiredPartFile> parts) {
    return RetiredPartSet{predecessor_generation, std::move(parts), {}};
  }

  [[nodiscard]] static TemporalRetiredPartSet make_temporal_retirement(
      const std::uint64_t predecessor_generation, std::vector<TemporalPartDescriptor> parts,
      std::vector<std::weak_ptr<const LoadedTemporalManifestGeneration>> generation_pins = {}) {
    return TemporalRetiredPartSet{predecessor_generation, std::move(parts),
                                  std::move(generation_pins)};
  }
};

} // namespace chronos::manifest::detail

#endif // CHRONOS_MANIFEST_STORAGE_INTERNAL_HPP_
